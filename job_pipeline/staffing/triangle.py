"""Read job records shipped with Triangle's public Next.js page, without API calls."""
import json
import re


def public_jobs(response):
    chunks = []
    for script in response.css('script::text').getall():
        match = re.fullmatch(r'\s*self\.__next_f\.push\((.*)\)\s*;?\s*', script, re.S)
        if match:
            try:
                payload = json.loads(match[1])
                if len(payload) > 1 and payload[0] == 1 and isinstance(payload[1], str):
                    chunks.append(payload[1])
            except (ValueError, TypeError):
                continue
    stream = ''.join(chunks)
    for match in re.finditer(r'"jobs"\s*:\s*\[', stream):
        try:
            records, _ = json.JSONDecoder().raw_decode(stream[match.end()-1:])
        except ValueError:
            continue
        if isinstance(records, list) and all(isinstance(row, dict) and 'title' in row for row in records):
            for row in records:
                company = row.get('company')
                if isinstance(company, str):
                    ref = re.fullmatch(r'\$[^:]+:.*:jobs:(\d+):company', company)
                    row['company'] = records[int(ref[1])].get('company') if ref and int(ref[1]) < len(records) else None
                description = row.get('description')
                if isinstance(description, str) and re.fullmatch(r'\$[a-f0-9]+', description):
                    marker = re.search(re.escape(description[1:]) + r':T([a-f0-9]+),', stream)
                    if marker:
                        row['description'] = stream[marker.end():].encode('utf-8')[:int(marker[1], 16)].decode('utf-8')
            return records
    raise ValueError('Triangle public job records unavailable; page format may have changed')


def crawl(spider, response, source):
    for row in public_jobs(response):
        if row.get('is_active') is False:
            continue
        # Long descriptions can use React Flight references. Plain description is
        # always used rather than treating an unresolved reference as job text.
        description = row.get('description') or ''
        if description.startswith('$'):
            continue
        company_data = row.get('company')
        company = company_data.get('name') if isinstance(company_data, dict) else None
        if not company:
            continue
        url = row.get('apply_external_url') or response.urljoin('/jobs/' + row['id'])
        obj = {'@type': 'JobPosting', 'title': row['title'], 'description': description,
               'url': url, 'datePosted': row.get('posted_at'),
               'hiringOrganization': {'name': company},
               'employmentType': row.get('employment_type'),
               'jobLocation': {'address': row.get('location') or ''},
               'jobLocationType': 'TELECOMMUTE' if row.get('remote') else ''}
        spider.accept(obj, source, response.url)
    return iter(())
