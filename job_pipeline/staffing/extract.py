"""Normalize public JobPosting structured data; never infer missing job descriptions."""
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from parsel import Selector

ROLE = re.compile(r'software|backend|full.?stack|\bpython\b|machine learning|\bml engineer|\bai engineer|data scientist|data analy|data engineer|applied scientist|forward.?deployed|cloud engineer', re.I)


def canonical_url(url):
    p = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith('utm_') and k.lower() not in {'source', 'ref', 'tracking'}]
    return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip('/'), '', urlencode(sorted(query)), ''))


def job_objects(value):
    if isinstance(value, list):
        for item in value:
            yield from job_objects(item)
    elif isinstance(value, dict):
        types = value.get('@type', [])
        if 'JobPosting' in (types if isinstance(types, list) else [types]):
            yield value
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                yield from job_objects(item)


def plain_text(html):
    if not html:
        return ''
    return ' '.join(Selector(text=str(html)).xpath('//text()[not(ancestor::script) and not(ancestor::style)]').getall()).strip()


def normalize_job(item, source, page_url):
    title = str(item.get('title') or item.get('name') or '').strip()
    description = plain_text(item.get('description'))
    url = canonical_url(str(item.get('url') or page_url))
    if not title or not ROLE.search(title) or len(description) < 80 or urlparse(url).scheme not in {'http', 'https'}:
        return None
    places = item.get('jobLocation') or []
    if not isinstance(places, list):
        places = [places]
    locations = []
    for place in places:
        if not isinstance(place, dict):
            continue
        address = place.get('address') or {}
        if isinstance(address, str):
            locations.append(address)
        elif isinstance(address, dict):
            locations.append(', '.join(str(address[k]) for k in ['addressLocality', 'addressRegion', 'addressCountry'] if address.get(k)))
    remote = str(item.get('jobLocationType', '')).upper() == 'TELECOMMUTE'
    location = '; '.join(filter(None, locations)) or ('Remote' if remote else '')
    fingerprint = hashlib.sha256(re.sub(r'\s+', ' ', f'{title.lower()}|{location.lower()}|{description.lower()}').encode()).hexdigest()
    return {'_id': hashlib.sha256(f'{source["id"]}|{url}'.encode()).hexdigest(), 'source_id': source['id'], 'site': 'staffing', 'company': source['name'], 'title': title, 'job_url': url, 'job_url_direct': url, 'description': description, 'summary': description[:200], 'location': location, 'is_remote': remote, 'date_posted': item.get('datePosted'), 'valid_through': item.get('validThrough'), 'fingerprint': fingerprint, 'observed_at': datetime.now(timezone.utc), 'employment_type': item.get('employmentType')}
