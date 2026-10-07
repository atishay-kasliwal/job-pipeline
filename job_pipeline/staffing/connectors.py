"""Verified public-board connectors. Listing discovery is separate from job extraction."""
import json
import re
from urllib.parse import quote, urlencode, urlparse
from job_pipeline.staffing.extract import ROLE

MOTION_CATEGORIES = ['software-engineering', 'python', 'data-engineering', 'data-analyst', 'machine-learning-data-science']
CONNECTORS = {
    # These verified public boards currently reject server requests. Keep their
    # daily access checks explicit; do not describe them as working adapters.
    'yoh': {'kind':'access_check','hosts':['jobs.yoh.com'],'seeds':['https://jobs.yoh.com/'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'eliassen-group': {'kind':'access_check','hosts':['careers.eliassen.com'],'seeds':['https://careers.eliassen.com/all-jobs'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'dexian': {'kind':'access_check','hosts':['dexian.com'],'seeds':['https://dexian.com/jobs/'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'apollo-technical': {'kind':'access_check','hosts':['www.apollotechnical.com'],'seeds':['https://www.apollotechnical.com/careers-engineering-it/find-jobs/'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'consulting-solutions': {'kind':'access_check','hosts':['www.consultingsolutions.com'],'seeds':['https://www.consultingsolutions.com/careers/openings/'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'aquent': {'kind':'access_check','hosts':['aquent.com'],'seeds':['https://aquent.com/find-work'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'mondo': {'kind':'access_check','hosts':['mondo.com'],'seeds':['https://mondo.com/jobs/all-jobs'],'access_note':'Public job board returns HTTP 403 from the deployment server'},
    'artech': {'kind':'jobdiva','hosts':['www1.jobdiva.com'],'seeds':['https://www1.jobdiva.com/candidates/myjobs/getportaljobs.jsp?a=kvjdnwtsxgckrpsoozx5qc0oueybw1005779v7x6soig8eyqqmzaubfdl9tcx21s&noofjobs=10000']},
    'nesco-resource': {'kind':'jobdiva','hosts':['www2.jobdiva.com'],'seeds':['https://www2.jobdiva.com/candidates/myjobs/getportaljobs.jsp?a=3bjdnwaxpar6fqztzu6mhx5d0xyrn100fc3iv75z44r9e0v7xa2dz7r3fyz3ex0n&noofjobs=10000']},
    'kellymitchell': {'kind':'sourceflow','hosts':['www.careers.kellymitchell.com'],'seeds':['https://www.careers.kellymitchell.com/jobs']},
    'manpower': {'kind':'manpower','hosts':['www.manpower.com'],'seeds':['https://www.manpower.com/en/search']},
    'adecco': {'kind':'job_sitemap','hosts':['www.adecco.com'],'seeds':['https://www.adecco.com/sitemap-jobs-unitedstates-en.xml'],'job_prefix':'/en-us/job-search/'},
    'collabera': {'kind':'public_html','hosts':['www.collabera.com'],'seeds':['https://www.collabera.com/job-search/?keyword=software','https://www.collabera.com/job-search/?keyword=data'], 'detail_pattern':r'/job-description/\?post=\d+', 'pagination_pattern':r'/job-search/.*[&?]q=\d+'},
    'ampcus': {'kind':'jobdiva','hosts':['www1.jobdiva.com'],'seeds':['https://ampcus.com/careers/current-openings/']},
    'compunnel': {'kind':'compunnel','hosts':['stafflineapi.compunnel.com','staffline.compunnel.com'],'seeds':['https://www.compunnel.com/job-search/']},
    'medix': {'kind':'medix','hosts':['jobs.medixteam.com','search.jobs.medixteam.com'],'seeds':['https://jobs.medixteam.com/jobs']},
    'cybercoders': {'kind':'cyber','hosts':['www.cybercoders.com'],'seeds':['https://www.cybercoders.com/ccv5-jobs/search?keyword=software&buid=1','https://www.cybercoders.com/ccv5-jobs/search?keyword=data&buid=1']},
    'randstad-digital': {'kind':'job_sitemap','hosts':['www.randstadusa.com'],'seeds':['https://www.randstadusa.com/sitemaps/us/sitemap-jobs.xml'],'job_prefix':'/jobs/4/'},
    'robert-half': {'kind':'public_html','hosts':['www.roberthalf.com'],'seeds':['https://www.roberthalf.com/us/en/jobs?keywords=software','https://www.roberthalf.com/us/en/jobs?keywords=data'], 'detail_pattern':r'/us/en/job/[^/]+/[^/]+/[^/]+', 'pagination_pattern':r'/us/en/jobs.*page=\d+'},
    'kelly': {'kind':'public_html','hosts':['www.mykelly.com'],'seeds':['https://www.mykelly.com/find-jobs/?_keyword=software','https://www.mykelly.com/find-jobs/?_keyword=data'], 'detail_pattern':r'/job/\d+-[^/]+', 'pagination_pattern':r'/find-jobs/.*_paged=\d+'},
    'insight-global': {'kind':'insight','hosts':['insightglobal.com'],'seeds':['https://insightglobal.com/all/jobs?keyword=software&page=0&size=100','https://insightglobal.com/all/jobs?keyword=data&page=0&size=100']},
    'genesis10': {'kind':'jobdiva','hosts':['www2.jobdiva.com'],'seeds':['https://www.genesis10.com/find-a-job']},
    'the-judge-group': {'kind':'judge','hosts':['www.judge.com'],'seeds':['https://www.judge.com/jobs/']},
    'beacon-hill': {'kind':'public_html','hosts':['bhsg.com'],'seeds':['https://bhsg.com/jobs/job-search/?_categories=technology'], 'detail_pattern':r'/jobs/job/[^/]+', 'pagination_pattern':r'/job-search/.*(?:paged|_paged|page)=\d+'},
    'tundra-technical-solutions': {'kind':'public_html','hosts':['community.tundratechnical.ca'],'seeds':['https://community.tundratechnical.ca/jobs/'], 'detail_pattern':r'/jobs/[a-z0-9-]+-\d+/', 'pagination_pattern':r'/jobs/page/\d+'},
    "triangle-startups": {"kind": "triangle", "hosts": ["triangle-startups.com"], "seeds": ["https://triangle-startups.com/jobs"]},
    'kforce': {'kind': 'kforce_azure', 'hosts': ['kforcewebeast.azureedge.net', 'kforcewebeast.search.windows.net', 'www.kforce.com'], 'seeds': ['https://kforcewebeast.azureedge.net/scripts/dist/js/app.min.js']},
    'teksystems': {'kind': 'phenom', 'hosts': ['careers.teksystems.com'], 'seeds': ['https://careers.teksystems.com/us/en/c/developer-jobs/']},
    'motion-recruitment': {'kind': 'motion', 'hosts': ['motionrecruitment.com'], 'seeds': ['https://motionrecruitment.com/tech-jobs/' + category for category in MOTION_CATEGORIES]},
    'matrix-resources': {'kind': 'motion', 'hosts': ['motionrecruitment.com'], 'seeds': ['https://motionrecruitment.com/tech-jobs/' + category for category in MOTION_CATEGORIES]},
    'mastech-digital': {'kind': 'datafrenzy', 'hosts': ['jobs.mastechdigital.com'], 'seeds': ['https://jobs.mastechdigital.com/job/GetJobs/1/50/null/16116/null/null/null/null/0/0/null']},
    'pyramid-consulting': {'kind': 'sprockets', 'hosts': ['jobs.sprockets.ai'], 'seeds': ['https://jobs.sprockets.ai/en-US/pyramidinc/jobs']},
}


def embedded_phenom(html):
    match = re.search(r'phApp\.ddo\s*=\s*', html)
    if not match:
        return {}
    try:
        value, _ = json.JSONDecoder().raw_decode(html[match.end():])
        return value if isinstance(value, dict) else {}
    except ValueError:
        return {}


def handle(spider, response, source):
    config = CONNECTORS[source['id']]
    kind = config['kind']
    spider.counts[source['id']]['pages'] += 1
    spider.on_source(source['id'], {'connector': kind, 'pages': spider.counts[source['id']]['pages'], 'matching_jobs': spider.counts[source['id']]['matching_jobs'], 'detail': f'Checking dedicated job board: {spider.counts[source["id"]]["pages"]} pages'})
    if kind == 'access_check':
        for raw in response.css('script[type="application/ld+json"]::text').getall():
            try: spider.accept(json.loads(raw), source, response.url)
            except (ValueError,TypeError): pass
        return
    if kind in {'insight','jobdiva','judge','public_html','compunnel','medix','cyber','randstad','sourceflow','manpower','job_sitemap'}:
        from job_pipeline.staffing.providers import handle
        yield from handle(spider,response,source,config)
        return
    if kind == 'triangle':
        from job_pipeline.staffing.triangle import crawl
        yield from crawl(spider, response, source)
        return
    if kind == 'kforce_azure':
        if '.azureedge.net' in response.url:
            # This is the public read-only search client used by Kforce's job board.
            # Read its current query credential at runtime; never store or log it.
            match = re.search(r'url:"(https://kforcewebeast.search.windows.net)",key:"([^"\\]+)"', response.text)
            if not match:
                raise ValueError('Kforce public search client changed')
            url = match[1] + '/indexes/kforcewebjobentity/docs/search?api-version=2020-06-30'
            body = {'search': 'software developer python backend data machine learning cloud', 'top': 100, 'skip': 0, 'count': True}
            request = spider.request(url, source, spider.parse_connector, method='POST', body=json.dumps(body).encode(), headers={'Content-Type': 'application/json', 'api-key': match[2]})
            if request:
                yield request
        else:
            data = response.json()
            for item in data.get('value', []):
                if not item.get('Id'):
                    continue
                url = 'https://www.kforce.com/find-work/search-jobs/#/detail/' + quote(item['Id']) + '/'
                description = (item.get('ResponsibilitiesHtml') or item.get('Responsibilities') or '') + '\n' + (item.get('SkillsHtml') or item.get('Skills') or '')
                obj = {'@type': 'JobPosting', 'title': item.get('Title'), 'description': description, 'url': url, 'datePosted': item.get('PostDate'), 'jobLocation': {'address': ', '.join(str(item.get(k) or '') for k in ['City', 'State'])}, 'jobLocationType': 'TELECOMMUTE' if item.get('Remote') is True else ''}
                spider.accept(obj, source, response.url)
            total = int(data.get('@odata.count') or 0)
            body = json.loads(response.request.body)
            next_skip = body.get('skip', 0) + len(data.get('value', []))
            if data.get('value') and next_skip < total:
                body['skip'] = next_skip
                url = response.url
                request = spider.request(url, source, spider.parse_connector, method='POST', body=json.dumps(body).encode(), headers={'Content-Type': 'application/json', 'api-key': response.request.headers.get('api-key')})
                if request:
                    yield request
        return
    if kind == 'datafrenzy' and '/job/GetJobs/' in response.url:
        items = response.json()
        if not isinstance(items, list):
            raise ValueError('Unexpected DataFrenzy job response')
        for item in items:
            title = str(item.get('JobTitle') or '')
            slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:50]
            city, state = str(item.get('City') or ''), str(item.get('State') or '')
            url = 'https://jobs.mastechdigital.com/client/Job/' + quote(f'{slug}-in-{city}-{state}') + '?' + urlencode({'jobNum': item['JobNum'], 'jc': item.get('JobCode', '')})
            obj = {'@type': 'JobPosting', 'title': title, 'description': item.get('JobDesc'), 'url': url, 'datePosted': item.get('DatePosted'), 'jobLocation': {'address': ', '.join(filter(None, [city, state, str(item.get('Country') or '')]))}, 'jobLocationType': 'TELECOMMUTE' if str(item.get('RemoteAvailable')).lower() == 'remote' else ''}
            if item.get('JobDesc'):
                spider.accept(obj, source, response.url)
            elif ROLE.search(title):
                request = spider.request(url, source, spider.parse_connector)
                if request:
                    yield request
        if len(items) == 50:
            page = int(response.url.split('/GetJobs/')[1].split('/')[0])
            request = spider.request(response.url.replace(f'/GetJobs/{page}/', f'/GetJobs/{page + 1}/'), source, spider.parse_connector)
            if request:
                yield request
        return
    # Details use the board's own JobPosting data, with the actual detail URL as fallback.
    for raw in response.css('script[type="application/ld+json"]::text').getall():
        try:
            spider.accept(json.loads(raw), source, response.url)
        except ValueError:
            continue
    if kind == 'sprockets' and re.search(r'/jobs/[a-f0-9-]{36}', response.url):
        description = response.css('.prose').get()
        title = response.css('h1::text').get()
        if description and title:
            spider.accept({'@type': 'JobPosting', 'title': title, 'description': description, 'url': response.url}, source, response.url)
        return
    if kind == 'phenom':
        data = embedded_phenom(response.text)
        listing = data.get('eagerLoadRefineSearch') or {}
        jobs = (listing.get('data') or {}).get('jobs') or []
        for item in jobs:
            if ROLE.search(item.get('title') or '') and item.get('jobId'):
                url = 'https://careers.teksystems.com/us/en/job/' + quote(item['jobId'])
                request = spider.request(url, source, spider.parse_connector)
                if request:
                    yield request
        if jobs and len(jobs) < int(listing.get('totalHits') or 0):
            from urllib.parse import parse_qs
            current = int(parse_qs(urlparse(response.url).query).get('from', ['0'])[0])
            offset = current + len(jobs)
            if offset < int(listing['totalHits']):
                request = spider.request(response.url.split('?')[0] + '?' + urlencode({'from': offset, 's': 1}), source, spider.parse_connector)
                if request:
                    yield request
        return
    pattern = r'/tech-jobs/[^/]+/(?:contract|direct-hire)/[^/]+/\d+' if kind == 'motion' else r'/en-US/pyramidinc/jobs/[a-f0-9-]{36}'
    for anchor in response.css('a[href]'):
        url = response.urljoin(anchor.attrib['href'])
        label = ' '.join(anchor.xpath('.//text()').getall())
        if re.search(pattern, url) and (kind == 'sprockets' or ROLE.search(label) or ROLE.search(url.replace('-', ' '))):
            request = spider.request(url, source, spider.parse_connector)
            if request:
                yield request
        elif anchor.attrib.get('rel') == 'next':
            request = spider.request(url, source, spider.parse_connector)
            if request:
                yield request
