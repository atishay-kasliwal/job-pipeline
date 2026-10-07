import json
import hashlib
import re
from collections import defaultdict
from urllib.parse import urlparse
import scrapy
from job_pipeline.staffing.extract import job_objects, normalize_job
from job_pipeline.staffing.connectors import CONNECTORS, handle as handle_connector

ATS_HOSTS = {'boards.greenhouse.io', 'job-boards.greenhouse.io', 'boards-api.greenhouse.io', 'jobs.lever.co', 'api.lever.co', 'jobs.ashbyhq.com', 'api.ashbyhq.com'}


class StaffingSpider(scrapy.Spider):
    name = 'staffing_daily'
    custom_settings = {'ROBOTSTXT_OBEY': True, 'USER_AGENT': 'AtriveoStaffingBot/1.0 (personal job search)', 'CONCURRENT_REQUESTS': 4, 'CONCURRENT_REQUESTS_PER_DOMAIN': 1, 'DOWNLOAD_DELAY': 1.5, 'AUTOTHROTTLE_ENABLED': True, 'AUTOTHROTTLE_START_DELAY': 2, 'AUTOTHROTTLE_MAX_DELAY': 20, 'DOWNLOAD_TIMEOUT': 25, 'RETRY_TIMES': 1, 'CLOSESPIDER_TIMEOUT': 1200, 'LOG_LEVEL': 'INFO', 'DEPTH_LIMIT': 4, 'DOWNLOAD_MAXSIZE': 5_000_000}

    def __init__(self, sources, on_job, on_source, max_pages=40, **kwargs):
        super().__init__(**kwargs)
        self.sources = sources
        self.on_job = on_job
        self.on_source = on_source
        self.max_pages = max_pages
        self.counts = defaultdict(lambda: {'pages': 0, 'structured_jobs': 0, 'matching_jobs': 0, 'errors': 0, 'scheduled': 0, 'blocked': 0})
        self.seen = set()

    async def start(self):
        for source in sorted(self.sources, key=lambda x: x['tier']):
            self.on_source(source['id'], {'status': 'running', 'detail': 'Checking public job pages'})
            if source['id'] in CONNECTORS:
                for url in CONNECTORS[source['id']]['seeds']:
                    request = self.request(url, source, self.parse_connector)
                    if request:
                        yield request
                continue
            for url in [source['url'], f'https://{urlparse(source["url"]).netloc}/sitemap.xml', *source.get('start_urls', [])]:
                request = self.request(url, source, self.parse)
                if request:
                    yield request

    def request(self, url, source, callback, **options):
        parsed = urlparse(url)
        host = parsed.hostname or ''
        if re.search(r'\.(?:jpg|jpeg|png|gif|svg|webp|pdf|zip|mp4|woff2?)(?:$)', parsed.path, re.I):
            return None
        root = (urlparse(source['url']).hostname or '').removeprefix('www.')
        if not (host == root or host.endswith('.' + root) or host in ATS_HOSTS or host in CONNECTORS.get(source['id'], {}).get('hosts', [])):
            return None
        key = (source['id'], url, hashlib.sha256(options.get('body', b'')).hexdigest())
        if key in self.seen or self.counts[source['id']]['scheduled'] >= self.max_pages:
            return None
        self.seen.add(key)
        self.counts[source['id']]['scheduled'] += 1
        return scrapy.Request(url, callback=callback, errback=self.failed, cb_kwargs={'source': source}, meta={'source_id': source['id']}, priority=(4 - source['tier']) * 100, dont_filter=True, **options)

    def failed(self, failure):
        sid = failure.request.meta['source_id']
        self.counts[sid]['errors'] += 1
        status = getattr(getattr(failure.value, 'response', None), 'status', None)
        if status in {401, 403, 429} or 'robots' in str(failure.value).lower():
            self.counts[sid]['blocked'] += 1

    def accept(self, data, source, url):
        for obj in job_objects(data):
            self.counts[source['id']]['structured_jobs'] += 1
            job = normalize_job(obj, source, url)
            if job:
                self.counts[source['id']]['matching_jobs'] += 1
                self.on_job(job)

    def parse(self, response, source):
        self.counts[source['id']]['pages'] += 1
        self.on_source(source['id'], {'pages': self.counts[source['id']]['pages'], 'matching_jobs': self.counts[source['id']]['matching_jobs'], 'detail': f'Checking public pages: {self.counts[source["id"]]["pages"]} read'})
        if not isinstance(response, scrapy.http.TextResponse):
            return
        if 'xml' in response.headers.get('Content-Type', b'').decode() or response.url.endswith('.xml'):
            urls = response.xpath('//*[local-name()="url" or local-name()="sitemap"]/*[local-name()="loc"]/text()').getall()
            urls.sort(key=lambda url: (bool(re.search(r'blog|resource|webinar|news', url, re.I)), not bool(re.search(r'software|data|developer|engineer|machine-learning', url, re.I))))
            for url in urls:
                if re.search(r'job|career|vacanc|opportun', url, re.I):
                    req = self.request(url, source, self.parse)
                    if req:
                        yield req
            return
        for raw in response.css('script[type="application/ld+json"]::text').getall():
            try:
                self.accept(json.loads(raw), source, response.url)
            except (ValueError, TypeError):
                continue
        candidates = response.css('main a[href], [role="main"] a[href]') or response.css('a[href]')
        links = sorted(candidates, key=lambda link: (bool(re.search(r'blog|resource|webinar|news', link.attrib['href'], re.I)), not bool(re.search(r'software|data|developer|engineer|machine-learning', link.attrib['href'], re.I))))
        followed = 0
        for link in links:
            if followed >= 10:
                break
            url = response.urljoin(link.attrib['href'])
            host = urlparse(url).hostname or ''
            if host in ATS_HOSTS:
                parts = urlparse(url).path.strip('/').split('/')
                if not parts or not parts[0]:
                    continue
                token = parts[0]
                if 'greenhouse' in host:
                    endpoint = f'https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true'
                elif 'lever' in host:
                    endpoint = f'https://api.lever.co/v0/postings/{token}?mode=json'
                elif 'ashby' in host:
                    endpoint = f'https://api.ashbyhq.com/posting-api/job-board/{token}'
                req = self.request(endpoint, source, self.parse_api)
                if req:
                    followed += 1
                    yield req
            if re.search(r'job|career|vacanc|opportun|next|page=', url + ' ' + ' '.join(link.css('::text').getall()), re.I):
                req = self.request(url, source, self.parse)
                if req:
                    followed += 1
                    yield req

    def parse_connector(self, response, source):
        yield from handle_connector(self, response, source)

    def parse_api(self, response, source):
        self.counts[source['id']]['pages'] += 1
        data = response.json()
        entries = data if isinstance(data, list) else data.get('jobs', [])
        for item in entries:
            cats = item.get('categories') or {}
            loc = item.get('location') or cats.get('location') or ''
            if isinstance(loc, dict):
                loc = loc.get('name', '')
            description = item.get('content') or item.get('descriptionHtml') or item.get('description') or item.get('descriptionPlain') or ''
            obj = {'@type': 'JobPosting', 'title': item.get('title') or item.get('text'), 'description': description, 'url': item.get('absolute_url') or item.get('hostedUrl') or item.get('jobUrl'), 'jobLocation': {'address': str(loc)}, 'jobLocationType': 'TELECOMMUTE' if item.get('isRemote') else '', 'datePosted': item.get('publishedAt') or item.get('createdAt') or item.get('updated_at')}
            self.accept(obj, source, response.url)

    def closed(self, reason):
        for source in self.sources:
            c = self.counts[source['id']]
            status = 'ready' if c['structured_jobs'] else 'blocked' if c['blocked'] else 'failed' if not c['pages'] else 'needs_connector'
            detail = f'{c["matching_jobs"]} matching jobs from {c["pages"]} pages' if c['structured_jobs'] else 'Job board needs a dedicated connector or renders jobs in JavaScript' if c['pages'] else 'No readable pages; check access or source URL'
            self.on_source(source['id'], {**c, 'status': status, 'detail': detail, 'finish_reason': reason})
