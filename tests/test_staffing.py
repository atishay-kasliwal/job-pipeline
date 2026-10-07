import json
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
from scrapy.http import HtmlResponse, Request
from job_pipeline.staffing.extract import job_objects, normalize_job, canonical_url
from job_pipeline.staffing.spider import StaffingSpider
from job_pipeline.staffing.service import next_run
from job_pipeline.staffing.store import catalog

SOURCE = {'id': 'acme', 'name': 'Acme Staffing', 'url': 'https://acme.example', 'tier': 1}
JOB = {'@type': 'JobPosting', 'title': 'Software Engineer', 'description': '<p>Build Python and FastAPI backend services with automated tests and cloud deployments for enterprise applications.</p>', 'url': 'https://acme.example/jobs/123?utm_source=mail', 'jobLocation': {'address': {'addressLocality': 'New York', 'addressRegion': 'NY', 'addressCountry': 'US'}}}


def test_catalog():
    sources = catalog()
    assert len(sources) == 40
    assert len({s['id'] for s in sources}) == 40
    assert [sum(s['tier'] == i for s in sources) for i in [1, 2, 3]] == [10, 15, 15]


def test_nested_job_and_normalization():
    assert list(job_objects({'@graph': [JOB]})) == [JOB]
    result = normalize_job(JOB, SOURCE, 'https://acme.example/')
    assert result['job_url'] == 'https://acme.example/jobs/123'
    assert result['company'] == 'Acme Staffing'
    assert result['location'] == 'New York, NY, US'
    assert '<p>' not in result['description']
    assert result['_id'] == normalize_job({**JOB, 'url': 'https://acme.example/jobs/123'}, SOURCE, 'https://acme.example/')[' _id'.strip()]
    assert normalize_job({**JOB, 'title': 'Registered Nurse'}, SOURCE, '') is None
    assert normalize_job({**JOB, 'description': ''}, SOURCE, '') is None


def test_spider_discovers_jobs_and_bounds_traversal():
    jobs, states = [], []
    spider = StaffingSpider([SOURCE], jobs.append, lambda sid, state: states.append(state), max_pages=3)
    response = HtmlResponse(url='https://acme.example/careers', body=('<script type="application/ld+json">' + json.dumps({'@graph': [JOB]}) + '</script><a href="https://evil.example/jobs/1">Jobs</a><a href="/jobs?page=2">Next</a>').encode(), encoding='utf-8', request=Request('https://acme.example/careers'))
    requests = list(spider.parse(response, SOURCE))
    assert len(jobs) == 1
    assert all('evil.example' not in r.url for r in requests)
    assert spider.request('https://acme.example/jobs?page=2', SOURCE, spider.parse) is None
    spider.closed('finished')
    assert states[-1]['status'] == 'ready'


def test_zero_structured_jobs_is_not_reported_as_success():
    states = []
    spider = StaffingSpider([SOURCE], lambda _: None, lambda sid, state: states.append(state))
    spider.counts['acme']['pages'] = 2
    spider.closed('finished')
    assert states[-1]['status'] == 'needs_connector'


def test_daily_schedule_uses_eastern_dst():
    zone = ZoneInfo('America/New_York')
    assert next_run(datetime(2026, 10, 7, 6, tzinfo=zone)).hour == 7
    assert next_run(datetime(2026, 10, 7, 8, tzinfo=zone)).day == 8
    assert next_run(datetime(2026, 11, 1, 8, tzinfo=zone)).utcoffset().total_seconds() == -18000


def test_storage_deduplicates_and_preserves_existing_resume():
    import mongomock
    from job_pipeline.staffing.store import initialize, claim, save_job, publish_jobs
    database = mongomock.MongoClient().job_pipeline
    initialize(database)
    assert len(list(database.staffing_sources.find())) == 40
    assert claim(database, 'run-1', ['kforce'])
    assert claim(database, 'run-2', ['kforce']) is None
    source = {**SOURCE, 'id': 'kforce', 'name': 'Kforce'}
    job = normalize_job({**JOB, 'datePosted': '2026-10-07T10:00:00Z'}, source, '')
    assert save_job(database, job, 'run-1')
    assert not save_job(database, job, 'run-1')
    duplicate = normalize_job({**JOB, 'url': 'https://other.example/jobs/123'}, {**source, 'id': 'other'}, '')
    save_job(database, duplicate, 'run-1')
    assert database.staffing_jobs.find_one({'_id': duplicate['_id']})['duplicate_of'] == job['_id']
    database.jobs.insert_one({'job_url': job['job_url'], 'resume': {'status': 'success', 'pdf_path': '/existing/resume.pdf'}})
    assert publish_jobs(database, 'run-1') == 1
    stored = database.jobs.find_one({'job_url': job['job_url']})
    assert stored['resume']['pdf_path'] == '/existing/resume.pdf'
    assert stored['score_pct'] >= 0
    assert database.descriptions.find_one({'job_url': job['job_url']})['description'] == job['description']
    assert database.jobs.count_documents({}) == 1


def test_private_http_status_and_controls(monkeypatch):
    import threading
    import requests
    import mongomock
    from http.server import ThreadingHTTPServer
    from job_pipeline.staffing import service
    from job_pipeline.staffing.store import initialize
    database = mongomock.MongoClient().job_pipeline
    initialize(database)
    monkeypatch.setattr(service, 'DATABASE', database)
    monkeypatch.setattr(service, 'TOKEN', 'fixture-token')
    monkeypatch.setattr(service, 'launch', lambda *args, **kwargs: 'fixture-run')
    server = ThreadingHTTPServer(('127.0.0.1', 0), service.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = f'http://127.0.0.1:{server.server_port}'
    headers = {'X-Tailor-Token': 'fixture-token'}
    try:
        assert requests.get(root + '/status').status_code == 401
        assert len(requests.get(root + '/status', headers=headers).json()['sources']) == 40
        assert requests.post(root + '/run', headers=headers, json={'sourceIds': ['unknown']}).status_code == 400
        assert requests.post(root + '/run', headers=headers, json={'sourceIds': ['kforce']}).status_code == 202
        assert requests.post(root + '/source', headers=headers, json={'sourceId': 'kforce', 'enabled': False}).status_code == 200
        assert database.staffing_sources.find_one({'_id': 'kforce'})['enabled'] is False
    finally:
        server.shutdown()
        server.server_close()


def test_sitemap_ignores_embedded_images_and_prioritizes_relevant_jobs():
    from scrapy.http import XmlResponse
    spider = StaffingSpider([SOURCE], lambda _: None, lambda *_: None)
    xml = '<urlset xmlns:image="https://www.google.com/schemas/sitemap-image/1.1"><url><loc>https://acme.example/jobs/software-engineer</loc><image:image><image:loc>https://acme.example/images/jobs.jpg</image:loc></image:image></url></urlset>'
    response = XmlResponse(url='https://acme.example/sitemap.xml', body=xml.encode(), encoding='utf-8')
    assert [r.url for r in spider.parse(response, SOURCE)] == ['https://acme.example/jobs/software-engineer']
    assert spider.request('https://acme.example/jobs.pdf', SOURCE, spider.parse) is None
