import json
from scrapy.http import HtmlResponse, TextResponse
from job_pipeline.staffing.spider import StaffingSpider
from job_pipeline.staffing.connectors import CONNECTORS, embedded_phenom


def source(sid):
    return {'id': sid, 'name': sid, 'url': 'https://company.example', 'tier': 1}


def test_phenom_discovers_real_ids_and_does_not_save_teasers():
    jobs = []
    spider = StaffingSpider([source('teksystems')], jobs.append, lambda *_: None)
    data = {'eagerLoadRefineSearch': {'totalHits': 1, 'data': {'jobs': [{'jobId': 'JP-123', 'title': 'Software Engineer', 'descriptionTeaser': 'A short preview'}]}}}
    response = HtmlResponse(url=CONNECTORS['teksystems']['seeds'][0], body=('<script>phApp.ddo = ' + json.dumps(data) + ';</script>').encode(), encoding='utf-8')
    requests = list(spider.parse_connector(response, source('teksystems')))
    assert len(requests) == 1
    assert requests[0].url == 'https://careers.teksystems.com/us/en/job/JP-123'
    assert jobs == []
    assert embedded_phenom('phApp.ddo = window.dynamic()') == {}


def test_motion_follows_job_details_not_navigation():
    spider = StaffingSpider([source('motion-recruitment')], lambda _: None, lambda *_: None)
    html = '<a href="/tech-jobs/software-engineering">Software Engineering</a><a href="/tech-jobs/boston/contract/software-engineer/123456">Software Engineer</a>'
    response = HtmlResponse(url=CONNECTORS['motion-recruitment']['seeds'][0], body=html.encode(), encoding='utf-8')
    requests = list(spider.parse_connector(response, source('motion-recruitment')))
    assert len(requests) == 1
    assert requests[0].url.endswith('/software-engineer/123456')


def test_datafrenzy_preserves_full_description_and_job_identity():
    jobs = []
    spider = StaffingSpider([source('mastech-digital')], jobs.append, lambda *_: None)
    data = [{'JobNum': 42, 'JobCode': 'ABC', 'JobTitle': 'Software Engineer', 'JobDesc': '<p>Build reliable Python services, maintain cloud infrastructure, and collaborate on backend application development.</p>', 'JobDescMini': 'Short preview', 'City': 'Boston', 'State': 'MA', 'Country': 'US'}]
    response = TextResponse(url=CONNECTORS['mastech-digital']['seeds'][0], body=json.dumps(data).encode(), encoding='utf-8')
    assert list(spider.parse_connector(response, source('mastech-digital'))) == []
    assert len(jobs) == 1
    assert 'Short preview' not in jobs[0]['description']
    assert 'jobNum=42' in jobs[0]['job_url']
    assert jobs[0]['location'] == 'Boston, MA, US'


def test_sprockets_limits_discovery_to_company_jobs():
    spider = StaffingSpider([source('pyramid-consulting')], lambda _: None, lambda *_: None)
    html = '<a href="/en-US/pyramidinc/jobs/0028042e-6f35-4a49-bbcd-468cbb62836e">Software Engineer</a><a href="/en-US/another/jobs/0028042e-6f35-4a49-bbcd-468cbb62836e">Software Engineer</a>'
    response = HtmlResponse(url=CONNECTORS['pyramid-consulting']['seeds'][0], body=html.encode(), encoding='utf-8')
    requests = list(spider.parse_connector(response, source('pyramid-consulting')))
    assert len(requests) == 1
    assert '/pyramidinc/jobs/' in requests[0].url


def test_triangle_public_records_resolve_shared_company_and_keep_employer():
    records = [{'id': 'one', 'title': 'Software Engineer', 'description': 'Build Python backend services and maintain cloud infrastructure with reliable automated tests and production monitoring.', 'company': {'name': 'Actual Employer'}, 'location': 'Durham, NC', 'apply_external_url': 'https://employer.example/jobs/one'}, {'id': 'two', 'title': 'Data Engineer', 'description': '$aa', 'company': '$1a:props:jobs:0:company', 'apply_external_url': 'https://employer.example/jobs/two'}]
    text = 'Maintain data ingestion and Python ETL pipelines with production monitoring, SQL warehouses, and cloud infrastructure.'
    flight = 'aa:T' + format(len(text.encode()), 'x') + ',' + text + '\n1a:' + json.dumps({'jobs': records})
    html = '<script>self.__next_f.push(' + json.dumps([1, flight]) + ')</script>'
    jobs = []
    spider = StaffingSpider([source('triangle-startups')], jobs.append, lambda *_: None)
    response = HtmlResponse(url='https://triangle-startups.com/jobs', body=html.encode(), encoding='utf-8')
    assert list(spider.parse_connector(response, source('triangle-startups'))) == []
    assert len(jobs) == 2
    assert all(j['company'] == 'Actual Employer' for j in jobs)
    assert jobs[1]['description'] == text
