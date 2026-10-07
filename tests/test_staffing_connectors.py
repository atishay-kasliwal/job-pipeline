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


def api_response(url, data, body=None, listing=None):
    from scrapy import Request
    req = Request(url, method='POST' if body else 'GET', body=json.dumps(body).encode() if body else b'', meta={'listing': listing} if listing else {})
    return TextResponse(url=url, request=req, body=json.dumps(data).encode(), encoding='utf-8')


DESCRIPTION = 'Build reliable Python services, maintain cloud infrastructure, and collaborate on backend development with automated tests and monitoring.'


def test_insight_keeps_real_detail_link_and_page_number():
    jobs = []
    sid = 'insight-global'; spider = StaffingSpider([source(sid)], jobs.append, lambda *_: None)
    row = {'requisitionId': 'abc', 'jobTitle': 'Software Engineer', 'jobStatus':'active', 'workAddress':{'locality':'Austin','administrativeArea':'TX'}}
    response = api_response('https://insightglobal.com/all/jobs?keyword=software&page=1', {'jobs':[row], 'pageMetadata':{'number':1,'totalPages':2}})
    requests = list(spider.parse_connector(response,source(sid)))
    assert len(requests) == 2 and 'page=2' in requests[1].url
    detail = api_response('https://insightglobal.com/all/jobs/abc/details', {'description':DESCRIPTION}, listing=row)
    list(spider.parse_connector(detail,source(sid)))
    assert jobs[0]['job_url'] == 'https://insightglobal.com/jobs/abc'
    assert jobs[0]['location'] == 'Austin, TX'


def test_judge_requests_usa_search_with_json_accept():
    sid='the-judge-group';spider=StaffingSpider([source(sid)],lambda _:None,lambda *_:None)
    response=HtmlResponse(url='https://www.judge.com/jobs/',body=b'<html></html>',encoding='utf-8')
    req=list(spider.parse_connector(response,source(sid)))[0]
    assert req.headers['Accept'] == b'application/json'
    assert json.loads(req.body)['payload']['countries'] == 'USA'
    data={'hits':[{'jobOrderId':42,'title':'Software Engineer','description':DESCRIPTION,'location':'Boston, MA'}],'size':20,'total':1}
    jobs=[];spider.on_job=jobs.append
    detail=api_response(req.url,data,body=json.loads(req.body))
    assert list(spider.parse_connector(detail,source(sid))) == []
    assert jobs[0]['job_url'] == 'https://www.judge.com/jobs/details/42'


def test_jobdiva_xml_reads_full_description_not_preview():
    sid='genesis10';jobs=[];spider=StaffingSpider([source(sid)],jobs.append,lambda *_:None)
    xml=f'<outertag><jobs><job><title>Software Engineer</title><jobdescription><![CDATA[<p>{DESCRIPTION}</p>]]></jobdescription><portal_url>https://www2.jobdiva.com/candidates/myjobs/openjob_outside.jsp?id=42</portal_url><location>NC - Raleigh</location></job></jobs></outertag>'
    response=TextResponse(url='https://www2.jobdiva.com/candidates/myjobs/getportaljobs.jsp?a=fixture',body=xml.encode(),encoding='utf-8')
    list(spider.parse_connector(response,source(sid)))
    assert len(jobs)==1 and jobs[0]['description']==DESCRIPTION


def test_medix_full_records_and_post_pagination():
    sid='medix';jobs=[];spider=StaffingSpider([source(sid)],jobs.append,lambda *_:None)
    response=api_response('https://search.jobs.medixteam.com/search/jobs/',{'total':2,'results':[{'id':42,'posting_title':'Software Engineer','posting_description':DESCRIPTION,'city':'Austin','state':'Texas','country_name':'United States'}]},body={'query':'software','offset':0,'limit':1})
    reqs=list(spider.parse_connector(response,source(sid)))
    assert len(jobs)==1 and jobs[0]['job_url'].endswith('/jobs/42/software-engineer-austin')
    assert json.loads(reqs[0].body)['offset']==1
    # Same endpoint with a different body is a distinct pagination request.
    assert spider.request(reqs[0].url,source(sid),spider.parse_connector,method='POST',body=b'{"offset":2}') is not None


def test_sourceflow_and_manpower_full_descriptions():
    cases=[('kellymitchell','https://www.careers.kellymitchell.com/_sf/api/v1/jobs/search.json',{'total_size':1,'results':[{'job':{'title':'Software Engineer','description':DESCRIPTION,'url_slug':'42','original_location':'Austin, TX'}}]}, {'job_search':{'offset':0,'jobs_per_page':100}}), ('manpower','https://www.manpower.com/api/services/Jobs/searchjobs',{'jobsItems':[{'jobTitle':'Software Engineer','publicDescription':DESCRIPTION,'jobURL':'/en/job/software/42','jobLocation':'Austin, TX'}]}, {'filter':{'offset':0,'limit':100}})]
    for sid,url,data,body in cases:
        jobs=[];spider=StaffingSpider([source(sid)],jobs.append,lambda *_:None)
        assert list(spider.parse_connector(api_response(url,data,body=body),source(sid))) == []
        assert len(jobs)==1 and jobs[0]['description']==DESCRIPTION


def test_parser_errors_and_crawl_limits_are_visible():
    sid='medix';states=[];spider=StaffingSpider([source(sid)],lambda _:None,lambda *s:states.append(s),max_pages=1)
    response=TextResponse(url='https://search.jobs.medixteam.com/search/jobs/',body=b'<html>Unexpected response</html>',encoding='utf-8')
    assert list(spider.parse_connector(response,source(sid))) == []
    assert spider.counts[sid]['parse_errors']==1
    assert spider.request('https://jobs.medixteam.com/jobs',source(sid),spider.parse_connector)
    assert spider.request('https://jobs.medixteam.com/next',source(sid),spider.parse_connector) is None
    spider.closed('finished')
    assert states[-1][1]['status']=='failed'
    assert states[-1][1]['limited'] is True
    assert 'incomplete' in states[-1][1]['detail']


def test_job_sitemaps_use_allowed_details_and_correct_business_unit():
    sid='randstad-digital';spider=StaffingSpider([source(sid)],lambda _:None,lambda *_:None)
    xml='<urlset><url><loc>https://www.randstadusa.com/jobs/4/123/software-engineer_raleigh/</loc></url><url><loc>https://www.randstadusa.com/jobs/309/456/software-engineer_boston/</loc></url><url><loc>https://www.randstadusa.com/jobs/4/789/warehouse-worker_raleigh/</loc></url></urlset>'
    response=TextResponse(url=CONNECTORS[sid]['seeds'][0],body=xml.encode(),encoding='utf-8')
    reqs=list(spider.parse_connector(response,source(sid)))
    assert len(reqs)==1 and '/jobs/4/123/' in reqs[0].url
    assert '/s-randstad' not in CONNECTORS[sid]['seeds'][0]


def test_original_31_sources_all_have_adapter_or_documented_access_check():
    from job_pipeline.staffing.store import catalog
    other={'apex-systems','experis','hays','lasalle-network','akkodis','brooksource','vaco','inspyr-solutions','kore1','triangle-startups'}
    remaining={s['id'] for s in catalog()}-other
    assert len(remaining)==31
    assert remaining <= CONNECTORS.keys()
    assert sum(CONNECTORS[s]['kind']=='access_check' for s in remaining)==7


def test_jobposting_tolerates_literal_newlines_from_public_wordpress_board():
    jobs=[];sid='lasalle-network';spider=StaffingSpider([source(sid)],jobs.append,lambda *_:None)
    raw=json.dumps({'@type':'JobPosting','title':'Software Engineer','description':DESCRIPTION+'\nMaintain reliable production infrastructure.'}).replace('\\n','\n')
    response=HtmlResponse(url='https://www.thelasallenetwork.com/jobs/software-engineer-42/',body=('<script type="application/ld+json">'+raw+'</script>').encode(),encoding='utf-8')
    assert list(spider.parse_connector(response,source(sid))) == []
    assert len(jobs)==1 and 'Maintain reliable' in jobs[0]['description']
