"""Read-only adapters for public staffing job boards and their search feeds."""
import json
import re
from urllib.parse import urlencode
from job_pipeline.staffing.extract import ROLE


def follow(spider, url, source, **options):
    request = spider.request(url, source, spider.parse_connector, **options)
    if request:
        yield request


def record(spider, source, response, title, description, url, location='', posted=None, remote=False):
    spider.accept({'@type': 'JobPosting', 'title': title, 'description': description, 'url': url,
                   'jobLocation': {'address': location}, 'datePosted': posted,
                   'jobLocationType': 'TELECOMMUTE' if remote else ''}, source, response.url)


def handle(spider, response, source, config):
    kind = config['kind']
    if kind == 'sourceflow':
        if '/_sf/' not in response.url:
            for term in ['software','data','python','cloud']:
                body={'job_search':{'query':term,'location':{},'filters':{},'commute_filter':{},'offset':0,'jobs_per_page':100}}
                yield from follow(spider,'https://www.careers.kellymitchell.com/_sf/api/v1/jobs/search.json',source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        else:
            data=response.json()
            for result in data.get('results',[]):
                row=result['job']
                record(spider,source,response,row['title'],row.get('description'),'https://www.careers.kellymitchell.com/jobs/'+row['url_slug'],row.get('original_location') or '; '.join(row.get('addresses') or []))
            body=json.loads(response.request.body);b=body['job_search'];b['offset']+=len(data.get('results',[]))
            if data.get('results') and b['offset']<int(data.get('total_size') or 0):
                yield from follow(spider,response.url,source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        return
    if kind == 'manpower':
        if '/api/services/Jobs/' not in response.url:
            for term in ['software','data','python']:
                body={'sf':True,'filter':{'offset':0,'totalCount':0,'limit':100,'searchkeyword':term,'haslocation':False,'language':'en'}}
                yield from follow(spider,config.get('api_origin','https://www.manpower.com')+'/api/services/Jobs/searchjobs',source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        else:
            data=response.json();rows=data.get('jobsItems',[])
            for row in rows:
                if not row.get('isExpired'):
                    record(spider,source,response,row['jobTitle'],row.get('publicDescription'),response.urljoin(row['jobURL']),row.get('jobLocation',''),row.get('publishfromDate'),row.get('remoteJobs') is True)
            body=json.loads(response.request.body);b=body['filter']
            if len(rows)==b['limit']:
                b['offset']+=1
                yield from follow(spider,response.url,source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        return
    if kind == 'job_sitemap':
        if response.url.endswith('.xml'):
            from parsel import Selector
            from urllib.parse import urlsplit
            urls=Selector(text=response.text,type='xml').xpath('//*[local-name()="url"]/*[local-name()="loc"]/text()').getall()
            for url in urls:
                path=urlsplit(url).path
                if path.startswith(config['job_prefix']) and ROLE.search(path.replace('-',' ').replace('_',' ')):
                    yield from follow(spider,url,source)
        else:
            for raw in response.css('script[type="application/ld+json"]::text').getall():
                try:spider.accept(json.loads(raw, strict=False),source,response.url)
                except (ValueError,TypeError):pass
        return
    if kind == 'compunnel':
        if 'stafflineapi.' in response.url:
            data=response.json()
            for row in data.get('data',[]):
                if ROLE.search(row.get('Job_Title','')):
                    req=spider.request(row['JobURL'],source,spider.parse_connector)
                    if req:
                        req.meta['listing']=row
                        yield req
            body=json.loads(response.request.body)
            if body['PageIndex']*body['PageSize']<int(data.get('totalrecord') or 0):
                body['PageIndex']+=1
                yield from follow(spider,response.url,source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':response.request.headers.get('Authorization')})
        elif 'staffline.compunnel.com' in response.url:
            row=response.meta['listing']
            description=response.css('.jobdetail .descHeight').get()
            if description:
                record(spider,source,response,row['Job_Title'],description,response.url,row.get('Location',''),row.get('PostedOn'))
        else:
            match=re.search(r"'Authorization':\s*'([^']+)'",response.text)
            if not match: raise ValueError('Compunnel public search client changed')
            for term in ['software','data','python']:
                body={'PageIndex':1,'PageSize':50,'PortalId':5007,'SearchKey':term,'CountryId':'','StateId':'-1','CityId':'-1'}
                yield from follow(spider,'https://stafflineapi.compunnel.com/job/jobsList',source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':match[1]})
        return
    if kind == 'medix':
        if 'search.jobs.medixteam.com' in response.url:
            data=response.json()
            for row in data.get('results',[]):
                slug=re.sub(r'[^a-z0-9]+','-',(row['posting_title']+'-'+(row.get('city') or '')).lower()).strip('-')
                record(spider,source,response,row['posting_title'],row.get('posting_description'),'https://jobs.medixteam.com/jobs/'+str(row['id'])+'/'+slug,', '.join(filter(None,[row.get('city'),row.get('state'),row.get('country_name')])),row.get('date_added'),row.get('position_type')=='Remote')
            body=json.loads(response.request.body);body['offset']+=len(data.get('results',[]))
            if data.get('results') and body['offset']<int(data.get('total') or 0):
                yield from follow(spider,response.url,source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json','api-key':response.request.headers.get('api-key')})
        elif '/_next/static/' in response.url:
            if 'https://search.jobs.medixteam.com/search/jobs/' not in response.text: return
            match=re.search(r'"api-key":"([^"\\]+)"',response.text)
            if not match: raise ValueError('Medix public search client changed')
            for term in ['software','data','python']:
                body={'query':term,'limit':100,'offset':0,'filters':{}}
                yield from follow(spider,'https://search.jobs.medixteam.com/search/jobs/',source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json','api-key':match[1]})
        else:
            for url in response.css('script[src*="/_next/static/"]::attr(src)').getall():
                yield from follow(spider,response.urljoin(url),source)
        return
    if kind == 'cyber':
        data=response.json()
        if '/details?' in response.url:
            if data.get('isActive'):
                record(spider,source,response,data['jobTitle'],data.get('description'),'https://www.cybercoders.com/'+data['jobURL']+'/',data.get('locationsSplit') or data.get('citySplit',''),data.get('dateCreated'))
        else:
            for row in data.get('jobs',[]):
                if ROLE.search(row.get('jobTitle','')):
                    yield from follow(spider,'https://www.cybercoders.com/ccv5-jobs/details?'+urlencode({'id':row['Id'],'buid':1}),source)
            from urllib.parse import urlsplit,parse_qsl,urlunsplit
            p=urlsplit(response.url);q=dict(parse_qsl(p.query));page=int(q.get('page',1));size=int(q.get('rows',20))
            if data.get('jobs') and page*size<int(data.get('numFound') or 0):
                q['page']=page+1;q['rows']=size
                yield from follow(spider,urlunsplit((p.scheme,p.netloc,p.path,urlencode(q),'')),source)
        return
    if kind == 'insight':
        data = response.json()
        if '/details' in response.url:
            row = response.meta['listing']
            address = row.get('workAddress') or {}
            record(spider, source, response, row['jobTitle'], data.get('description') or data.get('descriptionHtml'),
                   'https://insightglobal.com/jobs/' + row['requisitionId'],
                   ', '.join(filter(None, [address.get('locality'), address.get('administrativeArea')])), row.get('postedDate'), row.get('workRemote'))
        else:
            for row in data.get('jobs', []):
                if ROLE.search(row.get('jobTitle', '')) and row.get('jobStatus') == 'active':
                    req = spider.request('https://insightglobal.com/all/jobs/' + row['requisitionId'] + '/details', source, spider.parse_connector)
                    if req:
                        req.meta['listing'] = row
                        yield req
            meta = data.get('pageMetadata') or {}
            page = int(meta.get('number', meta.get('page', 1)))
            total = int(meta.get('totalPages', 0))
            if page < total:
                from urllib.parse import parse_qsl, urlsplit, urlunsplit
                p = urlsplit(response.url); query = dict(parse_qsl(p.query)); query['page'] = page + 1
                yield from follow(spider, urlunsplit((p.scheme,p.netloc,p.path,urlencode(query),'')), source)
        return
    if kind == 'jobdiva':
        if 'getportaljobs.jsp' in response.url:
            from parsel import Selector
            for job in Selector(text=response.text, type='xml').xpath('//jobs/job'):
                get = lambda key: job.xpath('./' + key + '/text()').get('')
                record(spider, source, response, get('title'), get('jobdescription'), get('portal_url'), get('location'), get('issuedate'))
        else:
            # HubSpot publishes the XML job-feed URL in its module configuration.
            import html
            text = html.unescape(response.text)
            match = re.search(r'https://www[12]\.jobdiva\.com/candidates/myjobs/getportaljobs\.jsp\?[^"<>\s]+', text)
            if not match:
                from urllib.parse import urlsplit, parse_qs
                urls=response.css('iframe::attr(src)').getall()
                portal=next((u for u in urls if re.match(r'https://www[12]\.jobdiva\.com/candidates/myjobs/searchjobsdone.jsp',u)),None)
                if not portal: raise ValueError('Public JobDiva feed link missing')
                p=urlsplit(portal);tenant=parse_qs(p.query).get('a',[''])[0]
                yield from follow(spider,p.scheme+'://'+p.netloc+'/candidates/myjobs/getportaljobs.jsp?'+urlencode({'a':tenant,'noofjobs':10000}),source)
                return
            yield from follow(spider, match[0], source)
        return
    if kind == 'judge':
        if '/admin-ajax.php' not in response.url:
            payload = {'categories': [], 'countries': 'USA', 'geo': [{'distance':'','latLong':[0],'location':''}], 'query': 'software', 'states':'','type':['Any'],'page':0,'remote':False}
            yield from follow(spider, 'https://www.judge.com/wp-admin/admin-ajax.php?action=jdg_get_jobs', source, method='POST', body=json.dumps({'payload':payload}).encode(), headers={'Content-Type':'application/json'})
        else:
            data = response.json()
            if isinstance(data,str): data=json.loads(data)
            if data.get('status',200) >= 400:
                raise ValueError('Judge public search rejected request')
            for row in data.get('hits',[]):
                record(spider, source, response, row.get('title'), row.get('description'), 'https://www.judge.com/jobs/details/' + str(row['jobOrderId']) + '/', row.get('location',''), row.get('opened'))
            body=json.loads(response.request.body); payload=body['payload']
            if (int(payload['page'])+1)*int(data.get('size') or 20)<int(data.get('total') or 0):
                payload['page']+=1
                yield from follow(spider,response.url,source,method='POST',body=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        return
    if source['id']=='collabera' and '/job-description/' in response.url:
        description=response.css('.col-md-7.position .card-body').get()
        title=response.css('main h4::text').get()
        location=response.css('.job_desp .job-bold::text').get('')
        if title and description:
            record(spider,source,response,title,description,response.url,location)
        return
    # Exact public-board links, rather than the site's marketing navigation.
    for raw in response.css('script[type="application/ld+json"]::text').getall():
        try: spider.accept(json.loads(raw, strict=False),source,response.url)
        except (ValueError,TypeError): pass
    for link in response.css(config.get('links','a[href]')):
        url=response.urljoin(link.attrib.get('href',''))
        label=' '.join(link.xpath('.//text()').getall())
        if re.search(config['detail_pattern'],url) and (ROLE.search(label) or ROLE.search(url.replace('-',' '))):
            yield from follow(spider,url,source)
        elif re.search(config.get('pagination_pattern',r'(?!)'),url) or 'next' in link.attrib.get('rel','').split():
            yield from follow(spider,url,source)
