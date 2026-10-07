import logging
import sys
from datetime import datetime, timezone
from scrapy.crawler import CrawlerProcess
from job_pipeline.staffing.spider import StaffingSpider
from job_pipeline.staffing.store import db, publish_jobs, save_job


def main():
    logging.basicConfig(level=logging.INFO)
    database = db()
    run_id = sys.argv[1]
    run = database.staffing_runs.find_one({'_id': run_id, 'status': 'running'})
    if not run:
        raise RuntimeError('Run is not claimed')
    sources = list(database.staffing_sources.find({'_id': {'$in': run['sources']}}))
    for s in sources:
        s['id'] = s['_id']
    counts = {'jobs_seen': 0, 'new_jobs': 0}

    def job(item):
        counts['jobs_seen'] += 1
        counts['new_jobs'] += int(save_job(database, item, run_id))
        database.staffing_runs.update_one({'_id': run_id}, {'$set': counts})

    def source(sid, update):
        database.staffing_sources.update_one({'_id': sid}, {'$set': {**update, 'last_checked_at': datetime.now(timezone.utc), 'last_run_id': run_id}})

    try:
        process = CrawlerProcess()
        crawler = process.create_crawler(StaffingSpider)
        process.crawl(crawler, sources=sources, on_job=job, on_source=source)
        process.start()
        if not crawler.stats.get_value('finish_reason'):
            raise RuntimeError('Crawler did not start successfully')
        published = publish_jobs(database, run_id)
        reason = crawler.stats.get_value('finish_reason')
        database.staffing_runs.update_one({'_id': run_id}, {'$set': {**counts, 'status': 'done' if reason == 'finished' else 'partial', 'finish_reason': reason, 'published_jobs': published, 'finished_at': datetime.now(timezone.utc)}})
    except Exception as error:
        logging.exception('Staffing crawl failed')
        database.staffing_runs.update_one({'_id': run_id}, {'$set': {'status': 'failed', 'error': str(error)[:500], 'finished_at': datetime.now(timezone.utc)}})
        raise
    finally:
        database.staffing_sources.update_many({'last_run_id': run_id, 'status': 'running'}, {'$set': {'status': 'failed', 'detail': 'Run stopped before this source completed'}})
        database.staffing_control.delete_one({'_id': 'lock', 'run_id': run_id})


if __name__ == '__main__':
    main()
