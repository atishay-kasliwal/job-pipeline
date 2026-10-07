import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError


def db():
    uri = os.environ.get('MONGO_URI')
    if not uri:
        raise RuntimeError('MONGO_URI is required')
    return MongoClient(uri, serverSelectionTimeoutMS=10000)[os.environ.get('MONGO_DB_NAME', 'job_pipeline')]


def catalog():
    return json.loads(Path(__file__).with_name('sources.json').read_text())


def initialize(database):
    for source in catalog():
        database.staffing_sources.update_one({'_id': source['id']}, {'$set': {'name': source['name'], 'url': source['url'], 'tier': source['tier'], 'start_urls': source.get('start_urls', [])}, '$setOnInsert': {'enabled': True, 'status': 'not_checked', 'detail': 'Waiting for the first daily crawl'}}, upsert=True)
    database.staffing_jobs.create_index([('source_id', 1), ('observed_at', -1)])
    database.staffing_jobs.create_index('fingerprint')
    database.staffing_runs.create_index('started_at')


def claim(database, run_id, source_ids=None, trigger='manual'):
    now = datetime.now(timezone.utc)
    try:
        lock = database.staffing_control.find_one_and_update({'_id': 'lock', '$or': [{'expires_at': {'$lt': now}}, {'expires_at': {'$exists': False}}]}, {'$set': {'run_id': run_id, 'expires_at': now + timedelta(minutes=30)}}, upsert=True, return_document=ReturnDocument.AFTER)
    except DuplicateKeyError:
        return None
    query = {'enabled': True}
    if source_ids:
        query['_id'] = {'$in': source_ids}
    sources = list(database.staffing_sources.find(query))
    if not sources:
        database.staffing_control.delete_one({'_id': 'lock', 'run_id': run_id})
        return None
    for source in sources:
        source['id'] = source['_id']
    database.staffing_runs.insert_one({'_id': run_id, 'status': 'running', 'trigger': trigger, 'started_at': now, 'sources': [s['id'] for s in sources], 'jobs_seen': 0, 'new_jobs': 0})
    return sources


def save_job(database, job, run_id):
    now = datetime.now(timezone.utc)
    expired = False
    if job.get('valid_through'):
        try:
            expired = datetime.fromisoformat(str(job['valid_through']).replace('Z', '+00:00')).replace(tzinfo=timezone.utc) < now
        except ValueError:
            pass
    existing = database.staffing_jobs.find_one({'fingerprint': job['fingerprint'], '_id': {'$ne': job['_id']}, 'duplicate_of': None})
    record = {**job, 'run_id': run_id, 'expired': expired, 'duplicate_of': existing['_id'] if existing else None}
    ident = record.pop('_id')
    result = database.staffing_jobs.update_one({'_id': ident}, {'$set': record, '$setOnInsert': {'first_seen_at': now}}, upsert=True)
    return bool(result.upserted_id)


def publish_jobs(database, run_id):
    """Apply existing eligibility/scoring rules, then feed the existing job collection."""
    import pandas as pd
    from job_pipeline.filters import filter_by_company, filter_by_role, filter_by_location, filter_by_sponsorship, filter_by_experience, tag_level, extract_exp_range
    from job_pipeline.scoring import apply_scores
    from job_pipeline.storage import _df_to_records
    rows = list(database.staffing_jobs.find({'run_id': run_id, 'duplicate_of': None, 'expired': False}))
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    for filter_fn in [filter_by_company, filter_by_role, filter_by_location, filter_by_sponsorship, filter_by_experience, tag_level, extract_exp_range, apply_scores]:
        df = filter_fn(df)
        if df.empty:
            return 0
    now = datetime.now(timezone.utc)
    df['batch_time'] = now.isoformat()
    records = _df_to_records(df, run_id, 'standard', now)
    for row in records:
        row.pop('_id', None)
        description = row.pop('description', '')
        database.jobs.update_one({'job_url': row['job_url']}, {'$set': row}, upsert=True)
        database.descriptions.update_one({'job_url': row['job_url']}, {'$set': {'description': description, 'updated_at': now}, '$setOnInsert': {'job_url': row['job_url']}}, upsert=True)
    database.sessions.update_one({'session_id': run_id, 'pipeline': 'standard'}, {'$set': {'run_at': now, 'pipeline': 'standard', 'job_count': len(records), 'archived': False, 'source': 'staffing'}}, upsert=True)
    return len(records)
