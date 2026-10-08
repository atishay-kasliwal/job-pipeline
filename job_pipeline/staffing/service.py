"""Private crawler control service and daily Eastern-time scheduler."""
import hmac
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo
from job_pipeline.staffing.store import db, initialize, claim, catalog

ET = ZoneInfo('America/New_York')
DATABASE = None
TOKEN = os.environ.get('TAILOR_TOKEN', '')


def next_run(now=None):
    now = now or datetime.now(ET)
    target = now.replace(hour=7, minute=0, second=0, microsecond=0)
    return target if now < target else target + timedelta(days=1)


def launch(source_ids=None, trigger='manual'):
    run_id = 'staffing-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
    sources = claim(DATABASE, run_id, source_ids, trigger)
    if not sources:
        return None
    try:
        child = subprocess.Popen([sys.executable, '-m', 'job_pipeline.staffing.worker', run_id], start_new_session=True)
        threading.Thread(target=child.wait, daemon=True).start()
    except Exception:
        DATABASE.staffing_control.delete_one({'_id': 'lock', 'run_id': run_id})
        DATABASE.staffing_runs.update_one({'_id': run_id}, {'$set': {'status': 'failed'}})
        raise
    return run_id


def schedule():
    while True:
        try:
            now = datetime.now(ET)
            day = now.date().isoformat()
            setting = DATABASE.staffing_control.find_one({'_id': 'schedule'}) or {}
            if now.hour >= 7 and setting.get('last_day') != day:
                if launch(trigger='daily'):
                    DATABASE.staffing_control.update_one({'_id': 'schedule'}, {'$set': {'last_day': day}}, upsert=True)
        except Exception as error:
            print('Daily scheduling error:', type(error).__name__, flush=True)
        time.sleep(60)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send(self, status, body):
        payload = json.dumps(body, default=lambda x: (x.replace(tzinfo=timezone.utc) if x.tzinfo is None else x).isoformat() if isinstance(x, datetime) else str(x)).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def authorized(self):
        if not TOKEN or not hmac.compare_digest(self.headers.get('X-Tailor-Token', ''), TOKEN):
            self.send(401, {'ok': False, 'error': 'Unauthorized'})
            return False
        return True

    def do_GET(self):
        if not self.authorized():
            return
        if self.path == '/status':
            sources = list(DATABASE.staffing_sources.find().sort([('tier', 1), ('name', 1)]))
            runs = list(DATABASE.staffing_runs.find().sort('started_at', -1).limit(10))
            self.send(200, {'ok': True, 'sources': sources, 'runs': runs, 'schedule': {'label': 'Daily at 7:00 a.m. Eastern', 'next_at': next_run()}, 'total_jobs': DATABASE.staffing_jobs.count_documents({'duplicate_of': None, 'expired': False})})
        elif self.path == '/jobs':
            jobs = list(DATABASE.staffing_jobs.find({'duplicate_of': None, 'expired': False}, {'description': 0}).sort([('observed_at', -1), ('_id', 1)]))
            self.send(200, {'ok': True, 'jobs': jobs})
        else:
            self.send(404, {'ok': False, 'error': 'Not found'})

    def do_POST(self):
        if not self.authorized():
            return
        try:
            size = int(self.headers.get('Content-Length', 0))
            if size < 0 or size > 10000:
                raise ValueError('Invalid request size')
            body = json.loads(self.rfile.read(size) or b'{}')
            if not isinstance(body, dict):
                raise ValueError('Expected an object')
            if self.path == '/run':
                ids = body.get('sourceIds')
                known = {s['id'] for s in catalog()}
                if ids is not None and (not isinstance(ids, list) or not ids or any(not isinstance(s, str) or s not in known for s in ids)):
                    raise ValueError('Unknown source')
                run_id = launch(ids)
                self.send(202 if run_id else 409, {'ok': bool(run_id), 'runId': run_id, 'error': None if run_id else 'A crawl is already running, or no sources are enabled'})
            elif self.path == '/source':
                sid = body.get('sourceId')
                if sid not in {s['id'] for s in catalog()} or not isinstance(body.get('enabled'), bool):
                    raise ValueError('Invalid source settings')
                DATABASE.staffing_sources.update_one({'_id': sid}, {'$set': {'enabled': body['enabled']}})
                self.send(200, {'ok': True})
            else:
                self.send(404, {'ok': False, 'error': 'Not found'})
        except (ValueError, TypeError) as error:
            self.send(400, {'ok': False, 'error': str(error)})
        except Exception as error:
            self.send(500, {'ok': False, 'error': type(error).__name__})


def main():
    global DATABASE
    if not TOKEN:
        raise RuntimeError('TAILOR_TOKEN is required')
    DATABASE = db()
    initialize(DATABASE)
    # Interrupted runs are recorded; never leave the UI claiming a stopped worker is running.
    DATABASE.staffing_runs.update_many({'status': 'running'}, {'$set': {'status': 'interrupted', 'finished_at': datetime.now(timezone.utc)}})
    DATABASE.staffing_sources.update_many({'status': 'running'}, {'$set': {'status': 'failed', 'detail': 'Previous worker was interrupted'}})
    DATABASE.staffing_control.delete_one({'_id': 'lock'})
    threading.Thread(target=schedule, daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', int(os.environ.get('STAFFING_PORT', '8791'))), Handler).serve_forever()


if __name__ == '__main__':
    main()
