#!/bin/bash
# Runs the scraper, then immediately refreshes the app's JD buckets.
#
# WHY: the scraper adds new jobs + JDs to MongoDB every hour, but the web app
# reads JDs from pre-exported bucket files (public/job_descriptions/NN.json).
# If the export does not run after each scrape, new jobs appear in the feed with
# no JD in the buckets and resumes fail with "No full JD captured".
#
# Driven by the com.atriveo.job-pipeline LaunchAgent (hourly).

set -uo pipefail

PIPELINE_DIR="${JOB_PIPELINE_DIR:-/Users/atishaykasliwal/job-pipeline}"
APP_DIR="${ATRIVEO_APP_DIR:-/Users/atishaykasliwal/atriveo-app}"
LOG="/tmp/atriveo_pipeline.log"

# Resolve node — Homebrew ARM first, then Intel, then PATH.
NODE_BIN=""
for candidate in /opt/homebrew/bin/node /usr/local/bin/node "$(command -v node 2>/dev/null)"; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    NODE_BIN="$candidate"
    break
  fi
done
[ -n "$NODE_BIN" ] || NODE_BIN="node"

ts() { date "+%Y-%m-%dT%H:%M:%S%z"; }

echo "[$(ts)] === pipeline+export run start ===" >> "$LOG"
echo "[$(ts)] node=$NODE_BIN app=$APP_DIR pipeline=$PIPELINE_DIR" >> "$LOG"

# 1. Scrape
cd "$PIPELINE_DIR" || { echo "[$(ts)] ERROR: cannot cd $PIPELINE_DIR" >> "$LOG"; exit 1; }
GITHUB_TOKEN="${GITHUB_TOKEN:-}" "$PIPELINE_DIR/.venv/bin/python3" -m job_pipeline.main --pipeline all --deploy >> "$LOG" 2>&1
PIPE_STATUS=$?
echo "[$(ts)] scraper exit=$PIPE_STATUS" >> "$LOG"

# 2. JD buckets from MongoDB
cd "$APP_DIR" || { echo "[$(ts)] ERROR: cannot cd $APP_DIR" >> "$LOG"; exit 1; }
"$NODE_BIN" scripts/export-job-descriptions.mjs >> "$LOG" 2>&1
EXPORT_STATUS=$?
echo "[$(ts)] jd:export exit=$EXPORT_STATUS" >> "$LOG"

# 3. Enqueue top jobs for Mongo compile worker
if [ -f "$APP_DIR/.env" ]; then
  "$NODE_BIN" --env-file="$APP_DIR/.env" scripts/resume-enqueue.mjs >> "$LOG" 2>&1
  ENQUEUE_STATUS=$?
  echo "[$(ts)] resume:enqueue exit=$ENQUEUE_STATUS" >> "$LOG"
else
  echo "[$(ts)] ERROR: missing $APP_DIR/.env — resume:enqueue skipped" >> "$LOG"
fi

echo "[$(ts)] === pipeline+export run done ===" >> "$LOG"
