#!/bin/bash
# Runs the scraper, then immediately refreshes the app's JD buckets.
#
# WHY: the scraper adds new jobs + JDs to MongoDB every hour, but the web app
# reads JDs from pre-exported bucket files (public/job_descriptions/NN.json).
# If the export does not run after each scrape, new jobs appear in the feed with
# no JD in the buckets and resumes fail with "No full JD captured" / "Only a
# short job snippet is available". This wrapper keeps the two in lockstep.
#
# Driven by the com.atriveo.job-pipeline LaunchAgent (hourly).

set -uo pipefail

PIPELINE_DIR="/Users/atishaykasliwal/job-pipeline"
APP_DIR="/Users/atishaykasliwal/atriveo-app"
LOG="/tmp/atriveo_pipeline.log"
# LaunchAgents run with a minimal PATH that omits /usr/local/bin, so resolve node
# to an absolute path. Falls back to PATH lookup for interactive runs.
NODE_BIN="/usr/local/bin/node"
[ -x "$NODE_BIN" ] || NODE_BIN="$(command -v node 2>/dev/null || echo node)"

ts() { date "+%Y-%m-%dT%H:%M:%S%z"; }

echo "[$(ts)] === pipeline+export run start ===" >> "$LOG"

# 1. Scrape (same command the plist used before)
cd "$PIPELINE_DIR" || { echo "[$(ts)] ERROR: cannot cd $PIPELINE_DIR" >> "$LOG"; exit 1; }
GITHUB_TOKEN="${GITHUB_TOKEN:-}" "$PIPELINE_DIR/.venv/bin/python3" -m job_pipeline.main --pipeline all --deploy >> "$LOG" 2>&1
PIPE_STATUS=$?
echo "[$(ts)] scraper exit=$PIPE_STATUS" >> "$LOG"

# 2. Refresh JD buckets from MongoDB (always attempt, even if scrape had warnings,
#    so an existing feed still gets fresh JDs).
cd "$APP_DIR" || { echo "[$(ts)] ERROR: cannot cd $APP_DIR" >> "$LOG"; exit 1; }
if [ -x "$NODE_BIN" ] || command -v "$NODE_BIN" >/dev/null 2>&1; then
  "$NODE_BIN" scripts/export-job-descriptions.mjs >> "$LOG" 2>&1
  EXPORT_STATUS=$?
  echo "[$(ts)] jd:export exit=$EXPORT_STATUS" >> "$LOG"
else
  echo "[$(ts)] ERROR: node not found ($NODE_BIN) — JD export skipped" >> "$LOG"
fi

echo "[$(ts)] === pipeline+export run done ===" >> "$LOG"
