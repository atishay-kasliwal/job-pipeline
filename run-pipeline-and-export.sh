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

# Pick a stable system python to (re)build the venv with.
resolve_base_python() {
  for candidate in \
    /opt/homebrew/bin/python3.12 /usr/local/bin/python3.12 \
    /opt/homebrew/bin/python3.11 /usr/local/bin/python3.11 \
    /opt/homebrew/bin/python3 /usr/local/bin/python3 \
    "$(command -v python3 2>/dev/null)"; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then echo "$candidate"; return 0; fi
  done
  echo "python3"
}

# Rebuild the venv if its interpreter is missing or dangling (a Homebrew python
# upgrade can delete the interpreter the venv was built against, which silently
# breaks every hourly run with "bad interpreter"). Self-heals instead of failing.
ensure_venv() {
  local venv="$PIPELINE_DIR/.venv"
  local py="$venv/bin/python3"
  if [ -x "$py" ] && "$py" -c "import sys" >/dev/null 2>&1; then return 0; fi
  echo "[$(ts)] WARN: venv missing/dead — rebuilding" >> "$LOG"
  local base_py; base_py="$(resolve_base_python)"
  [ -d "$venv" ] && { mv "$venv" "${venv}.broken-$(date +%Y%m%d-%H%M%S)" 2>>"$LOG" || rm -rf "$venv"; }
  "$base_py" -m venv "$venv" >> "$LOG" 2>&1 || { echo "[$(ts)] ERROR: venv rebuild failed (base=$base_py)" >> "$LOG"; return 1; }
  "$py" -m pip install -q --upgrade pip >> "$LOG" 2>&1 || true
  "$py" -m pip install -q -r "$PIPELINE_DIR/requirements.txt" >> "$LOG" 2>&1 || true
  echo "[$(ts)] venv rebuilt (base=$base_py)" >> "$LOG"
}

echo "[$(ts)] === pipeline+export run start ===" >> "$LOG"
echo "[$(ts)] node=$NODE_BIN app=$APP_DIR pipeline=$PIPELINE_DIR" >> "$LOG"

# 1. Scrape
cd "$PIPELINE_DIR" || { echo "[$(ts)] ERROR: cannot cd $PIPELINE_DIR" >> "$LOG"; exit 1; }

ensure_venv
if ! "$PIPELINE_DIR/.venv/bin/python3" -c "import pandas" 2>/dev/null; then
  echo "[$(ts)] WARN: pandas missing — pip install" >> "$LOG"
  "$PIPELINE_DIR/.venv/bin/python3" -m pip install -q -r requirements.txt >> "$LOG" 2>&1 || true
fi

GITHUB_TOKEN="${GITHUB_TOKEN:-}" "$PIPELINE_DIR/.venv/bin/python3" -m job_pipeline.main --pipeline all --deploy 2>&1 \
  | grep -v ": No such file or directory" >> "$LOG"
PIPE_STATUS="${PIPESTATUS[0]}"
echo "[$(ts)] scraper exit=$PIPE_STATUS" >> "$LOG"

# 2. JD buckets from MongoDB
cd "$APP_DIR" || { echo "[$(ts)] ERROR: cannot cd $APP_DIR" >> "$LOG"; exit 1; }
"$NODE_BIN" scripts/export-job-descriptions.mjs >> "$LOG" 2>&1
EXPORT_STATUS=$?
echo "[$(ts)] jd:export exit=$EXPORT_STATUS" >> "$LOG"

# 3. Feed deploy runs on its own schedule (:20 LaunchAgent) — avoid duplicate export here.
echo "[$(ts)] feed-sync deferred to com.atriveo.feed-sync (:20)" >> "$LOG"

echo "[$(ts)] === pipeline+export run done ===" >> "$LOG"
