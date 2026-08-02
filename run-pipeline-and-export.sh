#!/bin/bash
# On-demand pipeline run — triggered from the app's "Scrape now" button.
#
# WHY on-demand: this used to fire hourly from a LaunchAgent/cron on the Mac
# mini. Scraping on a timer burns LinkedIn quota overnight, and on a laptop that
# sleeps the schedule fires at random wake times or not at all. The app now
# drives it: the tailor sidecar spawns this script and polls the state file
# below, so a run happens exactly when it is asked for.
#
# The whole chain lives here because the steps are only useful together — a
# scrape with no JD export leaves jobs in the feed whose descriptions fail with
# "No full JD captured", and no feed deploy means the site still shows stale data.
#
#   1. scrape        job_pipeline.main --pipeline all --deploy   → MongoDB
#   2. jd_export     export-job-descriptions.mjs                 → public buckets
#   3. feed_deploy   sync-job-feed.sh                            → Cloudflare Pages
#   4. resume_queue  sync-resume-queue.sh                        → compile queue
#
# Progress is published as JSON to $STATE_FILE after every transition so the
# sidecar can report phase/elapsed/counts without parsing the log.
#
#   ./run-pipeline-and-export.sh [--run-id ID] [--skip-resume] [--skip-deploy]

set -uo pipefail

# Resolve our own directory rather than hardcoding /Users/<name>/job-pipeline —
# this repo now moves between machines (Mac mini → MacBook Air) and the old
# absolute path silently ran whichever checkout happened to live there.
PIPELINE_DIR="${JOB_PIPELINE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
APP_DIR="${ATRIVEO_APP_DIR:-$(cd "$PIPELINE_DIR/../atriveo-app" 2>/dev/null && pwd || echo "$HOME/atriveo-app")}"

LOG="${ATRIVEO_SCRAPE_LOG:-/tmp/atriveo_pipeline.log}"
STATE_FILE="${ATRIVEO_SCRAPE_STATE:-/tmp/atriveo_scrape_state.json}"
LOCK="${ATRIVEO_SCRAPE_LOCK:-/tmp/atriveo_scrape.lock}"

RUN_ID=""
SKIP_RESUME=0
SKIP_DEPLOY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --run-id) RUN_ID="${2:-}"; shift 2 ;;
    --run-id=*) RUN_ID="${1#*=}"; shift ;;
    --skip-resume) SKIP_RESUME=1; shift ;;
    --skip-deploy) SKIP_DEPLOY=1; shift ;;
    *) shift ;;
  esac
done
[ -n "$RUN_ID" ] || RUN_ID="$(date +%Y%m%dT%H%M%S)-$$"

ts() { date "+%Y-%m-%dT%H:%M:%S%z"; }
iso() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[$(ts)] $*" >> "$LOG"; }

STARTED_AT="$(iso)"
HOST="$(scutil --get ComputerName 2>/dev/null || hostname)"

# ─── run state ───────────────────────────────────────────────────────────────
# Written atomically (tmp + mv) because the sidecar polls this file about once a
# second and must never read a half-written object.
PHASE="starting"
STATUS="running"
EXIT_CODE=""
JOBS_BEFORE=""
JOBS_AFTER=""
PHASE_LOG=()

json_escape() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

write_state() {
  local finished_at="${1:-}"
  local tmp="${STATE_FILE}.tmp.$$"
  {
    printf '{'
    printf '"runId":"%s",' "$(json_escape "$RUN_ID")"
    printf '"status":"%s",' "$(json_escape "$STATUS")"
    printf '"phase":"%s",' "$(json_escape "$PHASE")"
    printf '"pid":%s,' "$$"
    printf '"host":"%s",' "$(json_escape "$HOST")"
    printf '"startedAt":"%s",' "$STARTED_AT"
    printf '"updatedAt":"%s",' "$(iso)"
    if [ -n "$finished_at" ]; then printf '"finishedAt":"%s",' "$finished_at"; else printf '"finishedAt":null,'; fi
    if [ -n "$EXIT_CODE" ]; then printf '"exitCode":%s,' "$EXIT_CODE"; else printf '"exitCode":null,'; fi
    if [ -n "$JOBS_BEFORE" ]; then printf '"jobsBefore":%s,' "$JOBS_BEFORE"; else printf '"jobsBefore":null,'; fi
    if [ -n "$JOBS_AFTER" ]; then printf '"jobsAfter":%s,' "$JOBS_AFTER"; else printf '"jobsAfter":null,'; fi
    printf '"log":"%s",' "$(json_escape "$LOG")"
    printf '"phases":['
    local first=1
    for entry in ${PHASE_LOG+"${PHASE_LOG[@]}"}; do
      [ "$first" -eq 1 ] || printf ','
      first=0
      printf '%s' "$entry"
    done
    printf ']}'
  } > "$tmp" 2>/dev/null
  mv -f "$tmp" "$STATE_FILE" 2>/dev/null || rm -f "$tmp"
}

phase_start() {
  PHASE="$1"
  PHASE_LOG+=("{\"name\":\"$(json_escape "$1")\",\"startedAt\":\"$(iso)\",\"status\":\"running\"}")
  log "phase → $1"
  write_state
}

phase_end() {
  local name="$1" status="$2" code="${3:-0}"
  local idx=$(( ${#PHASE_LOG[@]} - 1 ))
  if [ "$idx" -ge 0 ]; then
    local base="${PHASE_LOG[$idx]%\}}"
    base="${base/\"status\":\"running\"/\"status\":\"$status\"}"
    PHASE_LOG[$idx]="${base},\"finishedAt\":\"$(iso)\",\"exitCode\":${code}}"
  fi
  log "phase ✓ $name status=$status exit=$code"
  write_state
}

fail_out() {
  local code="$1" why="$2"
  STATUS="failed"
  EXIT_CODE="$code"
  log "=== run $RUN_ID failed ($why) ==="
  write_state "$(iso)"
  exit "$code"
}

# ─── locking ─────────────────────────────────────────────────────────────────
# A second click while a run is in flight must not start a parallel scrape —
# both would write the same Mongo session and fight over the venv.
if [ -f "$LOCK" ]; then
  OLD_PID="$(cat "$LOCK" 2>/dev/null || true)"
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    log "run $RUN_ID rejected — pid $OLD_PID still running"
    exit 75  # EX_TEMPFAIL — the sidecar maps this to HTTP 409
  fi
  rm -f "$LOCK"
fi
echo $$ > "$LOCK"

# Cancel arrives as SIGTERM to the process group. Record it as cancelled rather
# than failed so the UI can tell "you stopped it" from "it broke".
on_term() {
  STATUS="cancelled"
  log "run $RUN_ID cancelled (signal)"
  phase_end "$PHASE" "cancelled" 130
  write_state "$(iso)"
  rm -f "$LOCK"
  exit 130
}
trap on_term INT TERM
trap 'rm -f "$LOCK"' EXIT

# ─── toolchain resolution ────────────────────────────────────────────────────
NODE_BIN=""
for candidate in /opt/homebrew/bin/node /usr/local/bin/node "$(command -v node 2>/dev/null)"; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then NODE_BIN="$candidate"; break; fi
done
[ -n "$NODE_BIN" ] || NODE_BIN="node"

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

# A Homebrew python upgrade can delete the interpreter the venv was built
# against, which breaks every run with "bad interpreter". Self-heal instead.
ensure_venv() {
  local venv="$PIPELINE_DIR/.venv"
  local py="$venv/bin/python3"
  if [ -x "$py" ] && "$py" -c "import sys" >/dev/null 2>&1; then return 0; fi
  log "WARN: venv missing/dead — rebuilding"
  local base_py; base_py="$(resolve_base_python)"
  [ -d "$venv" ] && { mv "$venv" "${venv}.broken-$(date +%Y%m%d-%H%M%S)" 2>>"$LOG" || rm -rf "$venv"; }
  "$base_py" -m venv "$venv" >> "$LOG" 2>&1 || { log "ERROR: venv rebuild failed (base=$base_py)"; return 1; }
  "$py" -m pip install -q --upgrade pip >> "$LOG" 2>&1 || true
  "$py" -m pip install -q -r "$PIPELINE_DIR/requirements.txt" >> "$LOG" 2>&1 || true
  log "venv rebuilt (base=$base_py)"
}

# today_count from the dashboard metadata — used to report "N new" in the UI.
read_job_count() {
  local meta="$PIPELINE_DIR/docs/metadata.json"
  local py="$PIPELINE_DIR/.venv/bin/python3"
  [ -f "$meta" ] && [ -x "$py" ] || return 0
  "$py" - "$meta" <<'PY' 2>/dev/null
import json, sys
try:
    with open(sys.argv[1]) as fh:
        print(int(json.load(fh).get("today_count") or 0))
except Exception:
    pass
PY
}

# ─── run ─────────────────────────────────────────────────────────────────────
log "=== on-demand run start · id=$RUN_ID host=$HOST ==="
log "node=$NODE_BIN pipeline=$PIPELINE_DIR app=$APP_DIR"
write_state

cd "$PIPELINE_DIR" || fail_out 1 "cannot cd $PIPELINE_DIR"

# 1. Scrape → MongoDB (+ GitHub Pages deploy)
phase_start "scrape"
ensure_venv
if ! "$PIPELINE_DIR/.venv/bin/python3" -c "import pandas" 2>/dev/null; then
  log "WARN: pandas missing — pip install"
  "$PIPELINE_DIR/.venv/bin/python3" -m pip install -q -r requirements.txt >> "$LOG" 2>&1 || true
fi

JOBS_BEFORE="$(read_job_count)"

GITHUB_TOKEN="${GITHUB_TOKEN:-}" "$PIPELINE_DIR/.venv/bin/python3" -m job_pipeline.main --pipeline all --deploy 2>&1 \
  | grep -v ": No such file or directory" >> "$LOG"
SCRAPE_STATUS="${PIPESTATUS[0]}"
phase_end "scrape" "$([ "$SCRAPE_STATUS" -eq 0 ] && echo ok || echo failed)" "$SCRAPE_STATUS"
[ "$SCRAPE_STATUS" -eq 0 ] || fail_out "$SCRAPE_STATUS" "scrape"

# 2. JD buckets from MongoDB — must follow the scrape or new jobs have no JD.
phase_start "jd_export"
cd "$APP_DIR" || { phase_end "jd_export" "failed" 1; fail_out 1 "cannot cd $APP_DIR"; }
"$NODE_BIN" scripts/export-job-descriptions.mjs >> "$LOG" 2>&1
EXPORT_STATUS=$?
phase_end "jd_export" "$([ "$EXPORT_STATUS" -eq 0 ] && echo ok || echo failed)" "$EXPORT_STATUS"

# 3. Feed → Cloudflare Pages. Inline now: with no :20 timer, skipping this would
#    leave the site stale after a click, which is the whole point of the button.
if [ "$SKIP_DEPLOY" -eq 1 ]; then
  log "feed_deploy skipped (--skip-deploy)"
else
  phase_start "feed_deploy"
  JOB_PIPELINE_DIR="$PIPELINE_DIR" ATRIVEO_APP_DIR="$APP_DIR" \
    /bin/bash "$APP_DIR/scripts/sync-job-feed.sh" >> "$LOG" 2>&1
  DEPLOY_STATUS=$?
  phase_end "feed_deploy" "$([ "$DEPLOY_STATUS" -eq 0 ] && echo ok || echo failed)" "$DEPLOY_STATUS"
fi

# 4. Resume queue — non-fatal; the compile worker drains it independently.
if [ "$SKIP_RESUME" -eq 1 ]; then
  log "resume_queue skipped (--skip-resume)"
else
  phase_start "resume_queue"
  JOB_PIPELINE_DIR="$PIPELINE_DIR" ATRIVEO_APP_DIR="$APP_DIR" \
    /bin/bash "$APP_DIR/scripts/sync-resume-queue.sh" >> "$LOG" 2>&1
  RESUME_STATUS=$?
  phase_end "resume_queue" "$([ "$RESUME_STATUS" -eq 0 ] && echo ok || echo failed)" "$RESUME_STATUS"
fi

JOBS_AFTER="$(read_job_count)"
PHASE="done"
STATUS="done"
EXIT_CODE=0
log "=== run $RUN_ID done · jobs ${JOBS_BEFORE:-?} → ${JOBS_AFTER:-?} ==="
write_state "$(iso)"
exit 0
