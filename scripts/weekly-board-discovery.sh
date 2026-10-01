#!/bin/bash
# Weekly: find new Greenhouse / Lever / Ashby boards (Common Crawl + apply links already in jobs)
# and register them in Mongo ats_boards. The hourly scraper then polls them; a new board's first
# poll reads its last 30 days, so its open roles come in too.
# Installed as a launchd job by scripts/install-board-discovery.sh (Sundays 6:00).
set -uo pipefail
cd "$(dirname "$0")/.."
LOG="${BOARD_DISCOVERY_LOG:-$HOME/Library/Logs/atriveo-board-discovery.log}"
ts() { date "+%Y-%m-%dT%H:%M:%S%z"; }
echo "[$(ts)] === board discovery ===" >> "$LOG"
for ats in greenhouse lever ashby; do
  PYTHONPATH=. .venv/bin/python -m job_pipeline.sources.discover_boards --ats "$ats" --commoncrawl 5 --from-jobs --write >> "$LOG" 2>&1
  echo "[$(ts)] $ats exit=$?" >> "$LOG"
done
echo "[$(ts)] === done ===" >> "$LOG"
