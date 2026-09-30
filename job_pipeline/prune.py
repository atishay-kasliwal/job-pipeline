"""
Keep MongoDB under the Atlas free-tier quota by deleting old scrape history.

Every run adds ~150 job rows and ~55 full descriptions (~0.4 MB), and nothing
ever removed them, so the 512 MB cluster filled and blocked every write from
2026-09-05 until it was emptied by hand on 2026-09-29. Run before each scrape so
a nearly full cluster cannot block that run's own inserts (deletes are still
accepted over quota).

Kept regardless of age:
  - manual builds (resume.source == "manual"): Create-tab resumes are meant
    to persist, and their queue rows are how the dock lists them;
  - rows mid-build (resume.status queued/running), so the worker's lease
    target never disappears under it.

    python -m job_pipeline.prune [--days N] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

from job_pipeline.storage import get_db

logger = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = int(os.getenv("PRUNE_RETENTION_DAYS", "30"))
BATCH = 1000


def prune(retention_days: int = DEFAULT_RETENTION_DAYS, dry_run: bool = False) -> dict:
    db = get_db()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=retention_days)

    old_jobs = {
        "run_at": {"$lt": cutoff},
        "resume.source": {"$ne": "manual"},
        "resume.status": {"$nin": ["queued", "running"]},
    }
    if dry_run:
        jobs_deleted = db["jobs"].count_documents(old_jobs)
    else:
        jobs_deleted = db["jobs"].delete_many(old_jobs).deleted_count

    # Descriptions carry no timestamp; one is stale once no kept job row points
    # at it. $nor selects the kept rows in a dry run and after a real delete alike.
    live = set(db["jobs"].distinct("job_url", {"$nor": [old_jobs]}))
    stale = [
        d["_id"]
        for d in db["descriptions"].find({}, {"job_url": 1})
        if d.get("job_url") not in live
    ]
    descriptions_deleted = 0
    for i in range(0, len(stale), BATCH):
        chunk = stale[i:i + BATCH]
        if dry_run:
            descriptions_deleted += len(chunk)
        else:
            descriptions_deleted += db["descriptions"].delete_many({"_id": {"$in": chunk}}).deleted_count

    stats = db.command("dbStats")
    used_mb = (stats.get("dataSize", 0) + stats.get("indexSize", 0)) / 1_048_576
    summary = {
        "retention_days": retention_days,
        "jobs_deleted": jobs_deleted,
        "descriptions_deleted": descriptions_deleted,
        "used_mb": round(used_mb, 1),
        "dry_run": dry_run,
    }
    logger.info(
        "prune%s: -%d jobs, -%d descriptions older than %d days · %.1f MB used",
        " (dry run)" if dry_run else "", jobs_deleted, descriptions_deleted, retention_days, used_mb,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s", datefmt="%H:%M:%S")
    prune(args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
