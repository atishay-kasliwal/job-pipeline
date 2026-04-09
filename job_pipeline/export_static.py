"""
Static export: reads the latest session from MongoDB and writes JSON files
to docs/ so GitHub Pages can serve them without a backend.

Called by the GitHub Actions workflow every hour (30 min after the scraper).

Output files
------------
docs/jobs.json           — latest standard pipeline results
docs/important_jobs.json — latest important pipeline results
docs/metadata.json       — last_updated timestamp + counts
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialise(obj: Any) -> Any:
    """Make a MongoDB document JSON-safe (convert datetime → ISO string)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialise(i) for i in obj]
    return obj


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Wrote %s", path)


# ── Export logic ──────────────────────────────────────────────────────────────

def export_pipeline(pipeline: str) -> list[dict]:
    """
    Fetch the most recent non-archived session for *pipeline* and return
    its job documents as a list of plain dicts (MongoDB _id stripped).
    """
    from job_pipeline.storage import get_db
    db = get_db()

    session = db["sessions"].find_one(
        {"pipeline": pipeline, "archived": False},
        sort=[("run_at", -1)],
    )

    if not session:
        logger.warning("No active session found for pipeline '%s'.", pipeline)
        return []

    sid = session["session_id"]
    jobs = list(
        db["jobs"].find(
            {"session_id": sid},
            {"_id": 0, "run_at": 0},          # drop internal fields
        )
    )

    jobs = [_serialise(j) for j in jobs]
    logger.info(
        "Exported %d jobs from session '%s' (pipeline=%s).",
        len(jobs), sid, pipeline,
    )
    return jobs


def export_today_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from today (UTC) across all sessions.
    """
    from job_pipeline.storage import get_db
    from datetime import date
    db = get_db()

    today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)

    # Get all session_ids from today
    sessions = db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": today_start}},
        {"session_id": 1},
    )
    sids = [s["session_id"] for s in sessions]

    if not sids:
        return []

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))
    # Deduplicate by job_url
    seen, unique = set(), []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return [_serialise(j) for j in unique]


def export_yesterday_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from yesterday (UTC) across all sessions.
    """
    from job_pipeline.storage import get_db
    from datetime import date, timedelta
    db = get_db()

    yesterday = date.today() - timedelta(days=1)
    day_start = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=timezone.utc)
    day_end   = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)

    sessions = db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": day_start, "$lt": day_end}},
        {"session_id": 1},
    )
    sids = [s["session_id"] for s in sessions]

    if not sids:
        return []

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))
    seen, unique = set(), []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return [_serialise(j) for j in unique]


def export_run_history() -> list[dict]:
    """Fetch recent run history from MongoDB sessions in the format the frontend expects."""
    from job_pipeline.storage import get_db
    db = get_db()

    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False},
        {"_id": 0, "session_id": 1, "run_at": 1, "total_jobs": 1, "snapshot_file": 1},
        sort=[("run_at", -1)],
        limit=48,
    ))
    return [_serialise(s) for s in sessions]


def run_export() -> None:
    """
    Export both pipelines to docs/ and write a metadata file.
    Creates docs/ if it does not exist.
    """
    DOCS_DIR.mkdir(exist_ok=True)

    standard_jobs   = export_pipeline("standard")
    important_jobs  = export_pipeline("important")
    today_jobs      = export_today_jobs()
    yesterday_jobs  = export_yesterday_jobs()
    run_history     = export_run_history()

    metadata = {
        "last_updated":    datetime.now(tz=timezone.utc).isoformat(),
        "standard_count":  len(standard_jobs),
        "important_count": len(important_jobs),
        "today_count":     len(today_jobs),
    }

    _write_json(DOCS_DIR / "jobs.json",           standard_jobs)
    _write_json(DOCS_DIR / "important_jobs.json", important_jobs)
    _write_json(DOCS_DIR / "today_jobs.json",     today_jobs)
    _write_json(DOCS_DIR / "yesterday_jobs.json", yesterday_jobs)
    _write_json(DOCS_DIR / "run_history.json",    run_history)
    _write_json(DOCS_DIR / "metadata.json",       metadata)

    logger.info(
        "Export complete — %d standard, %d important jobs.",
        len(standard_jobs), len(important_jobs),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    run_export()
