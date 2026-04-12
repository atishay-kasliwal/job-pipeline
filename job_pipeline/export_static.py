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
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
DEFAULT_DASHBOARD_TZ = "America/New_York"

_tz_name = os.getenv("DASHBOARD_TZ", DEFAULT_DASHBOARD_TZ).strip() or DEFAULT_DASHBOARD_TZ
try:
    DASHBOARD_TZ = ZoneInfo(_tz_name)
except Exception:  # pragma: no cover - fallback only if host tz data is missing
    logger.warning("Invalid DASHBOARD_TZ='%s'; falling back to UTC.", _tz_name)
    DASHBOARD_TZ = timezone.utc
    _tz_name = "UTC"


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


def _local_day_bounds_utc(days_ago: int) -> tuple[datetime, datetime]:
    """
    Return [start_utc, end_utc) for a local calendar day in ``DASHBOARD_TZ``.
    """
    now_local = datetime.now(tz=DASHBOARD_TZ)
    day_local = now_local.date() - timedelta(days=days_ago)
    start_local = datetime.combine(day_local, datetime.min.time(), tzinfo=DASHBOARD_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


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
    Fetch all standard-pipeline jobs from today in ``DASHBOARD_TZ`` across all sessions.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    day_start, day_end = _local_day_bounds_utc(days_ago=0)

    # Get all session_ids from today
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
    Fetch all standard-pipeline jobs from yesterday in ``DASHBOARD_TZ`` across all sessions.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    day_start, day_end = _local_day_bounds_utc(days_ago=1)

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


def export_week_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from the last 7 days across all sessions.
    Deduplicates by job_url (keeps first/oldest occurrence).
    Adds a ``scraped_date`` field (YYYY-MM-DD in ``DASHBOARD_TZ``) so the
    frontend can filter by day.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)

    # Oldest-first so dedup keeps the earliest occurrence of each job
    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": cutoff}},
        {"session_id": 1, "run_at": 1},
        sort=[("run_at", 1)],
    ))

    if not sessions:
        return []

    sid_to_date: dict[str, str] = {}
    for s in sessions:
        run_at = s["run_at"]
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=timezone.utc)
        sid_to_date[s["session_id"]] = run_at.astimezone(DASHBOARD_TZ).strftime("%Y-%m-%d")

    sid_order = {s["session_id"]: i for i, s in enumerate(sessions)}
    sids = list(sid_order.keys())

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))

    # Sort by session chronological order so dedup picks the oldest session
    jobs.sort(key=lambda j: sid_order.get(j.get("session_id", ""), 999_999))

    seen: set[str] = set()
    unique: list[dict] = []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            j["scraped_date"] = sid_to_date.get(j.get("session_id", ""), "")
            unique.append(j)

    # Newest date first, then highest score
    unique.sort(
        key=lambda j: (j.get("scraped_date", ""), j.get("score") or 0),
        reverse=True,
    )

    logger.info("Exported %d unique jobs for weekly view (last 7 days).", len(unique))
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
    week_jobs       = export_week_jobs()
    run_history     = export_run_history()

    metadata = {
        "last_updated":    datetime.now(tz=timezone.utc).isoformat(),
        "standard_count":  len(standard_jobs),
        "important_count": len(important_jobs),
        "today_count":     len(today_jobs),
        "week_count":      len(week_jobs),
        "dashboard_tz":    _tz_name,
    }

    _write_json(DOCS_DIR / "jobs.json",           standard_jobs)
    _write_json(DOCS_DIR / "important_jobs.json", important_jobs)
    _write_json(DOCS_DIR / "today_jobs.json",     today_jobs)
    _write_json(DOCS_DIR / "yesterday_jobs.json", yesterday_jobs)
    _write_json(DOCS_DIR / "week_jobs.json",      week_jobs)
    _write_json(DOCS_DIR / "run_history.json",    run_history)
    _write_json(DOCS_DIR / "metadata.json",       metadata)

    logger.info(
        "Export complete — %d standard, %d important, %d weekly jobs.",
        len(standard_jobs), len(important_jobs), len(week_jobs),
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
