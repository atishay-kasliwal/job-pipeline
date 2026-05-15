"""
MongoDB storage layer for the job pipeline.

Collections
-----------
run_stats     : one summary document per pipeline run — easy to browse in Atlas
jobs          : active job documents — the last ARCHIVE_RETENTION_DAYS days
sessions      : lightweight run metadata (used by the archiver)
archived_jobs : jobs moved out of the active collection by the archiver

``run_stats`` is the main "table" for reviewing hourly runs at a glance.
Each document contains counts, level breakdown, top companies, and score
distribution so you can open Atlas and immediately see what each run found.

All writes are idempotent on job_url so re-running a scrape does not
produce duplicate documents in MongoDB.
"""
from __future__ import annotations
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from job_pipeline.config import MONGO_DB_NAME, MONGO_URI
from job_pipeline.identity import job_identity_key

logger = logging.getLogger(__name__)

# Module-level singleton — reuse the same TCP connection pool across calls.
_client: MongoClient | None = None


# ── Connection ────────────────────────────────────────────────────────────────

def get_client() -> MongoClient:
    """Return (and lazily create) the shared MongoClient."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        logger.debug("MongoDB client initialised.")
    return _client


def get_db() -> Database:
    """Return the job_pipeline database handle."""
    return get_client()[MONGO_DB_NAME]


def _col(name: str) -> Collection:
    return get_db()[name]


# ── Write ─────────────────────────────────────────────────────────────────────

def insert_run(
    df: pd.DataFrame,
    pipeline: str,
    session_id: str | None = None,
) -> str:
    """
    Persist one complete pipeline run to MongoDB.

    Creates:
    - One document in ``sessions`` describing the run.
    - N documents in ``jobs``, one per row, tagged with ``session_id``.

    Jobs are upserted on ``job_url`` so the same posting scraped in two
    consecutive hourly runs is stored only once per session (the session_id
    differentiates runs, not the job itself).

    Args:
        df:          Scored/filtered DataFrame to persist.
        pipeline:    ``"standard"`` or ``"important"``.
        session_id:  ISO-8601 string key for this run.  Auto-generated if None.

    Returns:
        The session_id used (useful for logging / testing).
    """
    if df.empty:
        logger.info("Empty DataFrame — nothing to store in MongoDB.")
        return ""

    now = datetime.now(tz=timezone.utc)
    sid = session_id or now.strftime("%Y-%m-%dT%H:%M:%SZ")

    records = _df_to_records(df, sid, pipeline, now)

    # Upsert each job on job_url to avoid exact duplicates within a session
    ops = [
        UpdateOne(
            {"session_id": sid, "job_url": r.get("job_url")},
            {"$setOnInsert": r},
            upsert=True,
        )
        for r in records
    ]

    try:
        result = _col("jobs").bulk_write(ops, ordered=False)
        inserted = result.upserted_count
    except BulkWriteError as exc:
        inserted = exc.details.get("nUpserted", 0)
        logger.warning("Bulk write partial error (duplicates skipped): %s", exc.details)

    # Session metadata
    _col("sessions").update_one(
        {"session_id": sid},
        {
            "$set": {
                "session_id": sid,
                "run_at": now,
                "pipeline": pipeline,
                "job_count": len(records),
                "archived": False,
            }
        },
        upsert=True,
    )

    logger.info(
        "MongoDB: %d jobs stored (session='%s', pipeline='%s', new_inserts=%d).",
        len(records), sid, pipeline, inserted,
    )
    return sid


def _build_run_summary(
    df: pd.DataFrame,
    pipeline: str,
    session_id: str,
    now: datetime,
) -> dict[str, Any]:
    """
    Build a compact summary document for one pipeline run.

    This document goes into the ``run_stats`` collection and is designed to be
    immediately human-readable when browsing MongoDB Atlas.
    """
    total = len(df)

    # Level breakdown
    level_counts: dict[str, int] = {}
    if "level" in df.columns:
        for lvl, cnt in df["level"].value_counts().items():
            level_counts[str(lvl)] = int(cnt)

    # Score stats (standard pipeline)
    score_stats: dict[str, Any] = {}
    score_col = "score" if "score" in df.columns else (
        "priority_score" if "priority_score" in df.columns else None
    )
    if score_col and total:
        scores = df[score_col].dropna()
        score_stats = {
            "avg":  round(float(scores.mean()), 1),
            "max":  int(scores.max()),
            "high": int((scores >= 5).sum()),   # high-quality jobs
        }

    # Top 5 companies by posting count
    top_companies: list[str] = []
    if "company" in df.columns:
        top_companies = [
            c for c, _ in Counter(df["company"].dropna().tolist()).most_common(5)
        ]

    return {
        "session_id":    session_id,
        "run_at":        now,
        "pipeline":      pipeline,
        "total_jobs":    total,
        "levels":        level_counts,
        "scores":        score_stats,
        "top_companies": top_companies,
    }


def insert_run_stats(
    df: pd.DataFrame,
    pipeline: str,
    session_id: str | None = None,
) -> None:
    """
    Write a human-readable summary of this run to the ``run_stats`` collection.

    One document per (session_id, pipeline) pair.  Safe to call multiple times —
    subsequent calls overwrite the existing document for that session.
    """
    if df.empty:
        return

    now = datetime.now(tz=timezone.utc)
    sid = session_id or now.strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = _build_run_summary(df, pipeline, sid, now)

    _col("run_stats").update_one(
        {"session_id": sid, "pipeline": pipeline},
        {"$set": doc},
        upsert=True,
    )
    logger.info("run_stats: stored summary for session='%s' pipeline='%s'.", sid, pipeline)


def update_daily_jobs(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Accumulate today's standard-pipeline jobs in ``today_jobs.json``.

    At midnight, today's file is rotated to ``yesterday_jobs.json`` so the
    dashboard can show both "Today" and "Yesterday" views without re-fetching
    dozens of snapshot files.

    Only call this from the standard pipeline to avoid duplicates.
    """
    today_str   = datetime.now().strftime("%Y-%m-%d")
    today_path  = output_dir / "today_jobs.json"
    yest_path   = output_dir / "yesterday_jobs.json"
    meta_path   = output_dir / "daily_meta.json"

    # Detect midnight rollover
    meta: dict = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except Exception:
            pass

    if meta.get("date") != today_str:
        # Rotate today → yesterday
        if today_path.exists():
            import shutil
            shutil.copy(today_path, yest_path)
            logger.info("Daily rotation: today_jobs.json → yesterday_jobs.json")
        today_path.write_text("[]")
        meta = {"date": today_str}

    # Load existing today jobs
    existing: list[dict] = []
    if today_path.exists():
        try:
            existing = json.loads(today_path.read_text())
        except Exception:
            existing = []

    # Merge new jobs (deduplicate by canonical identity key)
    seen: set[str] = set()
    for j in existing:
        seen.add(job_identity_key(j))

    new_records: list[dict] = []
    if not df.empty:
        new_records = json.loads(
            df.to_json(orient="records", date_format="iso", default_handler=str)
        )
    batch_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    added = 0
    for job in new_records:
        key = job_identity_key(job)
        if key not in seen:
            job["batch_time"] = batch_ts
            existing.append(job)
            seen.add(key)
            added += 1

    # Sort by score descending
    existing.sort(
        key=lambda j: j.get("score") or j.get("priority_score") or 0,
        reverse=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    today_path.write_text(json.dumps(existing, indent=2, default=str))
    meta_path.write_text(json.dumps(meta))
    logger.info("today_jobs.json: +%d new jobs (%d total).", added, len(existing))


def save_descriptions(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Append/update job_url → description mapping to descriptions.json.

    Only adds entries not already present so the file grows incrementally.
    Used later by the ATS resume analyzer to avoid re-scraping.
    """
    if "description" not in df.columns:
        return

    desc_path = output_dir / "descriptions.json"

    existing: dict[str, str] = {}
    if desc_path.exists():
        try:
            existing = json.loads(desc_path.read_text())
        except Exception:
            pass

    added = 0
    for _, row in df.iterrows():
        url = row.get("job_url")
        desc = row.get("description")
        if url and desc and not pd.isna(desc) and url not in existing:
            existing[url] = str(desc)
            added += 1

    if added:
        output_dir.mkdir(parents=True, exist_ok=True)
        desc_path.write_text(json.dumps(existing, indent=2))
        logger.info("descriptions.json: +%d new descriptions (%d total).", added, len(existing))


def save_resume(text: str) -> None:
    """Upsert the user's resume text into MongoDB (single document)."""
    _col("resume").replace_one(
        {},
        {"text": text, "updated_at": datetime.now(tz=timezone.utc)},
        upsert=True,
    )
    logger.info("Resume saved to MongoDB (%d chars).", len(text))


def get_resume() -> str | None:
    """Return the stored resume text, or None if not set."""
    doc = _col("resume").find_one({}, {"_id": 0, "text": 1})
    return doc.get("text") if doc else None


def upsert_descriptions(df: pd.DataFrame) -> int:
    """
    Upsert job_url → description pairs to the MongoDB ``descriptions`` collection.

    Uses $setOnInsert so existing descriptions are never overwritten.
    Returns the number of new documents inserted.
    """
    if "description" not in df.columns:
        return 0

    ops = []
    for _, row in df.iterrows():
        url = row.get("job_url")
        desc = row.get("description")
        if url and desc and not pd.isna(desc):
            ops.append(
                UpdateOne(
                    {"job_url": url},
                    {"$setOnInsert": {"job_url": url, "description": str(desc)}},
                    upsert=True,
                )
            )

    if not ops:
        return 0

    try:
        result = _col("descriptions").bulk_write(ops, ordered=False)
        inserted = result.upserted_count
    except BulkWriteError as exc:
        inserted = exc.details.get("nUpserted", 0)

    logger.info("descriptions (MongoDB): +%d new entries (%d submitted).", inserted, len(ops))
    return inserted


def _snapshot_filename(session_id: str, pipeline: str) -> str:
    """Return a filesystem-safe snapshot filename for a run."""
    safe_sid = session_id.replace(":", "-")
    return f"{safe_sid}_{pipeline}.json"


def save_run_snapshot(
    df: pd.DataFrame,
    pipeline: str,
    session_id: str,
    runs_dir: Path,
) -> str:
    """
    Save the jobs for this run as a JSON file in ``runs_dir``.

    Returns the filename (not full path) so it can be stored in run_history.json.
    """
    filename = _snapshot_filename(session_id, pipeline)
    runs_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = runs_dir / filename
    records = json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))
    snapshot_path.write_text(json.dumps(records, indent=2, default=str))
    logger.info("Snapshot saved → %s (%d jobs).", filename, len(records))
    return filename


def append_run_history(
    df: pd.DataFrame,
    pipeline: str,
    history_path: Path,
    max_runs: int = 720,  # 30 days × ~24 runs/day
) -> None:
    """
    Append a compact run summary to ``run_history.json`` (max_runs entries).

    Also saves a per-run job snapshot to ``output/runs/`` so the dashboard
    can fetch and display any previous hour's listings on demand.
    """
    now = datetime.now(tz=timezone.utc)
    sid = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = _build_run_summary(df, pipeline, sid, now)
    # Serialise datetime for JSON
    summary["run_at"] = now.isoformat()

    # Save per-run snapshot and record its filename
    runs_dir = history_path.parent / "runs"
    try:
        summary["snapshot_file"] = save_run_snapshot(df, pipeline, sid, runs_dir)
    except Exception as exc:
        logger.warning("Could not save run snapshot (non-fatal): %s", exc)

    history: list[dict] = []
    if history_path.exists():
        try:
            history = json.loads(history_path.read_text())
        except Exception:
            history = []

    history.append(summary)
    if len(history) > max_runs:
        history = history[-max_runs:]

    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(json.dumps(history, indent=2, default=str))
    logger.info("run_history.json: appended run %s (%s).", sid, pipeline)


def _df_to_records(
    df: pd.DataFrame,
    session_id: str,
    pipeline: str,
    run_at: datetime,
) -> list[dict[str, Any]]:
    """Convert a DataFrame to a list of MongoDB-safe dicts."""
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        doc: dict[str, Any] = {
            "session_id": session_id,
            "pipeline": pipeline,
            "run_at": run_at,
        }
        for col in df.columns:
            val = row[col]
            if isinstance(val, pd.Timestamp):
                val = val.to_pydatetime()
            elif hasattr(val, "item"):          # numpy scalar → Python native
                val = val.item()
            elif val is not None and pd.isna(val):
                val = None
            doc[col] = val
        records.append(doc)
    return records


# ── Read (used by archiver) ───────────────────────────────────────────────────

def get_sessions_to_archive(retention_days: int) -> list[dict]:
    """
    Return session documents that are older than ``retention_days`` and
    have not yet been archived.
    """
    from datetime import timedelta
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=retention_days)
    sessions = list(
        _col("sessions").find(
            {"run_at": {"$lt": cutoff}, "archived": False},
            sort=[("run_at", 1)],
        )
    )
    logger.info(
        "Found %d session(s) eligible for archival (older than %d days).",
        len(sessions), retention_days,
    )
    return sessions


def get_jobs_for_session(session_id: str) -> list[dict]:
    """Fetch all job documents belonging to a session."""
    return list(_col("jobs").find({"session_id": session_id}))


# ── Archive (used by archiver) ────────────────────────────────────────────────

def move_session_to_archive(session_id: str, archived_at: datetime) -> int:
    """
    Move all jobs for ``session_id`` from ``jobs`` → ``archived_jobs``
    and mark the session document as archived.

    Returns:
        Number of job documents moved.
    """
    jobs = get_jobs_for_session(session_id)
    if not jobs:
        logger.warning("No jobs found for session '%s' — skipping move.", session_id)
        return 0

    for doc in jobs:
        doc["archived_at"] = archived_at

    _col("archived_jobs").insert_many(jobs)
    _col("jobs").delete_many({"session_id": session_id})
    _col("sessions").update_one(
        {"session_id": session_id},
        {"$set": {"archived": True, "archived_at": archived_at}},
    )

    logger.info(
        "Archived %d jobs from session '%s' → archived_jobs collection.",
        len(jobs), session_id,
    )
    return len(jobs)
