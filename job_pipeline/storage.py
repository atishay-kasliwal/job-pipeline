"""
MongoDB storage layer for the job pipeline.

Collections
-----------
sessions      : one document per pipeline run (metadata + status)
jobs          : active job documents — the last ARCHIVE_RETENTION_DAYS days
archived_jobs : jobs moved out of the active collection by the archiver

All writes are idempotent on job_url so re-running a scrape does not
produce duplicate documents in MongoDB.
"""
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from job_pipeline.config import MONGO_DB_NAME, MONGO_URI

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
