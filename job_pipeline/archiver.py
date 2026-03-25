"""
7-day archival worker.

Lifecycle per session
---------------------
1. Detect sessions in MongoDB older than ARCHIVE_RETENTION_DAYS.
2. Fetch all job documents for each eligible session.
3. Write a CSV to  ./archives/YYYY-MM-DD/<session_id>_<pipeline>.csv
4. Move jobs from  ``jobs``  →  ``archived_jobs``  collection.
5. Mark the session as archived in the ``sessions`` collection.

Run directly:
    python -m job_pipeline.archiver
    python -m job_pipeline.archiver --dry-run
    python -m job_pipeline.archiver --retention-days 14
"""
import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from job_pipeline.config import ARCHIVE_DIR, ARCHIVE_RETENTION_DAYS
from job_pipeline.storage import (
    get_sessions_to_archive,
    get_jobs_for_session,
    move_session_to_archive,
)

logger = logging.getLogger(__name__)


# ── CSV export ────────────────────────────────────────────────────────────────

def export_session_csv(
    session_id: str,
    pipeline: str,
    jobs: list[dict],
    archive_dir: Path = ARCHIVE_DIR,
) -> Path:
    """
    Write one session's jobs to a CSV file inside a date-stamped folder.

    Path pattern:
        ./archives/YYYY-MM-DD/<session_id>_<pipeline>.csv

    Args:
        session_id:  The session identifier string (ISO timestamp).
        pipeline:    ``"standard"`` or ``"important"``.
        jobs:        List of MongoDB job documents for this session.
        archive_dir: Root directory for archives (default: config.ARCHIVE_DIR).

    Returns:
        Path to the written CSV file.
    """
    # Use the date portion of session_id for the folder name
    date_str = session_id[:10]  # "YYYY-MM-DD"
    folder = archive_dir / date_str
    folder.mkdir(parents=True, exist_ok=True)

    # Sanitise session_id for use as filename (colons → hyphens)
    safe_sid = session_id.replace(":", "-").replace("T", "_")
    csv_path = folder / f"{safe_sid}_{pipeline}.csv"

    df = pd.DataFrame(jobs)

    # Drop internal MongoDB fields from the CSV
    drop_cols = ["_id", "run_at", "archived_at"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    df.to_csv(csv_path, index=False)
    logger.info("Exported %d jobs → %s", len(df), csv_path)
    return csv_path


# ── Main archival logic ────────────────────────────────────────────────────────

def run_archival(
    retention_days: int = ARCHIVE_RETENTION_DAYS,
    archive_dir: Path = ARCHIVE_DIR,
    dry_run: bool = False,
) -> dict:
    """
    Scan for expired sessions, export each to CSV, then move to archive.

    Args:
        retention_days: Sessions older than this many days are archived.
        archive_dir:    Root path for CSV exports.
        dry_run:        When True, log what *would* happen but make no changes.

    Returns:
        Summary dict with keys:
        - ``sessions_found``  : int
        - ``sessions_archived``: int
        - ``jobs_archived``   : int
        - ``csv_files``       : list of Path strings
    """
    logger.info("=" * 55)
    logger.info("  Archiver — START  (retention=%d days, dry_run=%s)", retention_days, dry_run)
    logger.info("=" * 55)

    sessions = get_sessions_to_archive(retention_days)
    summary = {
        "sessions_found": len(sessions),
        "sessions_archived": 0,
        "jobs_archived": 0,
        "csv_files": [],
    }

    if not sessions:
        logger.info("Nothing to archive.")
        return summary

    archived_at = datetime.now(tz=timezone.utc)

    for session in sessions:
        sid: str = session["session_id"]
        pipeline: str = session.get("pipeline", "unknown")

        logger.info("Processing session '%s' (%s pipeline) …", sid, pipeline)

        jobs = get_jobs_for_session(sid)
        if not jobs:
            logger.warning("  Session '%s' has no jobs — marking archived anyway.", sid)

        if dry_run:
            logger.info("  [dry-run] Would export %d jobs and move to archive.", len(jobs))
            continue

        # Step 1: Export to CSV
        if jobs:
            csv_path = export_session_csv(sid, pipeline, jobs, archive_dir)
            summary["csv_files"].append(str(csv_path))

        # Step 2: Move to archived_jobs + mark session
        moved = move_session_to_archive(sid, archived_at)
        summary["sessions_archived"] += 1
        summary["jobs_archived"] += moved

    logger.info("=" * 55)
    logger.info(
        "  Archiver — DONE  (%d/%d sessions archived, %d jobs moved, %d CSVs written)",
        summary["sessions_archived"],
        summary["sessions_found"],
        summary["jobs_archived"],
        len(summary["csv_files"]),
    )
    logger.info("=" * 55)
    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive job pipeline sessions older than N days to CSV + MongoDB archive.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=ARCHIVE_RETENTION_DAYS,
        metavar="N",
        help="Archive sessions older than N days.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be archived without making any changes.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    result = run_archival(
        retention_days=args.retention_days,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
