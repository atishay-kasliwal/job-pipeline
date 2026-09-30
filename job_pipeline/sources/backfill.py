"""
Backfill: bring in postings that are already open on the ATS boards.

The hourly poll only keeps postings published since each board's last poll, so a
role that was open before its board was first polled is never seen. This reads
every active board once, keeps postings published in the last --days, and sends
them through the same filters, scoring and storage as the hourly run (upsert on
job_url, so re-running is safe).

    python -m job_pipeline.sources.backfill --days 30                     # dry run: what would come in
    python -m job_pipeline.sources.backfill --days 30 --write             # store in Mongo
    python -m job_pipeline.sources.backfill --days 30 --per-company 5     # at most 5 per company (best score first)

Board poll bookkeeping (last_polled_at etc.) is not touched, so the hourly rotation is unchanged.
"""
from __future__ import annotations

import argparse
import importlib
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.ats_identity import add_ats_identity

logger = logging.getLogger(__name__)


def active_boards(ats: str) -> list[dict[str, Any]]:
    from job_pipeline.ats_boards import _col

    return list(_col().find({"ats": ats, "status": "active"}))


def collect_open_postings(days: int, now: datetime | None = None) -> pd.DataFrame:
    """Postings published in the last `days` on every active board of every enabled ATS source."""
    now = now or datetime.now(tz=timezone.utc)
    since = now - timedelta(days=days)
    frames: list[pd.DataFrame] = []
    for ats, settings in config.ATS_SOURCES.items():
        if not settings.get("enabled"):
            continue
        # Pretend every board was last polled `days` ago: the sources then keep exactly what was published since.
        boards = [{**b, "last_polled_at": since} for b in active_boards(ats)]
        if not boards:
            continue
        module = importlib.import_module(f"job_pipeline.sources.{ats}")
        df = module.collect_boards(boards, now=now, workers=settings.get("workers", 8), timeout_s=settings.get("timeout_s", 15))
        logger.info("%s: %d boards → %d role-matching postings from the last %d days", ats, len(boards), len(df), days)
        if not df.empty:
            frames.append(df)
    return add_ats_identity(pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame()


def cap_per_company(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Keep each company's `n` best-scoring rows (0 = no cap)."""
    if n <= 0 or df.empty or "company" not in df.columns:
        return df
    score = "score" if "score" in df.columns else None
    ordered = df.sort_values(score, ascending=False) if score else df
    return ordered.groupby(ordered["company"].fillna("").str.lower(), sort=False).head(n)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=30, help="keep postings published in the last N days (default 30)")
    parser.add_argument("--per-company", type=int, default=0, help="at most N jobs per company, best score first (0 = no cap)")
    parser.add_argument("--write", action="store_true", help="store results in Mongo (default: dry run)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    from job_pipeline.pipeline import run_standard_pipeline
    from job_pipeline.storage import get_db

    raw = collect_open_postings(args.days)
    if raw.empty:
        print("No open postings found.")
        return
    # Dry run first in every case: same filters and scoring, nothing persisted.
    scored = run_standard_pipeline(raw_jobs=raw, save=False, store=False, deploy=False)
    scored = cap_per_company(scored, args.per_company)
    known = set(get_db()["jobs"].distinct("job_url"))
    new = scored[~scored["job_url"].isin(known)] if not scored.empty else scored

    print(f"\n{len(raw)} role-matching postings from the last {args.days} days → {len(scored)} after all filters"
          f"{f' (≤{args.per_company} per company)' if args.per_company else ''} → {len(new)} not already stored, "
          f"{new['company'].nunique() if not new.empty else 0} companies")
    if not new.empty:
        print("by ATS:", dict(Counter(new["site"])))
        print("top companies:", Counter(new["company"]).most_common(15))
        cols = [c for c in ("score", "company", "title", "location") if c in new.columns]
        print(new.sort_values("score", ascending=False)[cols].head(20).to_string(index=False) if "score" in new.columns else new[cols].head(20).to_string(index=False))

    if not args.write:
        print("\nDry run — add --write to store these.")
        return
    if new.empty:
        print("Nothing new to store.")
        return
    keep_urls = set(new["job_url"])
    out_dir = Path(config.OUTPUT_CSV).parent
    stored = run_standard_pipeline(
        raw_jobs=raw[raw["job_url"].isin(keep_urls)],
        output_csv=out_dir / "backfill_jobs.csv",
        output_json=out_dir / "backfill_jobs.json",
        save=True,
        store=True,
        deploy=False,
    )
    print(f"\nStored {len(stored)} jobs (descriptions included).")


if __name__ == "__main__":
    main()
