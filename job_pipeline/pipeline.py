"""
Standard pipeline: scrape → deduplicate → filter → score → output.

Orchestrates the individual modules in the correct order and owns the
save-to-disk responsibility for the standard (non-important) pipeline.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.filters import (
    deduplicate,
    extract_exp_range,
    filter_by_company,
    filter_by_experience,
    filter_by_location,
    filter_by_remote,
    filter_by_role,
    filter_by_sponsorship,
    tag_level,
)
from job_pipeline.identity import job_identity_key
from job_pipeline.scraper import scrape
from job_pipeline.scoring import apply_scores
from job_pipeline.storage import append_run_history, insert_run, insert_run_stats, save_descriptions, update_daily_jobs
from job_pipeline.deploy import deploy_output

logger = logging.getLogger(__name__)

# Columns guaranteed to appear in the output frame
_OUTPUT_COLUMNS: list[str] = [
    "title",
    "company",
    "location",
    "level",
    "min_exp",
    "max_exp",
    "job_url",
    "date_posted",
    "batch_time",
    "score",
    "score_pct",
    "competition_score",
    "site",
    "search_term",
]


def _ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add any missing output columns as NaN so the schema is consistent."""
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def _persist_standard_results(
    df_out: pd.DataFrame,
    output_csv: Path,
    output_json: Path,
    save: bool,
    store: bool,
    deploy: bool,
) -> None:
    """
    Persist standard-pipeline outputs for this run.

    Important: this is called even when ``df_out`` is empty so downstream views
    never keep showing stale data from a previous hour.
    """
    if save:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_csv, index=False)
        df_out.to_json(output_json, orient="records", indent=2)
        logger.info("Saved %d jobs → %s", len(df_out), output_csv)
        logger.info("Saved %d jobs → %s", len(df_out), output_json)

    if store:
        try:
            sid = insert_run(df_out, pipeline="standard")
            if sid:
                insert_run_stats(df_out, pipeline="standard", session_id=sid)
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    if save:
        history_path = output_csv.parent / "run_history.json"
        try:
            append_run_history(df_out, pipeline="standard", history_path=history_path)
        except Exception as exc:
            logger.error("run_history.json update failed (non-fatal): %s", exc)
        try:
            update_daily_jobs(df_out, output_csv.parent)
        except Exception as exc:
            logger.error("today_jobs.json update failed (non-fatal): %s", exc)

    if deploy:
        try:
            deploy_output()
        except Exception as exc:
            logger.error("Dashboard deploy failed (non-fatal): %s", exc)


def run_standard_pipeline(
    scraper_overrides: dict[str, Any] | None = None,
    raw_jobs: pd.DataFrame | None = None,
    remote_only: bool = config.REMOTE_ONLY,
    output_csv: Path = config.OUTPUT_CSV,
    output_json: Path = config.OUTPUT_JSON,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Execute the full standard pipeline end-to-end.

    Pipeline steps
    --------------
    1. **Scrape**         — fetch raw jobs from LinkedIn via JobSpy.
    2. **Deduplicate**    — drop exact URL and title+company duplicates.
    3. **Role filter**    — keep only matching entry-level engineering titles.
    4. **Location filter**— keep only US-located or location-less rows.
    5. **Sponsorship filter** — discard explicit sponsorship rejections.
    6. **Remote filter**  — optionally keep only remote positions.
    7. **Score & rank**   — add ``score`` and ``competition_score`` columns.

    Args:
        scraper_overrides: Dict passed to :func:`scraper.scrape` to override
                           config defaults (e.g. ``{"hours_old": 6}``).
        raw_jobs:          Optional pre-scraped raw DataFrame. When provided,
                           the pipeline reuses this dataset instead of calling
                           the scraper again (used by ``--pipeline all``).
        remote_only:       When True, drop non-remote rows.
        output_csv:        Destination path for the CSV output.
        output_json:       Destination path for the JSON output.
        save:              When False, skip writing files (useful for testing).
        store:             When True, persist results to MongoDB.

    Returns:
        Scored, filtered DataFrame with the columns defined in
        ``_OUTPUT_COLUMNS``.
    """
    logger.info("=" * 55)
    logger.info("  Standard Pipeline — START")
    logger.info("=" * 55)

    # 1. Scrape (or reuse pre-scraped raw jobs)
    if raw_jobs is not None:
        df = raw_jobs.copy()
        logger.info("Using shared pre-scraped dataset (%d raw rows).", len(df))
    else:
        df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_standard_results(
            df_out=empty_out,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 2. Deduplicate
    df = deduplicate(df)

    # 3–7. Filtering chain
    df = filter_by_company(df)
    df = filter_by_role(df)
    df = filter_by_location(df)
    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    df = filter_by_remote(df, remote_only=remote_only)

    if df.empty:
        logger.warning("All jobs filtered out — nothing to score.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_standard_results(
            df_out=empty_out,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 7. Level tagging + experience extraction
    df = tag_level(df)
    df = extract_exp_range(df)

    # 8. Scoring
    df = apply_scores(df)
    df = _ensure_output_columns(df)

    # Stamp batch_time so every view (This Hour, snapshots) shows when the job was found
    df["batch_time"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Save descriptions before dropping them — used by the ATS analyzer
    if save:
        try:
            save_descriptions(df, output_csv.parent)
        except Exception as exc:
            logger.warning("save_descriptions failed (non-fatal): %s", exc)

    # Select and order output columns (drop any extras)
    df_out = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]].copy()

    # 9. Drop jobs already seen today — only surface net-new postings each run
    today_path = output_csv.parent / "today_jobs.json"
    if today_path.exists():
        try:
            import json as _json
            seen = {job_identity_key(j) for j in _json.loads(today_path.read_text())}
            before = len(df_out)
            keys = df_out.apply(job_identity_key, axis=1)
            df_out = df_out[~keys.isin(seen)].copy()
            logger.info("Already-seen filter: %4d → %4d rows", before, len(df_out))
        except Exception as exc:
            logger.warning("Could not load today_jobs.json for dedup (non-fatal): %s", exc)

    if df_out.empty:
        logger.info("No new jobs this run — all already seen today.")
    _persist_standard_results(
        df_out=df_out,
        output_csv=output_csv,
        output_json=output_json,
        save=save,
        store=store,
        deploy=deploy,
    )

    logger.info("=" * 55)
    logger.info("  Standard Pipeline — DONE  (%d jobs)", len(df_out))
    logger.info("=" * 55)
    return df_out
