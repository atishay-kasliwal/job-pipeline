"""
Standard pipeline: scrape → deduplicate → filter → score → output.

Orchestrates the individual modules in the correct order and owns the
save-to-disk responsibility for the standard (non-important) pipeline.
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.filters import (
    deduplicate,
    filter_by_location,
    filter_by_remote,
    filter_by_role,
    filter_by_sponsorship,
)
from job_pipeline.scraper import scrape
from job_pipeline.scoring import apply_scores
from job_pipeline.storage import insert_run

logger = logging.getLogger(__name__)

# Columns guaranteed to appear in the output frame
_OUTPUT_COLUMNS: list[str] = [
    "title",
    "company",
    "location",
    "job_url",
    "date_posted",
    "score",
    "competition_score",
]


def _ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add any missing output columns as NaN so the schema is consistent."""
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def run_standard_pipeline(
    scraper_overrides: dict[str, Any] | None = None,
    remote_only: bool = config.REMOTE_ONLY,
    output_csv: Path = config.OUTPUT_CSV,
    output_json: Path = config.OUTPUT_JSON,
    save: bool = True,
    store: bool = True,
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

    # 1. Scrape
    df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper — aborting pipeline.")
        return df

    # 2. Deduplicate
    df = deduplicate(df)

    # 3–6. Filtering chain
    df = filter_by_role(df)
    df = filter_by_location(df)
    df = filter_by_sponsorship(df)
    df = filter_by_remote(df, remote_only=remote_only)

    if df.empty:
        logger.warning("All jobs filtered out — nothing to score.")
        return df

    # 7. Scoring
    df = apply_scores(df)
    df = _ensure_output_columns(df)

    # Select and order output columns (drop any extras)
    df_out = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]].copy()

    if save:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_csv, index=False)
        df_out.to_json(output_json, orient="records", indent=2)
        logger.info("Saved %d jobs → %s", len(df_out), output_csv)
        logger.info("Saved %d jobs → %s", len(df_out), output_json)

    if store:
        try:
            insert_run(df_out, pipeline="standard")
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    logger.info("=" * 55)
    logger.info("  Standard Pipeline — DONE  (%d jobs)", len(df_out))
    logger.info("=" * 55)
    return df_out
