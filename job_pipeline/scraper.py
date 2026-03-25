"""
Thin wrapper around JobSpy's scrape_jobs().

Keeps all scraping logic in one place so the rest of the pipeline
never imports from jobspy directly.
"""
import logging
from typing import Any

import pandas as pd
from jobspy import scrape_jobs

from job_pipeline.config import SCRAPER

logger = logging.getLogger(__name__)


def scrape(overrides: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Scrape jobs from LinkedIn via JobSpy.

    Args:
        overrides: Optional mapping of JobSpy parameters that supersede the
                   defaults defined in config.SCRAPER.  Useful for CLI
                   arguments such as ``--hours-old`` or ``--results``.

    Returns:
        Raw DataFrame exactly as returned by JobSpy (may contain nulls).

    Raises:
        Exception: Re-raises any exception thrown by JobSpy after logging it.
    """
    params: dict[str, Any] = {**SCRAPER, **(overrides or {})}

    logger.info(
        "Scraping up to %d jobs for '%s' in '%s' (hours_old=%s) …",
        params["results_wanted"],
        params["search_term"],
        params["location"],
        params["hours_old"],
    )

    try:
        df: pd.DataFrame = scrape_jobs(**params)
    except Exception as exc:
        logger.error("JobSpy scrape failed: %s", exc)
        raise

    logger.info("Raw results: %d rows", len(df))
    return df
