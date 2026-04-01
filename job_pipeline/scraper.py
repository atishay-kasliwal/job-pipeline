"""
Thin wrapper around JobSpy's scrape_jobs().

Keeps all scraping logic in one place so the rest of the pipeline
never imports from jobspy directly.

IP rotation
-----------
If Tor is running locally (brew install tor && brew services start tor)
and the ``stem`` package is installed, each scrape request is routed
through a fresh Tor circuit — giving a new exit-node IP every run.
Falls back to direct connection silently if Tor is unavailable.
"""
import logging
from typing import Any

import pandas as pd
from jobspy import scrape_jobs

from job_pipeline.config import SCRAPER, SEARCH_TERMS

logger = logging.getLogger(__name__)

# Tor SOCKS5 proxy address (default Tor port)
_TOR_PROXY = {
    "http":  "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}


def _rotate_tor_ip() -> bool:
    """
    Request a new Tor circuit (new exit-node IP).
    Returns True if successful, False if Tor/stem is not available.
    """
    try:
        from stem import Signal
        from stem.control import Controller
        with Controller.from_port(port=9051) as ctrl:
            ctrl.authenticate()
            ctrl.signal(Signal.NEWNYM)
        logger.info("Tor: new circuit requested — IP rotated.")
        return True
    except Exception as exc:
        logger.debug("Tor IP rotation unavailable (non-fatal): %s", exc)
        return False


def _scrape_one(params: dict[str, Any]) -> pd.DataFrame:
    """Run a single JobSpy scrape and return the raw DataFrame."""
    using_tor = _rotate_tor_ip()
    if using_tor:
        params = {**params, "proxies": _TOR_PROXY}

    logger.info(
        "Scraping up to %d jobs for '%s' in '%s' (hours_old=%s, tor=%s) …",
        params["results_wanted"],
        params["search_term"],
        params["location"],
        params["hours_old"],
        using_tor,
    )

    try:
        df: pd.DataFrame = scrape_jobs(**params)
    except Exception as exc:
        logger.error("JobSpy scrape failed for '%s': %s", params["search_term"], exc)
        return pd.DataFrame()

    logger.info("  → %d raw results for '%s'", len(df), params["search_term"])
    return df


def scrape(overrides: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Scrape jobs from LinkedIn across all SEARCH_TERMS and merge results.

    Runs one JobSpy request per search term, concatenates, and deduplicates
    on job_url so the same posting found under multiple terms is kept once.

    Args:
        overrides: Optional mapping of JobSpy parameters that supersede the
                   defaults defined in config.SCRAPER.  Useful for CLI
                   arguments such as ``--hours-old`` or ``--results``.

    Returns:
        Deduplicated raw DataFrame combining all search terms.
    """
    base_params: dict[str, Any] = {**SCRAPER, **(overrides or {})}

    # Use SEARCH_TERMS unless caller explicitly passed a *different* search_term
    explicit_term = (overrides or {}).get("search_term")
    if explicit_term and explicit_term not in SEARCH_TERMS:
        search_terms = [explicit_term]
    else:
        search_terms = SEARCH_TERMS
        base_params.pop("search_term", None)  # will be set per-term below

    frames: list[pd.DataFrame] = []
    for term in search_terms:
        params = {**base_params, "search_term": term}
        df = _scrape_one(params)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.warning("All search terms returned 0 results.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicate by job_url first, then also by title+company
    # to eliminate same job posted in multiple cities
    before = len(combined)
    if "job_url" in combined.columns:
        combined = combined.drop_duplicates(subset=["job_url"])
    if "title" in combined.columns and "company" in combined.columns:
        combined["_tc"] = combined["title"].str.strip().str.lower() + "||" + combined["company"].str.strip().str.lower()
        combined = combined.drop_duplicates(subset=["_tc"])
        combined = combined.drop(columns=["_tc"])
    combined = combined.reset_index(drop=True)

    logger.info(
        "Combined %d terms → %d raw rows (%d dupes removed)",
        len(search_terms), len(combined), before - len(combined),
    )
    return combined
