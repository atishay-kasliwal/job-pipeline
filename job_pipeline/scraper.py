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
from __future__ import annotations
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
from jobspy import scrape_jobs

from job_pipeline import config
from job_pipeline.config import SCRAPER, SEARCH_TERMS
from job_pipeline.ats_identity import add_ats_identity
from job_pipeline.identity import job_identity_key

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


def _scrape_one(params: dict[str, Any], rotate: bool = True) -> pd.DataFrame:
    """Run a single JobSpy scrape and return the raw DataFrame.

    ``rotate=False`` is used when several searches run together: a new Tor circuit
    mid-flight would cut the other searches' connections, so it is rotated once up front instead.
    """
    using_tor = _rotate_tor_ip() if rotate else False
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


def _scrape_terms(base_params: dict[str, Any], search_terms: list[str]) -> list[pd.DataFrame]:
    """One JobSpy request per search term, in term order. Up to LINKEDIN_WORKERS at a time."""
    workers = max(1, min(config.LINKEDIN_WORKERS, len(search_terms)))
    if workers == 1:
        results = [_scrape_one({**base_params, "search_term": term}) for term in search_terms]
    else:
        logger.info("Scraping %d LinkedIn searches, %d at a time", len(search_terms), workers)
        tor = _rotate_tor_ip()
        shared = {**base_params, "proxies": _TOR_PROXY} if tor else base_params
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda term: _scrape_one({**shared, "search_term": term}, rotate=False), search_terms))
    frames: list[pd.DataFrame] = []
    for term, df in zip(search_terms, results):
        if not df.empty:
            df["search_term"] = term
            frames.append(df)
    return frames


def _collect_ats() -> pd.DataFrame:
    """ATS boards (Greenhouse, …) go through the same pipeline as LinkedIn rows. Never raises."""
    try:
        from job_pipeline.sources import collect_ats_jobs
        return collect_ats_jobs()
    except Exception as exc:  # noqa: BLE001 — never block the LinkedIn scrape
        logger.warning("ATS sources failed (non-fatal): %s", exc)
        return pd.DataFrame()


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

    # The ATS boards are polled on another thread while LinkedIn is scraped: different sites, no contention.
    ats_pool = ThreadPoolExecutor(max_workers=1) if config.ATS_CONCURRENT else None
    try:
        ats_future = ats_pool.submit(_collect_ats) if ats_pool else None
        frames = _scrape_terms(base_params, search_terms)
        ats_df = ats_future.result() if ats_future else _collect_ats()
    finally:
        if ats_pool:
            ats_pool.shutdown(wait=True)
    if not ats_df.empty:
        frames.append(ats_df)

    if not frames:
        logger.warning("All search terms returned 0 results.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicate using canonical identity key so URL variants collapse.
    before = len(combined)
    combined = combined.copy()
    combined["_job_key"] = combined.apply(job_identity_key, axis=1)
    combined = combined.drop_duplicates(subset=["_job_key"]).drop(columns=["_job_key"])
    combined = combined.reset_index(drop=True)
    combined = add_ats_identity(combined)

    logger.info(
        "Combined %d terms → %d raw rows (%d dupes removed)",
        len(search_terms), len(combined), before - len(combined),
    )
    return combined
