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

from job_pipeline.config import SCRAPER

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


def scrape(overrides: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Scrape jobs from LinkedIn via JobSpy.

    Attempts to rotate the Tor exit-node IP before each scrape.
    Falls back to a direct connection if Tor is not running.

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

    # Rotate IP via Tor if available; inject proxy into jobspy params
    using_tor = _rotate_tor_ip()
    if using_tor:
        params.setdefault("proxies", _TOR_PROXY)

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
        logger.error("JobSpy scrape failed: %s", exc)
        raise

    logger.info("Raw results: %d rows", len(df))
    return df
