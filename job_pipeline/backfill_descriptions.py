"""
Description backfill — fills in LinkedIn job descriptions that JobSpy missed.

Roughly ~10% of LinkedIn postings come back from JobSpy with an empty
description (rate-limiting, layout variation, transient fetch failure).
Those jobs land in the ``jobs`` collection but never get an entry in the
``descriptions`` collection, so the Atriveo "Full Job Description" export
shows nothing for them.

This module finds jobs that are missing a description and re-fetches each
one individually via JobSpy's native LinkedIn single-job path
(``LinkedIn._get_job_details``), then writes the result to:

  * MongoDB ``descriptions`` collection  (via storage.upsert_descriptions)
  * output/descriptions.json             (via storage.save_descriptions)

Two entry points:

  * ``backfill_missing(...)``  — find all recent jobs with no description
    and fetch them. Call this from the pipeline after each scrape, or run
    it standalone to catch up.
  * ``backfill_url(url)``      — fetch one specific job on demand.

Only LinkedIn is supported: Indeed descriptions arrive inline with the
scrape, and portal (greenhouse/ashby/lever) listings have no description
text by design.

CLI
---
    # Backfill every LinkedIn job from the last 24h that has no description
    python -m job_pipeline.backfill_descriptions

    # Backfill a wider window / cap the work
    python -m job_pipeline.backfill_descriptions --since 2026-06-01 --limit 200

    # Fetch one specific job
    python -m job_pipeline.backfill_descriptions --url https://www.linkedin.com/jobs/view/4423974727
"""
from __future__ import annotations

import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from job_pipeline import config
from job_pipeline.storage import save_descriptions, upsert_descriptions

logger = logging.getLogger(__name__)

# LinkedIn job-view URL → numeric job id, e.g. .../jobs/view/4423974727
_LINKEDIN_ID_RE = re.compile(r"/jobs/view/(\d+)")

# Polite delay between single-job fetches to avoid tripping rate limits.
_MIN_DELAY_S = 1.0
_MAX_DELAY_S = 2.5


def _linkedin_job_id(url: str) -> str | None:
    """Extract the numeric LinkedIn job id from a job-view URL, or None."""
    m = _LINKEDIN_ID_RE.search(url or "")
    return m.group(1) if m else None


def _make_fetcher():
    """
    Build a configured JobSpy LinkedIn scraper for single-job fetches.

    Returns a callable ``fetch(job_id) -> str`` that returns the markdown
    description (empty string on failure). Returns None if JobSpy is not
    importable, so callers can degrade gracefully.
    """
    try:
        from jobspy.linkedin import LinkedIn
        from jobspy.model import ScraperInput, Site, DescriptionFormat
    except Exception as exc:  # pragma: no cover - import guard
        logger.error("jobspy not available for backfill: %s", exc)
        return None

    li = LinkedIn()
    # _get_job_details reads scraper_input.description_format; set it up once.
    li.scraper_input = ScraperInput(
        site_type=[Site.LINKEDIN],
        search_term="",
        description_format=DescriptionFormat.MARKDOWN,
    )

    def fetch(job_id: str) -> str:
        try:
            details = li._get_job_details(job_id)
        except Exception as exc:
            logger.debug("backfill: fetch failed for job_id=%s: %s", job_id, exc)
            return ""
        desc = (details or {}).get("description") or ""
        return str(desc).strip()

    return fetch


def _persist(pairs: list[tuple[str, str]]) -> int:
    """
    Write a list of (job_url, description) pairs to MongoDB + descriptions.json.

    Reuses the existing storage helpers, which both take a DataFrame with
    ``job_url`` and ``description`` columns and never overwrite existing
    descriptions.
    """
    if not pairs:
        return 0
    df = pd.DataFrame(pairs, columns=["job_url", "description"])
    try:
        upsert_descriptions(df)
    except Exception as exc:
        logger.error("backfill: MongoDB upsert failed (non-fatal): %s", exc)
    try:
        save_descriptions(df, config.OUTPUT_DIR)
    except Exception as exc:
        logger.error("backfill: descriptions.json write failed (non-fatal): %s", exc)
    return len(pairs)


def _missing_linkedin_urls(since: str, limit: int) -> list[str]:
    """
    Return LinkedIn job_urls scraped on/after *since* that have no entry in
    the ``descriptions`` collection. Capped at *limit*.
    """
    from pymongo import MongoClient

    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8_000)
    db = client[config.MONGO_DB_NAME]

    jobs = db["jobs"].find(
        {"site": "linkedin", "batch_time": {"$gte": since}},
        {"_id": 0, "job_url": 1},
    )
    urls = {j["job_url"] for j in jobs if j.get("job_url")}
    if not urls:
        return []

    # Subtract URLs that already have a description.
    url_list = list(urls)
    have: set[str] = set()
    for i in range(0, len(url_list), 500):
        chunk = url_list[i : i + 500]
        for d in db["descriptions"].find(
            {"job_url": {"$in": chunk}}, {"_id": 0, "job_url": 1}
        ):
            have.add(d["job_url"])

    missing = [u for u in url_list if u not in have]
    return missing[:limit]


def backfill_url(url: str) -> str | None:
    """
    Fetch and store the description for a single LinkedIn job URL on demand.

    Returns the description text on success, None if it could not be fetched
    (non-LinkedIn URL, removed posting, or fetch failure).
    """
    job_id = _linkedin_job_id(url)
    if not job_id:
        logger.warning("backfill_url: not a LinkedIn job-view URL: %s", url)
        return None

    fetch = _make_fetcher()
    if fetch is None:
        return None

    desc = fetch(job_id)
    if not desc:
        logger.warning("backfill_url: no description retrievable for %s", url)
        return None

    _persist([(url, desc)])
    logger.info("backfill_url: stored %d-char description for %s", len(desc), url)
    return desc


def backfill_missing(
    since: str | None = None,
    limit: int = 300,
) -> int:
    """
    Find recent LinkedIn jobs with no stored description and fetch them.

    Args:
        since: ISO date/time lower bound on batch_time. Defaults to 24h ago.
        limit: Maximum number of jobs to fetch this run (rate-limit guard).

    Returns:
        Number of descriptions successfully backfilled.
    """
    if since is None:
        since = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    try:
        missing = _missing_linkedin_urls(since, limit)
    except Exception as exc:
        logger.error("backfill: could not query missing URLs (non-fatal): %s", exc)
        return 0

    if not missing:
        logger.info("backfill: no LinkedIn jobs missing descriptions since %s.", since)
        return 0

    logger.info(
        "backfill: %d LinkedIn job(s) missing descriptions since %s — fetching (limit=%d).",
        len(missing), since, limit,
    )

    fetch = _make_fetcher()
    if fetch is None:
        return 0

    pairs: list[tuple[str, str]] = []
    failed = 0
    for i, url in enumerate(missing, 1):
        job_id = _linkedin_job_id(url)
        if not job_id:
            continue
        desc = fetch(job_id)
        if desc:
            pairs.append((url, desc))
        else:
            failed += 1
        # Persist in batches so a long run still makes partial progress.
        if len(pairs) >= 25:
            _persist(pairs)
            pairs = []
        # Polite jitter between requests.
        time.sleep(random.uniform(_MIN_DELAY_S, _MAX_DELAY_S))

    backfilled = _persist(pairs) if pairs else 0
    # _persist returns the size of its last batch; track the full total instead.
    total = len(missing) - failed
    logger.info(
        "backfill: done — %d/%d descriptions backfilled (%d failed/unavailable).",
        total, len(missing), failed,
    )
    return total


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    parser = argparse.ArgumentParser(
        description="Backfill missing LinkedIn job descriptions."
    )
    parser.add_argument(
        "--url",
        help="Backfill a single LinkedIn job-view URL and exit.",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="ISO date lower bound on batch_time (default: 24h ago).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="Max jobs to fetch this run (default: 300).",
    )
    args = parser.parse_args()

    if args.url:
        desc = backfill_url(args.url)
        if desc:
            print(f"\n--- {len(desc)} chars ---\n{desc}\n")
        else:
            print("No description retrievable.")
            sys.exit(1)
    else:
        n = backfill_missing(since=args.since, limit=args.limit)
        print(f"Backfilled {n} description(s).")
