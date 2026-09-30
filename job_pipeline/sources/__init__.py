"""
Extra discovery sources feeding the same pipeline as JobSpy.

Each source returns rows shaped like JobSpy's output (title, company, location,
job_url, job_url_direct, description, date_posted, site, is_remote, …), so the
existing dedupe → filters → scoring → storage stages apply unchanged.
"""
from __future__ import annotations

import logging

import pandas as pd

from job_pipeline.config import ATS_SOURCES

logger = logging.getLogger(__name__)


def collect_ats_jobs() -> pd.DataFrame:
    """Rows from every enabled ATS source. Never raises: a broken source only logs."""
    frames: list[pd.DataFrame] = []
    gh = ATS_SOURCES.get("greenhouse", {})
    if gh.get("enabled"):
        try:
            from job_pipeline.sources.greenhouse import collect_from_registry
            frames.append(collect_from_registry(**{k: v for k, v in gh.items() if k != "enabled"}))
        except Exception as exc:  # noqa: BLE001 — discovery must not break the hourly run
            logger.warning("Greenhouse source failed (non-fatal): %s", exc)
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
