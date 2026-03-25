"""
Reusable, side-effect-free filtering functions for the job pipeline.

Each filter accepts a DataFrame and returns a *new* filtered copy.
They are designed to be composed freely without order dependency
(except deduplicate, which should run first to avoid redundant work).
"""
import logging
import re
from typing import Optional

import pandas as pd

from job_pipeline.config import (
    ALLOWED_STATES,
    REMOTE_ONLY,
    ROLE_EXCLUDE_KEYWORDS,
    ROLE_INCLUDE_KEYWORDS,
    SPONSORSHIP_REJECT_PHRASES,
)

logger = logging.getLogger(__name__)


# ── Private helpers ───────────────────────────────────────────────────────────

def _concat_cols(row: pd.Series, *cols: str) -> str:
    """Concatenate multiple DataFrame columns into one lowercase string."""
    return " ".join(str(row.get(c, "") or "") for c in cols).lower()


def _phrase_match(text: str, phrases: list[str]) -> bool:
    """
    Return True if any phrase from *phrases* appears in *text*.

    Uses re.escape so special characters in phrases are treated literally.
    """
    for phrase in phrases:
        if re.search(re.escape(phrase), text):
            return True
    return False


# ── Public filters ────────────────────────────────────────────────────────────

def filter_by_role(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep rows whose title matches at least one include keyword
    and none of the exclude keywords.

    Include keywords : config.ROLE_INCLUDE_KEYWORDS
    Exclude keywords : config.ROLE_EXCLUDE_KEYWORDS
    """
    def _passes(row: pd.Series) -> bool:
        title = _concat_cols(row, "title")
        return (
            _phrase_match(title, ROLE_INCLUDE_KEYWORDS)
            and not _phrase_match(title, ROLE_EXCLUDE_KEYWORDS)
        )

    before = len(df)
    result = df[df.apply(_passes, axis=1)].copy()
    logger.info("Role filter        : %4d → %4d rows", before, len(result))
    return result


def filter_by_location(
    df: pd.DataFrame,
    allowed_states: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Keep rows whose location field contains at least one allowed state name
    or abbreviation.

    Rows with an empty/null location are kept (they may be fully remote).

    Args:
        df: Input DataFrame.
        allowed_states: Override the default list from config.ALLOWED_STATES.
    """
    states_lower = [s.lower() for s in (allowed_states or ALLOWED_STATES)]

    def _passes(row: pd.Series) -> bool:
        loc = str(row.get("location", "") or "").lower()
        if not loc:
            return True  # unknown location → keep (may be remote)
        return any(state in loc for state in states_lower)

    before = len(df)
    result = df[df.apply(_passes, axis=1)].copy()
    logger.info("Location filter    : %4d → %4d rows", before, len(result))
    return result


def filter_by_sponsorship(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that *explicitly* reject sponsorship.

    Philosophy:
    - Neutral language  → keep  (do not over-filter)
    - Positive language → keep
    - Explicit rejection → discard

    Rejection phrases come from config.SPONSORSHIP_REJECT_PHRASES.
    """
    def _passes(row: pd.Series) -> bool:
        text = _concat_cols(row, "description", "title")
        return not _phrase_match(text, SPONSORSHIP_REJECT_PHRASES)

    before = len(df)
    result = df[df.apply(_passes, axis=1)].copy()
    logger.info("Sponsorship filter : %4d → %4d rows", before, len(result))
    return result


def filter_by_remote(
    df: pd.DataFrame,
    remote_only: bool = REMOTE_ONLY,
) -> pd.DataFrame:
    """
    Optionally restrict results to remote positions.

    Args:
        df: Input DataFrame.
        remote_only: When True, drop all non-remote rows.
                     Defaults to config.REMOTE_ONLY (False).
    """
    if not remote_only:
        return df

    before = len(df)
    result = df[df["is_remote"].fillna(False).astype(bool)].copy()
    logger.info("Remote filter      : %4d → %4d rows", before, len(result))
    return result


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicate job postings.

    Strategy:
    1. Deduplicate on ``job_url`` (most precise).
    2. Then deduplicate on ``(title, company)`` to catch cross-site duplicates.
    """
    before = len(df)
    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"])
    df = df.drop_duplicates(subset=["title", "company"])
    logger.info("Deduplication      : %4d → %4d rows", before, len(df))
    return df.copy()
