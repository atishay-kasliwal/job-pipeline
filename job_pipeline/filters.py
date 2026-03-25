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


def filter_by_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove jobs that explicitly require more than 5 years of experience.

    Checks title + description for patterns like "6+ years", "7 or more years",
    "minimum 6 years", etc.  Neutral postings (no mention) are kept.
    """
    _OVERQUALIFIED = re.compile(
        # "6 years", "7+ years", "10 years", "15 years" — any standalone high number
        r"\b([6-9]|\d{2})\+?\s*years?\b"
        # ranges where the LOWER bound is already too high: "6-8 years", "8–10+ years"
        r"|\b([6-9]|\d{2})\s*[-–—to]+\s*\d+\+?\s*years?"
        # ranges where lower bound is 5 but upper is high: "5-8 years", "5–10 years"
        r"|\b5\s*[-–—]\s*([7-9]|\d{2})\+?\s*years?"
        # explicit phrases
        r"|minimum\s+(?:of\s+)?[6-9]\s+years?"
        r"|at\s+least\s+[6-9]\s+years?"
        r"|[6-9]\s+or\s+more\s+years?"
        r"|\d{2}\s+or\s+more\s+years?"
    )

    def _passes(row: pd.Series) -> bool:
        text = _concat_cols(row, "title", "description")
        return not bool(_OVERQUALIFIED.search(text))

    before = len(df)
    result = df[df.apply(_passes, axis=1)].copy()
    logger.info("Experience filter  : %4d → %4d rows", before, len(result))
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


def _normalize_title(title: str) -> str:
    """Normalize a job title for fuzzy deduplication (strip years, punctuation)."""
    t = str(title or "").lower()
    t = re.sub(r"\b20\d{2}\b", "", t)   # remove years like 2024 / 2025 / 2026
    t = re.sub(r"[^\w\s]", " ", t)      # punctuation → space
    t = re.sub(r"\s+", " ", t).strip()
    return t


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicate job postings.

    Strategy:
    1. Deduplicate on ``job_url`` (most precise).
    2. Exact ``(title, company)`` duplicates.
    3. Normalized ``(title, company)`` — catches reposts with minor variants
       like differing punctuation, appended years, or bracket suffixes.
    """
    before = len(df)
    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"])
    df = df.drop_duplicates(subset=["title", "company"])

    # Fuzzy pass: normalize titles then dedup again
    df = df.copy()
    df["_norm_title"]   = df["title"].apply(_normalize_title)
    df["_norm_company"] = df["company"].str.lower().str.strip() if "company" in df.columns else ""
    df = df.drop_duplicates(subset=["_norm_title", "_norm_company"])
    df = df.drop(columns=["_norm_title", "_norm_company"])

    logger.info("Deduplication      : %4d → %4d rows", before, len(df))
    return df.copy()


# ── Level tagger ──────────────────────────────────────────────────────────────

_LEVEL_SIGNALS: list[tuple[str, list[str]]] = [
    # Most specific first — checked in order, first match wins
    ("New Grad", [
        "new grad", "new-grad", "new graduate", "university graduate",
        "campus hire", "recent grad", "recent graduate",
    ]),
    ("Mid", [
        "mid-level", "mid level", "swe ii", "sde ii", "eng ii",
        "software engineer ii", "level ii", "level 2",
        "2-5 year", "3-5 year", "2 to 5 year", "3 to 5 year",
    ]),
    ("Entry", [
        "entry level", "entry-level", "junior", "jr.", " jr ",
        "associate", "0-2 year", "0 to 2 year", "swe i", "sde i",
        "eng i", "software engineer i", "level i", "level 1",
    ]),
]


def tag_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a ``level`` column classifying each job as New Grad / Entry / Mid.

    Because the pipeline's role filter already excludes senior/staff/lead,
    unlabelled rows default to 'Entry'.
    """
    def _detect(row: pd.Series) -> str:
        text = " " + _concat_cols(row, "title", "description") + " "
        for level, signals in _LEVEL_SIGNALS:
            if any(s in text for s in signals):
                return level
        return "Entry"

    df = df.copy()
    df["level"] = df.apply(_detect, axis=1)
    return df
