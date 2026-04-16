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

from job_pipeline.identity import canonical_job_url, job_identity_key
from job_pipeline.config import (
    ALLOWED_STATES,
    COMPANY_EXCLUDE,
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

def filter_by_company(df: pd.DataFrame) -> pd.DataFrame:
    """Drop jobs from aggregator/spam companies in COMPANY_EXCLUDE."""
    if df.empty or "company" not in df.columns:
        return df
    before = len(df)
    mask = df["company"].fillna("").str.lower().apply(
        lambda c: not any(ex in c for ex in COMPANY_EXCLUDE)
    )
    out = df[mask].copy()
    logger.info("Company filter     : %3d → %3d rows", before, len(out))
    return out


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

    Full state names (>2 chars) use substring matching.
    Two-letter abbreviations use word-boundary matching to avoid false positives
    like "ga" matching "singapore" or "in" matching "india".

    Args:
        df: Input DataFrame.
        allowed_states: Override the default list from config.ALLOWED_STATES.
    """
    states = allowed_states or ALLOWED_STATES
    full_names = [s.lower() for s in states if len(s) > 2]
    abbrevs = [s.lower() for s in states if len(s) == 2]
    abbrev_re = re.compile(
        r'\b(?:' + '|'.join(re.escape(a) for a in abbrevs) + r')\b',
        re.IGNORECASE,
    ) if abbrevs else None

    def _passes(row: pd.Series) -> bool:
        loc = str(row.get("location", "") or "").lower()
        if not loc:
            return True  # unknown location → keep (may be remote)
        if any(name in loc for name in full_names):
            return True
        if abbrev_re and abbrev_re.search(loc):
            return True
        return False

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
    Remove jobs that explicitly require more than 3 years of experience.

    Checks title + description for all common patterns. Neutral postings
    (no mention) are always kept — we only drop explicit rejections.

    Threshold: min years > 3  →  discard.
    """
    _OVERQUALIFIED = re.compile(
        # ── number > 3 followed by experience context ──────────────────────
        # "4 years experience", "4+ years of experience",
        # "5+ years of professional software development exp"
        # allows up to 5 words between "years" and "experience"
        r"(?<!\d)(?<!-)([4-9]|\d{2})\s*(?:\+|[-–—]\s*\d+)?\s*\+?\s*(?:years?|yrs?)\s+(?:\w+\s+){0,5}(?:experience|exp(?:erience)?)\b"
        # "experience of 4 years", "exp of 4+ years"
        r"|(?:experience|exp(?:erience)?)\s*(?:of|:)\s*([4-9]|\d{2})\s*(?:\+|[-–—]\s*\d+)?\s*\+?\s*(?:years?|yrs?)\b"
        # ── explicit requirement phrases ────────────────────────────────────
        # "minimum 4 years", "minimum of 4 years", "min. 4 years"
        r"|(?:minimum|min\.?)\s+(?:of\s+)?([4-9]|\d{2})\s*\+?\s*(?:years?|yrs?)"
        # "at least 4 years"
        r"|at\s+least\s+([4-9]|\d{2})\s*\+?\s*(?:years?|yrs?)"
        # "4 or more years"
        r"|\b([4-9]|\d{2})\+?\s*or\s+more\s+(?:years?|yrs?)"
        # "requires 4 years", "requiring 4+ years"
        r"|requires?\s+([4-9]|\d{2})\s*\+?\s*(?:years?|yrs?)"
        # ── ranges where lower bound > 3 ───────────────────────────────────
        # "4-6 years", "5–8 years", "4 to 6 years of experience"
        r"|\b([4-9]|\d{2})\s*(?:[-–—]|to)\s*\d+\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience|exp(?:erience)?)\b"
        # bare years requirement: "5+ years working...", "10 years"
        # This intentionally catches explicit 4+ year requirements even when
        # the word "experience" is omitted.
        r"|\b([4-9]|\d{2})\s*\+?\s*(?:years?|yrs?)\b",
        re.IGNORECASE,
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
    1. Deduplicate on canonical identity key (URL-normalized, LinkedIn ID aware).
    2. Fuzzy fallback on normalized ``(title, company, location, site)``.

    Including location keeps distinct city-specific openings for the same
    company/title while still removing exact repost duplicates.
    """
    before = len(df)
    if df.empty:
        return df

    df = df.copy()
    df["_canonical_url"] = (
        df["job_url"].apply(canonical_job_url)
        if "job_url" in df.columns else ""
    )
    df["_job_key"] = df.apply(job_identity_key, axis=1)
    df = df.drop_duplicates(subset=["_job_key"])

    # Fuzzy pass only for rows without canonical URLs.
    with_url = df[df["_canonical_url"] != ""].copy()
    no_url = df[df["_canonical_url"] == ""].copy()
    if not no_url.empty:
        no_url["_norm_title"] = (
            no_url["title"].apply(_normalize_title)
            if "title" in no_url.columns else ""
        )
        no_url["_norm_company"] = (
            no_url["company"].fillna("").astype(str).str.lower().str.strip()
            if "company" in no_url.columns else ""
        )
        no_url["_norm_location"] = (
            no_url["location"]
            .fillna("")
            .astype(str)
            .str.lower()
            .str.replace(r"[^\w\s]", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            if "location" in no_url.columns else ""
        )
        no_url["_norm_site"] = (
            no_url["site"].fillna("").astype(str).str.lower().str.strip()
            if "site" in no_url.columns else ""
        )
        no_url = no_url.drop_duplicates(
            subset=["_norm_title", "_norm_company", "_norm_location", "_norm_site"]
        )

    df = pd.concat([with_url, no_url], ignore_index=True)
    df = df.drop(
        columns=[
            "_job_key",
            "_canonical_url",
            "_norm_title",
            "_norm_company",
            "_norm_location",
            "_norm_site",
        ],
        errors="ignore",
    )

    logger.info("Deduplication      : %4d → %4d rows", before, len(df))
    return df.copy()


# ── Experience extractor ──────────────────────────────────────────────────────

_EXP_RANGE_RE = re.compile(
    # "2 years experience", "2-4 years of experience", "2+ years experience"
    r'\b(\d+)\s*(?:[-–—]\s*(\d+))?\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:relevant\s+|professional\s+|work\s+|related\s+)?(?:experience|exp(?:erience)?)\b'
    # "2+ years", "3-5 years" (standalone, without "experience" word after)
    r'|\b(\d+)\s*(?:[-–—]\s*(\d+))?\s*\+\s*(?:years?|yrs?)\b'
    # "minimum 2 years", "at least 3-5 years", "requires 2 years"
    r'|(?:minimum|at\s+least|requires?|minimum\s+of)\s+(?:of\s+)?(\d+)\s*(?:[-–—]\s*(\d+))?\s*\+?\s*(?:years?|yrs?)\b'
    # "experience of 2-4 years", "experience: 2 years", "experience (3+ years)"
    r'|(?:experience|exp(?:erience)?)\s*(?:of|:|\()\s*(\d+)\s*(?:[-–—]\s*(\d+))?\s*\+?\s*(?:years?|yrs?)\b'
    # "2 yrs experience", "3-5 yrs of experience"
    r'|\b(\d+)\s*(?:[-–—]\s*(\d+))?\s*\+?\s*yrs?\s+(?:of\s+)?(?:experience|exp(?:erience)?)\b',
    re.IGNORECASE,
)


def extract_exp_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``min_exp`` and ``max_exp`` columns from explicit experience requirements
    in the title or description.

    ``min_exp`` — lowest bound found (e.g. 2 from "2-4 years")
    ``max_exp`` — highest bound found (e.g. 4 from "2-4 years"); equals min if no range

    Both are None when no experience requirement is detected.
    """
    def _extract(row: pd.Series) -> "tuple[int | None, int | None]":
        text = _concat_cols(row, "title", "description")
        mins, maxs = [], []
        for m in _EXP_RANGE_RE.finditer(text):
            g = m.groups()
            # 5 patterns × 2 groups each = 10 groups: (mn0,mx0, mn1,mx1, ...)
            for i in range(0, 10, 2):
                if g[i]:
                    mn = int(g[i])
                    mx = int(g[i + 1]) if g[i + 1] else mn
                    mins.append(mn)
                    maxs.append(mx)
        min_exp = min(mins) if mins else None
        max_exp = max(maxs) if maxs else None
        return min_exp, max_exp

    df = df.copy()
    df[["min_exp", "max_exp"]] = df.apply(
        lambda row: pd.Series(_extract(row)), axis=1
    )
    return df


# Keep old name as a thin wrapper for backward compatibility with any callers.
def extract_exp_years(df: pd.DataFrame) -> pd.DataFrame:
    df = extract_exp_range(df)
    df["exp_years"] = df["max_exp"]
    return df


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
