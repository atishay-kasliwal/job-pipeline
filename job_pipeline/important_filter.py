"""
High-priority filter module.

A job is considered "important" when ALL of the following are true:
  1. The hiring company appears in the curated top-companies list.
  2. The job description does NOT explicitly reject visa sponsorship.

Neutral language (no mention either way) is treated as sponsorship-friendly
to avoid over-filtering opportunities.
"""
import logging
from pathlib import Path

import pandas as pd

from job_pipeline.config import SPONSORSHIP_REJECT_PHRASES, TOP_COMPANIES_CSV

logger = logging.getLogger(__name__)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_top_companies(path: Path = TOP_COMPANIES_CSV) -> set[str]:
    """
    Load the curated top-company names from a CSV file.

    Expected format:
        A single column named ``company`` (or any first column if unnamed).
        Each row contains one company name.

    Returns:
        A lowercase ``set`` of company names for O(1) membership checks.
        Returns an empty set if the file is missing (with a warning).
    """
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        logger.warning(
            "top_companies.csv not found at '%s' — important filter will be a no-op.",
            path,
        )
        return set()

    col = "company" if "company" in df.columns else df.columns[0]
    companies = {str(name).strip().lower() for name in df[col].dropna()}
    logger.info("Loaded %d top companies from '%s'", len(companies), path)
    return companies


def load_h1b_sponsors(path: Path | None = None) -> set[str]:
    """
    Load the known H1B-sponsoring companies from a CSV file.

    Returns an empty set if the file is missing (feature degrades gracefully).
    """
    from job_pipeline.config import H1B_SPONSORS_CSV
    file = path or H1B_SPONSORS_CSV

    try:
        df = pd.read_csv(file)
    except FileNotFoundError:
        logger.debug("h1b_sponsors.csv not found — skipping H1B boost list.")
        return set()

    col = "company" if "company" in df.columns else df.columns[0]
    sponsors = {str(name).strip().lower() for name in df[col].dropna()}
    logger.info("Loaded %d H1B sponsors from '%s'", len(sponsors), file)
    return sponsors


# ── Per-row predicates ────────────────────────────────────────────────────────

def is_top_company(company: str, top_companies: set[str]) -> bool:
    """Case-insensitive membership check against the top-companies set."""
    return company.strip().lower() in top_companies


def is_sponsorship_ok(row: pd.Series) -> bool:
    """
    Return True unless the job description/title *explicitly* rejects sponsorship.

    Neutral descriptions (no mention) → True (keep the job).
    """
    text = " ".join(
        str(row.get(col, "") or "") for col in ("description", "title")
    ).lower()
    return not any(phrase in text for phrase in SPONSORSHIP_REJECT_PHRASES)


# ── Main filter function ──────────────────────────────────────────────────────

def apply_important_filter(
    df: pd.DataFrame,
    top_companies: set[str] | None = None,
) -> pd.DataFrame:
    """
    Return only rows that pass the combined important filter:
      - Company is in the top-companies list  AND
      - Sponsorship is not explicitly rejected.

    Args:
        df: Input DataFrame (post-role-filter recommended).
        top_companies: Pre-loaded company set.  If ``None``, loads from disk.

    Returns:
        Filtered DataFrame (new copy).
    """
    if top_companies is None:
        top_companies = load_top_companies()

    if not top_companies:
        logger.warning("No top companies loaded — important filter returns empty DataFrame.")
        return df.iloc[0:0].copy()

    def _passes(row: pd.Series) -> bool:
        return (
            is_top_company(str(row.get("company", "") or ""), top_companies)
            and is_sponsorship_ok(row)
        )

    before = len(df)
    result = df[df.apply(_passes, axis=1)].copy()
    logger.info("Important filter   : %4d → %4d rows", before, len(result))
    return result
