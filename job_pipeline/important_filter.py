"""
High-priority filter module.

A job is considered "important" when ALL of the following are true:
  1. The hiring company appears in the curated top-companies list.
  2. The job description does NOT explicitly reject visa sponsorship.

Neutral language (no mention either way) is treated as sponsorship-friendly
to avoid over-filtering opportunities.
"""
import logging
import re
from pathlib import Path

import pandas as pd

from job_pipeline.config import SPONSORSHIP_REJECT_PHRASES, TOP_COMPANIES_CSV

# Legal suffixes to strip when normalising company names for fuzzy matching
_LEGAL_SUFFIXES = re.compile(
    r"\b("
    r"llc|llp|lp|inc|corp|corporation|ltd|limited|co|company|companies|"
    r"technologies|technology|tech|solutions|services|service|systems|system|"
    r"group|international|global|americas|america|north america|"
    r"us|usa|u\.s\.a?|holdings|holding|ventures|ventures|partners|"
    r"associates|consulting|consultants|staffing|software|"
    r"enterprises|enterprise"
    r")\b[.,]?",
    re.IGNORECASE,
)


def _norm_company(name: str) -> str:
    """
    Normalise a company name for fuzzy matching.

    Strips legal suffixes (LLC, Corp, Inc, etc.), punctuation, and extra spaces
    so that 'Waymo LLC' and 'Waymo' both reduce to 'waymo'.
    """
    n = str(name or "").lower()
    n = _LEGAL_SUFFIXES.sub(" ", n)
    n = re.sub(r"[^\w\s]", " ", n)   # remove remaining punctuation
    n = re.sub(r"\s+", " ", n).strip()
    return n

logger = logging.getLogger(__name__)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_companies(path: Path) -> set[str]:
    """
    Generic loader — reads any CSV with a ``company`` column (or first column).

    Returns a **normalised** lowercase set (legal suffixes stripped) for
    fuzzy matching against LinkedIn company names.
    Returns an empty set (with a warning) if the file does not exist.
    """
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        logger.warning("Company CSV not found at '%s' — filter will be a no-op.", path)
        return set()

    col = "company" if "company" in df.columns else df.columns[0]
    companies = {_norm_company(name) for name in df[col].dropna()}
    companies.discard("")   # drop any empty strings after normalisation
    logger.info("Loaded %d companies from '%s'", len(companies), path)
    return companies


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
    companies = {_norm_company(name) for name in df[col].dropna()}
    companies.discard("")
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
    """Normalised membership check against the top-companies set."""
    return _norm_company(company) in top_companies


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

def filter_by_companies(
    df: pd.DataFrame,
    companies: set[str],
    label: str = "company",
) -> pd.DataFrame:
    """
    Keep only rows whose company is in the provided ``companies`` set.

    Unlike :func:`apply_important_filter` this does NOT apply the sponsorship
    check — it is a pure company-membership filter.

    Args:
        df:        Input DataFrame.
        companies: Lowercase set of company names.
        label:     Name used in the log line (e.g. "top-500", "h1b-2026").

    Returns:
        Filtered DataFrame (new copy).
    """
    if not companies:
        logger.warning("Empty company set for '%s' filter — returning empty.", label)
        return df.iloc[0:0].copy()

    before = len(df)
    if "company" not in df.columns:
        return df.iloc[0:0].copy()
    mask = df["company"].apply(lambda c: _norm_company(c) in companies)
    result = df[mask].copy()
    logger.info("%-20s: %4d → %4d rows", label + " filter", before, len(result))
    return result


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
