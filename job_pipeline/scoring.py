"""
Scoring and competition-estimation logic.

``calculate_score``       — keyword-based relevance score for a single row.
``calculate_competition`` — proxy competition estimate for a single row.
``apply_scores``          — vectorised application across a full DataFrame.
"""
import logging
from datetime import datetime, timezone

import pandas as pd

from job_pipeline.config import BIG_TECH_COMPANIES, SCORE_BOOSTS, SCORE_PENALTIES

logger = logging.getLogger(__name__)

# Niche keywords that suggest a less-generic (lower-competition) role
_NICHE_TITLE_KEYWORDS: list[str] = [
    "ml", "machine learning", "embedded", "compiler",
    "distributed", "security", "graphics", "mobile", "ios", "android",
    "platform", "infrastructure", "devops", "sre",
]


# ── Private helpers ───────────────────────────────────────────────────────────

def _combined_text(row: pd.Series) -> str:
    """Lower-case concat of title and description for keyword matching."""
    return " ".join(
        str(row.get(c, "") or "") for c in ("title", "description")
    ).lower()


def _hours_since_posted(row: pd.Series) -> float:
    """
    Return the number of hours since the job was posted.

    Returns 0.0 if ``date_posted`` is missing or un-parseable so that
    neutral rows are not penalised.
    """
    posted = row.get("date_posted")
    if posted is None or pd.isna(posted):
        return 0.0

    if isinstance(posted, str):
        try:
            posted = pd.to_datetime(posted, utc=True)
        except Exception:
            return 0.0

    # Ensure timezone-aware for arithmetic
    if hasattr(posted, "tzinfo") and posted.tzinfo is None:
        posted = posted.replace(tzinfo=timezone.utc)

    delta = datetime.now(tz=timezone.utc) - posted
    return max(delta.total_seconds() / 3600, 0.0)


def _is_big_tech(row: pd.Series) -> bool:
    """Return True if the company is in the big-tech list."""
    company = str(row.get("company", "") or "").lower()
    return any(bt in company for bt in BIG_TECH_COMPANIES)


# ── Public scoring functions ──────────────────────────────────────────────────

def calculate_score(row: pd.Series) -> int:
    """
    Compute a relevance/priority score for a single job row.

    Positive signals: technology keywords new-grad / entry-level language.
    Negative signals: stale posting, big-tech company (higher competition).

    Returns:
        Integer score (can be negative).
    """
    text = _combined_text(row)
    score = 0

    for keyword, boost in SCORE_BOOSTS.items():
        if keyword in text:
            score += boost

    if _hours_since_posted(row) > 24:
        score += SCORE_PENALTIES["stale_job"]

    if _is_big_tech(row):
        score += SCORE_PENALTIES["big_tech"]

    return score


def calculate_competition(row: pd.Series) -> int:
    """
    Estimate competition level as a proxy integer (higher = more applicants).

    Signals used (applicant count is not available from LinkedIn scrape):
    - Big-tech company          → +3
    - Posting age > 48 h        → +3  (many applicants already)
    - Posting age 24–48 h       → +2
    - Generic title (no niche)  → +1
    """
    score = 0

    if _is_big_tech(row):
        score += 3

    age = _hours_since_posted(row)
    if age > 48:
        score += 3
    elif age > 24:
        score += 2

    title = str(row.get("title", "") or "").lower()
    if not any(kw in title for kw in _NICHE_TITLE_KEYWORDS):
        score += 1  # generic / broad title

    return score


def apply_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``score`` and ``competition_score`` columns to *df*, then sort by
    ``score`` descending.

    Returns a new DataFrame; the input is not modified.
    """
    df = df.copy()
    df["score"] = df.apply(calculate_score, axis=1)
    df["competition_score"] = df.apply(calculate_competition, axis=1)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    logger.info(
        "Scoring complete — top: %s, bottom: %s, median: %.1f",
        df["score"].max() if not df.empty else "N/A",
        df["score"].min() if not df.empty else "N/A",
        df["score"].median() if not df.empty else 0,
    )
    return df
