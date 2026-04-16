"""
Scoring engine — personal stack matching, synergy bonuses, experience fit,
recency, and H1B signal.

calculate_score()   — all signals → raw int + score_pct (0-100) + competition
apply_scores()      — vectorised application across a full DataFrame
"""
import logging
from datetime import date, datetime, timezone

import pandas as pd

from job_pipeline.config import (
    BIG_TECH_COMPANIES,
    H1B_SCORE_BONUS,
    TOP500_SCORE_BONUS,
    LEVEL_SCORES,
    PERSONAL_STACK,
    SCORE_MAX_RAW,
    SYNERGY_COMBOS,
    H1B_2026_CSV,
    TOP_500_COMPANIES_CSV,
)

logger = logging.getLogger(__name__)

# Module-level H1B set — loaded once from h1b_2026.csv.
_h1b_companies: set[str] | None = None

# Module-level Top-500 set — loaded once from top_500_companies.csv.
_top500_companies: set[str] | None = None


def _get_h1b_companies() -> set[str]:
    global _h1b_companies
    if _h1b_companies is None:
        try:
            from job_pipeline.important_filter import load_h1b_sponsors
            _h1b_companies = load_h1b_sponsors(H1B_2026_CSV)
        except Exception as exc:
            logger.warning("Could not load H1B company list: %s", exc)
            _h1b_companies = set()
    return _h1b_companies


def _get_top500_companies() -> set[str]:
    global _top500_companies
    if _top500_companies is None:
        try:
            from job_pipeline.important_filter import load_top_companies
            _top500_companies = load_top_companies(TOP_500_COMPANIES_CSV)
        except Exception as exc:
            logger.warning("Could not load Top-500 company list: %s", exc)
            _top500_companies = set()
    return _top500_companies


# ── Helpers ───────────────────────────────────────────────────────────────────

def _combined_text(row: pd.Series) -> str:
    return " ".join(
        str(row.get(c, "") or "") for c in ("title", "description")
    ).lower()


def _hours_since_posted(row: pd.Series) -> float:
    posted = row.get("date_posted")
    if posted is None or pd.isna(posted):
        return 0.0
    if isinstance(posted, str):
        try:
            posted = pd.to_datetime(posted, utc=True)
        except Exception:
            return 0.0
    if isinstance(posted, date) and not isinstance(posted, datetime):
        posted = datetime(posted.year, posted.month, posted.day, tzinfo=timezone.utc)
    if hasattr(posted, "tzinfo") and posted.tzinfo is None:
        posted = posted.replace(tzinfo=timezone.utc)
    return max((datetime.now(tz=timezone.utc) - posted).total_seconds() / 3600, 0.0)


def _to_int_or_none(val) -> "int | None":
    try:
        if val is None or pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _is_big_tech(row: pd.Series) -> bool:
    company = str(row.get("company", "") or "").lower()
    return any(bt in company for bt in BIG_TECH_COMPANIES)


def _is_h1b_sponsor(row: pd.Series) -> bool:
    company = str(row.get("company", "") or "").strip().lower()
    return company in _get_h1b_companies()


def _is_top500(row: pd.Series) -> bool:
    from job_pipeline.important_filter import _norm_company
    company = str(row.get("company", "") or "")
    return _norm_company(company) in _get_top500_companies()


# ── Scoring components ────────────────────────────────────────────────────────

def keyword_score(text: str) -> int:
    """Binary presence match — each keyword scores once regardless of frequency."""
    score = 0
    for category in PERSONAL_STACK.values():
        for keyword, weight in category.items():
            if keyword in text:
                score += weight
    return score


def synergy_bonus(text: str) -> int:
    """Extra points when a full tech combo appears together."""
    bonus = 0
    for keywords, points in SYNERGY_COMBOS:
        if all(k in text for k in keywords):
            bonus += points
    return bonus


def experience_score(min_exp: "int | None", max_exp: "int | None") -> int:
    """
    Score based on how well the experience requirement fits ~2 years.

    Sweet spot (range includes 2) → 10
    1-year max role              →  8
    Exactly 3-year min           →  6
    Min > 3 (too senior)         →  0
    No exp data                  →  0
    Anything else                →  4
    """
    if min_exp is None and max_exp is None:
        return 0
    if (min_exp is None or min_exp <= 2) and (max_exp is None or max_exp >= 2):
        return 10
    if max_exp is not None and max_exp == 1:
        return 8
    if min_exp is not None and min_exp == 3:
        return 6
    if min_exp is not None and min_exp > 3:
        return 0
    return 4


def level_score(title: str) -> int:
    """Points based on seniority keywords in the job title."""
    title = title.lower()
    for level, pts in LEVEL_SCORES.items():
        if level in title:
            return pts
    return 4  # unlabelled → assume entry-ish


def recency_score(hours_old: float) -> int:
    """Freshness bonus — decays sharply after 24 hours."""
    if hours_old < 6:
        return 10
    if hours_old < 12:
        return 8
    if hours_old < 24:
        return 5
    if hours_old < 48:
        return 2
    return -5


def source_score(source: str) -> int:
    return {"linkedin": 2, "company": 3}.get(source.lower(), 0)


def should_skip(text: str) -> bool:
    """
    Hard discard: senior-only roles, extreme experience requirements,
    or no overlap with the target stack at all.
    """
    if "staff" in text or "principal" in text:
        return True
    if any(f"{n}+ years" in text for n in range(4, 20)):
        return True
    if not any(k in text for k in ("java", "python", "backend", "api")):
        return True
    return False


# ── Public API ────────────────────────────────────────────────────────────────

def calculate_score(row: pd.Series) -> dict:
    """
    Compute all score signals for one job row.

    Returns a dict with:
      score            — raw integer (may be -999 for hard-filtered jobs)
      score_pct        — 0-100 normalised against SCORE_MAX_RAW
      competition_score — estimated applicant competition (higher = worse)
    """
    text = _combined_text(row)

    if should_skip(text):
        return {"score": -999, "score_pct": 0, "competition_score": 0}

    ks = keyword_score(text)
    sb = synergy_bonus(text)
    es = experience_score(
        _to_int_or_none(row.get("min_exp")),
        _to_int_or_none(row.get("max_exp")),
    )
    ls = level_score(str(row.get("title", "") or ""))
    age = _hours_since_posted(row)
    rs = recency_score(age)
    ss = source_score(str(row.get("site", "") or ""))

    big_tech = _is_big_tech(row)
    h1b      = _is_h1b_sponsor(row)
    top500   = _is_top500(row)

    raw = (
        ks
        + sb
        + es
        + ls
        + rs
        + ss
        + (2 if big_tech else 0)
        + (H1B_SCORE_BONUS if h1b else 0)
        + (TOP500_SCORE_BONUS if top500 else 0)
    )
    pct = min(100, max(0, round(raw / SCORE_MAX_RAW * 100)))

    competition = (
        (5 if big_tech else 0)
        + (5 if age > 48 else 2 if age > 24 else 0)
    )

    return {"score": raw, "score_pct": pct, "competition_score": competition}


def apply_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``score``, ``score_pct``, and ``competition_score`` columns to *df*,
    then sort by ``score`` descending.
    """
    df = df.copy()
    results = df.apply(calculate_score, axis=1, result_type="expand")
    df["score"]             = results["score"]
    df["score_pct"]         = results["score_pct"]
    df["competition_score"] = results["competition_score"]
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    logger.info(
        "Scoring complete — best: %d%% (%d raw), median raw: %.1f",
        df["score_pct"].max() if not df.empty else 0,
        df["score"].max()     if not df.empty else 0,
        df["score"].median()  if not df.empty else 0,
    )
    return df
