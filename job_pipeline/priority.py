"""
Application priority: the order jobs are applied to, and the tags that explain it.

Never a filter. Every eligible job keeps its place in the pool; this only decides
which go first. Rules are compared in order, each beating everything below it:

  1. Strong match       score_pct >= 70
  2. Preferred location  New York, Raleigh or Apex
  3. 0-1 years            minimum 0 years, or a new-grad title
  4. 1-2 years            minimum 1 year
  5. 2-5 years            minimum 2-5 years, or experience not stated
  6. 5+ years             minimum above 5 years

Ties: higher score_pct, then newer posting (consumers sort by
``(priority_group, -score_pct, -date_posted)``).

``priority_group`` is 0 (best) .. 15; ``priority_tags`` lists why, e.g.
["Strong match", "Raleigh", "New grad"].
"""

from __future__ import annotations

import math
import re

PRIORITY_VERSION = 1
STRONG_MATCH_PCT = 70

# (tag, patterns) checked against the lowercased location; the first match wins.
PREFERRED_LOCATIONS: list[tuple[str, list[str]]] = [
    ("New York", ["new york", "nyc", "manhattan", "brooklyn", ", ny"]),
    ("Raleigh", ["raleigh"]),
    ("Apex", ["apex, nc", "apex nc", "apex, north carolina"]),
]

NEW_GRAD = re.compile(r"\b(new[\s-]?grad(uate)?s?|recent grad(uate)?s?|university grad(uate)?|entry[\s-]level|early[\s-]career)\b", re.I)

# Experience bands: (rank, tag). Lower rank = earlier.
_EXP_0_1, _EXP_1_2, _EXP_2_5, _EXP_5_PLUS = 0, 1, 2, 3


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else int(f)


def location_tag(location: str | None) -> str | None:
    loc = (location or "").lower()
    for tag, patterns in PREFERRED_LOCATIONS:
        if any(p in loc for p in patterns):
            return tag
    return None


def experience_band(min_exp, title: str | None) -> tuple[int, str]:
    """Band by the minimum years asked for; a new-grad title counts as 0-1 years."""
    if NEW_GRAD.search(title or ""):
        return _EXP_0_1, "New grad"
    years = _int_or_none(min_exp)
    if years is None:
        return _EXP_2_5, "Exp. not stated"
    if years <= 0:
        return _EXP_0_1, "0–1 yrs"
    if years == 1:
        return _EXP_1_2, "1–2 yrs"
    if years <= 5:
        return _EXP_2_5, "2–5 yrs"
    return _EXP_5_PLUS, "5+ yrs"


def job_priority(score_pct, location: str | None, title: str | None, min_exp) -> dict:
    """``{"priority_group": int, "priority_tags": [str], "priority_version": int}`` for one job."""
    pct = _int_or_none(score_pct) or 0
    strong = pct >= STRONG_MATCH_PCT
    loc = location_tag(location)
    exp_rank, exp_tag = experience_band(min_exp, title)
    group = (0 if strong else 8) + (0 if loc else 4) + exp_rank
    tags = (["Strong match"] if strong else []) + ([loc] if loc else []) + [exp_tag]
    return {"priority_group": group, "priority_tags": tags, "priority_version": PRIORITY_VERSION}
