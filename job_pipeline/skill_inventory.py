"""
Approved skills inventory, exported by atriveo-app:

    node scripts/sync-skill-inventory.mjs --pipeline-dir ~/job-pipeline

Only the export's ``skills`` list is read here: skills Atishay approved with open-source
PR evidence. Since 2026-11-16 that includes open PRs (the work was done); each skill
says whether merged evidence backs it (``merged``), and merged evidence comes first.
A closed PR never reaches the list. The wider ``vocabulary`` list is the app's matching
dictionary, not a claim of experience.

Used for job search (which roles to keep and how to rank them) and for ATS advice.
It never counts toward a resume's ATS keyword score: that compares the JD with the
resume text only (export_static.compute_job_scores).
"""
from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path

from job_pipeline import config

logger = logging.getLogger(__name__)

# Bare "go" is everyday English. Count it only in a technical context:
# "golang", "Go developer", "in Go", "Python, Go", "Go/Rust", "(Go)".
_GO_RE = re.compile(
    r"\bgolang\b"
    r"|\bgo\s+(?:lang|programming|language|developers?|engineers?|services?|code|modules?|routines?)\b"
    r"|(?:\b(?:in|with|using|written in)\s+|[,/(]\s*)go\b(?![-\s]+(?:to|live|ahead|beyond|on|through|back|forward)\b)"
    r"|(?<!the )(?<!the-)(?<!to )\bgo\s*(?:[,/)])",
    re.IGNORECASE,
)


def _term_regex(term: str) -> re.Pattern:
    if term.lower() == "go":
        return _GO_RE
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", re.IGNORECASE)


@lru_cache(maxsize=4)
def _load(path: str) -> tuple[dict, ...]:
    file = Path(path)
    if not file.exists():
        logger.warning("Approved skills inventory not found at %s; scoring uses PERSONAL_STACK only.", file)
        return ()
    try:
        data = json.loads(file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("Could not read approved skills inventory %s: %s", file, exc)
        return ()
    skills = []
    for skill in data.get("skills") or []:
        sources = [s for s in skill.get("sources") or [] if isinstance(s, str) and s]
        terms = [t for t in skill.get("terms") or [skill.get("name")] if t]
        if skill.get("name") and sources and terms:
            skills.append({
                "name": skill["name"],
                "category": skill.get("category", ""),
                # Older exports were merged-only and had no flag.
                "merged": skill.get("merged", True),
                "terms": terms,
                "sources": sources,
                "patterns": [_term_regex(t) for t in terms],
            })
    return tuple(skills)


def approved_skills(path: Path | str | None = None) -> tuple[dict, ...]:
    return _load(str(path or config.SKILL_INVENTORY_JSON))


def approved_hits(text: str, path: Path | str | None = None) -> list[dict]:
    """Approved skills the text mentions (whole-word, Go only in a technical context)."""
    return [s for s in approved_skills(path) if any(p.search(text or "") for p in s["patterns"])]


def skills_context_for_jd(jd: str, path: Path | str | None = None) -> str:
    """Prompt block for ATS advice: Skills-section additions only, never resume coverage."""
    hits = approved_hits(jd, path)
    if not hits:
        return ""
    lines = [f"{s['name']}: {', '.join(s['sources'][:3])}" for s in hits]
    return (
        "\n\n--- APPROVED SKILLS BANK (separate from the resume above) ---\n"
        + "\n".join(lines)
        + "\nThese are backed by merged open-source PRs. Suggest them only as Skills-section additions; "
        "do not attribute them to a past employer, invent bullets, or count them in the Before score."
    )
