"""
ATS Resume Gap Analyzer.

For each high-scoring job, compares your resume against the job description
using the Claude API and outputs targeted changes to reach 90%+ ATS score.

Usage
-----
    # Analyze top 10 jobs at ≥50% match from today_jobs.json
    python -m job_pipeline.main --ats

    # Analyze top 5 jobs at ≥60% match
    python -m job_pipeline.main --ats --ats-top 5 --ats-threshold 60

Prerequisites
-------------
    1. Set ANTHROPIC_API_KEY in your environment (or .env file).
    2. Paste your resume text into data/resume.txt.
    3. Run the pipeline at least once so output/descriptions.json is populated.
"""
from __future__ import annotations
import json
import logging
import os
import re
from pathlib import Path

import pandas as pd

from job_pipeline import config

logger = logging.getLogger(__name__)

RESUME_PATH = config.DATA_DIR / "resume.txt"
DESCRIPTIONS_PATH = config.OUTPUT_DIR / "descriptions.json"
TODAY_JOBS_PATH = config.OUTPUT_DIR / "today_jobs.json"
ATS_OUTPUT_DIR = config.OUTPUT_DIR / "ats"

_SYSTEM_PROMPT = """\
You are an expert ATS (Applicant Tracking System) resume optimizer and technical recruiter.

Your task: compare a candidate's resume against a specific job description and identify the \
EXACT changes needed to reach 90%+ ATS score while keeping the resume human-readable, honest, \
and fitting on one page.

Rules:
- Only suggest changes that are truthful given the candidate's actual experience
- Focus on keyword alignment, not fabrication
- Prioritize the top 3-5 most impactful changes
- Be specific: quote the exact existing bullet and show the rewritten version
- Keep language concise and actionable

Output format (use exactly these headers):
## ATS Score Estimate
Before: X% | After (with changes): Y%

## Missing High-Impact Keywords
(keywords/phrases present in JD but absent from resume, ranked by impact)

## Bullet Rewrites
(up to 3 bullets from the resume to update — show BEFORE → AFTER)

## Quick Wins
(1-3 sentence summary of the most impactful changes to prioritize first)
"""


def _load_resume() -> str:
    if not RESUME_PATH.exists():
        raise FileNotFoundError(
            f"Resume not found at {RESUME_PATH}.\n"
            "Create data/resume.txt with your plain-text resume."
        )
    text = RESUME_PATH.read_text().strip()
    if not text:
        raise ValueError(f"{RESUME_PATH} is empty — paste your resume text there.")
    return text


def _load_jobs(jobs_path: Path | None = None) -> list[dict]:
    path = jobs_path or TODAY_JOBS_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"Jobs file not found: {path}\n"
            "Run the pipeline first to generate today_jobs.json."
        )
    return json.loads(path.read_text())


def _load_descriptions() -> dict[str, str]:
    if not DESCRIPTIONS_PATH.exists():
        return {}
    return json.loads(DESCRIPTIONS_PATH.read_text())


def _safe_filename(title: str, company: str) -> str:
    raw = f"{company}_{title}".lower()
    return re.sub(r"[^a-z0-9_-]", "_", raw)[:80] + ".md"


def _analyze_one(client, resume: str, job: dict, description: str) -> str:
    title   = job.get("title", "Unknown")
    company = job.get("company", "Unknown")
    score_pct = job.get("score_pct", 0)

    user_message = (
        f"Job Title: {title}\n"
        f"Company: {company}\n"
        f"Pipeline Match Score: {score_pct}%\n\n"
        f"--- JOB DESCRIPTION ---\n{description}\n\n"
        f"--- MY RESUME ---\n{resume}\n"
    )

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1500,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text


def run_ats_analysis(
    top: int = 10,
    threshold: int = 50,
    jobs_path: Path | None = None,
) -> None:
    """
    Run ATS gap analysis for the top-scoring jobs that have descriptions.

    Args:
        top:        Maximum number of jobs to analyze.
        threshold:  Minimum score_pct (0-100) a job must have to be analyzed.
        jobs_path:  Override path to jobs JSON (defaults to today_jobs.json).
    """
    try:
        import anthropic
    except ImportError:
        raise ImportError(
            "anthropic package not installed.\n"
            "Run: pip install anthropic"
        )

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not set.\n"
            "Add it to your .env file or export it in your shell."
        )

    resume       = _load_resume()
    jobs         = _load_jobs(jobs_path)
    descriptions = _load_descriptions()

    if not descriptions:
        logger.warning(
            "descriptions.json is empty or missing — run the pipeline first "
            "so job descriptions are captured."
        )
        return

    eligible = [
        j for j in jobs
        if (j.get("score_pct") or 0) >= threshold
        and j.get("job_url") in descriptions
    ]
    eligible.sort(key=lambda j: j.get("score_pct") or 0, reverse=True)
    selected = eligible[:top]

    if not selected:
        logger.warning(
            "No jobs found above threshold=%d%% with saved descriptions. "
            "Lower --ats-threshold or run the pipeline again.",
            threshold,
        )
        return

    logger.info(
        "ATS analysis: %d/%d eligible jobs (threshold=%d%%, top=%d)",
        len(selected), len(eligible), threshold, top,
    )

    client = anthropic.Anthropic(api_key=api_key)
    ATS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for i, job in enumerate(selected, 1):
        title     = job.get("title", "Unknown")
        company   = job.get("company", "Unknown")
        url       = job.get("job_url", "")
        score_pct = job.get("score_pct", 0)

        logger.info("[%d/%d] %s @ %s  (%d%%)", i, len(selected), title, company, score_pct)

        try:
            analysis = _analyze_one(client, resume, job, descriptions[url])
        except Exception as exc:
            logger.error("Claude API failed for '%s' @ %s: %s", title, company, exc)
            continue

        header = (
            f"# ATS Analysis: {title} @ {company}\n\n"
            f"**Match Score:** {score_pct}%  |  "
            f"**URL:** {url}\n\n---\n\n"
        )

        out_path = ATS_OUTPUT_DIR / _safe_filename(title, company)
        out_path.write_text(header + analysis)
        logger.info("Saved → %s", out_path.name)

    logger.info("Done. Results in %s/", ATS_OUTPUT_DIR)
