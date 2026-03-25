"""
High-priority pipeline: scrape → deduplicate → role filter →
important filter (top company + sponsorship-ok) → priority score.

Run as a module:
    python -m job_pipeline.more_important
    python -m job_pipeline.more_important --hours-old 3 --results 100
    python -m job_pipeline.more_important --no-save
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.filters import deduplicate, filter_by_experience, filter_by_role, filter_by_sponsorship, tag_level
from job_pipeline.important_filter import (
    apply_important_filter,
    filter_by_companies,
    load_companies,
    load_top_companies,
)
from job_pipeline.scraper import scrape
from job_pipeline.storage import append_run_history, insert_run, insert_run_stats
from job_pipeline.deploy import deploy_output

logger = logging.getLogger(__name__)

# Columns in the important-pipeline output
_OUTPUT_COLUMNS: list[str] = [
    "title",
    "company",
    "location",
    "level",
    "job_url",
    "date_posted",
    "priority_score",
]


# ── Scoring ───────────────────────────────────────────────────────────────────

def _calculate_priority_score(row: pd.Series) -> int:
    """
    Lightweight priority scorer for the high-priority pipeline.

    Boosts come from config.PRIORITY_SCORE_BOOSTS and target
    new-grad / entry-level / backend language in title+description.
    """
    text = " ".join(
        str(row.get(col, "") or "") for col in ("title", "description")
    ).lower()

    return sum(
        boost
        for keyword, boost in config.PRIORITY_SCORE_BOOSTS.items()
        if keyword in text
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_important_pipeline(
    scraper_overrides: dict[str, Any] | None = None,
    output_csv: Path = config.IMPORTANT_OUTPUT_CSV,
    output_json: Path = config.IMPORTANT_OUTPUT_JSON,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Execute the high-priority pipeline end-to-end.

    Pipeline steps
    --------------
    1. **Scrape**           — fetch raw jobs from LinkedIn via JobSpy.
    2. **Deduplicate**      — drop exact URL and title+company duplicates.
    3. **Role filter**      — keep only matching entry-level engineering titles.
    4. **Important filter** — keep only top-company + sponsorship-ok rows.
    5. **Priority score**   — add ``priority_score`` column and sort descending.

    Note: The standard location / sponsorship filters are *not* applied here
    because the important filter already handles sponsorship and top companies
    are assumed to be US-based.

    Args:
        scraper_overrides: Passed to :func:`scraper.scrape`.
        output_csv:        Destination path for important_jobs.csv.
        output_json:       Destination path for important_jobs.json.
        save:              When False, skip writing files.

    Returns:
        DataFrame sorted by ``priority_score`` descending.
    """
    logger.info("=" * 55)
    logger.info("  Important Pipeline — START")
    logger.info("=" * 55)

    # 1. Scrape
    df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper — aborting pipeline.")
        return df

    # 2. Deduplicate
    df = deduplicate(df)

    # 3. Role filter
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed the role filter.")
        return df

    # 4. Clearance / experience filters
    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    if df.empty:
        logger.warning("All jobs filtered by clearance/experience.")
        return df

    # 5. Important filter
    top_companies = load_top_companies()
    df = apply_important_filter(df, top_companies)
    if df.empty:
        logger.warning("No jobs passed the important filter.")
        return df

    # 5. Level tagging
    df = tag_level(df)

    # 6. Priority score
    df = df.copy()
    df["priority_score"] = df.apply(_calculate_priority_score, axis=1)
    df = df.sort_values("priority_score", ascending=False).reset_index(drop=True)

    df_out = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]].copy()

    if save:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_csv, index=False)
        df_out.to_json(output_json, orient="records", indent=2)
        logger.info("Saved %d important jobs → %s", len(df_out), output_csv)
        logger.info("Saved %d important jobs → %s", len(df_out), output_json)

    if store:
        try:
            insert_run(df_out, pipeline="important")
            insert_run_stats(df_out, pipeline="important")
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    if save:
        try:
            history_path = output_csv.parent / "run_history.json"
            append_run_history(df_out, pipeline="important", history_path=history_path)
        except Exception as exc:
            logger.error("run_history.json update failed (non-fatal): %s", exc)

    if deploy:
        try:
            deploy_output()
        except Exception as exc:
            logger.error("Dashboard deploy failed (non-fatal): %s", exc)

    logger.info("=" * 55)
    logger.info("  Important Pipeline — DONE  (%d jobs)", len(df_out))
    logger.info("=" * 55)
    return df_out


# ── Generic company-list pipeline ─────────────────────────────────────────────

def _run_company_list_pipeline(
    company_csv: Path,
    pipeline_name: str,
    output_csv: Path,
    output_json: Path,
    scraper_overrides: dict | None = None,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Shared core for Top-500 and H1B-2026 pipelines.

    Steps: scrape → dedup → role filter → company filter → level tag → score.
    No sponsorship filter is applied here (handled upstream for important;
    for top-500/h1b-2026 the user just wants to see all matching openings).
    """
    logger.info("=" * 55)
    logger.info("  %s Pipeline — START", pipeline_name)
    logger.info("=" * 55)

    companies = load_companies(company_csv)
    if not companies:
        logger.error("No companies loaded from '%s' — aborting.", company_csv)
        return pd.DataFrame()

    df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        return df

    df = deduplicate(df)
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed role filter.")
        return df

    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    df = filter_by_companies(df, companies, label=pipeline_name)
    if df.empty:
        logger.warning("No jobs matched the company list.")
        return df

    df = tag_level(df)
    df["priority_score"] = df.apply(_calculate_priority_score, axis=1)
    df = df.sort_values("priority_score", ascending=False).reset_index(drop=True)

    cols = [c for c in _OUTPUT_COLUMNS if c in df.columns]
    df_out = df[cols].copy()

    if save:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_csv, index=False)
        df_out.to_json(output_json, orient="records", indent=2)
        logger.info("Saved %d jobs → %s", len(df_out), output_json)
        try:
            history_path = output_csv.parent / "run_history.json"
            append_run_history(df_out, pipeline=pipeline_name, history_path=history_path)
        except Exception as exc:
            logger.error("run_history.json update failed (non-fatal): %s", exc)

    if store:
        try:
            insert_run(df_out, pipeline=pipeline_name)
            insert_run_stats(df_out, pipeline=pipeline_name)
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    if deploy:
        try:
            deploy_output()
        except Exception as exc:
            logger.error("Dashboard deploy failed (non-fatal): %s", exc)

    logger.info("=" * 55)
    logger.info("  %s Pipeline — DONE  (%d jobs)", pipeline_name, len(df_out))
    logger.info("=" * 55)
    return df_out


def run_top500_pipeline(
    scraper_overrides: dict | None = None,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """Run the Top-500 US tech companies pipeline."""
    return _run_company_list_pipeline(
        company_csv=config.TOP_500_COMPANIES_CSV,
        pipeline_name="top500",
        output_csv=config.TOP500_OUTPUT_CSV,
        output_json=config.TOP500_OUTPUT_JSON,
        scraper_overrides=scraper_overrides,
        save=save,
        store=store,
        deploy=deploy,
    )


def run_h1b2026_pipeline(
    scraper_overrides: dict | None = None,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Run the custom H1B-2026 companies pipeline.

    Expects ``data/h1b_2026.csv`` with a ``company`` column.
    Gracefully returns an empty DataFrame if the file doesn't exist yet.
    """
    if not config.H1B_2026_CSV.exists():
        logger.warning(
            "H1B 2026 CSV not found at '%s' — drop your CSV there and re-run.",
            config.H1B_2026_CSV,
        )
        return pd.DataFrame()

    return _run_company_list_pipeline(
        company_csv=config.H1B_2026_CSV,
        pipeline_name="h1b2026",
        output_csv=config.H1B2026_OUTPUT_CSV,
        output_json=config.H1B2026_OUTPUT_JSON,
        scraper_overrides=scraper_overrides,
        save=save,
        store=store,
        deploy=deploy,
    )


def run_keywords_pipeline(
    scraper_overrides: dict | None = None,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Resume-matched keywords pipeline.

    Keeps only jobs whose title+description contain at least one keyword from
    config.RESUME_KEYWORDS AND whose total keyword score meets the minimum
    threshold (config.KEYWORDS_MIN_SCORE).

    Each keyword that appears in the text contributes its SCORE_BOOSTS value;
    the job is kept only if the cumulative keyword score ≥ KEYWORDS_MIN_SCORE.
    """
    logger.info("=" * 55)
    logger.info("  Keywords Pipeline — START")
    logger.info("=" * 55)

    df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        return df

    df = deduplicate(df)
    df = filter_by_role(df)
    if df.empty:
        return df

    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    if df.empty:
        return df

    df = tag_level(df)

    # Keyword scoring — sum of SCORE_BOOSTS for matched keywords
    boosts = config.SCORE_BOOSTS

    def _keyword_score(row: pd.Series) -> int:
        text = " ".join(
            str(row.get(c, "") or "") for c in ("title", "description")
        ).lower()
        return sum(v for k, v in boosts.items() if k in text)

    df = df.copy()
    df["keyword_score"] = df.apply(_keyword_score, axis=1)

    # Keep only jobs that actually match the resume
    before = len(df)
    df = df[df["keyword_score"] >= config.KEYWORDS_MIN_SCORE].copy()
    logger.info("Keywords filter    : %4d → %4d rows (min score=%d)",
                before, len(df), config.KEYWORDS_MIN_SCORE)

    if df.empty:
        logger.warning("No jobs met the keyword threshold.")
        return df

    df = df.sort_values("keyword_score", ascending=False).reset_index(drop=True)

    out_cols = ["title", "company", "location", "level", "job_url",
                "date_posted", "keyword_score"]
    df_out = df[[c for c in out_cols if c in df.columns]].copy()

    if save:
        config.KEYWORDS_OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(config.KEYWORDS_OUTPUT_CSV, index=False)
        df_out.to_json(config.KEYWORDS_OUTPUT_JSON, orient="records", indent=2)
        logger.info("Saved %d keyword-matched jobs → %s",
                    len(df_out), config.KEYWORDS_OUTPUT_JSON)
        try:
            history_path = config.KEYWORDS_OUTPUT_CSV.parent / "run_history.json"
            append_run_history(df_out, pipeline="keywords", history_path=history_path)
        except Exception as exc:
            logger.error("run_history.json update failed (non-fatal): %s", exc)

    if store:
        try:
            insert_run(df_out, pipeline="keywords")
            insert_run_stats(df_out, pipeline="keywords")
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    if deploy:
        try:
            deploy_output()
        except Exception as exc:
            logger.error("Dashboard deploy failed (non-fatal): %s", exc)

    logger.info("=" * 55)
    logger.info("  Keywords Pipeline — DONE  (%d jobs)", len(df_out))
    logger.info("=" * 55)
    return df_out


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="High-priority job pipeline — top companies + visa-friendly roles.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--hours-old",
        type=int,
        default=config.SCRAPER["hours_old"],
        metavar="N",
        help="Include only jobs posted within the last N hours.",
    )
    parser.add_argument(
        "--results",
        type=int,
        default=config.SCRAPER["results_wanted"],
        metavar="N",
        help="Number of LinkedIn results to request.",
    )
    parser.add_argument(
        "--search",
        type=str,
        default=config.SCRAPER["search_term"],
        help="Job search term.",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write output files; print results only.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    overrides: dict[str, Any] = {
        "hours_old": args.hours_old,
        "results_wanted": args.results,
        "search_term": args.search,
    }

    result = run_important_pipeline(
        scraper_overrides=overrides,
        save=not args.no_save,
    )

    if result.empty:
        print("No important jobs found.")
    else:
        pd.set_option("display.max_colwidth", 60)
        print(result.to_string(index=False))
