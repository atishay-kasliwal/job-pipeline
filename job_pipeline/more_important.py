"""
High-priority pipeline: scrape → deduplicate → role filter →
important filter (top company + sponsorship-ok) → priority score.

Run as a module:
    python -m job_pipeline.more_important
    python -m job_pipeline.more_important --hours-old 3 --results 100
    python -m job_pipeline.more_important --no-save
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.filters import deduplicate, extract_exp_range, filter_by_company, filter_by_experience, filter_by_role, filter_by_sponsorship, tag_level
from job_pipeline.scoring import apply_scores
from job_pipeline.important_filter import (
    apply_important_filter,
    filter_by_companies,
    load_companies,
    load_top_companies,
)
from job_pipeline.scraper import scrape
from job_pipeline.storage import append_run_history, insert_run, insert_run_stats
from job_pipeline.deploy import deploy_output
from job_pipeline.pipeline import _make_summary

logger = logging.getLogger(__name__)

# Columns in the important-pipeline output
_OUTPUT_COLUMNS: list[str] = [
    "title",
    "company",
    "location",
    "level",
    "min_exp",
    "max_exp",
    "job_url",
    "date_posted",
    "priority_score",
    "score_pct",
    "site",
    "summary",
]


def _persist_pipeline_results(
    df_out: pd.DataFrame,
    pipeline_name: str,
    output_csv: Path,
    output_json: Path,
    save: bool,
    store: bool,
    deploy: bool,
) -> None:
    """
    Persist outputs for one non-standard pipeline run.

    Called for both non-empty and empty runs so dashboard files always reflect
    the latest execution (instead of showing stale data from an older hour).
    """
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
            sid = insert_run(df_out, pipeline=pipeline_name)
            if sid:
                insert_run_stats(df_out, pipeline=pipeline_name, session_id=sid)
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    if deploy:
        try:
            deploy_output()
        except Exception as exc:
            logger.error("Dashboard deploy failed (non-fatal): %s", exc)




# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_important_pipeline(
    scraper_overrides: dict[str, Any] | None = None,
    raw_jobs: pd.DataFrame | None = None,
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
        raw_jobs:          Optional pre-scraped raw DataFrame. When provided,
                           this pipeline reuses it instead of scraping again.
        output_csv:        Destination path for important_jobs.csv.
        output_json:       Destination path for important_jobs.json.
        save:              When False, skip writing files.

    Returns:
        DataFrame sorted by ``priority_score`` descending.
    """
    logger.info("=" * 55)
    logger.info("  Important Pipeline — START")
    logger.info("=" * 55)

    # 1. Scrape (or reuse pre-scraped raw jobs)
    if raw_jobs is not None:
        df = raw_jobs.copy()
        logger.info("Using shared pre-scraped dataset (%d raw rows).", len(df))
    else:
        df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="important",
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 2. Deduplicate
    df = deduplicate(df)

    # 3. Role filter
    df = filter_by_company(df)
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed the role filter.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="important",
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 4. Clearance / experience filters
    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    if df.empty:
        logger.warning("All jobs filtered by clearance/experience.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="important",
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 5. Important filter
    top_companies = load_top_companies()
    df = apply_important_filter(df, top_companies)
    if df.empty:
        logger.warning("No jobs passed the important filter.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="important",
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    # 5. Level tagging + experience extraction + scoring
    df = tag_level(df)
    df = extract_exp_range(df)
    df = apply_scores(df)
    df["priority_score"] = df["score"]
    df["summary"] = df["description"].apply(_make_summary) if "description" in df.columns else ""
    df = df.sort_values("priority_score", ascending=False).reset_index(drop=True)

    df_out = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]].copy()
    _persist_pipeline_results(
        df_out=df_out,
        pipeline_name="important",
        output_csv=output_csv,
        output_json=output_json,
        save=save,
        store=store,
        deploy=deploy,
    )

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
    raw_jobs: pd.DataFrame | None = None,
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
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name=pipeline_name,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    if raw_jobs is not None:
        df = raw_jobs.copy()
        logger.info("Using shared pre-scraped dataset (%d raw rows).", len(df))
    else:
        df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name=pipeline_name,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = deduplicate(df)
    df = filter_by_company(df)
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed role filter.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name=pipeline_name,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    df = filter_by_companies(df, companies, label=pipeline_name)
    if df.empty:
        logger.warning("No jobs matched the company list.")
        empty_out = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name=pipeline_name,
            output_csv=output_csv,
            output_json=output_json,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = tag_level(df)
    df = extract_exp_range(df)
    df = apply_scores(df)
    df["priority_score"] = df["score"]
    df["summary"] = df["description"].apply(_make_summary) if "description" in df.columns else ""
    df = df.sort_values("priority_score", ascending=False).reset_index(drop=True)

    cols = [c for c in _OUTPUT_COLUMNS if c in df.columns]
    df_out = df[cols].copy()
    _persist_pipeline_results(
        df_out=df_out,
        pipeline_name=pipeline_name,
        output_csv=output_csv,
        output_json=output_json,
        save=save,
        store=store,
        deploy=deploy,
    )

    logger.info("=" * 55)
    logger.info("  %s Pipeline — DONE  (%d jobs)", pipeline_name, len(df_out))
    logger.info("=" * 55)
    return df_out


def run_top500_pipeline(
    scraper_overrides: dict | None = None,
    raw_jobs: pd.DataFrame | None = None,
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
        raw_jobs=raw_jobs,
        save=save,
        store=store,
        deploy=deploy,
    )


def run_h1b2026_pipeline(
    scraper_overrides: dict | None = None,
    raw_jobs: pd.DataFrame | None = None,
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
        raw_jobs=raw_jobs,
        save=save,
        store=store,
        deploy=deploy,
    )


def run_keywords_pipeline(
    scraper_overrides: dict | None = None,
    raw_jobs: pd.DataFrame | None = None,
    save: bool = True,
    store: bool = True,
    deploy: bool = False,
) -> pd.DataFrame:
    """
    Resume-matched keywords pipeline.

    Keeps only jobs whose title+description contain at least one keyword from
    config.RESUME_KEYWORDS AND whose total keyword score meets the minimum
    threshold (config.KEYWORDS_MIN_SCORE).

    Each keyword that appears in the text contributes its PERSONAL_STACK weight;
    the job is kept only if the cumulative keyword score ≥ KEYWORDS_MIN_SCORE.
    """
    logger.info("=" * 55)
    logger.info("  Keywords Pipeline — START")
    logger.info("=" * 55)

    out_cols = [
        "title",
        "company",
        "location",
        "level",
        "min_exp",
        "job_url",
        "date_posted",
        "keyword_score",
        "site",
    ]

    if raw_jobs is not None:
        df = raw_jobs.copy()
        logger.info("Using shared pre-scraped dataset (%d raw rows).", len(df))
    else:
        df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper.")
        empty_out = pd.DataFrame(columns=out_cols)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="keywords",
            output_csv=config.KEYWORDS_OUTPUT_CSV,
            output_json=config.KEYWORDS_OUTPUT_JSON,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = deduplicate(df)
    df = filter_by_company(df)
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed role filter.")
        empty_out = pd.DataFrame(columns=out_cols)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="keywords",
            output_csv=config.KEYWORDS_OUTPUT_CSV,
            output_json=config.KEYWORDS_OUTPUT_JSON,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = filter_by_sponsorship(df)
    df = filter_by_experience(df)
    if df.empty:
        logger.warning("All jobs filtered by sponsorship/experience.")
        empty_out = pd.DataFrame(columns=out_cols)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="keywords",
            output_csv=config.KEYWORDS_OUTPUT_CSV,
            output_json=config.KEYWORDS_OUTPUT_JSON,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = tag_level(df)
    df = extract_exp_range(df)

    # Keyword scoring — uses new PERSONAL_STACK weights from scoring module
    from job_pipeline.scoring import keyword_score as _ks
    df = df.copy()
    df["keyword_score"] = df.apply(
        lambda row: _ks(
            " ".join(str(row.get(c, "") or "") for c in ("title", "description")).lower()
        ),
        axis=1,
    )

    # Keep only jobs that actually match the resume
    before = len(df)
    df = df[df["keyword_score"] >= config.KEYWORDS_MIN_SCORE].copy()
    logger.info("Keywords filter    : %4d → %4d rows (min score=%d)",
                before, len(df), config.KEYWORDS_MIN_SCORE)

    if df.empty:
        logger.warning("No jobs met the keyword threshold.")
        empty_out = pd.DataFrame(columns=out_cols)
        _persist_pipeline_results(
            df_out=empty_out,
            pipeline_name="keywords",
            output_csv=config.KEYWORDS_OUTPUT_CSV,
            output_json=config.KEYWORDS_OUTPUT_JSON,
            save=save,
            store=store,
            deploy=deploy,
        )
        return empty_out

    df = df.sort_values("keyword_score", ascending=False).reset_index(drop=True)

    df_out = df[[c for c in out_cols if c in df.columns]].copy()
    _persist_pipeline_results(
        df_out=df_out,
        pipeline_name="keywords",
        output_csv=config.KEYWORDS_OUTPUT_CSV,
        output_json=config.KEYWORDS_OUTPUT_JSON,
        save=save,
        store=store,
        deploy=deploy,
    )

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
