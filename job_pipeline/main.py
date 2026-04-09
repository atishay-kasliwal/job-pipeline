"""
Entry point for the job pipeline.

Supports five pipeline modes:
  standard  — full filter + score pipeline  → output/jobs.json
  important — curated top companies         → output/important_jobs.json
  top500    — top-500 US tech companies     → output/top500_jobs.json
  h1b2026   — known H1B 2026 sponsors      → output/h1b2026_jobs.json
  keywords  — keyword score ≥ 3            → output/keywords_jobs.json
  all       — runs all five sequentially

Usage examples
--------------
    # Standard pipeline with defaults
    python -m job_pipeline.main

    # Expand search window, remote only
    python -m job_pipeline.main --hours-old 6 --remote

    # High-priority pipeline only
    python -m job_pipeline.main --pipeline important

    # All pipelines, 50 results, no file output
    python -m job_pipeline.main --pipeline all --results 50 --no-save

    # All pipelines + deploy to dashboard
    python -m job_pipeline.main --pipeline all --deploy

    # ATS resume gap analysis for today's top jobs
    python -m job_pipeline.main --ats
    python -m job_pipeline.main --ats --ats-top 5 --ats-threshold 60

    # Debug mode
    python -m job_pipeline.main --log-level DEBUG
"""
import argparse
import fcntl
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from job_pipeline import config
from job_pipeline.more_important import (
    run_h1b2026_pipeline,
    run_important_pipeline,
    run_keywords_pipeline,
    run_top500_pipeline,
)
from job_pipeline.pipeline import run_standard_pipeline
from job_pipeline.scraper import scrape


@contextmanager
def _single_run_lock(lock_path: Path):
    """
    Acquire a non-blocking file lock for the process lifetime of one run.

    Prevents overlapping cron/manual invocations from writing mixed output
    files and run history at the same time.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("a+")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return

        fh.seek(0)
        fh.truncate()
        fh.write(
            f"pid={os.getpid()} started_at={datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        )
        fh.flush()
        yield True
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        fh.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job_pipeline",
        description="Job Aggregation & Ranking System — powered by JobSpy",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--pipeline",
        choices=["standard", "important", "top500", "h1b2026", "keywords", "all"],
        default="standard",
        help=(
            "'standard'  — full filter + scoring pipeline.  "
            "'important' — curated top companies + sponsorship filter.  "
            "'top500'    — top-500 US tech companies.  "
            "'h1b2026'   — custom H1B 2026 company list (data/h1b_2026.csv).  "
            "'all'       — runs all five sequentially."
        ),
    )

    # Scraper overrides
    scraper_group = parser.add_argument_group("Scraper options")
    scraper_group.add_argument(
        "--hours-old",
        type=int,
        default=config.SCRAPER["hours_old"],
        metavar="N",
        help="Include only jobs posted within the last N hours.",
    )
    scraper_group.add_argument(
        "--results",
        type=int,
        default=config.SCRAPER["results_wanted"],
        metavar="N",
        help="Number of LinkedIn results to request.",
    )
    scraper_group.add_argument(
        "--search",
        type=str,
        default=config.SCRAPER["search_term"],
        help="Job search term.",
    )
    scraper_group.add_argument(
        "--location",
        type=str,
        default=config.SCRAPER["location"],
        help="Geographic location for the search.",
    )

    # Filter overrides
    filter_group = parser.add_argument_group("Filter options")
    filter_group.add_argument(
        "--remote",
        action="store_true",
        default=False,
        help="Restrict results to remote positions only.",
    )

    # Output options
    output_group = parser.add_argument_group("Output options")
    output_group.add_argument(
        "--no-save",
        action="store_true",
        default=False,
        help="Do not write CSV/JSON files; print results to stdout only.",
    )
    output_group.add_argument(
        "--deploy",
        action="store_true",
        default=False,
        help=(
            "Push results to the GitHub Pages dashboard. "
            "For '--pipeline all', deploy happens once at the end."
        ),
    )
    output_group.add_argument(
        "--top",
        type=int,
        default=20,
        metavar="N",
        help="Number of top rows to print to stdout.",
    )

    # ATS analysis
    ats_group = parser.add_argument_group("ATS resume analysis")
    ats_group.add_argument(
        "--ats",
        action="store_true",
        default=False,
        help=(
            "Run ATS resume gap analysis instead of scraping. "
            "Reads data/resume.txt and today_jobs.json, calls Claude API "
            "for each top job, saves Markdown reports to output/ats/."
        ),
    )
    ats_group.add_argument(
        "--ats-top",
        type=int,
        default=10,
        metavar="N",
        help="Max number of jobs to analyze (highest score_pct first).",
    )
    ats_group.add_argument(
        "--ats-threshold",
        type=int,
        default=50,
        metavar="PCT",
        help="Minimum score_pct (0-100) required to include a job.",
    )

    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity.",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    lock_path = config.OUTPUT_DIR / ".pipeline.lock"
    with _single_run_lock(lock_path) as got_lock:
        if not got_lock:
            logging.warning(
                "Another pipeline run is already active (lock: %s). Skipping this invocation.",
                lock_path,
            )
            return

        overrides: dict[str, Any] = {
            "hours_old": args.hours_old,
            "results_wanted": args.results,
            "search_term": args.search,
            "location": args.location,
        }

        import pandas as pd
        pd.set_option("display.max_colwidth", 55)
        pd.set_option("display.width", 200)

        def _print(label: str, df: "pd.DataFrame") -> None:
            if not df.empty:
                print(f"\n{'─'*60}")
                print(f"  {label} — top {args.top} results")
                print(f"{'─'*60}")
                print(df.head(args.top).to_string(index=False))
            else:
                print(f"\n{label}: no results.")

        # ATS analysis mode — runs independently, no scraping
        if args.ats:
            from job_pipeline.resume.analyzer import run_ats_analysis
            run_ats_analysis(top=args.ats_top, threshold=args.ats_threshold)
            return

        run_standard  = args.pipeline in ("standard",  "all")
        run_important = args.pipeline in ("important", "all")
        run_top500    = args.pipeline in ("top500",    "all")
        run_h1b2026   = args.pipeline in ("h1b2026",   "all")
        run_keywords  = args.pipeline in ("keywords",  "all")

        # For "all", deploy once at the end to avoid partial dashboard states.
        deploy_each_pipeline = args.deploy and args.pipeline != "all"
        deploy_once_at_end = args.deploy and args.pipeline == "all"
        shared_raw_jobs = None
        if args.pipeline == "all":
            logger = logging.getLogger(__name__)
            logger.info("=" * 55)
            logger.info("  Shared Scrape For --pipeline all — START")
            logger.info("=" * 55)
            try:
                shared_raw_jobs = scrape(overrides)
            except Exception as exc:
                logger.error("Shared scrape failed (continuing with empty data): %s", exc)
                shared_raw_jobs = pd.DataFrame()
            logger.info("=" * 55)
            logger.info(
                "  Shared Scrape For --pipeline all — DONE  (%d raw rows)",
                len(shared_raw_jobs),
            )
            logger.info("=" * 55)

        if run_standard:
            df = run_standard_pipeline(
                scraper_overrides=overrides,
                raw_jobs=shared_raw_jobs,
                remote_only=args.remote,
                save=not args.no_save,
                deploy=deploy_each_pipeline,
            )
            _print("Standard Pipeline", df)

        if run_important:
            df = run_important_pipeline(
                scraper_overrides=overrides,
                raw_jobs=shared_raw_jobs,
                save=not args.no_save,
                deploy=deploy_each_pipeline,
            )
            _print("Important Pipeline", df)

        if run_top500:
            df = run_top500_pipeline(
                scraper_overrides=overrides,
                raw_jobs=shared_raw_jobs,
                save=not args.no_save,
                deploy=deploy_each_pipeline,
            )
            _print("Top-500 Pipeline", df)

        if run_h1b2026:
            df = run_h1b2026_pipeline(
                scraper_overrides=overrides,
                raw_jobs=shared_raw_jobs,
                save=not args.no_save,
                deploy=deploy_each_pipeline,
            )
            _print("H1B-2026 Pipeline", df)

        if run_keywords:
            df = run_keywords_pipeline(
                scraper_overrides=overrides,
                raw_jobs=shared_raw_jobs,
                save=not args.no_save,
                deploy=deploy_each_pipeline,
            )
            _print("Keywords Pipeline", df)

        if deploy_once_at_end:
            from job_pipeline.deploy import deploy_output
            try:
                deploy_output()
            except Exception as exc:
                logging.error("Final dashboard deploy failed (non-fatal): %s", exc)

        # Trigger the export workflow immediately after scraping
        # so data is live within ~2 min instead of waiting for the :30 cron.
        from job_pipeline.trigger_export import trigger_export
        trigger_export()


if __name__ == "__main__":
    main()
