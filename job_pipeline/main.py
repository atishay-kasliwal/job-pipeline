"""
Entry point for the job pipeline.

Supports three pipeline modes:
  standard  — full filter + score pipeline  → output/jobs.csv / jobs.json
  important — top-company + sponsorship     → output/important_jobs.csv / .json
  both      — runs both pipelines in sequence

Usage examples
--------------
    # Standard pipeline with defaults
    python -m job_pipeline.main

    # Expand search window, remote only
    python -m job_pipeline.main --hours-old 6 --remote

    # High-priority pipeline only
    python -m job_pipeline.main --pipeline important

    # Both pipelines, 50 results, no file output
    python -m job_pipeline.main --pipeline both --results 50 --no-save

    # Debug mode
    python -m job_pipeline.main --log-level DEBUG
"""
import argparse
import logging
import sys
from typing import Any

from job_pipeline import config
from job_pipeline.more_important import run_important_pipeline
from job_pipeline.pipeline import run_standard_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job_pipeline",
        description="Job Aggregation & Ranking System — powered by JobSpy",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--pipeline",
        choices=["standard", "important", "both"],
        default="standard",
        help=(
            "Which pipeline to run.  "
            "'standard' applies full filters + scoring.  "
            "'important' keeps only top companies with visa-friendly postings.  "
            "'both' runs both sequentially."
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
        "--top",
        type=int,
        default=20,
        metavar="N",
        help="Number of top rows to print to stdout.",
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

    overrides: dict[str, Any] = {
        "hours_old": args.hours_old,
        "results_wanted": args.results,
        "search_term": args.search,
        "location": args.location,
    }

    import pandas as pd
    pd.set_option("display.max_colwidth", 55)
    pd.set_option("display.width", 200)

    if args.pipeline in ("standard", "both"):
        df = run_standard_pipeline(
            scraper_overrides=overrides,
            remote_only=args.remote,
            save=not args.no_save,
        )
        if not df.empty:
            print(f"\n{'─'*60}")
            print(f"  Standard Pipeline — top {args.top} results")
            print(f"{'─'*60}")
            print(df.head(args.top).to_string(index=False))
        else:
            print("\nStandard Pipeline: no results.")

    if args.pipeline in ("important", "both"):
        df = run_important_pipeline(
            scraper_overrides=overrides,
            save=not args.no_save,
        )
        if not df.empty:
            print(f"\n{'─'*60}")
            print(f"  Important Pipeline — top {args.top} results")
            print(f"{'─'*60}")
            print(df.head(args.top).to_string(index=False))
        else:
            print("\nImportant Pipeline: no results.")


if __name__ == "__main__":
    main()
