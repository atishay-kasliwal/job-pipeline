# Atriveo Job Intelligence Pipeline

An end-to-end automated system that scrapes, filters, scores, and publishes early-career software engineering job postings — personalized for H1B sponsorship, tech-stack fit, and target company lists — with a live dashboard updated hourly.

**Live dashboard →** https://application.atriveo.com  
**Admin analytics →** https://application.atriveo.com/admin

---

## Demo

> Dashboard with 5 pipeline tabs, real-time search, level/location filters, and hourly run history

Live: **https://application.atriveo.com**

---

## Problem

Job searching for early-career SWE roles is broken:

- LinkedIn surfaces hundreds of irrelevant postings (senior, SDET, security clearance, no sponsorship)
- No built-in H1B sponsorship signal — you find out after applying
- No tech-stack scoring — a React job looks the same as a Kafka + Kubernetes job to a backend engineer
- No aggregated view across company tiers (FAANG vs mid-market vs H1B sponsors)
- The good roles disappear within hours — manual checking doesn't scale

---

## Solution

This pipeline runs **~20 times per day**, fully automated:

- Scrapes LinkedIn across 6 search terms simultaneously
- Runs a 5-stage filter cascade that cuts ~75% of irrelevant results
- Scores remaining jobs by tech-stack keyword match
- Splits results across 5 curated pipelines (Standard, Priority, Top 500, H1B 2026, My Keywords)
- Persists everything to MongoDB Atlas + publishes a live GitHub Pages dashboard via the GitHub Contents API

---

## Architecture

```
macOS cron (~20x/day)
        │
        ▼
  main.py (CLI entry point)
        │
        ├──► scraper.py         LinkedIn via JobSpy — 6 search terms, deduplicated
        │
        ├──► filters.py         5-stage cascade:
        │                         dedup → company filter → role filter
        │                         → sponsorship filter → experience filter
        │
        ├──► scoring.py         Keyword boost scoring (Spring Boot, FastAPI,
        │                         Kafka, K8s, AWS, Python, Java…)
        │
        ├──► pipeline.py        Standard pipeline orchestrator
        ├──► more_important.py  Priority / Top500 / H1B / Keywords pipelines
        │
        ├──► storage.py         MongoDB Atlas writes + run_history.json snapshots
        │
        └──► deploy.py          GitHub Contents API → GitHub Pages dashboard
                                        │
                                        ▼
                          application.atriveo.com
                          (5 tabs · run history · admin analytics)
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Python, [JobSpy](https://github.com/speedyapply/JobSpy), LinkedIn |
| Data pipeline | pandas, custom NLP keyword scoring |
| Storage | MongoDB Atlas (4 collections), JSON snapshots |
| Deploy | GitHub Contents API (no CI runner needed) |
| Frontend | Vanilla JS, CSS custom properties, IBM Plex Mono + Sora |
| Automation | macOS cron, Python venv |
| Auth | GitHub CLI (`gh auth token`) |

---

## Five Pipelines

| Pipeline | Company Filter | Output |
|---|---|---|
| **Standard** | None — all matching roles | `jobs.json` |
| **Priority** | ~112 curated top-tier companies | `important_jobs.json` |
| **Top 500** | 521 top US tech companies | `top500_jobs.json` |
| **H1B 2026** | 1,388 verified H1B 2026 sponsors | `h1b2026_jobs.json` |
| **My Keywords** | None — keyword score ≥ 15 | `keywords_jobs.json` |

---

## Key Features

- **Multi-term scraping** — 6 parallel search terms (`software engineer`, `new grad`, `backend engineer`, `ML engineer`, `data scientist`, `AI engineer`)
- **5-stage filter cascade** — drops senior/staff/lead/QA/clearance roles and explicit sponsorship rejections before scoring
- **H1B-aware ranking** — separate pipeline against 1,388 known 2026 H1B sponsors
- **Keyword scoring engine** — 30+ tech keywords with weighted boosts; top score normalized to 100%
- **Live dashboard** — 5 tabs, search bar, level/location chips, NEW badge, hourly run cards with modal job listings
- **Admin analytics** — hourly heatmap, 7/14/30-day trend chart, pipeline breakdown, last 50 runs table
- **Zero-downtime deploys** — pushes directly to GitHub Pages via Contents API; no GitHub Actions needed
- **ATS resume gap analysis** — optional `--ats` flag scores top jobs against a stored resume

---

## Results / Metrics

- **~480 raw jobs scraped per run** across 6 search terms
- **~75% filtered out** — only ~109 jobs survive the full cascade (signal, not noise)
- **~20 automated runs per day** — 8 AM to 1 AM on the hour, plus a 5 AM overnight run
- **15,000+ job descriptions** stored in MongoDB Atlas across all runs
- **5 company-tier pipelines** covering everything from any company to verified H1B sponsors
- **Sub-15-minute end-to-end runtime** per run (scrape → filter → score → store → deploy)

---

## Installation

### Prerequisites

- macOS (tested on macOS 15 / Darwin 25)
- Python 3.13 — `brew install python@3.13`
- GitHub CLI — `brew install gh`

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/atishay-kasliwal/job-pipeline.git
cd job-pipeline

# 2. Create virtualenv
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure credentials
cp .env.example .env
# Edit .env — add your MongoDB Atlas MONGO_URI

# 4. Authenticate GitHub CLI (for dashboard deploys)
gh auth login

# 5. Run once to verify
python -m job_pipeline.main --pipeline standard --no-save --top 10

# 6. Run all pipelines + deploy
python -m job_pipeline.main --pipeline all --deploy
```

### Cron Setup (automated hourly runs)

```bash
GH_TOKEN=$(gh auth token)
(cat <<EOF
GITHUB_TOKEN=${GH_TOKEN}
0 8-23 * * * cd "$HOME/job-pipeline" && .venv/bin/python -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
0 0,1 * * * cd "$HOME/job-pipeline" && .venv/bin/python -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
0 5 * * * cd "$HOME/job-pipeline" && .venv/bin/python -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
EOF
) | crontab -
```

---

## CLI Reference

```bash
# All pipelines + deploy (standard run)
python -m job_pipeline.main --pipeline all --deploy

# Single pipeline
python -m job_pipeline.main --pipeline h1b2026

# Expand search window
python -m job_pipeline.main --hours-old 6 --results 300

# ATS resume gap analysis
python -m job_pipeline.main --ats --ats-top 5

# Debug
python -m job_pipeline.main --log-level DEBUG
```

---

## Project Structure

```
job-pipeline/
├── job_pipeline/
│   ├── config.py           # All tuneable settings — edit here, nowhere else
│   ├── scraper.py          # JobSpy wrapper + multi-term dedup
│   ├── filters.py          # 5-stage filter cascade + level tagger
│   ├── scoring.py          # Keyword boost scoring engine
│   ├── important_filter.py # Company name normalisation + tier membership
│   ├── pipeline.py         # Standard pipeline orchestrator
│   ├── more_important.py   # Priority / Top500 / H1B / Keywords pipelines
│   ├── storage.py          # MongoDB + run_history.json + per-run snapshots
│   ├── deploy.py           # GitHub Contents API push (no Actions needed)
│   ├── identity.py         # ATS resume gap analysis
│   └── main.py             # CLI entry point
├── data/
│   ├── top_companies.csv       # ~112 curated priority companies
│   ├── top_500_companies.csv   # 521 top US tech companies
│   └── h1b_2026.csv            # 1,388 verified H1B 2026 sponsors
├── docs/                   # GitHub Pages frontend (index.html + data JSONs)
├── output/                 # Auto-created, gitignored — live pipeline output
├── .env.example
└── requirements.txt
```

---

## Future Work

- Resume embedding similarity scoring (beyond keyword matching)
- Email/Slack daily digest of top-ranked new jobs
- Multi-source scraping (Indeed, Greenhouse, Lever)
- Feedback loop — mark applied/rejected to tune scoring weights
- Glassdoor salary data enrichment

---

## Author

**Atishay Kasliwal** — Master's Student, Stony Brook University  
[LinkedIn](https://linkedin.com/in/atishaykasliwal) · [Portfolio](https://atishaykasliwal.com) · [GitHub](https://github.com/atishay-kasliwal)
