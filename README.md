# Atriveo Job Pipeline

A production job aggregation, filtering, and ranking system built on [JobSpy](https://github.com/speedyapply/JobSpy). Scrapes LinkedIn hourly for early-career software engineering roles, filters aggressively for sponsorship-friendly positions, scores by tech-stack fit, and publishes results to a live GitHub Pages dashboard.

**Live dashboard →** https://atriveo-airflow.github.io
**Admin analytics →** https://atriveo-airflow.github.io/admin
**Pipeline repo →** https://github.com/atishay-kasliwal/job-pipeline
**Dashboard repo →** https://github.com/atriveo-airflow/atriveo-airflow.github.io

---

## What It Does

1. Scrapes LinkedIn for `software engineer` postings in the US (posted within the last hour)
2. Filters out: senior/staff/lead/manager roles, SDET/QA roles, jobs requiring 6+ years experience, explicit sponsorship rejections, security clearance requirements
3. Scores remaining jobs by tech-stack match (Spring Boot, FastAPI, Java, Python, AWS, Kubernetes, Kafka, etc.)
4. Runs five parallel pipelines with different company lists
5. Saves results to MongoDB Atlas + GitHub Pages dashboard
6. Runs automatically via macOS cron on an hourly schedule

---

## System Architecture

```
macOS cron (hourly)
       │
       ▼
job_pipeline/main.py  ──── LinkedIn (via JobSpy)
       │
       ├─► filters.py        (role / experience / sponsorship / location)
       ├─► scoring.py        (keyword boosts, staleness penalty)
       ├─► storage.py        (MongoDB Atlas + run_history.json snapshots)
       └─► deploy.py         (GitHub Contents API → GitHub Pages)
                                        │
                                        ▼
                          atriveo-airflow.github.io
                          (5 tabs + hourly run history + admin analytics)
```

### Five pipelines

| Pipeline | Flag | Company filter | Output file |
|---|---|---|---|
| Standard | `standard` | None (all matching) | `jobs.json` |
| Priority | `important` | ~112 curated top companies | `important_jobs.json` |
| Top 500 | `top500` | 521 top US tech companies | `top500_jobs.json` |
| H1B 2026 | `h1b2026` | 1,388 known H1B sponsors | `h1b2026_jobs.json` |
| My Keywords | `keywords` | None (keyword score ≥ 3) | `keywords_jobs.json` |

---

## New Machine Setup

### Prerequisites

- macOS (tested on macOS 15 / Darwin 25)
- Python 3.13 (`brew install python@3.13`)
- Git
- GitHub CLI (`brew install gh`)

---

### Step 1 — Clone both repositories

```bash
# Pipeline (this repo)
git clone https://github.com/atishay-kasliwal/job-pipeline.git "Atriveo Airflow"
cd "Atriveo Airflow"

# Dashboard (separate repo — GitHub Pages)
cd ~
git clone https://github.com/atriveo-airflow/atriveo-airflow.github.io.git atriveo-airflow
```

---

### Step 2 — Create a virtual environment

```bash
cd ~/Atriveo\ Airflow
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

### Step 3 — Create the `.env` file

```bash
cp .env.example .env
```

Edit `.env` and fill in your MongoDB Atlas connection string:

```
MONGO_URI=mongodb+srv://<db_username>:<db_password>@atriveoairflow.lsyhpr5.mongodb.net/?appName=AtriveoAirflow
```

Get the real credentials from MongoDB Atlas → your cluster → **Connect** → **Drivers**.
The cluster name is `atriveoairflow` (already exists — just get the username/password).

---

### Step 4 — Authenticate GitHub CLI

```bash
gh auth login
# Choose: GitHub.com → HTTPS → Login with a web browser
# Follow the browser prompt
```

Verify it works:
```bash
gh auth token   # should print a token starting with gho_...
```

---

### Step 5 — Set up the cron job

The cron schedule runs the pipeline every hour from 8 AM to 1 AM, plus one overnight run at 5 AM.

```bash
# Get your GitHub token (needed for deploy to work in cron's bare environment)
GH_TOKEN=$(gh auth token)

# Install the crontab
(cat <<EOF
GITHUB_TOKEN=${GH_TOKEN}
0 8-23 * * * cd "$HOME/Atriveo Airflow" && .venv/bin/python3 -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
0 0,1 * * * cd "$HOME/Atriveo Airflow" && .venv/bin/python3 -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
0 5 * * * cd "$HOME/Atriveo Airflow" && .venv/bin/python3 -m job_pipeline.main --pipeline all --deploy >> /tmp/atriveo_pipeline.log 2>&1
EOF
) | crontab -

# Verify
crontab -l
```

**Note:** The `GITHUB_TOKEN` line at the top injects the token into every cron process. GitHub tokens can expire — if deploys start failing, re-run this step to refresh the token.

---

### Step 6 — Verify the setup

Run the pipeline once manually to confirm everything works end-to-end:

```bash
cd ~/Atriveo\ Airflow
source .venv/bin/activate
python -m job_pipeline.main --pipeline standard --no-save --top 5
```

If that works, run with deploy to test the full stack:

```bash
python -m job_pipeline.main --pipeline all --deploy
```

Check the live dashboard: https://atriveo-airflow.github.io

---

## Cron Schedule

| Time | What runs |
|---|---|
| 8 AM – 11 PM | Every hour on the hour (`0 8-23 * * *`) |
| Midnight + 1 AM | Two final runs (`0 0,1 * * *`) |
| 5 AM | Single overnight run (`0 5 * * *`) |

**Total: ~20 runs per day.** The 2–4 AM window is skipped (sleep time).

Check logs at any time:
```bash
tail -f /tmp/atriveo_pipeline.log
```

---

## Data Files

```
data/
├── top_companies.csv        # ~112 curated top-tier companies (Priority pipeline)
├── top_500_companies.csv    # 521 US tech companies (Top 500 pipeline)
├── h1b_2026.csv             # 1,388 known H1B 2026 sponsors (H1B pipeline)
└── h1b_sponsors.csv         # Legacy H1B sponsor boost list
```

These CSV files are committed to the repo — no manual download needed.

---

## Configuration

All tunable values are in `job_pipeline/config.py`. Nothing else needs to change for normal use.

| Setting | Purpose |
|---|---|
| `SCRAPER["hours_old"]` | How fresh results must be (default: 1 hour) |
| `SCRAPER["results_wanted"]` | Max LinkedIn results per run (default: 200) |
| `ROLE_INCLUDE_KEYWORDS` | Roles to keep (software engineer, backend, sde…) |
| `ROLE_EXCLUDE_KEYWORDS` | Roles to drop (senior, staff, SDET, QA…) |
| `SPONSORSHIP_REJECT_PHRASES` | Phrases that disqualify a job listing |
| `SCORE_BOOSTS` | Keyword → point boosts (Spring Boot +4, FastAPI +4…) |
| `ALLOWED_STATES` | All 50 US states — location chips on dashboard do client-side filtering |
| `KEYWORDS_MIN_SCORE` | Min keyword score for the My Keywords pipeline (default: 3) |

---

## Dashboard Features

### Main Dashboard (/)

- **5 tabs:** Standard · My Keywords · Priority · Top 500 · H1B 2026
- **Level chips:** All · New Grad · Entry · Mid
- **Location chips:** California · Texas · New York · N. Carolina · Seattle/WA · Remote (client-side, backend pulls all states)
- **Search bar:** filters title, company, location in real time
- **Hourly run cards:** click any card to open a modal with that hour's full job listing
- **NEW badge:** jobs posted within the last 24 hours

### Admin Analytics (/admin/)

- At-a-glance stats: runs today, runs this week, avg jobs/run, peak hour
- **Hourly heatmap:** average jobs scraped per hour of day (sleep window dimmed)
- **Daily trend:** 7 / 14 / 30 day bar chart toggle
- **Pipeline breakdown:** jobs + run counts per pipeline type
- **Recent runs table:** last 50 runs with levels, scores, and top companies

---

## MongoDB Collections

| Collection | Purpose |
|---|---|
| `jobs` | Active job documents, tagged by session_id |
| `sessions` | One metadata doc per run |
| `run_stats` | Human-readable run summaries (easy to browse in Atlas) |
| `archived_jobs` | Jobs older than 7 days (moved by archiver) |

Database name: `job_pipeline`
Cluster: `atriveoairflow.lsyhpr5.mongodb.net`

---

## Troubleshooting

### Deploy fails with "No GitHub token found"
```bash
# Refresh token in crontab
GH_TOKEN=$(gh auth token)
crontab -l | sed "s/GITHUB_TOKEN=.*/GITHUB_TOKEN=${GH_TOKEN}/" | crontab -
```

### MongoDB SSL errors in logs
Non-fatal — a known issue with Python 3.13 + MongoDB's TLS stack. The pipeline continues; jobs are saved locally and deployed to GitHub Pages regardless. Monitor at https://cloud.mongodb.com.

### Pipeline takes 1–2 hours
LinkedIn rate-limits description fetches. The `linkedin_fetch_description: True` setting causes each job URL to be fetched individually. This is expected — overlapping cron runs are harmless (each writes its own output).

### Check if cron is actually running
```bash
tail -50 /tmp/atriveo_pipeline.log
# Look for lines like: "Standard Pipeline — START"
```

### Manually trigger a deploy (without re-scraping)
```bash
cd ~/Atriveo\ Airflow && source .venv/bin/activate
GITHUB_TOKEN=$(gh auth token) python -c "
from job_pipeline.deploy import deploy_output
deploy_output()
"
```

---

## Project Structure

```
Atriveo Airflow/
├── job_pipeline/
│   ├── config.py           # All tunable settings
│   ├── scraper.py          # JobSpy wrapper
│   ├── filters.py          # Role / experience / sponsorship / dedup / level tagger
│   ├── scoring.py          # score + competition_score computation
│   ├── important_filter.py # Company name normalisation + membership filter
│   ├── pipeline.py         # Standard pipeline orchestrator
│   ├── more_important.py   # Priority / Top500 / H1B / Keywords pipelines
│   ├── storage.py          # MongoDB writes + run_history.json + snapshots
│   ├── deploy.py           # GitHub Contents API push
│   └── main.py             # CLI entry point
├── data/
│   ├── top_companies.csv
│   ├── top_500_companies.csv
│   ├── h1b_2026.csv
│   └── h1b_sponsors.csv
├── output/                 # Auto-created; gitignored
│   ├── jobs.json
│   ├── important_jobs.json
│   ├── top500_jobs.json
│   ├── h1b2026_jobs.json
│   ├── keywords_jobs.json
│   ├── run_history.json
│   └── runs/               # Per-run job snapshots (for dashboard click-through)
├── .env                    # NOT committed — contains MONGO_URI
├── .env.example            # Template
└── requirements.txt
```

---

## CLI Reference

```bash
# Run all pipelines + deploy to dashboard
python -m job_pipeline.main --pipeline all --deploy

# Standard only, no file output, print top 10
python -m job_pipeline.main --no-save --top 10

# Expand search to last 6 hours, 300 results
python -m job_pipeline.main --hours-old 6 --results 300

# Remote-only jobs
python -m job_pipeline.main --remote

# Debug logging
python -m job_pipeline.main --log-level DEBUG

# Available pipeline choices: standard | important | top500 | h1b2026 | keywords | all
python -m job_pipeline.main --pipeline keywords
```
