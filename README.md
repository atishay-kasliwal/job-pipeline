# Job Pipeline

A production-quality **job aggregation, filtering, and ranking system** built on top of [JobSpy](https://github.com/speedyapply/JobSpy), tailored specifically for early-career software engineers seeking US-based roles with visa sponsorship.

---

## Why This Exists

LinkedIn shows you hundreds of job postings. Most are:

- **Too senior** — requiring 5+ years of experience
- **Sponsorship-hostile** — explicitly excluding visa candidates
- **From large companies** — where your application competes against thousands

This pipeline cuts through the noise by:

1. Scraping fresh postings automatically
2. Filtering aggressively for entry-level / new-grad engineering roles
3. Discarding listings that explicitly reject sponsorship
4. Scoring remaining jobs by tech-stack fit and recency
5. Surfacing a **high-priority list** of top-company roles that are sponsorship-friendly

---

## Features

| Feature | Details |
|---|---|
| LinkedIn scraping | Via JobSpy — no API key required |
| Role filter | Keeps `software engineer`, `backend`, `sde`; drops `senior`, `manager`, `lead`, etc. |
| Location filter | US states + remote; configurable |
| Sponsorship filter | Rejects explicit "no sponsorship / citizens only" language; keeps neutral |
| Remote toggle | `--remote` flag for remote-only results |
| Scoring system | Keyword boosts (AWS, Kubernetes, new grad) + staleness / competition penalties |
| Competition score | Proxy estimate using company size, posting age, title specificity |
| **High-priority pipeline** | Top-company list × sponsorship-friendly = your best opportunities |
| Priority scoring | Separate lightweight scorer for the important pipeline |
| Deduplication | By URL first, then by title + company |
| Config-driven | All keywords, weights, and paths live in `config.py` |
| CLI interface | `argparse`-based with `--pipeline`, `--hours-old`, `--results`, `--remote`, etc. |
| Dual output | CSV and JSON for every pipeline |
| Logging | Structured, level-configurable logging throughout |

---

## Project Structure

```
.
├── job_pipeline/
│   ├── __init__.py
│   ├── config.py           # All tunable settings — edit here, not in code
│   ├── scraper.py          # JobSpy wrapper
│   ├── filters.py          # Role / location / sponsorship / remote filters
│   ├── scoring.py          # Score + competition_score computation
│   ├── important_filter.py # Top-company + sponsorship-ok predicate
│   ├── pipeline.py         # Standard pipeline orchestrator
│   ├── more_important.py   # High-priority pipeline + CLI
│   └── main.py             # Primary CLI entry point
├── data/
│   ├── top_companies.csv   # ~100 curated top US tech companies
│   └── h1b_sponsors.csv    # Known H1B-sponsoring companies (optional boost)
├── output/                 # Auto-created; pipeline writes here
│   ├── jobs.csv
│   ├── jobs.json
│   ├── important_jobs.csv
│   └── important_jobs.json
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone & create a virtual environment

```bash
git clone https://github.com/your-username/job-pipeline.git
cd job-pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Usage

All commands are run from the project root.

### Standard pipeline (full filter + scoring)

```bash
python -m job_pipeline.main
```

Outputs: `output/jobs.csv` and `output/jobs.json`

### High-priority pipeline (top companies + sponsorship-friendly)

```bash
python -m job_pipeline.main --pipeline important
```

Outputs: `output/important_jobs.csv` and `output/important_jobs.json`

### Run both pipelines

```bash
python -m job_pipeline.main --pipeline both
```

### Common options

```bash
# Expand search window to 6 hours, request 100 results
python -m job_pipeline.main --hours-old 6 --results 100

# Remote-only results
python -m job_pipeline.main --remote

# Different search term
python -m job_pipeline.main --search "backend engineer"

# Print results only, no files written
python -m job_pipeline.main --no-save

# Show top 30 instead of 20
python -m job_pipeline.main --top 30

# Verbose debugging
python -m job_pipeline.main --log-level DEBUG
```

### Run the important pipeline directly

```bash
python -m job_pipeline.more_important
python -m job_pipeline.more_important --hours-old 3 --results 50
```

---

## Output Schema

### Standard pipeline (`jobs.csv` / `jobs.json`)

| Column | Description |
|---|---|
| `title` | Job title |
| `company` | Hiring company |
| `location` | City, state (or Remote) |
| `job_url` | Direct LinkedIn URL |
| `date_posted` | Posting datetime |
| `score` | Relevance score (higher is better) |
| `competition_score` | Estimated competition (higher = more applicants) |

### Important pipeline (`important_jobs.csv` / `important_jobs.json`)

Same as above, with `score` and `competition_score` replaced by:

| Column | Description |
|---|---|
| `priority_score` | Weighted score for new-grad / backend / AWS signals |

---

## Scoring System

### Standard score boosts

| Signal | Points |
|---|---|
| Mentions AWS | +2 |
| Mentions Spring / Kubernetes | +2 each |
| Mentions Docker | +1 |
| Mentions Java | +1 |
| "new grad" / "new graduate" | +3 |
| "entry level" / "0-2 years" | +2 |

### Standard score penalties

| Signal | Points |
|---|---|
| Posted > 24 hours ago | −2 |
| Big-tech company (high competition) | −2 |

### Priority score (important pipeline)

| Signal | Points |
|---|---|
| "new grad" / "new graduate" | +3 |
| "backend" in title/description | +2 |
| AWS mentioned | +2 |
| "entry level" / "0-2 years" | +2 |

---

## Configuration

All settings live in `job_pipeline/config.py`. Key sections:

```python
SCRAPER = {
    "hours_old": 1,         # How fresh results must be
    "results_wanted": 200,  # LinkedIn result count
    ...
}

ROLE_INCLUDE_KEYWORDS = ["software engineer", "backend", "sde", ...]
ROLE_EXCLUDE_KEYWORDS = ["senior", "manager", "lead", ...]

SPONSORSHIP_REJECT_PHRASES = ["no sponsorship", "us citizen only", ...]

SCORE_BOOSTS = {"aws": 2, "new grad": 3, ...}
```

To add a company to the top-company list, simply add a row to `data/top_companies.csv` — no code change required.

---

## Extending the Pipeline

### Add a new filter

```python
# filters.py
def filter_by_salary(df: pd.DataFrame, min_salary: int = 100_000) -> pd.DataFrame:
    """Keep jobs with stated salary above min_salary."""
    ...
```

Then wire it into `pipeline.py` after the existing filters.

### Add a new scoring signal

```python
# config.py
SCORE_BOOSTS["rust"] = 2
SCORE_BOOSTS["golang"] = 2
```

No code changes needed — the scoring loop reads directly from the config dict.

### Swap in a different job source

JobSpy supports Indeed, Glassdoor, and ZipRecruiter in addition to LinkedIn. To include them:

```python
# config.py
SCRAPER["site_name"] = ["linkedin", "indeed", "glassdoor"]
```

---

## Future Improvements

- [ ] **Resume matching** — score jobs against a parsed resume using TF-IDF or embeddings
- [ ] **Salary extraction** — parse salary ranges from descriptions using regex / NLP
- [ ] **Email digest** — send a daily HTML summary of top-scored jobs
- [ ] **Database storage** — persist results in SQLite to track jobs across runs
- [ ] **De-duplicate across runs** — skip jobs already seen in previous scrapes
- [ ] **Streamlit dashboard** — interactive UI for browsing and filtering results
- [ ] **Slack / Discord bot** — push high-priority jobs to a channel in real time
- [ ] **Apply tracking** — mark jobs as applied, interviewing, rejected

---

## License

MIT
