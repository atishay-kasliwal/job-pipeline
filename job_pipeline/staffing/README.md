# Daily staffing sources

Scrapy checks the 41 sources in `sources.json` daily at **07:00 America/New_York**.
The private service runs independently of the hourly LinkedIn/ATS collector. The
server remains available for the UI while each crawl runs in a separate process.
Runs have a database lease, a 20-minute crawl deadline, bounded pages per source,
robots handling, per-domain delay and automatic throttling. No CAPTCHA bypass or
login crawling is attempted.

The generic connector reads JobPosting JSON-LD, discovers public career/job links
and job sitemaps, and supports discovered Greenhouse, Lever and Ashby boards.
`needs_connector` means readable pages did not expose structured job data; it is
not a claim that the company has no jobs. JS-only boards require dedicated APIs
or selectors. Of the 31 originally unreadable staffing boards, 24 have extraction adapters;
seven remain access checks because their public boards deny server
requests. `blocked` records robots/access restrictions, and `access_pending`
records a board awaiting provider access. Source coverage and
request counts are shown in the Staffing page.

`staffing_sources` contains enable flags and crawl status, `staffing_jobs` contains
normalized listings, and `staffing_runs` contains run history. URL identities and
content fingerprints group repeat and identical agency listings. Expired postings
are retained for history but hidden from the active results. Eligible records use
the pipeline's existing company/role/location/sponsorship/experience filters and
scoring; they are upserted into `jobs` without replacing resume state. Descriptions
and sessions feed existing exports. No application is submitted or approved.

## Run

```
pip install -r requirements.txt
export MONGO_URI=...
export TAILOR_TOKEN=...
python -m job_pipeline.staffing.service
```

The service listens on port 8791. It must remain private; every HTTP endpoint
requires the shared `X-Tailor-Token`. The Node sidecar exposes fixed `/staffing/*`
endpoints after its existing authentication, and the signed-in Apply UI relays
through its existing `/tailor` proxy. Set `STAFFING_SERVICE_URL` for local work.

For deployment, build `Dockerfile.staffing` and run the service on the existing
`atriveo-production` Docker network using the private dashboard environment file.
Do not publish its port. Use container name `atriveo-staffing`, restart policy
`unless-stopped`, and a bounded memory/CPU allocation. An automatic first crawl
runs after 07:00 if today's daily run has not been recorded. A manual run can be
requested at any time through the UI. Disabled sources are skipped.

## Verify

```
pip install -r requirements-dev.txt
python -m pytest tests/test_staffing.py tests/test_staffing_connectors.py -q
```

Triangle Startups is also checked daily from its public `/jobs` page. Its own
upstream employer-board cache may be several days old. The connector reads
public server-rendered records, resolves shared employer/description references,
and retains actual employers and original job URLs; it does not call restricted
API paths. Relevant roles use the same eligibility rules as other sources.

Adapters use verified public job feeds (JobDiva, Sourceflow, Azure search,
DataFrenzy, Phenom, public search APIs), published job sitemaps, or exact job-detail
links. Public search client query credentials are read at runtime and are never
stored in source records. The default budget is 150 requests per source; exceeding
that budget or encountering errors marks the run partial. The UI labels limited
source crawls and access restrictions; a partial crawl does not imply zero jobs.
