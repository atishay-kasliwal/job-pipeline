# Dedicated connector coverage

Verified October 7, 2026 against public live job boards. Of the original 31 boards,
24 returned complete job records; seven await provider access. Triangle Startups
is an additional source. This validation used a 35-request budget per source;
production allows 150 requests for dedicated adapters (40 for generic discovery).
Counts include repeated records across searches and are not unique-job totals.

| Source | Public board adapter | Live result | Matching records |
| --- | --- | --- | ---: |
| TEKsystems | phenom | Readable | 1 |
| Insight Global | insight | Readable; limited crawl | 33 |
| Randstad Digital | job sitemap | Readable; limited crawl | 34 |
| Robert Half | public html | Readable | 11 |
| Kforce | kforce azure | Readable | 138 |
| Motion Recruitment | motion | Readable; limited crawl | 26 |
| Artech | jobdiva | Readable | 713 |
| CyberCoders | cyber | Readable; limited crawl | 31 |
| Yoh | access check | Provider access required | 0 |
| Eliassen Group | access check | Provider access required | 0 |
| Collabera | public html | Readable; limited crawl | 15 |
| Dexian | access check | Provider access required | 0 |
| Beacon Hill | public html | Readable; limited crawl | 6 |
| The Judge Group | judge | Readable; limited crawl | 206 |
| KellyMitchell | sourceflow | Readable | 169 |
| Pyramid Consulting | sprockets | Readable | 2 |
| Compunnel | compunnel | Readable; limited crawl | 30 |
| Mastech Digital | datafrenzy | Readable | 12 |
| Genesis10 | jobdiva | Readable | 149 |
| Mondo | access check | Provider access required | 0 |
| Tundra Technical Solutions | public html | Readable; limited crawl | 11 |
| Ampcus | jobdiva | Readable | 247 |
| Nesco Resource | jobdiva | Readable | 23 |
| Aquent | access check | Provider access required | 0 |
| Apollo Technical | access check | Provider access required | 0 |
| MATRIX Resources | motion | Readable; limited crawl | 26 |
| Consulting Solutions | access check | Provider access required | 0 |
| Kelly | public html | Readable | 12 |
| Adecco | job sitemap | Readable | 4 |
| Manpower | manpower | Readable | 16 |
| Medix | medix | Readable | 8 |
| Triangle Startups | triangle | Readable | 31 |

Seven access-restricted sites also returned HTTP 403 when tested from the Oracle
deployment server. They remain daily access checks; they are not working extraction
adapters. No robots exclusions, authentication gates, or CAPTCHAs are bypassed.

Randstad uses the permitted job sitemap and Randstad Digital detail paths under
`/jobs/4/`. Its category search paths are excluded by robots.txt and are not crawled.
Adecco uses its US English job sitemap and the original linked job details.

Artech and Nesco use public JobDiva feeds for the tenants linked by their official
candidate portals. Ampcus discovers its public JobDiva tenant from the embedded
job board. Feed reads do not access candidate or employee account data.
