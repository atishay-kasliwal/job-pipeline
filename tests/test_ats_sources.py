"""Run: .venv/bin/python -m unittest discover -s tests"""
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from job_pipeline.ats_identity import add_ats_identity, parse_posting_identity
from job_pipeline.identity import canonical_job_url
from job_pipeline.sources.discover_boards import token_from_url
from job_pipeline.sources.greenhouse import collect_boards, html_to_text


class IdentityTests(unittest.TestCase):
    def test_same_greenhouse_posting_same_key(self):
        urls = [
            "https://job-boards.greenhouse.io/stripe/jobs/8172487",
            "https://boards.greenhouse.io/stripe/jobs/8172487?gh_src=linkedin",
            "https://boards.greenhouse.io/embed/job_app?for=stripe&token=8172487",
            "https://stripe.com/jobs/search?gh_jid=8172487",
        ]
        self.assertEqual({parse_posting_identity(u)["application_key"] for u in urls}, {"greenhouse:8172487"})
        self.assertEqual(parse_posting_identity(urls[0])["ats_board"], "stripe")

    def test_other_ats(self):
        uid = "6ed76ce8-4156-4b60-b120-403538bd66cd"
        self.assertEqual(parse_posting_identity(f"https://jobs.lever.co/palantir/{uid}/apply")["application_key"], f"lever:{uid}")
        self.assertEqual(parse_posting_identity(f"https://jobs.ashbyhq.com/ramp/{uid}/application")["application_key"], f"ashby:{uid}")
        self.assertEqual(
            parse_posting_identity("https://acme.wd5.myworkdayjobs.com/en-US/External/job/NY/Software-Engineer_R12345")["application_key"],
            "workday:acme:R12345")
        self.assertIsNone(parse_posting_identity("https://www.linkedin.com/jobs/view/123"))
        self.assertIsNone(parse_posting_identity(None))

    def test_add_ats_identity_prefers_job_url_then_direct(self):
        df = add_ats_identity(pd.DataFrame([
            {"job_url": "https://www.linkedin.com/jobs/view/1", "job_url_direct": "https://job-boards.greenhouse.io/x/jobs/5"},
            {"job_url": "https://www.linkedin.com/jobs/view/2", "job_url_direct": None},
        ]))
        self.assertEqual(df.loc[0, "application_key"], "greenhouse:5")
        self.assertIsNone(df.loc[1, "application_key"])

    def test_canonical_url_keeps_posting_params_only(self):
        a = canonical_job_url("https://stripe.com/jobs/search?gh_jid=1&utm_source=x")
        b = canonical_job_url("https://stripe.com/jobs/search?gh_jid=2")
        self.assertNotEqual(a, b)
        self.assertEqual(a, "https://stripe.com/jobs/search?gh_jid=1")
        self.assertEqual(canonical_job_url("https://acme.com/jobs/1/?ref=abc"), "https://acme.com/jobs/1")
        self.assertEqual(canonical_job_url("https://www.linkedin.com/jobs/view/123/?trk=x"), "linkedin:123")


class _Resp:
    def __init__(self, status, body):
        self.status_code, self._body, self.ok = status, body, 200 <= status < 300
    def json(self):
        return self._body
    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(self.status_code)


class _Session:
    def __init__(self, routes):
        self.routes, self.calls = routes, []
    def get(self, url, **_):
        self.calls.append(url)
        return self.routes.get(url, _Resp(404, {}))


class GreenhouseSourceTests(unittest.TestCase):
    def test_html_to_text(self):
        self.assertEqual(html_to_text("&lt;p&gt;Build &amp;amp; ship&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Python&lt;/li&gt;&lt;/ul&gt;"), "Build & ship\nPython")

    def test_emits_only_new_matching_postings_since_last_poll(self):
        now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
        api = "https://boards-api.greenhouse.io/v1/boards/acme"
        iso = lambda h: (now - timedelta(hours=h)).isoformat()  # noqa: E731
        session = _Session({
            f"{api}/jobs": _Resp(200, {"jobs": [
                {"id": 1, "title": "Software Engineer", "first_published": iso(1), "location": {"name": "New York, NY"}},
                {"id": 2, "title": "Software Engineer II", "first_published": iso(5), "location": {"name": "Remote - US"}},
                {"id": 3, "title": "Account Executive", "first_published": iso(1), "location": {"name": "New York, NY"}},
            ]}),
            f"{api}/jobs/1": _Resp(200, {"company_name": "Acme", "content": "&lt;p&gt;Python and Kafka&lt;/p&gt;"}),
            f"{api}/jobs/2": _Resp(200, {"company_name": "Acme", "content": ""}),
        })
        polled = []
        df = collect_boards([{"token": "acme", "last_polled_at": now - timedelta(hours=2)}], now=now, session=session,
                            on_polled=lambda *a: polled.append(a))
        self.assertEqual(df["job_url"].tolist(), ["https://job-boards.greenhouse.io/acme/jobs/1"])
        row = df.iloc[0]
        self.assertEqual((row["site"], row["company"], row["description"], row["date_posted"]), ("greenhouse", "Acme", "Python and Kafka", "2026-09-29"))
        self.assertNotIn(f"{api}/jobs/3", session.calls)  # role filter runs before detail fetches
        self.assertEqual(polled, [("acme", "ok", 3, 1)])

    def test_first_poll_uses_window_and_gone_boards_are_reported(self):
        now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
        polled = []
        collect_boards([{"token": "gone"}], now=now, session=_Session({}), on_polled=lambda *a: polled.append(a))
        self.assertEqual(polled, [("gone", "not_found", 0, 0)])


class DiscoveryTests(unittest.TestCase):
    def test_token_from_url(self):
        self.assertEqual(token_from_url("https://job-boards.greenhouse.io/discord/jobs/1"), "discord")
        self.assertIsNone(token_from_url("https://boards.greenhouse.io/embed/job_app?for=x"))
        self.assertIsNone(token_from_url("https://example.com/discord"))


if __name__ == "__main__":
    unittest.main()


class LeverAshbySourceTests(unittest.TestCase):
    def test_lever_rows(self):
        from job_pipeline.sources import lever
        now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
        ms = lambda h: int((now - timedelta(hours=h)).timestamp() * 1000)  # noqa: E731
        uid = "6ed76ce8-4156-4b60-b120-403538bd66cd"
        session = _Session({"https://api.lever.co/v0/postings/acme": _Resp(200, [
            {"id": uid, "text": "Backend Engineer", "createdAt": ms(1), "categories": {"location": "New York, NY"},
             "descriptionPlain": "Build APIs", "hostedUrl": f"https://jobs.lever.co/acme/{uid}", "applyUrl": f"https://jobs.lever.co/acme/{uid}/apply"},
            {"id": "old", "text": "Software Engineer", "createdAt": ms(50), "categories": {}},
        ])})
        # _Session ignores query params: route on the bare URL
        session.get = (lambda orig: (lambda url, **kw: orig(url)))(session.get)
        df = lever.collect_boards([{"token": "acme"}], now=now, session=session)
        self.assertEqual(df["job_url_direct"].tolist(), [f"https://jobs.lever.co/acme/{uid}/apply"])
        self.assertEqual(parse_posting_identity(df.iloc[0]["job_url_direct"])["application_key"], f"lever:{uid}")

    def test_ashby_rows(self):
        from job_pipeline.sources import ashby
        now = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)
        uid = "34413f8d-26bf-4bbc-8ade-eb309a0e2245"
        session = _Session({"https://api.ashbyhq.com/posting-api/job-board/ramp": _Resp(200, {"jobs": [
            {"id": uid, "title": "Software Engineer", "publishedAt": (now - timedelta(hours=2)).isoformat(), "location": "New York",
             "isListed": True, "descriptionPlain": "Ship", "jobUrl": f"https://jobs.ashbyhq.com/ramp/{uid}", "applyUrl": f"https://jobs.ashbyhq.com/ramp/{uid}/application"},
            {"id": "hidden", "title": "Software Engineer", "publishedAt": now.isoformat(), "isListed": False},
        ]})})
        df = ashby.collect_boards([{"token": "ramp", "company_name": "Ramp"}], now=now, session=session)
        self.assertEqual(df["company"].tolist(), ["Ramp"])
        self.assertEqual(parse_posting_identity(df.iloc[0]["job_url_direct"])["application_key"], f"ashby:{uid}")

    def test_lever_token_and_backfill_parser(self):
        from job_pipeline.backfill_apply_urls import apply_url_from_html
        self.assertEqual(token_from_url("https://jobs.lever.co/palantir/abc", "lever"), "palantir")
        html = '<code id="applyUrl" style="display:none"><!--"https://www.linkedin.com/jobs/view/externalApply/1?url=https%3A%2F%2Fjob-boards.greenhouse.io%2Facme%2Fjobs%2F9&urlHash=x"--></code>'
        self.assertEqual(apply_url_from_html(html), "https://job-boards.greenhouse.io/acme/jobs/9")

    def test_repairs_linkedin_suffix_on_job_url_direct(self):
        self.assertEqual(parse_posting_identity("https://job-boards.greenhouse.io/acme/jobs/9&urlHash=Ab1&trk=x")["application_key"], "greenhouse:9")
        self.assertEqual(parse_posting_identity("https://boards.greenhouse.io/acme/jobs/9?gh_src=z&urlHash=Ab1")["application_key"], "greenhouse:9")


class GreenhouseDetailFailureTest(unittest.TestCase):
    def test_timeout_on_detail_only_drops_the_description(self):
        import requests
        from job_pipeline.sources import greenhouse

        class Session:
            def get(self, url, headers=None, timeout=None):
                raise requests.exceptions.ReadTimeout("slow")

        self.assertIsNone(greenhouse.job_detail(Session(), "acme", 1, 1))


class BackfillCapTest(unittest.TestCase):
    def test_keeps_each_companys_best_rows(self):
        import pandas as pd
        from job_pipeline.sources.backfill import cap_per_company

        df = pd.DataFrame({"company": ["A", "A", "a", "B"], "score": [1, 3, 2, 5], "title": ["x", "y", "z", "w"]})
        kept = cap_per_company(df, 2)
        self.assertEqual(sorted(kept["title"]), ["w", "y", "z"])
        self.assertEqual(len(cap_per_company(df, 0)), 4)
