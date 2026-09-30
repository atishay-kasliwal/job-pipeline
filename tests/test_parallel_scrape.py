"""The scrape fans out LinkedIn searches and the ATS poll without changing what comes back."""
import sys
import threading
import time
import types
import unittest
from unittest import mock

import pandas as pd

# jobspy is only needed to talk to LinkedIn; the tests replace _scrape_one.
sys.modules.setdefault("jobspy", types.SimpleNamespace(scrape_jobs=lambda **_: pd.DataFrame()))

from job_pipeline import config, scraper  # noqa: E402


def _rows(term: str) -> pd.DataFrame:
    return pd.DataFrame([{"title": f"{term} job", "company": "Acme", "location": "NY", "job_url": f"https://www.linkedin.com/jobs/view/{abs(hash(term)) % 10**8}", "site": "linkedin"}])


class ParallelScrapeTest(unittest.TestCase):
    def setUp(self):
        self.terms = ["a", "b", "c", "d"]
        patch_terms = mock.patch.object(scraper, "SEARCH_TERMS", self.terms)
        patch_terms.start()
        self.addCleanup(patch_terms.stop)
        rot = mock.patch.object(scraper, "_rotate_tor_ip", return_value=False)
        rot.start()
        self.addCleanup(rot.stop)

    def _run(self, workers: int, ats_concurrent: bool, one, ats):
        with mock.patch.object(config, "LINKEDIN_WORKERS", workers), \
             mock.patch.object(config, "ATS_CONCURRENT", ats_concurrent), \
             mock.patch.object(scraper, "_scrape_one", side_effect=one), \
             mock.patch.object(scraper, "_collect_ats", side_effect=ats):
            return scraper.scrape()

    def test_one_worker_keeps_the_original_order_and_runs_ats_last(self):
        order: list[str] = []
        def one(params, rotate=True):
            order.append(params["search_term"]); return _rows(params["search_term"])
        def ats():
            order.append("ats"); return pd.DataFrame()
        df = self._run(1, False, one, ats)
        self.assertEqual(order, ["a", "b", "c", "d", "ats"])
        self.assertEqual(list(df["search_term"].dropna()), ["a", "b", "c", "d"])

    def test_workers_run_searches_together_but_results_keep_term_order(self):
        live = 0; peak = 0; lock = threading.Lock()
        def one(params, rotate=True):
            nonlocal live, peak
            with lock:
                live += 1; peak = max(peak, live)
            time.sleep(0.05 * (4 - self.terms.index(params["search_term"])))  # earlier terms finish last
            with lock:
                live -= 1
            return _rows(params["search_term"])
        df = self._run(3, False, one, lambda: pd.DataFrame())
        self.assertGreaterEqual(peak, 2)
        self.assertLessEqual(peak, 3)
        self.assertEqual(list(df["search_term"].dropna()), ["a", "b", "c", "d"])

    def test_ats_is_polled_while_linkedin_is_still_running(self):
        ats_started = threading.Event(); saw_ats_during_linkedin = []
        def one(params, rotate=True):
            saw_ats_during_linkedin.append(ats_started.wait(timeout=2)); return _rows(params["search_term"])
        def ats():
            ats_started.set(); return _rows("ats")
        df = self._run(1, True, one, ats)
        self.assertTrue(all(saw_ats_during_linkedin))
        self.assertEqual(len(df), 5)

    def test_a_failing_ats_poll_never_drops_linkedin_results(self):
        def one(params, rotate=True): return _rows(params["search_term"])
        boom = types.SimpleNamespace(collect_ats_jobs=mock.Mock(side_effect=RuntimeError("boards down")))
        with mock.patch.object(config, "LINKEDIN_WORKERS", 2), mock.patch.object(config, "ATS_CONCURRENT", True), \
             mock.patch.object(scraper, "_scrape_one", side_effect=one), \
             mock.patch.dict(sys.modules, {"job_pipeline.sources": boom}):
            df = scraper.scrape()
        self.assertEqual(len(df), 4)


if __name__ == "__main__":
    unittest.main()
