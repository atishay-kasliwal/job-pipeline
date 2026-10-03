import math
import unittest

import pandas as pd

from job_pipeline.priority import experience_band, job_priority, location_tag
from job_pipeline.scoring import apply_scores


def group(score_pct=50, location="Austin, TX", title="Software Engineer", min_exp=None):
    return job_priority(score_pct, location, title, min_exp)["priority_group"]


class PriorityOrderTest(unittest.TestCase):
    def test_each_rule_beats_everything_below_it(self):
        # 1. a strong match beats any weaker one, wherever it is and whatever it asks for
        self.assertLess(group(score_pct=70, location="Austin, TX", min_exp=4), group(score_pct=69, location="New York, NY", min_exp=0))
        # 2. among equals on rule 1, a preferred location beats any experience band
        self.assertLess(group(location="Raleigh, NC", min_exp=4), group(location="Austin, TX", min_exp=0))
        # 3-6. then experience: 0-1, 1-2, 2-5 (or not stated), 5+
        bands = [group(min_exp=0), group(min_exp=1), group(min_exp=3), group(min_exp=None), group(min_exp=7)]
        self.assertEqual(bands, sorted(bands))
        self.assertLess(bands[0], bands[1])
        self.assertLess(bands[1], bands[2])
        self.assertEqual(bands[2], bands[3])
        self.assertLess(bands[3], bands[4])

    def test_best_and_worst(self):
        best = job_priority(85, "New York, NY", "Software Engineer, New Grad", None)
        self.assertEqual(best, {"priority_group": 0, "priority_tags": ["Strong match", "New York", "New grad"], "priority_version": 2})
        worst = job_priority(10, "Remote", "Backend Engineer", 8)
        self.assertEqual(worst["priority_group"], 15)
        self.assertEqual(worst["priority_tags"], ["5+ yrs"])

    def test_internships_come_last(self):
        intern = job_priority(95, "New York, NY", "Software Engineering Intern, Summer 2027", 0)
        self.assertEqual(intern["priority_group"], 16)
        self.assertIn("Internship", intern["priority_tags"])
        self.assertGreater(intern["priority_group"], job_priority(5, "Remote", "Backend Engineer", 9)["priority_group"])
        for title in ["Data Science Internship", "Software Engineer Co-op", "SWE Interns 2027"]:
            self.assertEqual(job_priority(80, None, title, None)["priority_group"], 16, title)
        self.assertNotEqual(job_priority(80, None, "Internal Tools Engineer", None)["priority_group"], 16)

    def test_nothing_is_excluded(self):
        for args in [(0, None, None, None), (None, "", "", float("nan")), (100, "Apex, NC", "Entry Level Engineer", 0)]:
            p = job_priority(*args)
            self.assertIn(p["priority_group"], range(17))
            self.assertTrue(p["priority_tags"])


class LocationTest(unittest.TestCase):
    def test_preferred_locations(self):
        for loc, tag in [("New York, NY", "New York"), ("NYC", "New York"), ("Brooklyn, New York, United States", "New York"),
                         ("Raleigh, NC", "Raleigh"), ("Raleigh-Durham, North Carolina", "Raleigh"), ("Apex, NC", "Apex"), ("Apex, North Carolina", "Apex")]:
            self.assertEqual(location_tag(loc), tag, loc)

    def test_other_locations(self):
        for loc in ["Seattle, WA", "Remote", "Austin, TX", "Apex Fintech (Remote)", "", None]:
            self.assertIsNone(location_tag(loc), loc)


class ExperienceTest(unittest.TestCase):
    def test_bands_by_minimum_years(self):
        self.assertEqual(experience_band(0, "Engineer"), (0, "0–1 yrs"))
        self.assertEqual(experience_band(1, "Engineer"), (1, "1–2 yrs"))
        self.assertEqual(experience_band(2, "Engineer"), (2, "2–5 yrs"))
        self.assertEqual(experience_band(5, "Engineer"), (2, "2–5 yrs"))
        self.assertEqual(experience_band(6, "Engineer"), (3, "5+ yrs"))
        self.assertEqual(experience_band(None, "Engineer"), (2, "Exp. not stated"))
        self.assertEqual(experience_band(math.nan, "Engineer"), (2, "Exp. not stated"))

    def test_new_grad_titles_rank_with_0_to_1_years(self):
        for title in ["Software Engineer, New Grad", "New Graduate Engineer", "Software Engineer - 2026 New-Grad",
                      "University Graduate, Data", "Entry Level Developer", "Early Career Software Engineer", "Recent Grad SWE"]:
            self.assertEqual(experience_band(3, title), (0, "New grad"), title)
        self.assertEqual(experience_band(3, "Graduate Research Assistant")[0], 2)


class ApplyScoresTest(unittest.TestCase):
    def test_scoring_adds_priority_columns(self):
        df = pd.DataFrame([
            {"title": "Software Engineer, New Grad", "company": "Acme", "location": "New York, NY", "description": "python backend api", "min_exp": None, "max_exp": None, "site": "linkedin"},
            {"title": "Backend Engineer", "company": "Beta", "location": "Remote", "description": "java api", "min_exp": 3, "max_exp": 5, "site": "linkedin"},
        ])
        out = apply_scores(df)
        for col in ("priority_group", "priority_tags", "priority_version"):
            self.assertIn(col, out.columns)
        by_title = {r["title"]: r for r in out.to_dict("records")}
        self.assertIn("New grad", by_title["Software Engineer, New Grad"]["priority_tags"])
        self.assertIn("New York", by_title["Software Engineer, New Grad"]["priority_tags"])
        self.assertIn("2–5 yrs", by_title["Backend Engineer"]["priority_tags"])


if __name__ == "__main__":
    unittest.main()
