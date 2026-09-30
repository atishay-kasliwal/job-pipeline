import unittest

import pandas as pd

from job_pipeline.filters import filter_by_role


class RoleFilterTest(unittest.TestCase):
    def kept(self, titles):
        return filter_by_role(pd.DataFrame({"title": titles}))["title"].tolist()

    def test_keeps_added_role_families(self):
        titles = [
            "Full Stack Engineer", "Full-Stack Developer", "Fullstack Engineer",
            "Research Engineer, Agents", "Forward Deployed Engineer", "Forward-Deployed AI Engineer",
            "Data Analyst", "AI Engineer", "Data Scientist",
        ]
        self.assertEqual(self.kept(titles), titles)

    def test_seniority_exclusions_still_apply(self):
        self.assertEqual(self.kept(["Senior Full Stack Engineer", "Staff Research Engineer", "Lead Data Analyst"]), [])

    def test_unrelated_roles_still_dropped(self):
        self.assertEqual(self.kept(["Research Scientist", "Account Executive", "Platform Engineer"]), [])


if __name__ == "__main__":
    unittest.main()
