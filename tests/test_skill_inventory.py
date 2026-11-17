import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from job_pipeline import scoring, skill_inventory
from job_pipeline.skill_inventory import approved_hits, skills_context_for_jd


def _inventory(path: Path) -> Path:
    path.write_text(json.dumps({
        "version": 1,
        "skills": [
            {"name": "Go", "category": "Languages", "terms": ["Go", "golang"], "sources": ["https://github.com/o/r/pull/1"]},
            {"name": "Rust", "category": "Languages", "terms": ["Rust"], "sources": ["https://github.com/o/r/pull/2"]},
            {"name": "Tokio", "category": "Systems Libraries", "terms": ["Tokio"], "sources": ["https://github.com/o/r/pull/3"]},
            {"name": "Python", "category": "Languages", "terms": ["Python"], "sources": ["https://github.com/o/r/pull/4"]},
            # No sources at all: never counts.
            {"name": "Svelte", "category": "Frontend", "terms": ["Svelte"], "sources": []},
            # Open PR evidence only: counts at half weight (merged first).
            {"name": "Kustomize", "category": "Cloud & DevOps", "terms": ["Kustomize"], "merged": False, "sources": ["https://github.com/o/r/pull/5"]},
        ],
        # The vocabulary is a matching dictionary, never approved evidence.
        "vocabulary": [{"name": "Haskell", "category": "Languages", "terms": ["Haskell"], "sources": []}],
    }))
    return path


class _InventoryFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = _inventory(Path(self.tmp.name) / "skill_inventory.json")
        skill_inventory._load.cache_clear()
        self.patch = mock.patch.object(skill_inventory.config, "SKILL_INVENTORY_JSON", self.path)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        skill_inventory._load.cache_clear()
        self.tmp.cleanup()

    def names(self, text):
        return [s["name"] for s in approved_hits(text)]


class SkillInventoryTest(_InventoryFixture):
    def test_go_only_in_technical_context(self):
        for text in ("Experience with Go, Rust, or C++", "Backend services written in Go", "Python/Go",
                     "Strong Golang skills", "Go developers wanted", "languages (Go)"):
            self.assertIn("Go", self.names(text), text)
        for text in ("Build our go-to-market motion", "Ready to go live next quarter",
                     "We go beyond the basics", "Work on the go, anywhere", "Let's go"):
            self.assertNotIn("Go", self.names(text), text)

    def test_whole_words_and_only_merged_skills(self):
        self.assertEqual(self.names("We value trust and rusty pipes"), [])
        self.assertEqual(self.names("Svelte and Haskell"), [])
        self.assertEqual(self.names("Rust with Tokio"), ["Rust", "Tokio"])

    def test_missing_inventory_is_empty(self):
        self.assertEqual(approved_hits("Rust", self.path.with_name("missing.json")), [])

    def test_skills_context_is_skills_only_advice(self):
        block = skills_context_for_jd("Rust and Tokio required")
        self.assertIn("Rust: https://github.com/o/r/pull/2", block)
        self.assertIn("Skills-section additions", block)
        self.assertEqual(skills_context_for_jd("Accounting role"), "")


class ApprovedScoringTest(_InventoryFixture):
    def test_rust_role_without_old_stack_words_is_kept(self):
        text = "systems engineer\nwe build storage engines in rust with tokio."
        with mock.patch.object(scoring, "_is_top500", return_value=False):
            self.assertFalse(scoring.should_skip(text))
            self.assertTrue(scoring.should_skip("marketing manager\nown our go-to-market plan."))

    def test_open_only_skill_counts_at_half_weight(self):
        # Cloud & DevOps weight 3, open only: 1.5 rounds to 2.
        self.assertEqual(scoring.approved_skill_score("kustomize overlays"), 2)

    def test_approved_points_skip_personal_stack_and_cap(self):
        # Python is already scored by PERSONAL_STACK; Rust (6) + Tokio (4) are new.
        self.assertEqual(scoring.approved_skill_score("python, rust and tokio"), 10)
        with mock.patch.object(scoring, "APPROVED_SKILL_SCORE_CAP", 5):
            self.assertEqual(scoring.approved_skill_score("rust and tokio"), 5)

    def test_calculate_score_rewards_approved_skills(self):
        row = pd.Series({"title": "Software Engineer", "description": "Rust and Tokio services.", "company": "Acme",
                         "site": "", "location": "", "date_posted": None, "min_exp": None, "max_exp": None})
        with mock.patch.object(scoring, "_is_top500", return_value=False), \
             mock.patch.object(scoring, "_is_h1b_sponsor", return_value=False):
            with_bank = scoring.calculate_score(row)["score"]
            with mock.patch.object(scoring, "approved_skill_score", return_value=0):
                without = scoring.calculate_score(row)["score"]
        self.assertEqual(with_bank - without, 10)


if __name__ == "__main__":
    unittest.main()
