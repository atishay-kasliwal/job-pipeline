import json
from job_pipeline.export_static import skip_in_week


def test_data_scientist_and_analyst_roles_are_left_out_of_the_week_file():
    for job in [{"search_term": "data scientist", "title": "ML Scientist"}, {"search_term": "", "title": "Senior Data Analyst"},
                {"title": "Data Science Intern"}, {"search_term": "Data  Analytics", "title": "x"}]:
        assert skip_in_week(job)
    for job in [{"search_term": "software engineer", "title": "Data Engineer"}, {"search_term": "machine learning engineer", "title": "Analytics Engineer"},
                {"title": "Database Administrator"}, {}]:
        assert not skip_in_week(job)


def test_the_feed_keeps_only_the_headline_of_fit_and_resume():
    from job_pipeline.export_static import slim_job
    job = {"title": "SWE", "fit_score": 71, "fit": {"score": 84, "confidence": "medium", "as_of": "2026-10-28", "ledger": [{"x": 1}] * 50, "lost": ["y"]},
           "resume": {"status": "success", "updated_at": "t", "object": {"big": "x" * 500}, "cover_letter": "Dear...", "pdf_path": "/p"}}
    out = slim_job(job)
    assert out["fit"] == {"score": 84, "confidence": "medium", "as_of": "2026-10-28"}
    assert out["resume"] == {"status": "success", "updated_at": "t"}
    assert out["title"] == "SWE" and out["fit_score"] == 71
    assert "ledger" in job["fit"], "the job itself is not changed"
    assert slim_job({"title": "x"}) == {"title": "x"} and slim_job({"fit": None})["fit"] is None


def test_a_file_never_outgrows_the_limit_and_the_oldest_jobs_go_first():
    from job_pipeline.export_static import cap_size
    jobs = [{"id": i, "batch_time": f"2026-10-{20 + i:02d}", "pad": "x" * 1000} for i in range(8)]
    assert cap_size(jobs, "t", limit=100_000) is jobs, "under the limit nothing changes"
    kept = cap_size(jobs, "t", limit=4_200)
    assert [j["id"] for j in kept] == [4, 5, 6, 7]
    assert len(json.dumps(kept, separators=(",", ":"))) <= 4_200
