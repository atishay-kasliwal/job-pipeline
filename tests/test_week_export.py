from job_pipeline.export_static import skip_in_week


def test_data_scientist_and_analyst_roles_are_left_out_of_the_week_file():
    for job in [{"search_term": "data scientist", "title": "ML Scientist"}, {"search_term": "", "title": "Senior Data Analyst"},
                {"title": "Data Science Intern"}, {"search_term": "Data  Analytics", "title": "x"}]:
        assert skip_in_week(job)
    for job in [{"search_term": "software engineer", "title": "Data Engineer"}, {"search_term": "machine learning engineer", "title": "Analytics Engineer"},
                {"title": "Database Administrator"}, {}]:
        assert not skip_in_week(job)
