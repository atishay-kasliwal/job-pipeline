from datetime import datetime, timezone

import numpy as np
import pandas as pd

from job_pipeline.storage import _df_to_records


def test_list_and_array_columns_become_lists():
    # Priority tags are a list per job; this used to fail every standard run's Mongo insert.
    df = pd.DataFrame({
        "job_url": ["a", "b"],
        "priority_tags": [["Strong match", "New York"], []],
        "arr": [np.array([1, 2]), np.array([])],
        "score": [np.int64(5), np.nan],
        "posted": [pd.Timestamp("2026-10-04T10:00:00Z"), None],
    })
    recs = _df_to_records(df, "sid", "standard", datetime.now(tz=timezone.utc))
    assert recs[0]["priority_tags"] == ["Strong match", "New York"]
    assert recs[1]["priority_tags"] == []
    assert recs[0]["arr"] == [1, 2] and type(recs[0]["arr"][0]) is int
    assert recs[0]["score"] == 5 and recs[1]["score"] is None
    assert recs[1]["posted"] is None


def test_nested_values_and_missing_scalars_are_bson_safe():
    from bson import BSON
    df = pd.DataFrame({"values": [{"tags": [np.int64(3), np.nan, pd.NA], "matrix": np.array([[1, 2], [3, 4]])}], "scalar": [np.array(7)]})
    record = _df_to_records(df, "sid", "standard", datetime.now(tz=timezone.utc))[0]
    assert record["values"] == {"tags": [3, None, None], "matrix": [[1, 2], [3, 4]]}
    assert record["scalar"] == 7
    BSON.encode(record)
