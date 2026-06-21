import pandas as pd
from quality.expectations_silver import validate_silver


def test_validate_silver_catches_null_request_id():
    bad = pd.DataFrame({"request_id": ["r1", None],
                        "is_verified": [True, False]})
    result = validate_silver(bad)
    assert result["ok"] is False
    assert "request_id" in result["failures"]


def test_validate_silver_passes_clean():
    good = pd.DataFrame({"request_id": ["r1", "r2"],
                         "is_verified": [True, False]})
    assert validate_silver(good)["ok"] is True
