import pandas as pd
from batch.features import build_entity_features


def test_features_compute_verify_rate_and_volume():
    df = pd.DataFrame({
        "enterprise_id": ["ent_3"] * 4 + ["ent_1"] * 2,
        "number_range": ["550199"] * 4 + ["447700"] * 2,
        "is_verified": [False, False, False, True, True, True],
        "operator_latency_ms": [100, 110, 120, 130, 90, 95],
    })
    feats = build_entity_features(df).set_index("enterprise_id")
    assert feats.loc["ent_3", "requests"] == 4
    assert feats.loc["ent_3", "verify_rate"] == 0.25
    assert feats.loc["ent_1", "verify_rate"] == 1.0
