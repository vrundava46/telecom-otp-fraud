import pandas as pd
from batch.anomaly import score_anomalies


def test_isolation_forest_flags_outlier_entity():
    # 9 normal entities + 1 obvious AIT outlier (huge volume, ~0 verify)
    rows = [dict(enterprise_id=f"ent_{i}", requests=20, verify_rate=0.7,
                 avg_latency=120, range_spread=15) for i in range(9)]
    rows.append(dict(enterprise_id="ent_bad", requests=5000, verify_rate=0.01,
                     avg_latency=130, range_spread=1))
    feats = pd.DataFrame(rows)
    scored = score_anomalies(feats)
    worst = scored.sort_values("anomaly_score").iloc[0]
    assert worst["enterprise_id"] == "ent_bad"
    assert worst["is_anomaly"] == True
