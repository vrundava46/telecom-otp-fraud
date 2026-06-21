import pandas as pd
from sklearn.ensemble import IsolationForest

FEATURE_COLS = ["requests", "verify_rate", "avg_latency", "range_spread"]


def score_anomalies(feats: pd.DataFrame, contamination: float = 0.1) -> pd.DataFrame:
    model = IsolationForest(contamination=contamination, random_state=42)
    X = feats[FEATURE_COLS].values
    model.fit(X)
    out = feats.copy()
    out["anomaly_score"] = model.decision_function(X)   # lower = more anomalous
    out["is_anomaly"] = model.predict(X) == -1
    return out
