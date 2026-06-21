import pandas as pd


def build_entity_features(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("enterprise_id")
    out = g.agg(
        requests=("is_verified", "size"),
        verify_rate=("is_verified", "mean"),
        avg_latency=("operator_latency_ms", "mean"),
        range_spread=("number_range", "nunique"),
    ).reset_index()
    return out
