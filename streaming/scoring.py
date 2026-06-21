from streaming import rules

RULES = [
    rules.velocity_score,
    rules.ait_score,
    rules.never_verified_score,
    rules.high_risk_route_score,
]


def score_aggregate(agg: dict) -> float:
    return float(sum(rule(agg) for rule in RULES))


def severity(score: float) -> str:
    if score >= 100:
        return "high"
    if score >= 20:
        return "medium"
    return "low"
