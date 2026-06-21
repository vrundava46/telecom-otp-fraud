"""Pure fraud-rule functions over a windowed aggregate dict.

Each returns a non-negative float contribution to the severity score.
No I/O, no Spark — unit-tested deterministically.
"""

HIGH_RISK_ROUTES = {("247", "1"), ("88", "0"), ("881", "0")}  # known AIT-heavy ranges
AIT_MIN_REQUESTS = 100
AIT_VERIFY_RATE_CEILING = 0.10
AIT_VOLUME_WEIGHT = 2.0       # sensitivity of AIT score to request volume
VELOCITY_MSISDN_CEILING = 30
NEVER_VERIFIED_MIN_REQUESTS = 50


def _verify_rate(agg: dict) -> float:
    r = agg.get("requests", 0)
    return (agg.get("verifications", 0) / r) if r else 0.0


def velocity_score(agg: dict) -> float:
    over = agg.get("msisdn_max_count", 0) - VELOCITY_MSISDN_CEILING
    return float(min(over, 100)) * 0.5 if over > 0 else 0.0


def ait_score(agg: dict) -> float:
    requests = agg.get("requests", 0)
    if requests < AIT_MIN_REQUESTS:
        return 0.0
    rate = _verify_rate(agg)
    if rate >= AIT_VERIFY_RATE_CEILING:
        return 0.0
    # verification deficit (1.0 at zero verify) scaled by request volume:
    # a sustained, high-volume, near-zero-verify burst scores decisively high.
    deficit = (AIT_VERIFY_RATE_CEILING - rate) / AIT_VERIFY_RATE_CEILING
    return deficit * requests * AIT_VOLUME_WEIGHT


def never_verified_score(agg: dict) -> float:
    if agg.get("requests", 0) >= NEVER_VERIFIED_MIN_REQUESTS and \
       agg.get("verifications", 0) == 0:
        return 40.0
    return 0.0


def high_risk_route_score(agg: dict) -> float:
    return 25.0 if (agg.get("mcc"), agg.get("mnc")) in HIGH_RISK_ROUTES else 0.0
