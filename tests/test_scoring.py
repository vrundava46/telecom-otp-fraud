from streaming.scoring import score_aggregate, severity


def agg(**kw):
    base = dict(requests=10, verifications=7, distinct_ip=8,
                msisdn_max_count=2, mcc="310", mnc="260")
    base.update(kw)
    return base


def test_clean_traffic_scores_zero_and_low():
    s = score_aggregate(agg())
    assert s == 0
    assert severity(s) == "low"


def test_ait_attack_scores_high():
    s = score_aggregate(agg(requests=500, verifications=2, mcc="247", mnc="1"))
    assert s > 100
    assert severity(s) == "high"


def test_severity_bands():
    assert severity(0) == "low"
    assert severity(30) == "medium"
    assert severity(150) == "high"
