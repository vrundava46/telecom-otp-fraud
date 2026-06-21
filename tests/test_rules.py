from streaming import rules


def agg(**kw):
    base = dict(requests=10, verifications=7, distinct_ip=8,
                msisdn_max_count=2, mcc="310", mnc="260")
    base.update(kw)
    return base


def test_velocity_fires_when_single_msisdn_floods():
    assert rules.velocity_score(agg(msisdn_max_count=60)) > 0
    assert rules.velocity_score(agg(msisdn_max_count=2)) == 0


def test_ait_signature_high_volume_low_verify():
    hot = agg(requests=500, verifications=5)      # 1% verify rate
    cold = agg(requests=500, verifications=350)   # 70% verify rate
    assert rules.ait_score(hot) > rules.ait_score(cold)
    assert rules.ait_score(cold) == 0


def test_repeated_never_verified():
    assert rules.never_verified_score(agg(requests=200, verifications=0)) > 0
    assert rules.never_verified_score(agg(requests=200, verifications=50)) == 0


def test_high_risk_route():
    assert rules.high_risk_route_score(agg(mcc="247", mnc="1")) > 0   # Latvia premium
    assert rules.high_risk_route_score(agg(mcc="310", mnc="260")) == 0
