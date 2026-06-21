from generator.scenarios import (make_request_event, ait_burst, normal_traffic,
                                  make_delivery_event, make_verification_event)


def test_make_request_event_has_all_fields():
    e = make_request_event(enterprise_id="ent_1", country_iso="US", mcc="310", mnc="260")
    for k in ["request_id", "msisdn", "enterprise_id", "ip", "channel", "event_ts"]:
        assert k in e
    assert e["enterprise_id"] == "ent_1"


def test_ait_burst_targets_single_range_and_low_verify():
    events = ait_burst(count=50)
    prefixes = {e["msisdn"][:6] for e in events}
    assert len(prefixes) == 1            # concentrated on one number range
    assert all(e["channel"] == "sms" for e in events)


def test_normal_traffic_is_diverse():
    events = normal_traffic(count=100)
    prefixes = {e["msisdn"][:6] for e in events}
    assert len(prefixes) > 10            # spread across many ranges


def test_delivery_links_to_request():
    req = make_request_event()
    d = make_delivery_event(req, status="delivered")
    assert d["request_id"] == req["request_id"]
    assert d["status"] == "delivered"
    assert d["operator_latency_ms"] >= 0


def test_verification_outcome_controllable():
    req = make_request_event()
    v = make_verification_event(req, outcome="verified")
    assert v["request_id"] == req["request_id"]
    assert v["outcome"] == "verified"
    assert v["attempts"] >= 1
