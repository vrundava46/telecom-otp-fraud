import json
from collections import defaultdict
from generator.producer import emit_batch
from streaming.fraud_job import apply_scoring


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def _aggregate(sent):
    reqs = [json.loads(v) for (t, _, v) in sent if t == "otp.requests"]
    verifs = [json.loads(v) for (t, _, v) in sent if t == "otp.verification"]
    verified_ids = {v["request_id"] for v in verifs if v["outcome"] == "verified"}
    groups = defaultdict(lambda: dict(requests=0, verifications=0,
                                      msisdn_max_count=1, distinct_ip=0))
    for r in reqs:
        key = (r["enterprise_id"], r["msisdn"][:6], r["mcc"], r["mnc"])
        g = groups[key]
        g.update(enterprise_id=r["enterprise_id"], number_range=r["msisdn"][:6],
                 mcc=r["mcc"], mnc=r["mnc"])
        g["requests"] += 1
        if r["request_id"] in verified_ids:
            g["verifications"] += 1
    return list(groups.values())


def test_ait_attack_produces_high_severity_alert():
    fake = FakeProducer()
    emit_batch(fake, n_requests=300, fraud=True)     # AIT attack
    alerts = apply_scoring(_aggregate(fake.sent))
    high = [a for a in alerts if a["severity"] == "high"]
    assert high, "expected at least one high-severity AIT alert"


def test_normal_traffic_produces_no_high_alert():
    fake = FakeProducer()
    emit_batch(fake, n_requests=300, fraud=False)
    alerts = apply_scoring(_aggregate(fake.sent))
    assert not [a for a in alerts if a["severity"] == "high"]
