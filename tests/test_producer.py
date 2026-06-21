from generator.producer import emit_batch


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        pass


def test_emit_batch_produces_three_event_types():
    fake = FakeProducer()
    emit_batch(fake, n_requests=5, fraud=False)
    topics = {t for (t, _, _) in fake.sent}
    assert "otp.requests" in topics
    assert "otp.delivery" in topics
    assert "otp.verification" in topics


def test_fraud_batch_has_low_verification_rate():
    fake = FakeProducer()
    emit_batch(fake, n_requests=40, fraud=True)
    verifs = [v for (t, _, v) in fake.sent if t == "otp.verification"]
    verified = [v for v in verifs if '"verified"' in v]
    # AIT: almost nothing genuinely verifies
    assert len(verified) <= max(1, len(verifs) // 5)
