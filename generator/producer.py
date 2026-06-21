import json
import random
from generator import scenarios
from common.schemas import TOPICS


def _send(producer, topic, event):
    producer.produce(topic, key=event["request_id"], value=json.dumps(event))


def emit_batch(producer, n_requests: int, fraud: bool) -> None:
    requests = (scenarios.ait_burst(n_requests) if fraud
                else scenarios.normal_traffic(n_requests))
    for req in requests:
        _send(producer, TOPICS["requests"], req)
        delivered = random.random() < 0.95
        _send(producer, TOPICS["delivery"], scenarios.make_delivery_event(
            req, status="delivered" if delivered else "failed"))
        if delivered:
            # normal traffic verifies ~70%; AIT verifies ~5%
            verify_p = 0.05 if fraud else 0.70
            outcome = "verified" if random.random() < verify_p else "timeout"
            _send(producer, TOPICS["verification"],
                  scenarios.make_verification_event(req, outcome=outcome))
    producer.flush()


def build_kafka_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap})


def main():  # pragma: no cover
    import time
    from common.config import Settings
    s = Settings.from_env()
    p = build_kafka_producer(s.kafka_bootstrap)
    while True:
        emit_batch(p, n_requests=200, fraud=False)
        if random.random() < 0.10:
            emit_batch(p, n_requests=80, fraud=True)  # periodic AIT attack
        time.sleep(1)


if __name__ == "__main__":  # pragma: no cover
    main()
