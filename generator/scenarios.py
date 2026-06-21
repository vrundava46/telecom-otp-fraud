import random
import uuid
from datetime import datetime, timezone

CHANNELS = ["sms", "voice", "whatsapp"]
ENTERPRISES = [f"ent_{i}" for i in range(1, 11)]
ROUTES = [("US", "310", "260"), ("IN", "404", "45"),
          ("GB", "234", "10"), ("BR", "724", "5")]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_request_event(enterprise_id=None, country_iso=None, mcc=None,
                       mnc=None, msisdn=None, ip=None) -> dict:
    route = random.choice(ROUTES)
    return {
        "request_id": str(uuid.uuid4()),
        "msisdn": msisdn or f"{random.randint(100000, 999999)}{random.randint(1000, 9999)}",
        "enterprise_id": enterprise_id or random.choice(ENTERPRISES),
        "country_iso": country_iso or route[0],
        "mcc": mcc or route[1],
        "mnc": mnc or route[2],
        "ip": ip or f"{random.randint(1, 223)}.{random.randint(0, 255)}."
                    f"{random.randint(0, 255)}.{random.randint(0, 255)}",
        "channel": "sms",
        "event_ts": _now_iso(),
    }


def normal_traffic(count: int) -> list[dict]:
    out = []
    for _ in range(count):
        e = make_request_event()
        e["channel"] = random.choice(CHANNELS)
        out.append(e)
    return out


def ait_burst(count: int, enterprise_id: str = "ent_3") -> list[dict]:
    """Mass requests to ONE number-range, one enterprise — the AIT signature."""
    prefix = "550199"
    return [
        make_request_event(
            enterprise_id=enterprise_id, country_iso="LV", mcc="247", mnc="1",
            msisdn=f"{prefix}{random.randint(1000, 9999)}",
        )
        for _ in range(count)
    ]


def make_delivery_event(request: dict, status: str = "delivered") -> dict:
    return {
        "request_id": request["request_id"],
        "status": status,
        "operator_latency_ms": random.randint(50, 4000),
        "event_ts": _now_iso(),
    }


def make_verification_event(request: dict, outcome: str = "verified") -> dict:
    return {
        "request_id": request["request_id"],
        "outcome": outcome,
        "attempts": random.randint(1, 3),
        "event_ts": _now_iso(),
    }
