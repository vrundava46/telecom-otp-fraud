TOPICS = {
    "requests": "otp.requests",
    "delivery": "otp.delivery",
    "verification": "otp.verification",
}
ALERTS_TOPIC = "otp.alerts"

REQUEST_FIELDS = [
    "request_id", "msisdn", "enterprise_id", "country_iso",
    "mcc", "mnc", "ip", "channel", "event_ts",
]
DELIVERY_FIELDS = ["request_id", "status", "operator_latency_ms", "event_ts"]
VERIFICATION_FIELDS = ["request_id", "outcome", "attempts", "event_ts"]
