from streaming.fraud_job import apply_scoring


def test_apply_scoring_flags_ait_rows():
    rows = [
        dict(enterprise_id="ent_3", number_range="550199", requests=500,
             verifications=3, msisdn_max_count=5, distinct_ip=2, mcc="247", mnc="1"),
        dict(enterprise_id="ent_1", number_range="447700", requests=20,
             verifications=15, msisdn_max_count=2, distinct_ip=18, mcc="310", mnc="260"),
    ]
    alerts = apply_scoring(rows)
    flagged = {a["enterprise_id"]: a for a in alerts}
    assert flagged["ent_3"]["severity"] == "high"
    assert flagged["ent_1"]["severity"] == "low"
    assert flagged["ent_3"]["score"] > flagged["ent_1"]["score"]
