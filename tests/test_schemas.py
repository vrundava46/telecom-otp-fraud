from common.schemas import REQUEST_FIELDS, DELIVERY_FIELDS, VERIFICATION_FIELDS, TOPICS


def test_topics_present():
    assert TOPICS == {"requests": "otp.requests",
                      "delivery": "otp.delivery",
                      "verification": "otp.verification"}


def test_request_fields_have_correlation_key():
    assert "request_id" in REQUEST_FIELDS
    assert "msisdn" in REQUEST_FIELDS
    assert REQUEST_FIELDS[0] == "request_id"


def test_all_topics_share_request_id():
    for fields in (REQUEST_FIELDS, DELIVERY_FIELDS, VERIFICATION_FIELDS):
        assert "request_id" in fields
