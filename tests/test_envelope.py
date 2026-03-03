from shared.envelope import build_message


def test_build_message_contains_envelope_and_payload() -> None:
    result = build_message(
        receiver_id="syslog-receiver-01",
        receiver_type="syslog",
        source_ip="10.0.0.1",
        source_port=12345,
        protocol="UDP",
        payload="<msg>",
    )

    assert result["envelope"]["receiver_type"] == "syslog"
    assert result["payload"] == "<msg>"
    assert result["envelope"]["schema_version"] == "1.0"
