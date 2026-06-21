from common.config import Settings


def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "host:1234")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://u:p@h/db")
    s = Settings.from_env()
    assert s.kafka_bootstrap == "host:1234"
    assert s.postgres_dsn.endswith("/db")


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    s = Settings.from_env()
    assert s.kafka_bootstrap == "localhost:9092"
