import pandas as pd


def validate_silver(df: pd.DataFrame) -> dict:
    failures = []
    if df["request_id"].isnull().any():
        failures.append("request_id")
    if not df["is_verified"].isin([True, False]).all():
        failures.append("is_verified")
    return {"ok": len(failures) == 0, "failures": failures}


def main():  # pragma: no cover
    raise SystemExit("wire to read silver from Trino in production")


if __name__ == "__main__":  # pragma: no cover
    main()
