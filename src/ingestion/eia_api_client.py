
import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = (
    "https://api.eia.gov/v2/"
    "electricity/rto/region-data/data/"
)

RAW_DIR = Path("data/raw")
STATE_FILE = Path("data/state/eia_us48_watermark.json")

INITIAL_START = "2026-09-26T00"
PAGE_SIZE = 100
OVERLAP_HOURS = 24


def make_session():
    """Create an HTTP session with automatic retries."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    return session


def load_watermark():
    """Read the last successfully downloaded hour."""
    if not STATE_FILE.exists():
        return None

    with STATE_FILE.open("r", encoding="utf-8") as file:
        state = json.load(file)

    return state.get("last_successful_hour")


def save_json_atomic(path, payload):
    """Save JSON using a temporary file before replacing."""
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_name(path.name + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)
        file.flush()
        os.fsync(file.fileno())

    temporary_path.replace(path)


def calculate_start():
    """Use a 24-hour overlap to capture revised records."""
    watermark = load_watermark()

    if watermark is None:
        return INITIAL_START

    last_hour = datetime.strptime(
        watermark, "%Y-%m-%dT%H"
    )

    initial_hour = datetime.strptime(
        INITIAL_START, "%Y-%m-%dT%H"
    )

    start = max(
        initial_hour,
        last_hour - timedelta(hours=OVERLAP_HOURS - 1),
    )

    return start.strftime("%Y-%m-%dT%H")


def calculate_end():
    """
    Use an explicitly configured EIA period end.

    EIA reporting periods should not be confused
    with the computer's local timezone.
    """
    end = os.getenv("EIA_END_HOUR")

    if not end:
        raise ValueError(
            "Set EIA_END_HOUR, for example: 2026-09-27T23"
        )

    datetime.strptime(end, "%Y-%m-%dT%H")
    return end


def fetch_eia_data(start, end):
    """Download every page for the requested time window."""
    api_key = os.getenv("EIA_API_KEY")

    if not api_key:
        raise ValueError("EIA_API_KEY is not configured")

    session = make_session()

    all_records = []
    offset = 0
    expected_total = None

    try:
        while True:
            params = {
                "api_key": api_key,
                "frequency": "hourly",
                "data[0]": "value",
                "facets[respondent][]": "US48",
                "facets[type][]": ["D", "DF"],
                "start": start,
                "end": end,
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "sort[1][column]": "type",
                "sort[1][direction]": "asc",
                "offset": offset,
                "length": PAGE_SIZE,
            }

            response = session.get(
                BASE_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            payload = response.json()
            result = payload["response"]

            records = result["data"]
            total = int(result["total"])

            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise ValueError(
                    "API total changed during pagination. "
                    "Retry this ingestion window."
                )

            if not records and offset < expected_total:
                raise ValueError(
                    "API returned an empty page unexpectedly."
                )

            all_records.extend(records)
            offset += len(records)

            print(
                f"Downloaded {len(all_records)} "
                f"of {expected_total} records"
            )

            if offset >= expected_total:
                break

            time.sleep(0.2)

    finally:
        session.close()

    if len(all_records) != expected_total:
        raise ValueError("Incomplete API download")

    # Verify uniqueness within this download.
    keys = [
        (
            record["period"],
            record["respondent"],
            record["type"],
        )
        for record in all_records
    ]

    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate records in API response")

    return all_records


def main():
    start = calculate_start()
    end = calculate_end()

    if start > end:
        print("No new time window to process.")
        return

    print(f"Downloading EIA data: {start} to {end}")

    records = fetch_eia_data(start, end)

    if not records:
        print(
            "No records returned. "
            "Watermark remains unchanged."
        )
        return

    # Preserve the original record structure for Spark.
    payload = {
        "ingestion_metadata": {
            "source": "EIA API v2",
            "respondent": "US48",
            "start": start,
            "end": end,
            "downloaded_at_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "record_count": len(records),
        },
        "response": {
            "data": records,
            "total": str(len(records)),
        },
    }

    filename = (
        f"eia_us48_{start.replace(':', '')}"
        f"_to_{end.replace(':', '')}.json"
    )

    output_path = RAW_DIR / filename

    # Save the raw batch before updating the watermark.
    save_json_atomic(output_path, payload)

    latest_hour = max(
        record["period"] for record in records
    )

    save_json_atomic(
        STATE_FILE,
        {"last_successful_hour": latest_hour},
    )

    print(f"Saved {len(records)} records")
    print(f"File: {output_path}")
    print(f"Watermark: {latest_hour}")


if __name__ == "__main__":
    main()
