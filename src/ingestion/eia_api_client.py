
import os
import json
from pathlib import Path

import requests

BASE_URL = (
    "https://api.eia.gov/v2/"
    "electricity/rto/region-data/data/"
)


def fetch_eia_data():
    api_key = os.getenv("EIA_API_KEY")

    if not api_key:
        raise ValueError("EIA_API_KEY is not configured")

    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "US48",
        "facets[type][]": ["D", "DF"],
        "start": "2026-09-26T00",
        "end": "2026-09-26T23",
        "length": 100,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()
    records = payload["response"]["data"]
    total = int(payload["response"]["total"])

    if len(records) != total:
        raise ValueError(
            f"Incomplete response: {len(records)} of {total}"
        )

    return payload


def main():
    payload = fetch_eia_data()

    output_path = Path("data/raw/eia_us48_2026-09-26.json")
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    print(f"Saved {len(payload['response']['data'])} records")


if __name__ == "__main__":
    main()
