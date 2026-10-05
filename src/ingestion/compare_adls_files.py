
import json
from pathlib import Path

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient


ACCOUNT = "stdata2026"
CONTAINER = "raw"

LOCAL_FILE = Path(
    "data/raw/daily/eia_us48_2026-09-26.json"
)

AZURE_FILE = (
    "eia/region-data/"
    "respondent=US48/"
    "year=2026/month=09/day=26/"
    "eia_us48_2026-09-26.json"
)


def build_record_map(payload):
    records = payload["response"]["data"]

    result = {}

    for record in records:
        key = (
            record["period"],
            record["respondent"],
            record["type"],
        )

        if key in result:
            raise ValueError(
                f"Duplicate record detected: {key}"
            )

        result[key] = record

    return result


def main():
    # Read the local file.
    with LOCAL_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        local_payload = json.load(file)

    # Connect to Azure.
    credential = AzureCliCredential()

    service = DataLakeServiceClient(
        account_url=(
            f"https://{ACCOUNT}.dfs.core.windows.net"
        ),
        credential=credential,
    )

    filesystem = service.get_file_system_client(
        CONTAINER
    )

    remote_client = filesystem.get_file_client(
        AZURE_FILE
    )

    remote_payload = json.loads(
        remote_client.download_file().readall()
    )

    # Compare records by business key.
    local_records = build_record_map(
        local_payload
    )

    remote_records = build_record_map(
        remote_payload
    )

    local_keys = set(local_records)
    remote_keys = set(remote_records)

    common_keys = sorted(
        local_keys & remote_keys
    )

    value_changes = []
    metadata_changes = []

    for key in common_keys:
        local = local_records[key]
        remote = remote_records[key]

        if local.get("value") != remote.get("value"):
            value_changes.append(
                {
                    "key": key,
                    "old_value": remote.get("value"),
                    "new_value": local.get("value"),
                    "unit": local.get("value-units"),
                }
            )

        elif local != remote:
            metadata_changes.append(key)

    print("=" * 55)
    print("EIA DATA COMPARISON")
    print("=" * 55)

    print(
        "Local records:",
        len(local_records)
    )

    print(
        "Azure records:",
        len(remote_records)
    )

    print(
        "Missing in Azure:",
        len(local_keys - remote_keys)
    )

    print(
        "Extra in Azure:",
        len(remote_keys - local_keys)
    )

    print(
        "Changed values:",
        len(value_changes)
    )

    print(
        "Metadata-only changes:",
        len(metadata_changes)
    )

    print()
    print("VALUE CHANGE EXAMPLES")

    for item in value_changes[:10]:
        print(
            f"{item['key']} | "
            f"Old: {item['old_value']} | "
            f"New: {item['new_value']} | "
            f"Unit: {item['unit']}"
        )

    print()
    print("METADATA CHANGE EXAMPLES")

    for key in metadata_changes[:5]:
        local = local_records[key]
        remote = remote_records[key]

        changed_fields = [
            field
            for field in set(local) | set(remote)
            if local.get(field) != remote.get(field)
        ]

        print(
            f"{key}: {changed_fields}"
        )


if __name__ == "__main__":
    main()
