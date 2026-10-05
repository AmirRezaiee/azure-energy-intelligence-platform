import os
import json
import time
import hashlib

from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient


# =====================================================
# CONFIGURATION
# =====================================================

BASE_URL = (
    "https://api.eia.gov/v2/"
    "electricity/rto/region-data/data/"
)

STORAGE_ACCOUNT = "stdata2026"
CONTAINER = "raw"
RESPONDENT = "US48"

INITIAL_DATE = "2026-09-26"

# Local state is retained only for one-time migration/bootstrap
# and local visibility. Cloud state becomes the source of truth.
LOCAL_STATE_FILE = Path(
    "data/state/eia_us48_adls_watermark.json"
)

CLOUD_STATE_PATH = (
    "_pipeline_state/"
    "eia/"
    "region-data/"
    f"{RESPONDENT}/"
    "watermark.json"
)

LOCAL_DIR = Path("data/raw/daily")

PAGE_SIZE = 100
CHUNK_SIZE = 4 * 1024 * 1024

DATE_FORMAT = "%Y-%m-%d"
HOUR_FORMAT = "%Y-%m-%dT%H"


# =====================================================
# FILE AND CHECKSUM HELPERS
# =====================================================

def sha256(data):
    return hashlib.sha256(data).hexdigest()


def save_json_atomic(path, payload):
    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_name(
        path.name + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            payload,
            file,
            indent=2,
            ensure_ascii=False
        )
        file.flush()
        os.fsync(file.fileno())

    temporary.replace(path)


def serialize_payload(payload):
    return json.dumps(
        payload,
        indent=2,
        ensure_ascii=False
    ).encode("utf-8")


def normalize_records(payload):
    """
    Compare actual EIA records rather than
    JSON formatting or response metadata.
    """

    records = payload["response"]["data"]

    result = {}

    for record in records:
        key = (
            record["period"],
            record["respondent"],
            record["type"]
        )

        if key in result:
            raise ValueError(
                "Duplicate record detected."
            )

        result[key] = record

    return result


def records_are_equal(first, second):
    return (
        normalize_records(first)
        == normalize_records(second)
    )


# =====================================================
# LOCAL WATERMARK
# =====================================================

def load_local_watermark_payload():
    """
    Local watermark is used only to bootstrap the
    cloud watermark when the cloud state does not
    exist yet.
    """

    if not LOCAL_STATE_FILE.exists():
        return None

    with LOCAL_STATE_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        payload = json.load(file)

    validate_watermark_payload(payload)

    return payload


def save_local_watermark_copy(payload):
    """
    Keep a local copy for development visibility.

    The local file is not the source of truth after
    cloud watermark initialization.
    """

    save_json_atomic(
        LOCAL_STATE_FILE,
        payload
    )


# =====================================================
# WATERMARK VALIDATION
# =====================================================

def validate_watermark_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError(
            "Watermark payload must be a JSON object."
        )

    last_uploaded_hour = payload.get(
        "last_uploaded_hour"
    )

    if not last_uploaded_hour:
        raise ValueError(
            "Watermark is missing last_uploaded_hour."
        )

    try:
        parsed = datetime.strptime(
            last_uploaded_hour,
            HOUR_FORMAT
        )
    except ValueError as exc:
        raise ValueError(
            "Invalid watermark hour format."
        ) from exc

    if parsed.hour != 23:
        raise ValueError(
            "Watermark must represent a complete day "
            "ending at hour 23."
        )


# =====================================================
# AZURE STORAGE
# =====================================================

def create_filesystem():
    credential = AzureCliCredential()

    service = DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT}"
            ".dfs.core.windows.net"
        ),
        credential=credential
    )

    return service.get_file_system_client(
        CONTAINER
    )


def ensure_directories(filesystem, path):
    parent = path.rsplit("/", 1)[0]
    current = ""

    for part in parent.split("/"):
        current = (
            f"{current}/{part}"
            if current
            else part
        )

        directory = (
            filesystem.get_directory_client(
                current
            )
        )

        if not directory.exists():
            directory.create_directory()


def upload_new_file(filesystem, path, data):
    """
    Create, append, flush and verify.

    Never automatically overwrite an
    existing immutable data file.
    """

    ensure_directories(
        filesystem,
        path
    )

    remote = filesystem.get_file_client(
        path
    )

    if remote.exists():
        existing = (
            remote.download_file().readall()
        )

        if sha256(existing) == sha256(data):
            print(
                "Identical file already exists. "
                "Upload skipped."
            )
            return

        raise FileExistsError(
            "Destination already exists "
            "with different content."
        )

    remote.create_file()

    offset = 0

    for position in range(
        0,
        len(data),
        CHUNK_SIZE
    ):
        chunk = data[
            position:position + CHUNK_SIZE
        ]

        remote.append_data(
            data=chunk,
            offset=offset,
            length=len(chunk)
        )

        offset += len(chunk)

    remote.flush_data(offset)

    uploaded = (
        remote.download_file().readall()
    )

    if sha256(uploaded) != sha256(data):
        raise ValueError(
            "Azure checksum verification failed."
        )

    print(
        f"Upload verified: {len(data)} bytes"
    )


def write_cloud_state_file(filesystem, path, data):
    """
    Write mutable pipeline state to ADLS.

    Unlike immutable raw data files, the watermark
    is intentionally replaced as the pipeline
    successfully advances.

    The uploaded bytes are read back and verified
    before the state update is considered successful.
    """

    ensure_directories(
        filesystem,
        path
    )

    remote = filesystem.get_file_client(
        path
    )

    remote.create_file()

    offset = 0

    for position in range(
        0,
        len(data),
        CHUNK_SIZE
    ):
        chunk = data[
            position:position + CHUNK_SIZE
        ]

        remote.append_data(
            data=chunk,
            offset=offset,
            length=len(chunk)
        )

        offset += len(chunk)

    remote.flush_data(offset)

    uploaded = (
        remote.download_file().readall()
    )

    if sha256(uploaded) != sha256(data):
        raise ValueError(
            "Cloud watermark checksum "
            "verification failed."
        )


# =====================================================
# CLOUD WATERMARK
# =====================================================

def load_cloud_watermark_payload(filesystem):
    remote = filesystem.get_file_client(
        CLOUD_STATE_PATH
    )

    if not remote.exists():
        return None

    raw = remote.download_file().readall()

    try:
        payload = json.loads(
            raw.decode("utf-8")
        )
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(
            "Cloud watermark is not valid JSON. "
            "Manual inspection required."
        ) from exc

    validate_watermark_payload(payload)

    return payload


def initialize_cloud_watermark(filesystem):
    """
    Establish ADLS as the source of truth.

    If cloud state already exists, use it.

    If cloud state does not exist but the previous
    local watermark exists, migrate that state to
    ADLS exactly once.

    If neither exists, the pipeline starts from
    INITIAL_DATE and creates cloud state only after
    the first successfully processed day.
    """

    cloud_payload = load_cloud_watermark_payload(
        filesystem
    )

    if cloud_payload is not None:
        print(
            "Cloud watermark found: "
            f"{cloud_payload['last_uploaded_hour']}"
        )

        save_local_watermark_copy(
            cloud_payload
        )

        return cloud_payload

    print(
        "Cloud watermark not found."
    )

    local_payload = load_local_watermark_payload()

    if local_payload is None:
        print(
            "Local watermark not found. "
            "Pipeline will start from INITIAL_DATE."
        )
        return None

    print(
        "Bootstrapping cloud watermark from "
        "existing local state: "
        f"{local_payload['last_uploaded_hour']}"
    )

    data = serialize_payload(
        local_payload
    )

    write_cloud_state_file(
        filesystem,
        CLOUD_STATE_PATH,
        data
    )

    verified = load_cloud_watermark_payload(
        filesystem
    )

    if (
        verified["last_uploaded_hour"]
        != local_payload["last_uploaded_hour"]
    ):
        raise ValueError(
            "Cloud watermark bootstrap "
            "verification failed."
        )

    print(
        "Cloud watermark bootstrap: VERIFIED"
    )

    return verified


def load_watermark(filesystem):
    payload = load_cloud_watermark_payload(
        filesystem
    )

    if payload is None:
        return None

    return payload["last_uploaded_hour"]


def calculate_start_date(filesystem):
    watermark = load_watermark(
        filesystem
    )

    if watermark is None:
        return datetime.strptime(
            INITIAL_DATE,
            DATE_FORMAT
        ).date()

    last_date = datetime.strptime(
        watermark,
        HOUR_FORMAT
    ).date()

    # Revisit the previous calendar day as well
    # as the most recently uploaded day.
    start_date = (
        last_date - timedelta(days=1)
    )

    initial_date = datetime.strptime(
        INITIAL_DATE,
        DATE_FORMAT
    ).date()

    return max(
        start_date,
        initial_date
    )


def save_watermark(filesystem, day):
    """
    Advance cloud checkpoint monotonically.

    Cloud state is the source of truth.
    A local copy is updated only after the cloud
    write has been successfully verified.
    """

    new_hour = f"{day.isoformat()}T23"

    previous = load_watermark(
        filesystem
    )

    if previous is not None:
        new_hour = max(
            previous,
            new_hour
        )

    payload = {
        "last_uploaded_hour": new_hour,
        "updated_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        )
    }

    data = serialize_payload(
        payload
    )

    write_cloud_state_file(
        filesystem,
        CLOUD_STATE_PATH,
        data
    )

    verified = load_cloud_watermark_payload(
        filesystem
    )

    if (
        verified["last_uploaded_hour"]
        != new_hour
    ):
        raise ValueError(
            "Cloud watermark verification failed."
        )

    # Local copy is informational only.
    save_local_watermark_copy(
        verified
    )

    return new_hour


# =====================================================
# EIA API
# =====================================================

def create_session():
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[
            429, 500, 502, 503, 504
        ],
        allowed_methods=["GET"],
        respect_retry_after_header=True
    )

    session = requests.Session()

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry)
    )

    return session


def fetch_day(session, api_key, day):
    start = f"{day.isoformat()}T00"
    end = f"{day.isoformat()}T23"

    records = []
    offset = 0
    expected_total = None

    while True:
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[0]": "value",
            "facets[respondent][]": RESPONDENT,
            "facets[type][]": ["D", "DF"],
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "sort[1][column]": "type",
            "sort[1][direction]": "asc",
            "offset": offset,
            "length": PAGE_SIZE
        }

        response = session.get(
            BASE_URL,
            params=params,
            timeout=30
        )

        # Do not expose the request URL:
        # it contains the EIA API key.
        if not response.ok:
            raise RuntimeError(
                "EIA request failed. "
                f"HTTP status: {response.status_code}"
            )

        result = response.json()["response"]

        page = result["data"]
        total = int(result["total"])

        if expected_total is None:
            expected_total = total

        if total != expected_total:
            raise ValueError(
                "EIA total changed during pagination."
            )

        if not page and offset < total:
            raise ValueError(
                "Unexpected empty EIA page."
            )

        records.extend(page)
        offset += len(page)

        if offset >= total:
            break

        time.sleep(0.2)

    if len(records) != expected_total:
        raise ValueError(
            "Incomplete EIA download."
        )

    return records


# =====================================================
# DATA QUALITY
# =====================================================

def validate_day(records, day):
    expected = {
        (
            f"{day.isoformat()}T{hour:02d}",
            RESPONDENT,
            record_type
        )
        for hour in range(24)
        for record_type in ("D", "DF")
    }

    actual = []

    for record in records:
        if record.get("value") is None:
            raise ValueError(
                "Null EIA value detected."
            )

        actual.append(
            (
                record["period"],
                record["respondent"],
                record["type"]
            )
        )

    if len(actual) != len(set(actual)):
        raise ValueError(
            "Duplicate EIA records detected."
        )

    if set(actual) != expected:
        raise ValueError(
            "Incomplete or unexpected daily data. "
            f"Expected 48 records; received {len(actual)}."
        )

    print(
        f"Data Quality: PASSED "
        f"({len(records)} records)"
    )


# =====================================================
# RAW ADLS PROCESSING
# =====================================================

def process_azure_file(
    filesystem,
    base_path,
    filename,
    payload
):
    canonical_path = (
        f"{base_path}/{filename}"
    )

    canonical = (
        filesystem.get_file_client(
            canonical_path
        )
    )

    new_data = serialize_payload(
        payload
    )

    # CASE 1: No canonical file exists.
    if not canonical.exists():
        print("New daily file detected.")

        upload_new_file(
            filesystem,
            canonical_path,
            new_data
        )

        return "NEW"

    # CASE 2: Compare existing EIA records.
    existing_data = (
        canonical.download_file().readall()
    )

    try:
        existing_payload = json.loads(
            existing_data
        )
    except (ValueError, UnicodeDecodeError):
        raise ValueError(
            "Existing canonical file is not "
            "valid JSON. Manual inspection required."
        )

    if records_are_equal(
        existing_payload,
        payload
    ):
        print(
            "Canonical EIA records unchanged. "
            "No upload needed."
        )
        return "UNCHANGED"

    # CASE 3: Historical EIA revision.
    print(
        "Historical EIA revision detected."
    )

    # Use normalized business records for
    # a stable content-based revision ID.
    normalized = sorted(
        normalize_records(payload).values(),
        key=lambda r: (
            r["period"],
            r["respondent"],
            r["type"]
        )
    )

    normalized_data = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False
    ).encode("utf-8")

    revision_id = sha256(
        normalized_data
    )

    revision_name = (
        filename.removesuffix(".json")
        + f"_revision_{revision_id}.json"
    )

    revision_path = (
        f"{base_path}/revisions/"
        f"{revision_name}"
    )

    upload_new_file(
        filesystem,
        revision_path,
        new_data
    )

    return "REVISION"


# =====================================================
# MAIN PIPELINE
# =====================================================

def main():
    api_key = os.getenv(
        "EIA_API_KEY"
    )

    if not api_key:
        raise ValueError(
            "EIA_API_KEY is not configured."
        )

    end_value = os.getenv(
        "EIA_END_HOUR"
    )

    if not end_value:
        raise ValueError(
            "EIA_END_HOUR is not configured."
        )

    end_hour = datetime.strptime(
        end_value,
        HOUR_FORMAT
    )

    # This version deliberately processes
    # complete calendar days only.
    if end_hour.hour != 23:
        raise ValueError(
            "EIA_END_HOUR must end at 23."
        )

    # Azure connection is established before
    # calculating the start date because ADLS
    # is now the checkpoint source of truth.
    filesystem = create_filesystem()

    initialize_cloud_watermark(
        filesystem
    )

    start_date = calculate_start_date(
        filesystem
    )

    end_date = end_hour.date()

    print("=" * 55)
    print("EIA TO ADLS INCREMENTAL PIPELINE")
    print("Checkpoint source: ADLS Gen2")
    print(f"Cloud state: {CLOUD_STATE_PATH}")
    print(f"Start date: {start_date}")
    print(f"End date:   {end_date}")
    print("=" * 55)

    if start_date > end_date:
        print("Nothing to process.")
        return

    session = create_session()

    current_day = start_date

    try:
        while current_day <= end_date:
            print()
            print(
                f"Processing: {current_day}"
            )

            # 1. Download
            records = fetch_day(
                session,
                api_key,
                current_day
            )

            # 2. Validate
            validate_day(
                records,
                current_day
            )

            # 3. Create deterministic payload
            payload = {
                "response": {
                    "data": sorted(
                        records,
                        key=lambda r: (
                            r["period"],
                            r["respondent"],
                            r["type"]
                        )
                    ),
                    "total": str(
                        len(records)
                    )
                }
            }

            filename = (
                f"eia_us48_"
                f"{current_day.isoformat()}.json"
            )

            local_path = (
                LOCAL_DIR / filename
            )

            save_json_atomic(
                local_path,
                payload
            )

            # 4. Determine daily ADLS path
            base_path = (
                "eia/region-data/"
                f"respondent={RESPONDENT}/"
                f"year={current_day.year:04d}/"
                f"month={current_day.month:02d}/"
                f"day={current_day.day:02d}"
            )

            # 5. Upload or version
            result = process_azure_file(
                filesystem,
                base_path,
                filename,
                payload
            )

            print(
                f"Storage result: {result}"
            )

            # 6. Advance cloud checkpoint only
            # after successful raw storage processing.
            watermark = save_watermark(
                filesystem,
                current_day
            )

            print(
                f"Cloud watermark: {watermark}"
            )

            current_day += timedelta(
                days=1
            )

    finally:
        session.close()

    print()
    print(
        "PIPELINE COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    main()