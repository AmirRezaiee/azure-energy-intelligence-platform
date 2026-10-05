
import hashlib
from pathlib import Path

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient


ACCOUNT = "stdata2026"
CONTAINER = "raw"

LOCAL_FILE = Path(
    "data/raw/daily/eia_us48_2026-09-26.json"
)

BASE_PATH = (
    "eia/region-data/"
    "respondent=US48/"
    "year=2026/month=09/day=26"
)

CHUNK_SIZE = 4 * 1024 * 1024


def ensure_directories(filesystem, path):
    current = ""

    for part in path.split("/"):
        current = f"{current}/{part}" if current else part
        directory = filesystem.get_directory_client(current)

        if not directory.exists():
            directory.create_directory()
            print(f"Created directory: {current}")


def main():
    if not LOCAL_FILE.is_file():
        raise FileNotFoundError(LOCAL_FILE)

    data = LOCAL_FILE.read_bytes()
    content_hash = hashlib.sha256(data).hexdigest()

    # A content-based name makes identical retries idempotent.
    filename = (
        f"eia_us48_2026-09-26_"
        f"sha256_{content_hash}.json"
    )

    destination = f"{BASE_PATH}/revisions/{filename}"

    credential = AzureCliCredential()

    service = DataLakeServiceClient(
        account_url=f"https://{ACCOUNT}.dfs.core.windows.net",
        credential=credential,
    )

    filesystem = service.get_file_system_client(CONTAINER)

    ensure_directories(
        filesystem,
        f"{BASE_PATH}/revisions",
    )

    remote = filesystem.get_file_client(destination)

    if remote.exists():
        existing = remote.download_file().readall()

        if hashlib.sha256(existing).hexdigest() == content_hash:
            print("Identical revision already exists.")
            print("Upload skipped safely.")
            print(f"SHA256: {content_hash}")
            return

        raise ValueError(
            "Existing revision has unexpected content. "
            "Manual inspection required."
        )

    print("Creating new revision...")
    remote.create_file()

    offset = 0

    for position in range(0, len(data), CHUNK_SIZE):
        chunk = data[position:position + CHUNK_SIZE]

        remote.append_data(
            data=chunk,
            offset=offset,
            length=len(chunk),
        )

        offset += len(chunk)

    remote.flush_data(offset)

    uploaded = remote.download_file().readall()

    if hashlib.sha256(uploaded).hexdigest() != content_hash:
        raise ValueError(
            "Upload verification failed. "
            "Do not advance the watermark."
        )

    print("REVISION UPLOAD: SUCCESS")
    print(f"Records file: {LOCAL_FILE}")
    print(f"Uploaded bytes: {len(uploaded)}")
    print(f"SHA256: {content_hash}")
    print(f"ADLS path: {destination}")


if __name__ == "__main__":
    main()
