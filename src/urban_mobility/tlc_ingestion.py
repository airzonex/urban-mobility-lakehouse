import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import httpx
from botocore.config import Config
from dotenv import load_dotenv


def _make_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def _make_url(filename: str) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"


def _download_file(
    url: str,
    destination: Path,
) -> tuple[str, int, str]:
    checksum = hashlib.sha256()
    size_bytes = 0

    with (
        httpx.Client(timeout=30, follow_redirects=True) as client,
        client.stream("GET", url) as response,
    ):
        response.raise_for_status()

        with destination.open("wb") as output:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                output.write(chunk)
                checksum.update(chunk)
                size_bytes += len(chunk)

    downloaded_at = datetime.now(timezone.utc).isoformat()

    return checksum.hexdigest(), size_bytes, downloaded_at


def _upload_file(
    s3,
    destination: Path,
    bucket: str,
    object_key: str,
) -> None:
    s3.upload_file(
        Filename=str(destination),
        Bucket=bucket,
        Key=object_key,
    )

    print(f"Uploaded: s3://{bucket}/{object_key}")


def _upload_metadata(
    s3,
    bucket: str,
    metadata_key: str,
    metadata: dict[str, str | int],
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Uploaded: s3://{bucket}/{metadata_key}")


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()

    if not 1000 <= args.year <= 9999:
        parser.error("--year must be a four-digit year")

    filename = _make_filename(args.year, args.month)
    url = _make_url(filename)

    endpoint = f"http://localhost:{os.environ['MINIO_API_PORT']}"
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    region = os.environ["MINIO_REGION"]
    bucket = os.environ["MINIO_RAW_BUCKET"]

    with TemporaryDirectory() as temp_dir:
        destination = Path(temp_dir) / filename

        sha256, size_bytes, downloaded_at = _download_file(url, destination)

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(s3={"addressing_style": "path"}),
        )

        object_prefix = (
            f"tlc/yellow/year={args.year}/month={args.month:02d}/sha256={sha256}"
        )

        object_key = f"{object_prefix}/{filename}"
        metadata_key = f"{object_prefix}/metadata.json"

        _upload_file(
            s3=s3,
            destination=destination,
            bucket=bucket,
            object_key=object_key,
        )

        metadata = {
            "source_url": url,
            "year": args.year,
            "month": args.month,
            "filename": filename,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "downloaded_at": downloaded_at,
        }

        _upload_metadata(
            s3=s3,
            bucket=bucket,
            metadata_key=metadata_key,
            metadata=metadata,
        )


if __name__ == "__main__":
    main()
