import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import httpx
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class InvalidMetadataError(ValueError):
    pass


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

    logger.info("Uploaded: s3://%s/%s", bucket, object_key)


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

    logger.info("Uploaded: s3://%s/%s", bucket, metadata_key)


def _read_metadata(
    s3,
    bucket: str,
    metadata_key: str,
) -> dict | None:
    try:
        response = s3.get_object(
            Bucket=bucket,
            Key=metadata_key
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise

    body = response["Body"]
    try:
        try:
            metadata = json.loads(body.read())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise InvalidMetadataError(
                f"Invalid JSON: s3://{bucket}/{metadata_key}"
            ) from e
    finally:
        body.close()

    if not isinstance(metadata, dict):
        raise InvalidMetadataError(
            f"Metadata must be a JSON object: s3://{bucket}/{metadata_key}"
        )

    return metadata


def _get_parquet_length(s3, bucket: str, object_key: str) -> int | None:
    try:
        response = s3.head_object(
            Bucket=bucket,
            Key=object_key
        )
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return None
        raise

    return response["ContentLength"]

def ingest_month(
    s3,
    bucket: str,
    year: int,
    month: int,
) -> None:
    def _upload_data() -> None:
        _upload_file(
            s3=s3,
            destination=destination,
            bucket=bucket,
            object_key=object_key,
        )

        metadata = {
            "source_url": url,
            "year": year,
            "month": month,
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


    def _repair_data() -> None:
        logger.info("Removing invalid metadata: s3://%s/%s", bucket, metadata_key)

        s3.delete_object(
            Bucket=bucket,
            Key=metadata_key
        )

        _upload_data()


    filename = _make_filename(year, month)
    url = _make_url(filename)

    with TemporaryDirectory() as temp_dir:
        destination = Path(temp_dir) / filename

        sha256, size_bytes, downloaded_at = _download_file(url, destination)

        object_prefix = (
            f"tlc/yellow/year={year}/month={month:02d}/sha256={sha256}"
        )

        object_key = f"{object_prefix}/{filename}"
        metadata_key = f"{object_prefix}/metadata.json"

        try:
            metadata = _read_metadata(s3, bucket, metadata_key)
        except InvalidMetadataError:
            logger.warning(
                "Invalid metadata: s3://%s/%s; starting repair",
                bucket,
                metadata_key,
            )
            _repair_data()
            return

        if metadata is None:
            _upload_data()
            return
        
        expected = {
            "source_url": url,
            "year": year,
            "month": month,
            "filename": filename,
            "sha256": sha256,
            "size_bytes": size_bytes,
        }

        downloaded_at_value = metadata.get("downloaded_at")

        if (
            any(metadata.get(key) != value for key, value in expected.items())
            or not isinstance(downloaded_at_value, str)
            or not downloaded_at_value
        ):
            logger.warning(
                "Metadata mismatch: s3://%s/%s; starting repair",
                bucket,
                metadata_key,
            )
            _repair_data()
            return
        
        parquet_length = _get_parquet_length(s3, bucket, object_key)

        if parquet_length is None:
            logger.warning(
                "Stored Parquet file is missing: s3://%s/%s; starting repair",
                bucket,
                object_key,
            )
            _repair_data()
            return
        
        if parquet_length != size_bytes:
            logger.warning(
                "Stored Parquet size mismatch: expected=%s, actual=%s; starting repair",
                size_bytes,
                parquet_length,
            )
            _repair_data()
            return

        logger.info("SKIPPED: s3://%s/%s", bucket, object_key)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()

    if not 1000 <= args.year <= 9999:
        parser.error("--year must be a four-digit year")

    endpoint = f"http://localhost:{os.environ['MINIO_API_PORT']}"
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    region = os.environ["MINIO_REGION"]
    bucket = os.environ["MINIO_RAW_BUCKET"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}),
    )

    try:
        ingest_month(
            s3=s3,
            bucket=bucket,
            year=args.year,
            month=args.month,
        )
    finally:
        s3.close()

if __name__ == "__main__":
    main()
