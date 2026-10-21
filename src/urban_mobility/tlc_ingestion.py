import argparse
import os
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


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()

    if not 1000 <= args.year <= 9999:
        parser.error("--year must be a four-digit year")

    filename = _make_filename(int(args.year), int(args.month))
    url = _make_url(filename)

    endpoint = f'http://localhost:{os.environ["MINIO_API_PORT"]}'
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    region = os.environ["MINIO_REGION"]
    bucket = os.environ["MINIO_RAW_BUCKET"]
    object_key = f"tlc/yellow/year={args.year}/month={args.month:02d}/{filename}"

    with TemporaryDirectory() as temp_dir:
        destination = Path(temp_dir) / filename

        with (
            httpx.Client(timeout=30, follow_redirects=True) as client,
            client.stream("GET", url) as response
        ):
                response.raise_for_status()

                with destination.open("wb") as output:
                    for chunk in response.iter_bytes():
                        output.write(chunk)

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(s3={"addressing_style": "path"}),
        )

        s3.upload_file(
            Filename=str(destination),
            Bucket=bucket,
            Key=object_key,
        )

        print(f"Uploaded: s3://{bucket}/{object_key}")


if __name__ == "__main__":
    main()
