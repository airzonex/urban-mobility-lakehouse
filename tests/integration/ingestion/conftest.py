import os
from collections.abc import Iterator
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from dotenv import load_dotenv


@pytest.fixture
def s3_client(pytestconfig: pytest.Config):
    load_dotenv(pytestconfig.rootpath / ".env")

    client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{os.environ['MINIO_API_PORT']}",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name=os.environ["MINIO_REGION"],
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=10,
            retries={"total_max_attempts": 1},
        ),
    )

    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def test_bucket(s3_client) -> Iterator[str]:
    bucket = f"test-tlc-{uuid4().hex}"

    region = s3_client.meta.region_name
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

    try:
        yield bucket
    finally:
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3_client.delete_object(
                    Bucket=bucket,
                    Key=obj["Key"],
                )

        s3_client.delete_bucket(Bucket=bucket)