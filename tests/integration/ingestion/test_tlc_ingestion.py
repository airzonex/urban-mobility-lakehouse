import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from urban_mobility.tlc_ingestion import ingest_month


def test_ingest_month_uploads_file_and_metadata(
    s3_client,
    test_bucket: str,
) -> None:
    content = b"TLC integration test content"
    sha256 = hashlib.sha256(content).hexdigest()
    downloaded_at = "2024-02-01T12:00:00+00:00"

    filename = "yellow_tripdata_2024-01.parquet"
    source_url = (
        "https://d37ci6vzurychx.cloudfront.net/"
        f"trip-data/{filename}"
    )
    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/{filename}"
    metadata_key = f"{object_prefix}/metadata.json"

    def fake_download(
        url: str,
        destination: Path,
    ) -> tuple[str, int, str]:
        assert url == source_url
        destination.write_bytes(content)
        return sha256, len(content), downloaded_at

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        side_effect=fake_download,
    ):
        ingest_month(
            s3=s3_client,
            bucket=test_bucket,
            year=2024,
            month=1,
        )

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=object_key,
    )
    body = response["Body"]
    try:
        actual_content = body.read()
    finally:
        body.close()

    assert actual_content == content
    assert response["ContentLength"] == len(content)

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=metadata_key,
    )
    body = response["Body"]
    try:
        actual_metadata = json.loads(body.read())
    finally:
        body.close()

    assert response["ContentType"] == "application/json"
    assert actual_metadata == {
        "source_url": source_url,
        "year": 2024,
        "month": 1,
        "filename": filename,
        "sha256": sha256,
        "size_bytes": len(content),
        "downloaded_at": downloaded_at,
    }

    paginator = s3_client.get_paginator("list_objects_v2")
    actual_keys = {
        obj["Key"]
        for page in paginator.paginate(Bucket=test_bucket)
        for obj in page.get("Contents", [])
    }

    assert actual_keys == {object_key, metadata_key}

def test_ingest_month_skips_existing_data(
    s3_client,
    test_bucket: str,
) -> None:
    content = b"TLC integration test content"
    sha256 = hashlib.sha256(content).hexdigest()

    first_downloaded_at = "2024-02-01T12:00:00+00:00"
    second_downloaded_at = "2024-02-02T12:00:00+00:00"
    download_times = iter([
        first_downloaded_at,
        second_downloaded_at,
    ])

    filename = "yellow_tripdata_2024-01.parquet"
    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/{filename}"
    metadata_key = f"{object_prefix}/metadata.json"

    def fake_download(
        url: str,
        destination: Path,
    ) -> tuple[str, int, str]:
        destination.write_bytes(content)
        return sha256, len(content), next(download_times)

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        side_effect=fake_download,
    ):
        # Первый запуск действительно записывает объекты в MinIO.
        ingest_month(
            s3=s3_client,
            bucket=test_bucket,
            year=2024,
            month=1,
        )

        # Наблюдаем за операциями второго запуска.
        # wraps сохраняет настоящее поведение методов.
        with (
            patch.object(
                s3_client,
                "upload_file",
                wraps=s3_client.upload_file,
            ) as upload_file,
            patch.object(
                s3_client,
                "put_object",
                wraps=s3_client.put_object,
            ) as put_object,
            patch.object(
                s3_client,
                "delete_object",
                wraps=s3_client.delete_object,
            ) as delete_object,
        ):
            ingest_month(
                s3=s3_client,
                bucket=test_bucket,
                year=2024,
                month=1,
            )

        upload_file.assert_not_called()
        put_object.assert_not_called()
        delete_object.assert_not_called()

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=object_key,
    )
    body = response["Body"]
    try:
        assert body.read() == content
    finally:
        body.close()

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=metadata_key,
    )
    body = response["Body"]
    try:
        metadata = json.loads(body.read())
    finally:
        body.close()

    assert metadata["sha256"] == sha256
    assert metadata["size_bytes"] == len(content)
    assert metadata["downloaded_at"] == first_downloaded_at

def test_ingest_month_completes_upload_without_metadata(
    s3_client,
    test_bucket: str,
) -> None:
    content = b"TLC integration test content"
    sha256 = hashlib.sha256(content).hexdigest()
    downloaded_at = "2024-02-01T12:00:00+00:00"

    filename = "yellow_tripdata_2024-01.parquet"
    source_url = (
        "https://d37ci6vzurychx.cloudfront.net/"
        f"trip-data/{filename}"
    )
    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/{filename}"
    metadata_key = f"{object_prefix}/metadata.json"

    # Имитируем результат сбоя после загрузки Parquet
    # metadata.json отсутствует, parquet присутствует 
    s3_client.put_object(
        Bucket=test_bucket,
        Key=object_key,
        Body=content,
    )

    def fake_download(
        url: str,
        destination: Path,
    ) -> tuple[str, int, str]:
        assert url == source_url
        destination.write_bytes(content)
        return sha256, len(content), downloaded_at

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        side_effect=fake_download,
    ):
        ingest_month(
            s3=s3_client,
            bucket=test_bucket,
            year=2024,
            month=1,
        )

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=object_key,
    )
    body = response["Body"]
    try:
        assert body.read() == content
    finally:
        body.close()

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=metadata_key,
    )
    body = response["Body"]
    try:
        metadata = json.loads(body.read())
    finally:
        body.close()

    assert metadata == {
        "source_url": source_url,
        "year": 2024,
        "month": 1,
        "filename": filename,
        "sha256": sha256,
        "size_bytes": len(content),
        "downloaded_at": downloaded_at,
    }

    paginator = s3_client.get_paginator("list_objects_v2")
    actual_keys = {
        obj["Key"]
        for page in paginator.paginate(Bucket=test_bucket)
        for obj in page.get("Contents", [])
    }

    assert actual_keys == {object_key, metadata_key}

@pytest.mark.parametrize(
    "broken_state",
    [
        pytest.param("invalid-metadata", id="invalid-metadata"),
        pytest.param("missing-parquet", id="missing-parquet"),
        pytest.param("wrong-parquet-size", id="wrong-parquet-size"),
    ],
)
def test_ingest_month_repairs_inconsistent_storage(
    s3_client,
    test_bucket: str,
    broken_state: str,
) -> None:
    content = b"TLC integration test content"
    sha256 = hashlib.sha256(content).hexdigest()
    downloaded_at = "2024-02-01T12:00:00+00:00"

    filename = "yellow_tripdata_2024-01.parquet"
    source_url = (
        "https://d37ci6vzurychx.cloudfront.net/"
        f"trip-data/{filename}"
    )
    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/{filename}"
    metadata_key = f"{object_prefix}/metadata.json"

    expected_metadata = {
        "source_url": source_url,
        "year": 2024,
        "month": 1,
        "filename": filename,
        "sha256": sha256,
        "size_bytes": len(content),
        "downloaded_at": downloaded_at,
    }

    # Подготавливаем некорректное состояние хранилища.
    metadata_body = (
        b'{"year":'
        if broken_state == "invalid-metadata"
        else json.dumps(expected_metadata).encode("utf-8")
    )

    s3_client.put_object(
        Bucket=test_bucket,
        Key=metadata_key,
        Body=metadata_body,
        ContentType="application/json",
    )

    if broken_state != "missing-parquet":
        stored_content = (
            b"wrong"
            if broken_state == "wrong-parquet-size"
            else content
        )

        s3_client.put_object(
            Bucket=test_bucket,
            Key=object_key,
            Body=stored_content,
        )

    def fake_download(
        url: str,
        destination: Path,
    ) -> tuple[str, int, str]:
        assert url == source_url
        destination.write_bytes(content)
        return sha256, len(content), downloaded_at

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        side_effect=fake_download,
    ):
        ingest_month(
            s3=s3_client,
            bucket=test_bucket,
            year=2024,
            month=1,
        )

    # Проверяем фактическое содержимое восстановленных объектов.
    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=object_key,
    )
    body = response["Body"]
    try:
        assert body.read() == content
    finally:
        body.close()

    response = s3_client.get_object(
        Bucket=test_bucket,
        Key=metadata_key,
    )
    body = response["Body"]
    try:
        actual_metadata = json.loads(body.read())
    finally:
        body.close()

    assert actual_metadata == expected_metadata
    assert response["ContentType"] == "application/json"

    paginator = s3_client.get_paginator("list_objects_v2")
    actual_keys = {
        obj["Key"]
        for page in paginator.paginate(Bucket=test_bucket)
        for obj in page.get("Contents", [])
    }

    assert actual_keys == {object_key, metadata_key}