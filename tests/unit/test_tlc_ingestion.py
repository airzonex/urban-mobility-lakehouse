import hashlib
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import httpx
import pytest
from botocore.exceptions import ClientError

from urban_mobility.tlc_ingestion import (
    IngestionResult,
    InvalidMetadataError,
    _download_file,
    _get_parquet_length,
    _read_metadata,
    ingest_month,
)


def test_read_metadata_returns_json_object() -> None:
    expected = {
        "year": 2024,
        "month": 1,
        "sha256": "abc123",
        "size_bytes": 100,
    }

    body = io.BytesIO(json.dumps(expected).encode("utf-8"))

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    actual = _read_metadata(
        s3=s3,
        bucket="raw",
        metadata_key="tlc/test/metadata.json",
    )

    assert actual == expected
    assert body.closed

    s3.get_object.assert_called_once_with(
        Bucket="raw",
        Key="tlc/test/metadata.json",
    )

def test_read_metadata_returns_none_when_object_is_missing() -> None:
    s3 = Mock()
    s3.get_object.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "NoSuchKey",
            },
        },
        operation_name="GetObject",
    )

    actual = _read_metadata(
        s3=s3,
        bucket="raw",
        metadata_key="tlc/test/metadata.json",
    )

    assert actual is None

def test_read_metadata_raises_when_access_denied() -> None:
    error = ClientError(
        error_response={
            "Error": {
                "Code": "AccessDenied",
            }
        },
        operation_name="GetObject"
    )
    
    s3 = Mock()
    s3.get_object.side_effect = error

    with pytest.raises(ClientError) as exc_info:
        _read_metadata(
            s3=s3,
            bucket="raw",
            metadata_key="tlc/test/metadata.json"
        )

    assert exc_info.value is error

def test_read_metadata_rejects_invalid_json() -> None:
    body = io.BytesIO(b'{"year": 2024,')
    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with pytest.raises(InvalidMetadataError) as exc_info:
        _read_metadata(
            s3=s3,
            bucket="raw",
            metadata_key="tlc/test/metadata.json",
        )

    assert body.closed
    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)

def test_read_metadata_rejects_json_in_non_utf() -> None:
    body = io.BytesIO(b'{"year": \xff,')
    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with pytest.raises(InvalidMetadataError) as exc_info:
        _read_metadata(
            s3=s3,
            bucket="raw",
            metadata_key="tlc/test/metadata.json",
        )

    assert body.closed
    assert isinstance(exc_info.value.__cause__, UnicodeDecodeError)

@pytest.mark.parametrize(
    "input_value",
    [
        ["year", 2024],
        [],
        "year",
        2024,
        None
    ]
)
def test_read_metadata_raises_if_metadata_is_not_dict(
    input_value: list | str | int | None
) -> None:
    body = io.BytesIO(json.dumps(input_value).encode("utf-8"))
    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with pytest.raises(
        InvalidMetadataError,
        match="Metadata must be a JSON object",
    ):
        _read_metadata(
            s3=s3,
            bucket="raw",
            metadata_key="tlc/test/metadata.json"
        )

    assert body.closed

def test_read_metadata_returns_empty_dict() -> None:
    body = io.BytesIO(b'{}')
    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    actual = _read_metadata(
        s3=s3,
        bucket="raw",
        metadata_key="tlc/test/metadata.json"
    )

    assert actual == {}
    assert body.closed

def test_read_metadata_raises_when_connection_interrupted() -> None:
    error = OSError("Connection interrupted")

    body = Mock()
    body.read.side_effect = error

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with pytest.raises(OSError) as exc_info:
        _read_metadata(
            s3=s3,
            bucket="raw",
            metadata_key="tlc/test/metadata.json",
        )

    assert exc_info.value is error
    body.close.assert_called_once_with()

def test_get_parquet_length_returns_content_length() -> None:
    expected = 100
    s3 = Mock()
    s3.head_object.return_value = {"ContentLength": expected}

    actual = _get_parquet_length(
        s3=s3,
        bucket="raw",
        object_key="test/object.parquet",
    )

    assert actual == expected
    s3.head_object.assert_called_once_with(
        Bucket="raw",
        Key="test/object.parquet"
    )

def test_get_parquet_length_returns_none_when_obj_is_missing() -> None:
    error = ClientError(
        error_response={
            "ResponseMetadata": {
                "HTTPStatusCode": 404
            }
        },
        operation_name="HeadObject"
    )

    s3 = Mock()
    s3.head_object.side_effect = error

    actual = _get_parquet_length(
        s3=s3,
        bucket="raw",
        object_key="test/object.parquet",
    )

    assert actual is None

def test_get_parquet_length_raises_when_connection_error() -> None:
    error = OSError("Connection interrupted")

    s3 = Mock()
    s3.head_object.side_effect = error

    with pytest.raises(OSError) as exc_info:
        _get_parquet_length(
            s3=s3,
            bucket="raw",
            object_key="test/object.parquet",
        )

    assert exc_info.value is error

def test_get_parquet_length_raises_when_access_denied() -> None:
    error = ClientError(
        error_response={
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        operation_name="HeadObject",
    )

    s3 = Mock()
    s3.head_object.side_effect = error

    with pytest.raises(ClientError) as exc_info:
        _get_parquet_length(
            s3=s3,
            bucket="raw",
            object_key="test/object.parquet",
        )

    assert exc_info.value is error

def test_download_file_writes_content_and_returns_metadata(
    tmp_path: Path,
) -> None:
    chunks = [b"first_chunk", b"second_chunk"]
    content = b"".join(chunks)
    destination = tmp_path / "test.parquet"

    response = Mock()
    response.iter_bytes.return_value = iter(chunks)

    with patch("urban_mobility.tlc_ingestion.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.stream.return_value.__enter__.return_value = response

        sha256, size_bytes, downloaded_at = _download_file(
            url="https://example.com/test.parquet",
            destination=destination
        )

    assert destination.read_bytes() == content
    assert sha256 == hashlib.sha256(content).hexdigest()
    assert size_bytes == len(content)

    timestamp = datetime.fromisoformat(downloaded_at)
    assert timestamp.utcoffset() == timedelta(0)

    response.raise_for_status.assert_called_once_with()

def test_download_file_raises_when_http_error_occurs(
    tmp_path: Path
) -> None:
    url = "https://example.com/missing.parquet"
    destination = tmp_path / "test.parquet"

    request = httpx.Request("GET", url)
    http_response = httpx.Response(404, request=request)
    error = httpx.HTTPStatusError(
        "Not Found",
        request=request,
        response=http_response,
    )

    response = Mock()
    response.raise_for_status.side_effect = error

    with patch("urban_mobility.tlc_ingestion.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.stream.return_value.__enter__.return_value = response

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _download_file(
                url=url,
                destination=destination
            )

    assert exc_info.value is error
    assert not destination.exists()
    response.iter_bytes.assert_not_called()

def test_download_file_raises_when_client_stream_open_error_occurs(
    tmp_path: Path
) -> None:
    url = "https://example.com/test.parquet"
    destination = tmp_path / "test.parquet"
    error = httpx.HTTPError("Connection refused")
    
    with patch("urban_mobility.tlc_ingestion.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.stream.side_effect = error

        with pytest.raises(httpx.HTTPError) as exc_info:
            _download_file(
                url=url,
                destination=destination
            )

    assert exc_info.value is error
    assert not destination.exists()

def test_download_file_raises_when_client_stream_interrupts(
    tmp_path: Path
) -> None:
    url = "https://example.com/test.parquet"
    destination = tmp_path / "test.parquet"
    error = httpx.ReadError("Connection interrupted")

    def broken_stream():
        yield b"first_chunk"
        raise error

    response = Mock()
    response.iter_bytes.return_value = broken_stream()

    with patch("urban_mobility.tlc_ingestion.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.stream.return_value.__enter__.return_value = response

        with pytest.raises(httpx.ReadError) as exc_info:
            _download_file(
                url=url,
                destination=destination
            )

    assert exc_info.value is error
    assert destination.read_bytes() == b"first_chunk"

def test_ingest_month_skips_load_if_object_and_metadata_exist() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    metadata = {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

    body = io.BytesIO(json.dumps(metadata).encode("utf-8"))

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}
    s3.head_object.return_value = {"ContentLength": size_bytes}

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ) as download:
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    download.assert_called_once()

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"

    assert result == IngestionResult(
        status="skipped",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    s3.get_object.assert_called_once_with(
        Bucket="raw",
        Key=f"{object_prefix}/metadata.json",
    )

    s3.head_object.assert_called_once_with(
        Bucket="raw",
        Key=f"{object_prefix}/yellow_tripdata_2024-01.parquet",
    )

    s3.upload_file.assert_not_called()
    s3.put_object.assert_not_called()
    s3.delete_object.assert_not_called()

def test_ingest_month_uploads_data_when_metadata_is_missing() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    s3 = Mock()
    s3.get_object.side_effect = ClientError(
        error_response={"Error": {"Code": "NoSuchKey"}},
        operation_name="GetObject",
    )

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ):
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert result == IngestionResult(
        status="uploaded",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    expected_metadata = {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json"
        )
    ]

    uploaded_metadata = json.loads(
        s3.put_object.call_args.kwargs["Body"]
    )
    assert uploaded_metadata == expected_metadata

    uploaded_path = Path(
        s3.upload_file.call_args.kwargs["Filename"]
    )
    assert uploaded_path.name == expected_metadata["filename"]

def test_ingest_month_repairs_invalid_json_metadata() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    body = io.BytesIO(b'{"year":')

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ):
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert result == IngestionResult(
        status="uploaded",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    assert body.closed

    assert s3.mock_calls == [
        call.get_object(
            Bucket="raw",
            Key=metadata_key,
        ),
        call.delete_object(
            Bucket="raw",
            Key=metadata_key,
        ),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json",
        ),
    ]

    uploaded_metadata = json.loads(
        s3.put_object.call_args.kwargs["Body"]
    )

    assert uploaded_metadata == {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

def test_ingest_month_repairs_mismatched_metadata() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    metadata = {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

    stored_metadata = {**metadata, "size_bytes": 999}
    body = io.BytesIO(json.dumps(stored_metadata).encode("utf-8"))

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ):
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert result == IngestionResult(
        status="uploaded",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    assert body.closed

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.delete_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json",
        ),
    ]

    uploaded_metadata = json.loads(
        s3.put_object.call_args.kwargs["Body"]
    )
    assert uploaded_metadata == metadata

@pytest.mark.parametrize(
    "downloaded_at_fields",
    [
        pytest.param({}, id="missing"),
        pytest.param({"downloaded_at": ""}, id="empty"),
        pytest.param({"downloaded_at": None}, id="null"),
        pytest.param({"downloaded_at": 2024}, id="wrong-type"),
    ],
)
def test_ingest_month_repairs_invalid_downloaded_at(
    downloaded_at_fields: dict[str, str | int | None],
) -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    expected_metadata = {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

    stored_metadata = {
        key: value
        for key, value in expected_metadata.items()
        if key != "downloaded_at"
    }
    stored_metadata.update(downloaded_at_fields)

    body = io.BytesIO(json.dumps(stored_metadata).encode("utf-8"))

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ):
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert result == IngestionResult(
        status="uploaded",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    assert body.closed

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.delete_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json",
        ),
    ]

    uploaded_metadata = json.loads(
        s3.put_object.call_args.kwargs["Body"]
    )
    assert uploaded_metadata == expected_metadata

@pytest.mark.parametrize(
    "stored_size",
    [
        pytest.param(None, id="missing-parquet"),
        pytest.param(999, id="size-mismatch"),
    ],
)
def test_ingest_month_repairs_missing_or_wrong_size_parquet(
    stored_size: int | None,
) -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    metadata = {
        "source_url": (
            "https://d37ci6vzurychx.cloudfront.net/"
            "trip-data/yellow_tripdata_2024-01.parquet"
        ),
        "year": 2024,
        "month": 1,
        "filename": "yellow_tripdata_2024-01.parquet",
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded_at": downloaded_at,
    }

    body = io.BytesIO(json.dumps(metadata).encode("utf-8"))

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}

    if stored_size is None:
        s3.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "404"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )
    else:
        s3.head_object.return_value = {"ContentLength": stored_size}

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ):
        result = ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert result == IngestionResult(
        status="uploaded",
        bucket="raw",
        object_key=object_key,
    )
    assert result.input_path == f"s3a://raw/{object_key}"

    assert body.closed

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.head_object(Bucket="raw", Key=object_key),
        call.delete_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json",
        ),
    ]

    uploaded_metadata = json.loads(
        s3.put_object.call_args.kwargs["Body"]
    )
    assert uploaded_metadata == metadata

def test_ingest_month_does_not_write_metadata_when_upload_fails() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    error = OSError("Upload interrupted")

    s3 = Mock()
    s3.get_object.side_effect = ClientError(
        error_response={"Error": {"Code": "NoSuchKey"}},
        operation_name="GetObject",
    )
    s3.upload_file.side_effect = error

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ), pytest.raises(OSError) as exc_info:
        ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert exc_info.value is error

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
    ]

    s3.put_object.assert_not_called()

def test_ingest_month_stops_repair_when_metadata_deletion_fails() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    metadata_key = f"{object_prefix}/metadata.json"

    body = io.BytesIO(b'{"year":')

    error = ClientError(
        error_response={"Error": {"Code": "AccessDenied"}},
        operation_name="DeleteObject",
    )

    s3 = Mock()
    s3.get_object.return_value = {"Body": body}
    s3.delete_object.side_effect = error

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ), pytest.raises(ClientError) as exc_info:
        ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert exc_info.value is error
    assert body.closed

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.delete_object(Bucket="raw", Key=metadata_key),
    ]

    s3.upload_file.assert_not_called()
    s3.put_object.assert_not_called()

def test_ingest_month_propagates_metadata_upload_error() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    object_prefix = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}"
    )
    object_key = f"{object_prefix}/yellow_tripdata_2024-01.parquet"
    metadata_key = f"{object_prefix}/metadata.json"

    error = ClientError(
        error_response={"Error": {"Code": "AccessDenied"}},
        operation_name="PutObject",
    )

    s3 = Mock()
    s3.get_object.side_effect = ClientError(
        error_response={"Error": {"Code": "NoSuchKey"}},
        operation_name="GetObject",
    )
    s3.put_object.side_effect = error

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ), pytest.raises(ClientError) as exc_info:
        ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert exc_info.value is error

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
        call.upload_file(
            Filename=ANY,
            Bucket="raw",
            Key=object_key,
        ),
        call.put_object(
            Bucket="raw",
            Key=metadata_key,
            Body=ANY,
            ContentType="application/json",
        ),
    ]

def test_ingest_month_propagates_metadata_access_denied() -> None:
    sha256 = "a" * 64
    size_bytes = 100
    downloaded_at = "2024-02-01T12:00:00+00:00"

    metadata_key = (
        f"tlc/yellow/year=2024/month=01/sha256={sha256}/metadata.json"
    )

    error = ClientError(
        error_response={"Error": {"Code": "AccessDenied"}},
        operation_name="GetObject",
    )

    s3 = Mock()
    s3.get_object.side_effect = error

    with patch(
        "urban_mobility.tlc_ingestion._download_file",
        return_value=(sha256, size_bytes, downloaded_at),
    ), pytest.raises(ClientError) as exc_info:
        ingest_month(
            s3=s3,
            bucket="raw",
            year=2024,
            month=1,
        )

    assert exc_info.value is error

    assert s3.mock_calls == [
        call.get_object(Bucket="raw", Key=metadata_key),
    ]

    s3.upload_file.assert_not_called()
    s3.put_object.assert_not_called()
    s3.delete_object.assert_not_called()