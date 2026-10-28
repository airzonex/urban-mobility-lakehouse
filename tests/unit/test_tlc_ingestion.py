import io
import json
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from urban_mobility.tlc_ingestion import (
    InvalidMetadataError,
    _read_metadata,
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
    assert body.closed