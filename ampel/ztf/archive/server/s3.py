import io
from functools import lru_cache
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import boto3
from botocore.exceptions import ClientError
from starlette.status import HTTP_404_NOT_FOUND

from .cutouts import ALERT_SCHEMAS, get_parsed_schema, read_schema
from .settings import settings

if TYPE_CHECKING:
    from typing import BinaryIO

    from mypy_boto3_s3.service_resource import Bucket


class NoSuchKey(KeyError): ...


@lru_cache(maxsize=1)
def get_s3_bucket() -> "Bucket":
    return boto3.resource(
        "s3",
        endpoint_url=str(settings.s3_endpoint_url)
        if settings.s3_endpoint_url
        else None,
    ).Bucket(settings.s3_bucket)


def get_object(bucket: "Bucket", key: str) -> bytes:
    buffer = io.BytesIO()
    try:
        bucket.download_fileobj(Key=key, Fileobj=buffer)
    except ClientError as err:
        if err.response["Error"]["Code"] == str(HTTP_404_NOT_FOUND):
            raise KeyError() from err
        raise
    return buffer.getvalue()


def get_stream(bucket: "Bucket", key: str) -> "BinaryIO":
    response = bucket.Object(key).get()
    if response["ResponseMetadata"]["HTTPStatusCode"] <= 400:  # noqa: PLR2004
        return response["Body"]  # type: ignore[return-value]
    raise KeyError


def get_range(
    bucket: "Bucket", key: str, start: int, end: int
) -> tuple["BinaryIO", dict]:
    obj = bucket.Object(key)
    try:
        response = obj.get(Range=f"={start}-{end}")
    except bucket.meta.client.exceptions.NoSuchKey as err:
        raise KeyError(f"bucket {bucket.name} has no key {key}") from err
    if response["ResponseMetadata"]["HTTPStatusCode"] <= 400:  # noqa: PLR2004
        schema_key = (
            obj.metadata["schema-name"],
            obj.metadata["schema-version"],
        )
        if schema_key not in ALERT_SCHEMAS:
            schema = get_parsed_schema(read_schema(get_stream(bucket, key)))
        else:
            schema = ALERT_SCHEMAS[schema_key]
        return response["Body"], schema  # type: ignore[return-value]
    raise KeyError


def get_url_for_key(bucket: "Bucket", key: str) -> str:
    return f"{settings.s3_endpoint_url or ''}/{bucket.name}/{key}"


def get_key_for_url(bucket: "Bucket", uri: str) -> str:
    path = urlsplit(uri).path.split("/")
    assert path[-2] == bucket.name
    return path[-1]
