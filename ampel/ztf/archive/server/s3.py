from functools import lru_cache
import io
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError
from starlette.status import HTTP_404_NOT_FOUND

from .settings import settings

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket


@lru_cache(maxsize=1)
def get_s3_bucket() -> "Bucket":
    return boto3.resource("s3", endpoint_url=settings.s3_endpoint_url).Bucket(
        settings.s3_bucket
    )


def get_object(bucket: "Bucket", key: str) -> bytes:
    buffer = io.BytesIO()
    try:
        bucket.download_fileobj(Key=key, Fileobj=buffer)
    except ClientError as err:
        if err.response["Error"]["Code"] == str(HTTP_404_NOT_FOUND):
            raise KeyError() from err
        else:
            raise
    return buffer.getvalue()
