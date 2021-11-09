from functools import lru_cache
import io
from typing import TYPE_CHECKING

import boto3

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
    bucket.download_fileobj(Key=key, Fileobj=buffer)
    return buffer.getvalue()
