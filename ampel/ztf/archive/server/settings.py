import secrets
from typing import Optional

from pydantic import Field, HttpUrl, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    root_path: str = Field("", validation_alias="ROOT_PATH")
    archive_uri: PostgresDsn = Field(
        PostgresDsn("postgresql://localhost:5432/ztfarchive"),
        validation_alias="ARCHIVE_URI",
    )
    default_statement_timeout: int = Field(
        60,
        validation_alias="DEFAULT_STATEMENT_TIMEOUT",
        description="Timeout for synchronous queries, in seconds",
    )
    stream_query_timeout: int = Field(
        8 * 60 * 60,
        validation_alias="STREAM_QUERY_TIMEOUT",
        description="Timeout for asynchronous queries, in seconds",
    )
    s3_endpoint_url: Optional[HttpUrl] = Field(None, validation_alias="S3_ENDPOINT_URL")
    s3_bucket: str = Field("ampel-ztf-cutout-archive", validation_alias="S3_BUCKET")
    jwt_secret_key: str = Field(
        secrets.token_urlsafe(64), validation_alias="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field("HS256", validation_alias="JWT_ALGORITHM")
    allowed_identities: set[str] = Field(
        {"AmpelProject", "ZwickyTransientFacility"},
        validation_alias="ALLOWED_IDENTITIES",
        description="Usernames, teams, and orgs allowed to create persistent tokens",
    )
    partnership_identities: set[str] = Field(
        {"ZwickyTransientFacility"},
        validation_alias="PARTNERSHIP_IDENTITIES",
        description="Usernames, teams, and orgs allowed to create persistent tokens with access to ZTF partnership alerts",
    )
    query_debug: bool = Field(False, validation_alias="QUERY_DEBUG")
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # type: ignore[call-arg]

if settings.query_debug:
    from ampel.ztf.archive.ArchiveDB import ArchiveDB

    ArchiveDB.query_debug = True
