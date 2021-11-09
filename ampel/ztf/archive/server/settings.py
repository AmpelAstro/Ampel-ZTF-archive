import secrets

from typing import Optional, Set, TYPE_CHECKING

from pydantic import (
    AnyHttpUrl,
    AnyUrl,
    BaseSettings,
    DirectoryPath,
    Field,
    stricturl,
)

# see: https://github.com/samuelcolvin/pydantic/issues/975#issuecomment-551147305
if TYPE_CHECKING:
    PostgresUrl = AnyUrl
    HttpsUrl = AnyHttpUrl
else:
    PostgresUrl = stricturl(allowed_schemes={"postgresql"}, tld_required=False)
    HttpsUrl = stricturl(allowed_schemes={"https"})


class Settings(BaseSettings):
    root_path: str = Field("", env="ROOT_PATH")
    archive_uri: Optional[PostgresUrl] = Field(
        "postgresql://localhost:5432/ztfarchive", env="ARCHIVE_URI"
    )
    s3_endpoint_url: Optional[HttpsUrl] = Field(None, env="S3_ENDPOINT_URL")
    s3_bucket: str = Field("ampel-ztf-cutout-archive", env="S3_BUCKET")
    jwt_secret_key: str = Field(secrets.token_urlsafe(64), env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    allowed_identities: Set[str] = Field(
        {"AmpelProject"},
        env="ALLOWED_IDENTITIES",
        description="Usernames, teams, and orgs allowed to create persistent tokens",
    )

    class Config:
        env_file = ".env"


settings = Settings()
