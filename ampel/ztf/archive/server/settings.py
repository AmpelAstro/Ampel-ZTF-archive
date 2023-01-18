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
else:
    PostgresUrl = stricturl(allowed_schemes={"postgresql"}, tld_required=False)


class Settings(BaseSettings):
    root_path: str = Field("", env="ROOT_PATH")
    archive_uri: Optional[PostgresUrl] = Field(
        "postgresql://localhost:5432/ztfarchive", env="ARCHIVE_URI"
    )
    jwt_secret_key: str = Field(secrets.token_urlsafe(64), env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    allowed_identities: Set[str] = Field(
        {"AmpelProject"},
        env="ALLOWED_IDENTITIES",
        description="Usernames, teams, and orgs allowed to create persistent tokens",
    )
    query_debug: bool = Field(False, env="QUERY_DEBUG")

    class Config:
        env_file = ".env"


settings = Settings()

if settings.query_debug:
    from ampel.ztf.archive.ArchiveDB import ArchiveDB
    ArchiveDB.query_debug = True
