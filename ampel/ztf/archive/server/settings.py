from typing import Optional, TYPE_CHECKING

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
    archive_uri: Optional[PostgresUrl] = Field("postgresql://localhost:5432/ztfarchive", env="ARCHIVE_URI")
    auth_user: str = Field(..., env="AUTH_USER")
    auth_password: str = Field(..., env="AUTH_PASSWORD")

    class Config:
        env_file = ".env"
