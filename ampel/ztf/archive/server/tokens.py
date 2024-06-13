from dataclasses import dataclass
from typing import Optional

import jwt
import sqlalchemy
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer
from fastapi.security.http import HTTPAuthorizationCredentials
from pydantic import BaseModel, ValidationError

from ampel.ztf.archive.ArchiveDB import ArchiveDB, select

from .db import get_archive
from .settings import settings

user_bearer = HTTPBearer(scheme_name="Ampel API token")
token_bearer = HTTPBearer(scheme_name="ZTF archive access token")


class User(BaseModel):
    name: str
    orgs: list[str]
    teams: list[str]

    @property
    def identities(self) -> list[str]:
        return [self.name] + self.orgs + self.teams


class TokenRequest(BaseModel):
    token: str


@dataclass
class AuthToken:
    id: int
    role: str
    partnership: bool


async def get_user(auth: HTTPAuthorizationCredentials = Depends(user_bearer)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            auth.credentials,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
        )
        try:
            token_data = User(**payload)
            if not settings.allowed_identities.intersection(token_data.identities):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
            return token_data
        except ValidationError:
            raise credentials_exception from None
    except jwt.PyJWTError:
        raise credentials_exception from None


def find_access_token(db: ArchiveDB, token: str) -> Optional[AuthToken]:
    Token = db._meta.tables["access_token"]
    try:
        with db._engine.connect() as conn:
            try:
                cursor = conn.execute(
                    select([Token.c.token_id, Token.c.role, Token.c.partnership])
                    .where(Token.c.token == token)
                    .limit(1)
                )
            except sqlalchemy.exc.DataError:
                # e.g. invalid input syntax for type uuid
                return None
            return AuthToken(*cursor.fetchone())
    except sqlalchemy.exc.TimeoutError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)
        ) from None


async def verify_access_token(
    auth: HTTPAuthorizationCredentials = Depends(token_bearer), db=Depends(get_archive)
) -> AuthToken:
    if (token := find_access_token(db, auth.credentials)) is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


async def verify_write_token(
    auth: HTTPAuthorizationCredentials = Depends(token_bearer), db=Depends(get_archive)
) -> bool:
    if not (token := find_access_token(db, auth.credentials)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    elif token.role != "writer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return True


router = APIRouter(tags=["tokens"])


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_token(user: User = Depends(get_user), db: ArchiveDB = Depends(get_archive)):
    Token = db._meta.tables["access_token"]
    with db._engine.connect() as conn:
        cursor = conn.execute(
            Token.insert(
                {
                    "owner": user.name,
                    "partnership": bool(
                        settings.partnership_identities.intersection(user.identities)
                    ),
                }
            ).returning(Token.c.token)
        )
        return cursor.fetchone()["token"]


@router.get("/")
def list_tokens(user: User = Depends(get_user), db: ArchiveDB = Depends(get_archive)):
    Token = db._meta.tables["access_token"]
    with db._engine.connect() as conn:
        cursor = conn.execute(Token.select().where(Token.c.owner == user.name))
        return cursor.fetchall()


@router.get("/{token_id}")
def get_token(
    token_id: int, user: User = Depends(get_user), db: ArchiveDB = Depends(get_archive)
):
    Token = db._meta.tables["access_token"]
    with db._engine.connect() as conn:
        cursor = conn.execute(
            Token.select().where(
                Token.c.token_id == token_id and Token.c.owner == user.name
            )
        )
        if result := cursor.fetchone():
            return result
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


@router.delete("/{token_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_token(
    token_id: int, user: User = Depends(get_user), db: ArchiveDB = Depends(get_archive)
):
    Token = db._meta.tables["access_token"]
    with db._engine.connect() as conn:
        cursor = conn.execute(
            Token.delete().where(
                Token.c.token_id == token_id and Token.c.owner == user.name
            )
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
