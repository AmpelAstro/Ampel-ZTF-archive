from functools import lru_cache

from sqlalchemy.exc import OperationalError
from psycopg2.errors import QueryCanceled  # type: ignore[import]
from fastapi.responses import JSONResponse
from fastapi import status, Request
from fastapi.encoders import jsonable_encoder

from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

from ..ArchiveDB import ArchiveDB

from .settings import settings


@lru_cache(maxsize=1)
def get_archive():
    return ArchiveDB(
        settings.archive_uri,
        default_statement_timeout=settings.default_statement_timeout * 1000,
    )


@lru_cache(maxsize=1)
def get_archive_updater() -> ArchiveUpdater:
    return ArchiveUpdater(settings.archive_uri)


async def handle_operationalerror(request: Request, exc: OperationalError):
    if isinstance(exc.orig, QueryCanceled):
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content=jsonable_encoder(
                {
                    "detail": {
                        "msg": f"Query canceled after {settings.default_statement_timeout} s"
                    }
                }
            ),
            headers={"retry-after": str(2 * settings.default_statement_timeout)},
        )
    raise exc
