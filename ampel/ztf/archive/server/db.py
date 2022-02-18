from functools import lru_cache

from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

from ..ArchiveDB import ArchiveDB, GroupNotFoundError

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
