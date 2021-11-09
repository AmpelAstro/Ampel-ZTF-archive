from functools import lru_cache

from ..ArchiveDB import ArchiveDB, GroupNotFoundError

from .settings import settings

@lru_cache(maxsize=1)
def get_archive():
    return ArchiveDB(settings.archive_uri)