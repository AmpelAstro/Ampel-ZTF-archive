import itertools
import tarfile
from os import environ
from os.path import dirname, join

import pytest
import fastavro

@pytest.fixture(scope="session")
def archive():
    if "ARCHIVE_HOSTNAME" in environ and "ARCHIVE_PORT" in environ:
        yield "postgresql://ampel@{}:{}/ztfarchive".format(
            environ["ARCHIVE_HOSTNAME"], environ["ARCHIVE_PORT"]
        )
    else:
        pytest.skip("Requires a Postgres database")


@pytest.fixture
def empty_archive(archive):
    """
    Yield archive database, dropping all rows when finished
    """
    from sqlalchemy import create_engine, MetaData

    engine = create_engine(archive)
    meta = MetaData()
    meta.reflect(bind=engine)
    try:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != "versions":
                    connection.execute(table.delete())
        yield archive
    finally:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != "versions":
                    connection.execute(table.delete())


@pytest.fixture(scope="session")
def alert_tarball():
    return join(dirname(__file__), "test-data", "ztf_public_20180819_mod1000.tar.gz")


def walk_tarball(fname, extension='.avro'):
    with tarfile.open(fname) as archive:
        for info in archive:
            if info.isfile():
                fo = archive.extractfile(info)
                if info.name.endswith(extension):
                    yield fo
                elif info.name.endswith('.tar.gz'):
                    yield from walk_tarball(fname, extension)


@pytest.fixture(scope="session")
def alert_generator(alert_tarball):

    def alerts(with_schema=False):
        for fileobj in itertools.islice(walk_tarball(alert_tarball), 0, 1000, 1):
            reader = fastavro.reader(fileobj)
            alert = next(reader)
            if with_schema:
                yield alert, reader.writer_schema
            else:
                yield alert

    return alerts
