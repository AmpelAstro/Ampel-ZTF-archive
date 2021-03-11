import itertools
import json
import subprocess
import tarfile
from pathlib import Path
from os import environ
from os.path import dirname, join

import pytest
import fastavro

@pytest.fixture(scope="session")
def archive():
    container = None
    try:
        container = (
            subprocess.check_output(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-d",
                    "-e",
                    "POSTGRES_USER=ampel",
                    "-e",
                    "POSTGRES_DB=ztfarchive",
                    "-e",
                    "ARCHIVE_READ_USER=archive-readonly",
                    "-e",
                    "ARCHIVE_WRITE_USER=ampel-client",
                    "-P",
                    "-v",
                    f"{str(Path(__file__).parent/'test-data'/'initdb'/'archive')}:/docker-entrypoint-initdb.d",
                    "postgres:10.6",
                ],
            )
            .decode()
            .strip()
        )
        # wait for startup
        subprocess.check_call(
            [
                "docker",
                "run",
                "--link",
                f"{container}:postgres",
                "postgres:10.6",
                "sh",
                "-c",
                "for _ in $(seq 1 60); do if pg_isready -U ampel -h ${POSTGRES_PORT_5432_TCP_ADDR} -p ${POSTGRES_PORT_5432_TCP_PORT}; then break; fi; sleep 1; done",
            ]
        )
        info = subprocess.check_output(["docker", "inspect", container]).decode()
        port = json.loads(info)[0]["NetworkSettings"]["Ports"]["5432/tcp"][0][
            "HostPort"
        ]
        yield f"postgresql://ampel@localhost:{port}/ztfarchive"
    finally:
        if container is not None:
            subprocess.check_call(
                ["docker", "stop", container], stdout=subprocess.DEVNULL
            )


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


def walk_tarball(fname, extension=".avro"):
    with tarfile.open(fname) as archive:
        for info in archive:
            if info.isfile():
                fo = archive.extractfile(info)
                if info.name.endswith(extension):
                    yield fo
                elif info.name.endswith(".tar.gz"):
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
