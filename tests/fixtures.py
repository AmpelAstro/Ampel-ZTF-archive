import itertools
import json
import os
import secrets
import subprocess
import tarfile
import time
from os.path import dirname, join
from pathlib import Path

import fastavro
import httpx
import pytest
from moto import mock_s3

from ampel.ztf.archive.server.s3 import get_s3_bucket

POSTGRES_IMAGE = "ampelproject/postgres:14.1"
LOCALSTACK_IMAGE = "localstack/localstack:3.4.0"


@pytest.fixture(scope="session")
def integration(pytestconfig):
    return pytestconfig.getoption("--integration")


@pytest.fixture(scope="session")
def archive(integration):
    if "POSTGRES_URI" in os.environ:
        yield os.environ["POSTGRES_URI"]
        return
    elif not integration:
        raise pytest.skip(
            "integration tests require --integration flag or POSTGRES_URI env var"
        )
    password = secrets.token_hex()
    container = None
    try:
        container = (
            subprocess.check_output(
                [
                    "docker",
                    "run",
                    "-d",
                    "-e",
                    "POSTGRES_USER=ampel",
                    "-e",
                    f"POSTGRES_PASSWORD={password}",
                    "-e",
                    "POSTGRES_DB=ztfarchive",
                    "-e",
                    "ARCHIVE_READ_USER=archive-readonly",
                    "-e",
                    "ARCHIVE_WRITE_USER=ampel-client",
                    "-P",
                    "-v",
                    f"{str(Path(__file__).parent/'test-data'/'initdb'/'archive')}:/docker-entrypoint-initdb.d",
                    POSTGRES_IMAGE,
                ],
            )
            .decode()
            .strip()
        )
        # wait for startup
        try:
            subprocess.check_call(
                [
                    "docker",
                    "run",
                    "--link",
                    f"{container}:postgres",
                    POSTGRES_IMAGE,
                    "sh",
                    "-c",
                    "for _ in $(seq 1 60); do if pg_isready -U ampel -p "
                    + password
                    + " -h ${POSTGRES_PORT_5432_TCP_ADDR} -p ${POSTGRES_PORT_5432_TCP_PORT}; then break; fi; sleep 1; done",
                ],
                timeout=30,
            )
        finally:
            subprocess.call(["docker", "logs", container])
        info = subprocess.check_output(["docker", "inspect", container]).decode()
        port = json.loads(info)[0]["NetworkSettings"]["Ports"]["5432/tcp"][0][
            "HostPort"
        ]
        yield f"postgresql://ampel:{password}@localhost:{port}/ztfarchive"
    finally:
        if container is not None:
            subprocess.call(["docker", "stop", container], stdout=subprocess.DEVNULL)
            subprocess.check_call(
                ["docker", "rm", container], stdout=subprocess.DEVNULL
            )


@pytest.fixture(scope="session")
def localstack_s3(integration):
    if os.environ.get("LOCALSTACK_PORT"):
        yield f"http://localhost:{os.environ['LOCALSTACK_PORT']}"
        return
    elif not integration:
        raise pytest.skip(
            "integration tests require --integration flag or LOCALSTACK_PORT env var"
        )
    container = None
    try:
        container = (
            subprocess.check_output(
                [
                    "docker",
                    "run",
                    "-d",
                    "-e",
                    "SERVICES=s3",
                    "-P",
                    LOCALSTACK_IMAGE,
                ],
            )
            .decode()
            .strip()
        )
        info = subprocess.check_output(["docker", "inspect", container]).decode()
        port = json.loads(info)[0]["NetworkSettings"]["Ports"]["4566/tcp"][0][
            "HostPort"
        ]

        def raise_on_4xx_5xx(response):
            response.raise_for_status()

        for _ in range(30):
            try:
                with httpx.Client(
                    event_hooks={"response": [raise_on_4xx_5xx]}
                ) as client:
                    if (
                        status := client.get(
                            f"http://localhost:{port}/_localstack/health"
                        ).json()["services"]["s3"]
                    ) == "available":
                        break
                    print(f"s3 status: {status}")
            except httpx.HTTPError as exc:
                print(
                    f"Failed to fetch http://localhost:{port}/_localstack/health: {exc}"
                )
            time.sleep(1)
        else:
            subprocess.call(["docker", "logs", container])
            raise RuntimeError("Could not contact localstack s3")

        yield f"http://localhost:{port}"
    finally:
        if container is not None:
            subprocess.call(["docker", "stop", container], stdout=subprocess.DEVNULL)
            subprocess.check_call(
                ["docker", "rm", container], stdout=subprocess.DEVNULL
            )


@pytest.fixture()
def empty_archive(archive):
    """
    Yield archive database, dropping all rows when finished
    """
    from sqlalchemy import MetaData, create_engine

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


@pytest.fixture()
def alert_archive(empty_archive, alert_generator):
    from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

    updater = ArchiveUpdater(empty_archive)
    from itertools import islice

    for alert, schema in islice(alert_generator(with_schema=True), 10):
        assert schema["version"] == "3.0", "Need alerts with current schema"
        updater.insert_alert(alert, schema, 0, 0)
    return empty_archive


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


@pytest.fixture(scope="module")
def _aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "KEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "SEEKRIT"


@pytest.fixture()
def mock_s3_bucket(_aws_credentials):
    get_s3_bucket.cache_clear()
    with mock_s3():
        bucket = get_s3_bucket()
        bucket.create()
        yield bucket
    get_s3_bucket.cache_clear()


@pytest.fixture()
def localstack_s3_bucket(_aws_credentials, localstack_s3, monkeypatch):
    from ampel.ztf.archive.server.settings import settings

    monkeypatch.setattr(settings, "s3_endpoint_url", localstack_s3)
    get_s3_bucket.cache_clear()
    bucket = get_s3_bucket()
    bucket.create()
    yield bucket
    bucket.objects.all().delete()
    bucket.delete()
    get_s3_bucket.cache_clear()


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_s3_bucket", "localstack_s3_bucket"])
def s3_bucket(request):
    return request.getfixturevalue(request.param)
