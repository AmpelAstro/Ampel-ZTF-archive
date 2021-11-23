import base64
from contextlib import contextmanager
import io
import secrets
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from ampel.ztf.archive.server.cutouts import extract_alert, pack_records, ALERT_SCHEMAS
from ampel.ztf.archive.server.db import get_archive, get_archive_updater
from ampel.ztf.archive.server.s3 import get_object, get_range, get_s3_bucket
from fastapi.security import http
from urllib.parse import urlsplit
import fastavro

import jwt
import httpx
import pytest
from fastapi import status
from ampel.ztf.archive.ArchiveDB import ArchiveDB
from starlette.status import HTTP_200_OK, HTTP_201_CREATED, HTTP_404_NOT_FOUND
from tests.fixtures import walk_tarball

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock import MockerFixture

DEFAULT = object()


class BearerAuth(httpx.Auth):
    def __init__(self, token):
        self.token = token

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


@pytest.fixture
def mocked_app(monkeypatch: "MonkeyPatch", mocker: "MockerFixture", mock_s3_bucket):
    monkeypatch.setenv("ALLOWED_IDENTITIES", '["someorg","someorg/a-team"]')
    from ampel.ztf.archive.server import app
    from ampel.ztf.archive.server.settings import settings
    from ampel.ztf.archive.server import db, tokens

    mocker.patch.object(db, "ArchiveDB")
    mocker.patch.object(db, "ArchiveUpdater")
    mocker.patch.object(
        tokens, "find_access_token", side_effect=lambda *args: {"role": "writer"}
    )
    get_archive.cache_clear()
    get_archive_updater.cache_clear()
    yield app
    get_archive.cache_clear()
    get_archive_updater.cache_clear()


@pytest.fixture
def mock_db(mocked_app, alert_generator):
    db = mocked_app.get_archive()
    alert = next(alert_generator())
    # remove cutouts (not valid JSON strings)
    for k in list(alert.keys()):
        if k.startswith("cutout"):
            del alert[k]
    # add fake drbversion to pre-drb alert
    alert["candidate"]["drbversion"] = "0.0"
    db.get_alert.return_value = alert
    db.get_alerts_for_object.return_value = [alert]
    yield db
    mocked_app.get_archive.cache_clear()


@pytest.fixture
async def mock_client(mocked_app):
    async with httpx.AsyncClient(
        app=mocked_app.app,
        base_url="http://test",
        auth=BearerAuth("blah"),
    ) as client:
        yield client


@pytest.fixture
def integration_app(monkeypatch: "MonkeyPatch", alert_archive, localstack_s3_bucket):
    monkeypatch.setattr(
        "ampel.ztf.archive.server.settings.settings.archive_uri", alert_archive
    )
    monkeypatch.setattr(
        "ampel.ztf.archive.server.settings.settings.allowed_identities",
        {"someorg", "someorg/a-team"},
    )

    from ampel.ztf.archive.server import app

    assert app.settings.archive_uri == alert_archive
    assert app.settings.allowed_identities == {"someorg", "someorg/a-team"}
    app.get_archive.cache_clear()
    yield app
    app.get_archive.cache_clear()


@pytest.fixture
async def integration_client(integration_app):
    async with httpx.AsyncClient(
        app=integration_app.app,
        base_url="http://test",
    ) as client:
        yield client


@contextmanager
def set_token_role(token: str, role: str):
    db = get_archive()
    Token = db._meta.tables["access_token"]
    with db._engine.connect() as conn:
        prev_role = conn.execute(
            Token.select().where(Token.c.token == token)
        ).fetchone()["role"]
        try:
            conn.execute(
                Token.update(values={"role": role}).where(Token.c.token == token)
            )
            yield
        finally:
            conn.execute(
                Token.update(values={"role": prev_role}).where(Token.c.token == token)
            )


@pytest.fixture
def write_token(integration_app, access_token):
    with set_token_role(access_token, "writer"):
        yield access_token


@pytest.fixture
async def authed_integration_client(integration_app, write_token):
    async with httpx.AsyncClient(
        app=integration_app.app,
        base_url="http://test",
        auth=BearerAuth(write_token),
    ) as client:
        yield client


def test_parse_json_from_env(monkeypatch):
    """
    JSON gets parsed from env variables
    """
    from ampel.ztf.archive.server.settings import Settings

    assert Settings().allowed_identities != {"someorg", "someorg/a-team"}
    monkeypatch.setenv("ALLOWED_IDENTITIES", '["someorg","someorg/a-team"]')
    assert Settings().allowed_identities == {"someorg", "someorg/a-team"}


@pytest.mark.asyncio
async def test_get_alert(mock_client: httpx.AsyncClient, mock_db: MagicMock):
    response = await mock_client.get("/alert/123")
    response.raise_for_status()
    assert mock_db.get_alert.called_once
    assert mock_db.get_alert.call_args.args[0] == 123


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_client", "authed_integration_client"])
def client(request):
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize(
    "auth,status",
    [
        (DEFAULT, 200),
        (None, status.HTTP_403_FORBIDDEN),
        (BearerAuth, status.HTTP_401_UNAUTHORIZED),
    ],
)
@pytest.mark.asyncio
async def test_basic_auth(
    mock_client: httpx.AsyncClient,
    mock_db: MagicMock,
    auth,
    status,
    mocker,
):
    mocker.patch("ampel.ztf.archive.server.tokens.find_access_token").return_value = (
        auth is not BearerAuth
    )
    kwargs = {}
    if auth is DEFAULT or auth is BearerAuth:
        kwargs["auth"] = BearerAuth("tokeytoken")
    elif auth is None:
        kwargs["auth"] = None
    response = await mock_client.get("/object/thingamajig/alerts", **kwargs)
    assert response.status_code == status


@pytest.mark.asyncio
async def test_get_healpix(mock_client: httpx.AsyncClient, mock_db: MagicMock):
    ipix = [1, 2]
    params = {"jd_start": 0, "jd_end": 1}
    response = await mock_client.get(
        "/alerts/healpix", params={"ipix": [1, 2], **params}
    )
    response.raise_for_status()
    assert mock_db.get_alerts_in_healpix.call_count == 1
    assert (
        mock_db.get_alerts_in_healpix.call_args.kwargs
        | {"pixels": {64: [1, 2]}, **params}
        == mock_db.get_alerts_in_healpix.call_args.kwargs
    ), "kwargs contain supplied params"


@pytest.mark.asyncio
async def test_get_healpix_skymap(mock_client: httpx.AsyncClient, mock_db: MagicMock):
    query = {
        "nside": 4,
        "pixels": [0, 56, 79, 81]
        + list(range(10 * 16, 11 * 16))
        + list(range(4 * 4, 5 * 4)),
        "jd": {"gt": 0, "lt": 1},
    }
    response = await mock_client.post("/alerts/healpix/skymap", json=query)
    response.raise_for_status()
    assert mock_db.get_alerts_in_healpix.call_count == 1
    assert mock_db.get_alerts_in_healpix.call_args.kwargs["pixels"] == {
        1: [10],
        2: [4],
        4: [0, 56, 79, 81],
    }, "map is decomposed into superpixels"


@pytest.mark.parametrize(
    "auth,status",
    [(DEFAULT, status.HTTP_200_OK), (BearerAuth, status.HTTP_401_UNAUTHORIZED)],
)
@pytest.mark.asyncio
async def test_get_photopoints(
    authed_integration_client: httpx.AsyncClient,
    auth,
    status,
):
    kwargs = {} if auth is DEFAULT else {"auth": BearerAuth("badtoken")}
    response = await authed_integration_client.get(
        "/object/ZTF18abaqwse/photopoints", **kwargs
    )
    assert response.status_code == status
    if auth is DEFAULT:
        assert len(response.json()["prv_candidates"]) == 48


@pytest.mark.asyncio
async def test_create_stream(
    authed_integration_client: httpx.AsyncClient, integration_app
):
    response = await authed_integration_client.post("/streams/from_query", json={})
    assert response.status_code == 201
    body = response.json()
    assert body["chunks"] > 0
    archive: ArchiveDB = integration_app.get_archive()
    assert body["chunks"] == archive.get_remaining_chunks(body["resume_token"])


@pytest.mark.asyncio
async def test_read_stream(
    integration_client: httpx.AsyncClient,
    authed_integration_client: httpx.AsyncClient,
):
    response = await authed_integration_client.post("/streams/from_query", json={})
    assert response.status_code == 201
    body = response.json()

    response = await integration_client.get(f"/stream/{body['resume_token']}/chunk")
    response.raise_for_status()
    chunk = response.json()
    assert len(chunk["alerts"]) == 10
    assert chunk["chunks_remaining"] == 0

    response = await integration_client.get(f"/stream/{body['resume_token']}/chunk")
    response.raise_for_status()
    chunk = response.json()
    assert len(chunk["alerts"]) == 0
    assert chunk["chunks_remaining"] == 0

    # read a nonexistant chunk
    response = await integration_client.get(
        f"/stream/{secrets.token_urlsafe(32)}/chunk"
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_read_topic(
    integration_client: httpx.AsyncClient,
    authed_integration_client: httpx.AsyncClient,
):

    candids = [595147624915010001, 595193335915010017, 595211874215015018]
    description = "the bird is the word"
    response = await authed_integration_client.post(
        "/topics", json={"description": description, "candids": candids}
    )
    assert response.status_code == 201
    topic = response.json()
    assert isinstance(topic, str)

    response = await integration_client.get("/topic/" + topic)
    response.raise_for_status()
    assert response.json() == {
        "topic": topic,
        "description": description,
        "size": len(candids),
    }

    response = await integration_client.post(
        "/streams/from_topic", json={"topic": topic}
    )
    assert response.status_code == 201
    stream = response.json()

    response = await integration_client.get(f"/stream/{stream['resume_token']}/chunk")
    response.raise_for_status()
    assert [alert["candid"] for alert in response.json()["alerts"]] == candids
    assert response.json()["chunks_remaining"] == 0


@pytest.mark.asyncio
async def test_create_topic_with_bad_ids(
    authed_integration_client: httpx.AsyncClient, integration_app
):
    candids = [1, 2, 3]
    description = "these are not the candids you're looking for"
    response = await authed_integration_client.post(
        "/topics", json={"description": description, "candids": candids}
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    detail = response.json()
    assert set(detail["detail"].keys()) == {"msg", "topic"}


@pytest.mark.asyncio
async def test_read_invalid_topic(
    integration_client: httpx.AsyncClient, integration_app
):
    response = await integration_client.post(
        "/streams/from_topic", json={"topic": secrets.token_urlsafe()}
    )
    assert response.status_code == 404


@pytest.fixture
def test_user():
    from ampel.ztf.archive.server.tokens import User

    return User(name="flerpyherp", orgs=["someorg"], teams=["someorg/a-team"])


@pytest.fixture
def user_token(test_user):
    from ampel.ztf.archive.server.settings import settings

    return jwt.encode(
        test_user.dict(), settings.jwt_secret_key, algorithm=settings.jwt_algorithm
    )


@pytest.mark.asyncio
@pytest.fixture
async def access_token(
    integration_client: httpx.AsyncClient,
    integration_app,
    user_token: str,
    test_user,
):
    response = await integration_client.post("/tokens", auth=BearerAuth(user_token))
    assert response.status_code == 201
    return response.json()


@pytest.mark.asyncio
async def test_create_token(
    integration_app,
    access_token: str,
):
    db: ArchiveDB = integration_app.get_archive()
    with db._engine.connect() as conn:
        Token = db._meta.tables["access_token"]
        cursor = conn.execute(Token.select().where(Token.c.token == access_token))
        assert len(cursor.fetchall()) == 1


@pytest.mark.asyncio
async def test_delete_token(
    integration_client: httpx.AsyncClient,
    integration_app,
    user_token: str,
    access_token: str,
):
    tokens = (
        await integration_client.get("/tokens", auth=BearerAuth(user_token))
    ).json()
    token_id = next(t["token_id"] for t in tokens if t["token"] == access_token)
    response = await integration_client.delete(
        f"/tokens/{token_id}", auth=BearerAuth(user_token)
    )
    assert response.status_code == 204
    token = response.json()
    db: ArchiveDB = integration_app.get_archive()
    with db._engine.connect() as conn:
        Token = db._meta.tables["access_token"]
        cursor = conn.execute(Token.select().where(Token.c.token == token))
        assert len(cursor.fetchall()) == 0


@pytest.mark.asyncio
async def test_list_tokens(
    integration_client: httpx.AsyncClient,
    user_token: str,
    access_token: str,
):
    response = await integration_client.get("/tokens", auth=BearerAuth(user_token))
    assert response.status_code == 200
    tokens = response.json()
    assert any(token["token"] == access_token for token in tokens)


@pytest.mark.asyncio
async def test_forbidden_identity(
    integration_client: httpx.AsyncClient,
    user_token: str,
    access_token: str,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        "ampel.ztf.archive.server.tokens.settings.allowed_identities", {"none", "such"}
    )
    response = await integration_client.get("/tokens", auth=BearerAuth(user_token))
    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.fixture
def packed_alert_chunk(
    alert_generator,
) -> tuple[dict, bytes, list[dict], list[tuple[int, int]]]:
    records, schemas = zip(*alert_generator(with_schema=True))

    blob, ranges = pack_records(records, schema=schemas[0])

    return schemas[0], blob, records, ranges


def test_extract_block(
    packed_alert_chunk: tuple[dict, bytes, list[dict], list[tuple[int, int]]]
):
    """
    We can read an individual block straight out of an AVRO file
    """
    schema, blob, records, ranges = packed_alert_chunk

    for record, span in zip(records, ranges):
        reco = extract_alert(record["candid"], io.BytesIO(blob[slice(*span)]), schema)
        assert reco == record


def test_extract_block_from_s3(
    packed_alert_chunk: tuple[dict, bytes, list[dict], list[tuple[int, int]]],
    mock_s3_bucket,
):
    schema, blob, records, ranges = packed_alert_chunk

    bucket = get_s3_bucket()
    obj = bucket.Object("blobsy.avro")
    response = obj.put(
        Body=blob,
        Metadata={"schema-name": schema["name"], "schema-version": schema["version"]},
    )

    obj.load()
    assert obj.metadata == {
        "schema-name": schema["name"],
        "schema-version": schema["version"],
    }

    for record, span in zip(records, ranges):
        start, end = span
        reco = extract_alert(record["candid"], *get_range(bucket, obj.key, start, end))
        assert reco == record


@pytest.fixture
async def post_alert_chunk(
    authed_integration_client: httpx.AsyncClient, alert_generator
):
    records, schemas = zip(*alert_generator(with_schema=True))

    payload, _ = pack_records(records, schema=schemas[0])

    response = await authed_integration_client.post(f"/alerts", content=payload)
    response.raise_for_status()
    assert response.status_code == HTTP_200_OK

    return records


def test_post_alert_chunk(mock_client: httpx.AsyncClient, mocked_app, post_alert_chunk):

    bucket = get_s3_bucket()
    objects = list(bucket.objects.all())
    assert len(objects) == 1
    obj = bucket.Object(objects[0].key)

    call_args = mocked_app.get_archive_updater().insert_alert_chunk.call_args

    assert urlsplit(call_args.kwargs["archive_uri"]).path.split("/")[-1] == obj.key


@pytest.mark.asyncio
async def test_get_cutouts_from_chunk(
    authed_integration_client: httpx.AsyncClient, post_alert_chunk
):
    response = await authed_integration_client.get("/alert/0/cutouts")
    assert response.status_code == HTTP_404_NOT_FOUND, "nonexistant alert not found"

    response = await authed_integration_client.get(
        f"/alert/{post_alert_chunk[0]['candid']}/cutouts"
    )
    assert response.status_code == HTTP_200_OK, "found alert cutouts"

    bucket = get_s3_bucket()
    objects = list(bucket.objects.all())
    assert len(objects) == 1
    obj = bucket.Object(objects[0].key)
    obj.delete()

    response = await authed_integration_client.get(
        f"/alert/{post_alert_chunk[0]['candid']}/cutouts"
    )
    assert response.status_code == HTTP_404_NOT_FOUND, "archive blob missing"


@pytest.mark.asyncio
async def test_repost_alert_chunk(
    authed_integration_client: httpx.AsyncClient, alert_generator
):
    records, schemas = zip(*alert_generator(with_schema=True))

    payload, _ = pack_records(records, schema=schemas[0])

    for _ in range(2):
        response = await authed_integration_client.post(f"/alerts", content=payload)
        response.raise_for_status()
        assert response.status_code == HTTP_200_OK

    bucket = get_s3_bucket()
    objects = list(bucket.objects.all())
    assert len(objects) == 1
    obj = bucket.Object(objects[0].key)

    db = get_archive()

    ALERT_SCHEMAS.clear()

    for record in records:
        response = await authed_integration_client.get(f"/alert/{record['candid']}")
        response.raise_for_status()
        assert response.json()["candid"] == record["candid"]

        uri, start, end = db.get_archive_segment(record["candid"])
        assert urlsplit(uri).path.split("/")[-1] == obj.key
        assert end - start > 0

        response = await authed_integration_client.get(
            f"/alert/{record['candid']}/cutouts"
        )
        response.raise_for_status()
        cutouts = response.json()
        assert set(cutouts.keys()) == {"template", "science", "difference"}
        for kind, cutout in cutouts.items():
            base64.b64encode(
                record[f"cutout{kind.capitalize()}"]["stampData"]
            ) == cutout


@pytest.mark.asyncio
async def test_post_alert_unauthorized(integration_client: httpx.AsyncClient):
    response = await integration_client.post("/alerts", content=b"")
    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_post_alert_malformed(mock_client: httpx.AsyncClient):
    response = await mock_client.post("/alerts", content=b"")
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert response.headers["x-exception"] == "cannot read header - is it an avro file?"
