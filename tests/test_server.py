import secrets
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import status
from ampel.ztf.archive.ArchiveDB import ArchiveDB
from ampel.ztf.archive.server.settings import Settings

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch
    from pytest_mock import MockerFixture

DEFAULT = object()


@pytest.fixture
def mocked_app(monkeypatch: "MonkeyPatch", mocker: "MockerFixture"):
    monkeypatch.setenv("AUTH_USER", "yogi")
    monkeypatch.setenv("AUTH_PASSWORD", "bear")
    from ampel.ztf.archive.server import app

    mocker.patch.object(app, "ArchiveDB")
    yield app


@pytest.fixture
def mock_db(mocked_app, alert_generator):
    db =  mocked_app.ArchiveDB()
    alert = next(alert_generator())
    # remove cutouts (not valid JSON strings)
    for k in list(alert.keys()):
        if k.startswith("cutout"):
            del alert[k]
    # add fake drbversion to pre-drb alert
    alert["candidate"]["drbversion"] = "0.0"
    db.get_alert.return_value = alert
    db.get_alerts_for_object.return_value = [alert]
    return db


@pytest.fixture
async def mock_client(mocked_app):
    async with httpx.AsyncClient(
        app=mocked_app.app, base_url="http://test", auth=httpx.BasicAuth("yogi", "bear")
    ) as client:
        yield client


@pytest.fixture
def integration_app(monkeypatch: "MonkeyPatch", alert_archive):
    monkeypatch.setenv("AUTH_USER", "yogi")
    monkeypatch.setenv("AUTH_PASSWORD", "bear")
    monkeypatch.setenv("ARCHIVE_URI", alert_archive)
    monkeypatch.setattr("ampel.ztf.archive.server.app.settings", Settings())
    from ampel.ztf.archive.server import app

    assert app.settings.archive_uri == alert_archive
    app.get_archive.cache_clear()
    yield app
    app.get_archive.cache_clear()


@pytest.fixture
async def integration_client(integration_app):
    async with httpx.AsyncClient(
        app=integration_app.app,
        base_url="http://test",
        auth=httpx.BasicAuth("yogi", "bear"),
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_get_alert(mock_client: httpx.AsyncClient, mock_db: MagicMock):
    response = await mock_client.get("/alert/123")
    response.raise_for_status()
    assert mock_db.get_alert.called_once
    assert mock_db.get_alert.call_args.args[0] == 123


@pytest.mark.parametrize(
    "auth,status",
    [(DEFAULT, 200), (None, 401), (httpx.BasicAuth("pickanick", "basket"), 401)],
)
@pytest.mark.asyncio
async def test_basic_auth(
    mock_client: httpx.AsyncClient, mock_db: MagicMock, auth, status
):
    kwargs = {} if auth is DEFAULT else {"auth": auth}
    response = await mock_client.get("/object/thingamajig/alerts", **kwargs)
    assert response.status_code == status


@pytest.mark.asyncio
async def test_create_stream(integration_client: httpx.AsyncClient, integration_app):
    response = await integration_client.post("/streams/from_query", json={})
    assert response.status_code == 201
    body = response.json()
    assert body["chunks"] > 0
    archive: ArchiveDB = integration_app.get_archive()
    assert body["chunks"] == archive.get_remaining_chunks(body["resume_token"])


@pytest.mark.asyncio
async def test_read_stream(integration_client: httpx.AsyncClient, integration_app):
    response = await integration_client.post("/streams/from_query", json={})
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
async def test_read_topic(integration_client: httpx.AsyncClient, integration_app):

    candids = [595147624915010001, 595193335915010017, 595211874215015018]
    description = "the bird is the word"
    response = await integration_client.post(
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
    integration_client: httpx.AsyncClient, integration_app
):
    candids = [1, 2, 3]
    description = "these are not the candids you're looking for"
    response = await integration_client.post(
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
