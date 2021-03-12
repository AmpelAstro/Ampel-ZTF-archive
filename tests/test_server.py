from typing import TYPE_CHECKING
from unittest.mock import MagicMock
import httpx
import pytest


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
def mock_db(mocked_app):
    return mocked_app.ArchiveDB()


@pytest.fixture
async def mock_client(mocked_app):
    async with httpx.AsyncClient(
        app=mocked_app.app, base_url="http://test", auth=httpx.BasicAuth("yogi", "bear")
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
