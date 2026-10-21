"""Tests for EKZ session retry behavior."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from custom_components.ekz_ha.api.session import JSON_HEADERS, Session


class FakeResponse:
    def __init__(self, data=None, content_type_error=False, status=200):
        self.ok = status < 400
        self.status = status
        self.data = data
        self.content_type_error = content_type_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def json(self):
        if self.content_type_error:
            raise aiohttp.ContentTypeError(None, (), message="unexpected text/html response")
        return self.data

    async def text(self):
        return self.data


def make_session(monkeypatch, responses):
    session = Session.__new__(Session)
    http_session = Mock()
    http_session.get.side_effect = responses
    session._session = http_session

    next_http_session = Mock()
    next_http_session.get.side_effect = responses[1:]
    reset = AsyncMock(side_effect=lambda: setattr(session, "_session", next_http_session))
    monkeypatch.setattr(session, "_reset_session", reset)
    ensure_logged_in = AsyncMock()
    monkeypatch.setattr(session, "_ensure_logged_in", ensure_logged_in)
    return session, http_session, next_http_session, reset, ensure_logged_in


async def test_fetch_with_retry_resets_session_for_html_response(monkeypatch):
    responses = [FakeResponse(content_type_error=True), FakeResponse(data={"ok": True})]
    session, first_http, retry_http, reset, ensure_logged_in = make_session(monkeypatch, responses)

    result = await session._fetch_with_retry("https://example.test/data", "Data", {})

    assert result == {"ok": True}
    first_http.get.assert_called_once_with("https://example.test/data", headers=JSON_HEADERS)
    retry_http.get.assert_called_once_with("https://example.test/data", headers=JSON_HEADERS)
    reset.assert_awaited_once()
    ensure_logged_in.assert_awaited_once()


async def test_installation_fallback_authenticates_after_empty_variant(monkeypatch):
    session = Session.__new__(Session)
    first_http = Mock(close=AsyncMock())
    first_http.get.return_value = FakeResponse(data=[])
    fallback_http = Mock(close=AsyncMock())
    fallback_http.get.side_effect = [
        FakeResponse(data=[]),
        FakeResponse(data={"contracts": [{"anlage": "825821"}]}),
    ]
    session._session = first_http
    session._logged_in = True

    async def login():
        if session._session is None:
            session._session = fallback_http
            session._logged_in = True

    monkeypatch.setattr(session, "_ensure_logged_in", login)

    result = await session.installation_selection_data()

    assert result == {"contracts": [{"anlage": "825821"}]}
    first_http.close.assert_awaited_once()
    assert fallback_http.get.call_count == 2
    assert "installationVariant=CONSUMPTION" in fallback_http.get.call_args_list[0].args[0]
    assert "installationVariant" not in fallback_http.get.call_args_list[1].args[0]


@pytest.mark.parametrize("first_is_html", [False, True])
async def test_exhausted_http_retries_raise_instead_of_returning_empty(monkeypatch, first_is_html):
    first = FakeResponse(content_type_error=True) if first_is_html else FakeResponse(status=503)
    session, _, _, reset, _ = make_session(monkeypatch, [first, FakeResponse(status=503)])

    with pytest.raises(aiohttp.ClientError, match="503"):
        await session._fetch_with_retry("https://example.test/data", "Data", {})

    reset.assert_awaited_once()


async def test_fetch_with_retry_raises_if_reauthenticated_response_is_html(monkeypatch):
    responses = [FakeResponse(content_type_error=True), FakeResponse(content_type_error=True)]
    session, _, _, _, _ = make_session(monkeypatch, responses)

    with pytest.raises(aiohttp.ContentTypeError):
        await session._fetch_with_retry("https://example.test/data", "Data", {})


@pytest.mark.parametrize(
    ("method_name", "arguments", "query"),
    [
        ("installation_selection_data", (), "installationVariant=CONSUMPTION"),
        ("production_installation_selection_data", (), "installationVariant=PRODUCTION"),
        (
            "get_consumption_data",
            ("825821", "PK_VERB_15MIN", "2026-10-17", "2026-10-18"),
            "type=PK_VERB_15MIN",
        ),
        (
            "get_consumption_data",
            ("825821", "WIRK_NEG_15MIN", "2026-10-17", "2026-10-18"),
            "type=WIRK_NEG_15MIN",
        ),
    ],
)
@pytest.mark.parametrize("first_response", ["html", "empty"])
async def test_public_api_recovers_expired_session(
    monkeypatch, method_name, arguments, query, first_response
):
    payload = {"contracts": [{"anlage": "825821"}]}
    session, first_http, retry_http, reset, login = make_session(
        monkeypatch,
        [
            FakeResponse(
                data=[] if first_response == "empty" else None,
                content_type_error=first_response == "html",
            ),
            FakeResponse(data=payload),
        ],
    )

    result = await getattr(session, method_name)(*arguments)

    assert result == payload
    assert query in first_http.get.call_args.args[0]
    assert first_http.get.call_args == retry_http.get.call_args
    reset.assert_awaited_once()
    assert login.await_count == 2


async def test_empty_response_is_confirmed_after_one_retry(monkeypatch):
    session, first_http, retry_http, reset, login = make_session(
        monkeypatch, [FakeResponse(data=[]), FakeResponse(data=[])]
    )

    result = await session._fetch_with_retry("https://example.test/data", "Data", {})

    assert result == []
    assert first_http.get.call_count == retry_http.get.call_count == 1
    reset.assert_awaited_once()
    login.assert_awaited_once()


@pytest.mark.parametrize("retry_failure", ["http", "html"])
async def test_failed_empty_response_retry_is_not_successful_empty_data(monkeypatch, retry_failure):
    retry = (
        FakeResponse(status=503)
        if retry_failure == "http"
        else FakeResponse(content_type_error=True)
    )
    session, _, _, reset, _ = make_session(monkeypatch, [FakeResponse(data=[]), retry])

    with pytest.raises(aiohttp.ClientError):
        await session._fetch_with_retry("https://example.test/data", "Data", {})

    reset.assert_awaited_once()


@pytest.mark.parametrize(
    "method_name", ["installation_selection_data", "production_installation_selection_data"]
)
async def test_installation_lookup_propagates_failed_retry(monkeypatch, method_name):
    session, first_http, retry_http, reset, _ = make_session(
        monkeypatch, [FakeResponse(content_type_error=True), FakeResponse(content_type_error=True)]
    )

    with pytest.raises(aiohttp.ContentTypeError):
        await getattr(session, method_name)()

    assert first_http.get.call_count == 1
    assert retry_http.get.call_count == 1
    reset.assert_awaited_once()


async def test_login_never_logs_authentication_code(monkeypatch, caplog):
    session = Session.__new__(Session)
    session._logged_in = False
    session._username = "test"
    session._password = "test"
    session._totp_secret = "test-secret"
    session._device_name = None
    session._session = Mock()
    session._session.get.return_value = FakeResponse(
        data='<form id="kc-form-login" action="https://example.test/login"></form>'
    )
    session._session.post.side_effect = [
        FakeResponse(data='<form id="otp" action="https://example.test/otp"></form>'),
        FakeResponse(data="<html>Logged in</html>"),
    ]
    code = "739184"
    monkeypatch.setattr(
        "custom_components.ekz_ha.api.session.pyotp.TOTP",
        lambda secret: SimpleNamespace(now=lambda: code),
    )
    caplog.set_level(logging.DEBUG, logger="custom_components.ekz_ha.api.session")

    await session._ensure_logged_in()

    assert session._logged_in is True
    assert session._session.post.call_args.kwargs["data"] == {"otp": code}
    assert "Generated code at" in caplog.text
    assert code not in caplog.text
    assert code[:3] + "***" not in caplog.text
    assert "Code:" not in caplog.text


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
@pytest.mark.parametrize("retry_result", ["readings", "empty", "failed"])
async def test_empty_session_retry_preserves_historical_import(
    monkeypatch, freezer, entity_type, retry_result
):
    from datetime import date, datetime

    from custom_components.ekz_ha.api.client import EkzApiClient
    from custom_components.ekz_ha.api.models import ConsumptionData
    from custom_components.ekz_ha.sensor import EkzMetaEntity
    from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
    from custom_components.ekz_ha.statistics.production import ProductionImporter

    freezer.move_to("2026-10-20 12:00:00+00:00")
    payload = {
        "level": "QUARTER_HOUR",
        "series": {"values": [{"timestamp": "20260815120000", "value": 0.5}]},
    }
    retry = (
        FakeResponse(status=503)
        if retry_result == "failed"
        else FakeResponse(data=payload if retry_result == "readings" else [])
    )
    session, first_http, retry_http, reset, login = make_session(
        monkeypatch, [FakeResponse(data=[]), retry]
    )
    client = EkzApiClient.__new__(EkzApiClient)
    client.session = session
    monkeypatch.setattr(client, "get_consumption_daily", AsyncMock(return_value=ConsumptionData()))
    importer = (
        ConsumptionImporter(client) if entity_type == "consumption" else ProductionImporter(client)
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 8, 1))

    if retry_result == "failed":
        with pytest.raises(aiohttp.ClientError):
            await importer.import_statistics(None, "825821", datetime(2026, 8, 1), meta)
        assert meta._history_cursor is None
        assert meta._last_import == date(2026, 8, 1)
    else:
        result = await importer.import_statistics(None, "825821", datetime(2026, 8, 1), meta)
        assert meta._history_cursor == date(2026, 8, 30)
        if retry_result == "readings":
            assert result["statistics"][0]["state"] == 0.5
            assert meta._last_import == date(2026, 8, 15)
        else:
            assert result["statistics"] == []
    assert first_http.get.call_args == retry_http.get.call_args
    reset.assert_awaited_once()
    assert login.await_count == 2
