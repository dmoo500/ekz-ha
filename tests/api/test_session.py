"""Tests for EKZ session retry behavior."""

from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from custom_components.ekz_ha.api.session import JSON_HEADERS, Session


class FakeResponse:
    def __init__(self, data=None, content_type_error=False):
        self.ok = True
        self.status = 200
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
async def test_public_api_recovers_expired_session(monkeypatch, method_name, arguments, query):
    payload = {"contracts": [{"anlage": "825821"}]}
    session, first_http, retry_http, reset, login = make_session(
        monkeypatch, [FakeResponse(content_type_error=True), FakeResponse(data=payload)]
    )

    result = await getattr(session, method_name)(*arguments)

    assert result == payload
    assert query in first_http.get.call_args.args[0]
    assert first_http.get.call_args == retry_http.get.call_args
    reset.assert_awaited_once()
    assert login.await_count == 2


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
