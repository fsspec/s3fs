import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiobotocore.httpsession import AIOHTTPSession
from fsspec.exceptions import FSTimeoutError

from s3fs.core import S3FileSystem


def client_context(http_session):
    return SimpleNamespace(
        _client=SimpleNamespace(_endpoint=SimpleNamespace(http_session=http_session))
    )


@pytest.mark.parametrize("loop_state", ["none", "stopped", "closed"])
def test_close_session_fallback(loop_state):
    loop = asyncio.new_event_loop()
    http_session = AIOHTTPSession(verify=False)

    async def create_sessions():
        await http_session.__aenter__()
        await http_session._get_session(None)
        closed_session = await http_session._get_session("http://127.0.0.1:8081")
        await closed_session.close()
        await http_session._get_session("http://127.0.0.1:8080")

    loop.run_until_complete(create_sessions())
    sessions = list(http_session._sessions.values())
    connectors = [session.connector for session in sessions if not session.closed]
    if loop_state == "closed":
        loop.close()
    close_loop = None if loop_state == "none" else loop
    try:
        S3FileSystem.close_session(close_loop, client_context(http_session))
        assert all(session.closed for session in sessions)
        assert all(connector.closed for connector in connectors)
        S3FileSystem.close_session(close_loop, client_context(http_session))
    finally:
        if loop.is_closed():
            asyncio.run(http_session.close())
        else:
            loop.run_until_complete(http_session.close())
            loop.close()


@pytest.mark.parametrize("sessions", [{}, None])
def test_close_session_empty(sessions):
    S3FileSystem.close_session(
        None, client_context(SimpleNamespace(_sessions=sessions))
    )


def test_close_session_missing_client():
    S3FileSystem.close_session(None, SimpleNamespace())


def test_close_session_legacy_connector():
    connector = Mock()
    S3FileSystem.close_session(
        None, client_context(SimpleNamespace(_connector=connector))
    )
    connector._close.assert_called_once_with()


def test_close_session_running_loop():
    async def run():
        s3 = SimpleNamespace(__aexit__=AsyncMock())
        S3FileSystem.close_session(asyncio.get_running_loop(), s3)
        await asyncio.sleep(0)
        s3.__aexit__.assert_awaited_once_with(None, None, None)

    asyncio.run(run())


@pytest.mark.parametrize("timeout", [False, True])
def test_close_session_other_running_loop(monkeypatch, timeout):
    loop = SimpleNamespace(is_running=lambda: True)
    connector = Mock()
    session = SimpleNamespace(closed=False, connector=connector)
    s3 = client_context(SimpleNamespace(_sessions={None: session}))
    s3.__aexit__ = AsyncMock()
    sync = Mock(side_effect=FSTimeoutError if timeout else None)
    monkeypatch.setattr("s3fs.core.sync", sync)

    S3FileSystem.close_session(loop, s3)

    sync.assert_called_once_with(loop, s3.__aexit__, None, None, None, timeout=0.1)
    assert connector._close.call_count == int(timeout)
