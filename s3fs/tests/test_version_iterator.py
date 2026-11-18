import asyncio
import inspect
import os
import socket
import subprocess

import pytest

from s3fs import S3FileSystem

EVENTS = []


@pytest.fixture(autouse=True)
def deny_network_process(monkeypatch):
    def deny(*args, **kwargs):
        EVENTS.append("network/process attempt")
        raise AssertionError("network/process forbidden")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(subprocess, "Popen", deny)
    monkeypatch.setattr(os, "system", deny)
    yield
    assert EVENTS == []


def make(pages, asynchronous=False, version_aware=True):
    fs = S3FileSystem(
        anon=True,
        version_aware=version_aware,
        asynchronous=asynchronous,
        skip_instance_cache=True,
    )
    calls = []

    async def call(method, options, **kwargs):
        calls.append((method, dict(options), kwargs))
        page = pages[len(calls) - 1]
        if isinstance(page, Exception):
            raise page
        return page

    fs._call_s3 = call
    return fs, calls


def page(items=(), truncated=False, **kwargs):
    return dict(Versions=list(items), IsTruncated=truncated, **kwargs)


def v(key="key", ident="v1"):
    return {"Key": key, "VersionId": ident}


def test_sync_lazy_early_close():
    fs, calls = make([page([v(), v(ident="v2")], True), AssertionError("prefetch")])
    it = fs.object_version_iter("bucket/key")
    assert not calls
    assert next(it) == v()
    assert len(calls) == 1
    it.close()
    assert len(calls) == 1


def test_markers_empty_delete_pages_exact_and_kwargs():
    pages = [
        dict(
            IsTruncated=True,
            DeleteMarkers=[v()],
            NextKeyMarker="key",
            NextVersionIdMarker="d1",
        ),
        page([v("key.bak"), v()], True, NextKeyMarker="key", NextVersionIdMarker="v1"),
        page([v(ident="v2")]),
    ]
    fs, calls = make(pages)
    options = {"MaxKeys": 2}
    assert list(fs.object_version_iter("bucket/key", **options)) == [v(), v(ident="v2")]
    assert options == {"MaxKeys": 2}
    assert calls[0][1] == options
    assert calls[1][1] == dict(MaxKeys=2, KeyMarker="key", VersionIdMarker="d1")
    assert calls[2][1] == dict(MaxKeys=2, KeyMarker="key", VersionIdMarker="v1")
    assert all(
        x[0] == "list_object_versions" and x[2] == dict(Bucket="bucket", Prefix="key")
        for x in calls
    )


@pytest.mark.parametrize(
    "path,items",
    [("bucket/", [v("a"), v("b")]), ("bucket/prefix/", [v("prefix/a"), v("prefix/b")])],
)
def test_prefix_preserved(path, items):
    fs, calls = make([page(items)])
    assert list(fs.object_version_iter(path)) == items


def test_empty_final_page():
    fs, calls = make([dict(IsTruncated=False)])
    assert list(fs.object_version_iter("bucket/key")) == []
    assert len(calls) == 1


def test_error_propagation_after_first_page():
    error = RuntimeError("second page unavailable")
    fs, calls = make([page([v()], True), error])
    it = fs.object_version_iter("bucket/key")
    assert next(it) == v()
    with pytest.raises(RuntimeError) as exc:
        next(it)
    assert exc.value is error
    assert len(calls) == 2


def test_disabled_no_request():
    fs, calls = make([], version_aware=False)
    with pytest.raises(ValueError, match="disabled"):
        next(fs.object_version_iter("bucket/key"))
    assert not calls


def test_existing_info_unchanged_list_semantics():
    fs, calls = make(
        [
            page(
                [v(), v("key.bak")], True, NextKeyMarker="key", NextVersionIdMarker="v1"
            ),
            page([v(ident="v2")]),
        ]
    )
    result = fs.object_version_info("bucket/key")
    assert isinstance(result, list)
    assert result == [v(), v(ident="v2")]


def test_async_and_mirror_sync_methods():
    async def scenario():
        fs, calls = make(
            [
                page([v()], True, NextKeyMarker="key", NextVersionIdMarker="v1"),
                page([v(ident="v2")]),
            ],
            asynchronous=True,
        )
        assert inspect.isasyncgenfunction(fs._object_version_iter)
        assert inspect.isgeneratorfunction(fs.object_version_iter)
        assert [x async for x in fs._object_version_iter("bucket/key")] == [
            v(),
            v(ident="v2"),
        ]
        assert len(calls) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_close_closes_inner_generator(asynchronous):
    fs, calls = make([], asynchronous=asynchronous)
    closed = []

    async def tracked(*args, **kwargs):
        try:
            yield [v(), v(ident="v2")]
            raise AssertionError("unexpected prefetch")
        finally:
            closed.append(True)

    fs._object_version_pages = tracked
    if asynchronous:

        async def run():
            it = fs._object_version_iter("bucket/key")
            assert await anext(it) == v()
            await it.aclose()

        asyncio.run(run())
    else:
        it = fs.object_version_iter("bucket/key")
        assert next(it) == v()
        it.close()
    assert closed == [True]


def test_no_network_or_process_events():
    assert EVENTS == []


def test_page_bounded_retention_against_existing_list():
    import gc
    import weakref

    class Version(dict):
        pass

    def build():
        fs = S3FileSystem(anon=True, version_aware=True, skip_instance_cache=True)
        refs = []
        calls = []
        live_at_request = []

        async def call(method, options, **kwargs):
            live_at_request.append(sum(ref() is not None for ref in refs))
            page_index = len(calls)
            calls.append(dict(options))
            items = [
                Version(Key="key", VersionId=f"{page_index}-{i}") for i in range(20)
            ]
            refs.extend(weakref.ref(item) for item in items)
            return page(
                items,
                page_index < 49,
                NextKeyMarker="key",
                NextVersionIdMarker=str(page_index),
            )

        fs._call_s3 = call
        return fs, calls, live_at_request

    fs, calls, retained = build()
    assert sum(1 for _ in fs.object_version_iter("bucket/key")) == 1000
    assert len(calls) == 50
    assert max(retained) <= 40  # bounded pages, not all previous results
    fs, calls, retained = build()
    result = fs.object_version_info("bucket/key")
    assert len(result) == 1000
    assert retained[-1] == 980  # original API intentionally collects prior pages
    gc.collect()


def test_keyboard_interrupt_cancels_inflight_page_and_preserves_error():
    import _thread
    import threading

    fs, _ = make([])
    started = threading.Event()
    cleaned = threading.Event()
    tasks = []

    async def slow_page(*args, **kwargs):
        tasks.append(asyncio.current_task())
        started.set()
        try:
            await asyncio.sleep(30)
        finally:
            cleaned.set()

    fs._call_s3 = slow_page

    def interrupt():
        if started.wait(5):
            _thread.interrupt_main()

    thread = threading.Thread(target=interrupt)
    thread.start()
    iterator = fs.object_version_iter("bucket/key")
    try:
        with pytest.raises(KeyboardInterrupt):
            next(iterator)
        assert cleaned.is_set()
        assert all(task.done() for task in tasks)
    finally:
        for task in tasks:
            if not task.done():
                fs.loop.call_soon_threadsafe(task.cancel)
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert cleaned.wait(5)
