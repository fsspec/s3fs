import asyncio

import pytest

from s3fs import S3FileSystem


@pytest.mark.parametrize("path", ["bucket/prefix", "bucket"])
def test_recursive_chmod_forwards_request_options(monkeypatch, path):
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    files = ["bucket/prefix/first", "bucket/prefix/second"]
    options = {"ExpectedBucketOwner": "123456789012"}
    calls = []

    async def find(requested_path, withdirs):
        assert requested_path == path
        assert withdirs is False
        return files

    async def call_s3(method, request_options, **kwargs):
        calls.append((method, request_options, kwargs))

    monkeypatch.setattr(fs, "_find", find)
    monkeypatch.setattr(fs, "_call_s3", call_s3)
    asyncio.run(fs._chmod(path, "private", recursive=True, **options))

    object_calls = [call for call in calls if call[0] == "put_object_acl"]
    assert len(object_calls) == len(files)
    assert {call[2]["Key"] for call in object_calls} == {
        "prefix/first",
        "prefix/second",
    }
    assert len(calls) == len(files) + (path == "bucket")
    for method, request_options, kwargs in calls:
        assert request_options == options
        assert kwargs["Bucket"] == "bucket"
        assert kwargs["ACL"] == "private"
