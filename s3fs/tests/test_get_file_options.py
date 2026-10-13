import asyncio
from io import BytesIO

import pytest

from s3fs import S3FileSystem


class Body:
    def __init__(self, data):
        self.stream = BytesIO(data)
        self.closed = False

    async def read(self, size=-1):
        return self.stream.read(size)

    def close(self):
        self.closed = True
        self.stream.close()


@pytest.mark.parametrize("max_concurrency", [1, 2])
def test_get_file_forwards_request_options(tmp_path, max_concurrency):
    fs = S3FileSystem(asynchronous=True, requester_pays=True, skip_instance_cache=True)
    data = b"encrypted contents"
    options = {
        "SSECustomerAlgorithm": "AES256",
        "SSECustomerKey": "test customer key",
        "ExpectedBucketOwner": "123456789012",
    }
    calls = []
    bodies = []

    async def call_s3(method, **kwargs):
        assert method == "get_object"
        calls.append(kwargs)
        if "Range" in kwargs:
            start, end = kwargs["Range"].removeprefix("bytes=").split("-")
            content = data[int(start) : int(end) + 1 if end else None]
        else:
            content = data
        body = Body(content)
        bodies.append(body)
        return {"Body": body, "ContentLength": len(content)}

    fs._call_s3 = call_s3
    target = tmp_path / "downloaded"
    asyncio.run(
        fs._get_file(
            "bucket/key",
            str(target),
            chunksize=4,
            max_concurrency=max_concurrency,
            **options
        )
    )
    assert target.read_bytes() == data
    assert all(body.closed for body in bodies)
    assert len(calls) > 1 if max_concurrency == 2 else len(calls) == 1
    for call in calls:
        assert call["Bucket"] == "bucket"
        assert call["Key"] == "key"
        assert call["RequestPayer"] == "requester"
        for name, value in options.items():
            assert call[name] == value


def test_get_file_keeps_request_options_on_retry(tmp_path):
    fs = S3FileSystem(
        asynchronous=True, requester_pays=True, retries=2, skip_instance_cache=True
    )
    data = b"retried contents"
    calls = []

    class InterruptedBody(Body):
        async def read(self, size=-1):
            if self.stream.tell():
                raise TimeoutError("interrupted download")
            return self.stream.read(3)

    async def call_s3(method, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            body = InterruptedBody(data)
            content = data
        else:
            start = int(kwargs["Range"].removeprefix("bytes=").split("-")[0])
            content = data[start:]
            body = Body(content)
        return {"Body": body, "ContentLength": len(content)}

    fs._call_s3 = call_s3
    target = tmp_path / "retried"
    asyncio.run(
        fs._get_file(
            "bucket/key", str(target), max_concurrency=1, SSECustomerKey="test key"
        )
    )
    assert target.read_bytes() == data
    assert len(calls) == 2
    assert calls[1]["Range"] == "bytes=3-"
    assert all(call["SSECustomerKey"] == "test key" for call in calls)
    assert all(call["RequestPayer"] == "requester" for call in calls)
