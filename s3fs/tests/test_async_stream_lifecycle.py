import asyncio
import gc
import unittest
import warnings
from s3fs import S3FileSystem


class Body:
    def __init__(self):
        self.closes = 0

    async def read(self, length=-1):
        return b"data"

    def close(self):
        self.closes += 1


class Tests(unittest.IsolatedAsyncioTestCase):
    async def make_file(self):
        fs = S3FileSystem(anon=True, asynchronous=True, skip_instance_cache=True)
        body = Body()
        calls = []

        async def call(method, **kwargs):
            calls.append((method, kwargs))
            return {
                "Body": body,
                "ResponseMetadata": {"HTTPHeaders": {"content-length": "4"}},
            }

        fs._call_s3 = call
        f = await fs.open_async("bucket/key", "rb")
        return f, body, calls

    async def test_open_and_close_before_read(self):
        f, body, calls = await self.make_file()
        self.assertFalse(f.closed)
        await f.close()
        await f.close()
        self.assertTrue(f.closed)
        self.assertEqual(calls, [])
        self.assertEqual(body.closes, 0)
        with self.assertRaisesRegex(ValueError, "closed"):
            await f.read()
        self.assertEqual(calls, [])

    async def test_read_close_once(self):
        f, body, calls = await self.make_file()
        self.assertEqual(await f.read(), b"data")
        await f.close()
        await f.close()
        self.assertEqual(body.closes, 1)
        self.assertTrue(f.closed)
        with self.assertRaisesRegex(ValueError, "closed"):
            await f.read()
        self.assertEqual(len(calls), 1)

    async def test_context_exception_closes_body(self):
        f, body, calls = await self.make_file()
        with self.assertRaisesRegex(RuntimeError, "primary"):
            async with f:
                await f.read()
                raise RuntimeError("primary")
        self.assertEqual(body.closes, 1)
        self.assertTrue(f.closed)

    async def test_destructor_closes_without_coroutine_warning(self):
        f, body, calls = await self.make_file()
        await f.read()
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            del f
            gc.collect()
        self.assertEqual(body.closes, 1)
        self.assertFalse(any("was never awaited" in str(w.message) for w in seen))

    async def test_close_while_acquiring_body(self):
        f, body, calls = await self.make_file()
        entered = asyncio.Event()
        release = asyncio.Event()
        original = f.fs._call_s3

        async def delayed(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        f.fs._call_s3 = delayed
        pending = asyncio.create_task(f.read())
        await asyncio.wait_for(entered.wait(), 1)
        await f.close()
        release.set()
        with self.assertRaisesRegex(ValueError, "closed"):
            await asyncio.wait_for(pending, 1)
        self.assertEqual(body.closes, 1)
        await f.close()
        self.assertEqual(body.closes, 1)

    async def test_invalid_metadata_closes_body(self):
        f, body, calls = await self.make_file()

        async def malformed(*args, **kwargs):
            return {
                "Body": body,
                "ResponseMetadata": {"HTTPHeaders": {"content-length": "invalid"}},
            }

        f.fs._call_s3 = malformed
        with self.assertRaises(ValueError):
            await f.read()
        self.assertTrue(f.closed)
        self.assertEqual(body.closes, 1)
