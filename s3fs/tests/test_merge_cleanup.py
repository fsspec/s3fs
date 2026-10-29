import asyncio
import unittest

from s3fs import S3FileSystem


class FS(S3FileSystem):
    cachable = False

    def __init__(self, fail=None, abort_fail=False, slow_cleanup=False):
        super().__init__(anon=True, asynchronous=True)
        self.fail = fail
        self.abort_fail = abort_fail
        self.slow_cleanup = slow_cleanup
        self.calls = []
        self.events = []
        self.active = 0
        self.invalidated = []
        self.started = asyncio.Event()
        self.cleaning = asyncio.Event()
        self.release = asyncio.Event()
        self.original = RuntimeError("primary error")

    def invalidate_cache(self, path):
        self.invalidated.append(path)

    async def _call_s3(self, method, *args, **kwargs):
        self.calls.append((method, args, kwargs.copy()))
        if method == "create_multipart_upload":
            if self.fail == "create":
                raise self.original
            return {"UploadId": "u"}
        if method == "upload_part_copy":
            self.active += 1
            if self.active == 2:
                self.started.set()
            try:
                await self.started.wait()
                if self.fail == "copy" and kwargs["PartNumber"] == 1:
                    raise self.original
                if self.fail in ("copy", "cancel"):
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        self.events.append("cancel_received")
                        self.cleaning.set()
                        if self.slow_cleanup:
                            await self.release.wait()
                        raise
                return {"CopyPartResult": {"ETag": str(kwargs["PartNumber"])}}
            finally:
                self.active -= 1
                self.events.append("part_stopped")
        if method == "complete_multipart_upload":
            self.events.append("complete")
            if self.fail == "complete":
                raise self.original
            return {}
        if method == "abort_multipart_upload":
            self.events.append("abort")
            if self.active:
                raise AssertionError("abort raced active copy")
            if self.abort_fail:
                raise ValueError("abort failed")
            return {}
        raise AssertionError(method)


class Tests(unittest.IsolatedAsyncioTestCase):
    async def run_bounded(self, coro):
        return await asyncio.wait_for(coro, 2)

    async def test_normal_request_options_and_concurrency(self):
        fs = FS()
        options = dict(
            RequestPayer="requester", CopySourceIfMatch="caller", Metadata={"x": "y"}
        )
        await self.run_bounded(fs._merge("dest/key", ["src/a", "src/b"], **options))
        self.assertEqual(
            [c[0] for c in fs.calls],
            [
                "create_multipart_upload",
                "upload_part_copy",
                "upload_part_copy",
                "complete_multipart_upload",
            ],
        )
        self.assertEqual(fs.calls[0][1], (options,))
        for index, call in enumerate(fs.calls[1:3], 1):
            self.assertEqual(call[1], (options,))
            self.assertEqual(
                call[2],
                dict(
                    Bucket="dest",
                    Key="key",
                    UploadId="u",
                    CopySource=["src/a", "src/b"][index - 1],
                    PartNumber=index,
                ),
            )
        self.assertEqual(
            fs.calls[-1][2]["MultipartUpload"],
            {"Parts": [{"PartNumber": 1, "ETag": "1"}, {"PartNumber": 2, "ETag": "2"}]},
        )
        self.assertEqual(fs.invalidated, ["dest/key"])
        self.assertEqual(fs.events.count("part_stopped"), 2)
        self.assertNotIn("abort", fs.events)

    async def test_copy_failure_drains_before_abort(self):
        fs = FS(fail="copy", slow_cleanup=True)
        task = asyncio.create_task(
            fs._merge("dest/key", ["a", "b"], RequestPayer="requester")
        )
        await self.run_bounded(fs.cleaning.wait())
        self.assertNotIn("abort", fs.events)
        fs.release.set()
        with self.assertRaises(RuntimeError) as caught:
            await self.run_bounded(task)
        self.assertIs(caught.exception, fs.original)
        self.assertEqual(fs.events[-1], "abort")
        self.assertEqual(fs.active, 0)
        self.assertEqual(fs.calls[-1][1], ({"RequestPayer": "requester"},))
        self.assertEqual(fs.invalidated, [])

    async def test_abort_failure_does_not_mask_copy_or_complete(self):
        for stage in ["copy", "complete"]:
            with self.subTest(stage=stage):
                fs = FS(fail=stage, abort_fail=True)
                with self.assertRaises(RuntimeError) as caught:
                    await self.run_bounded(fs._merge("dest/key", ["a", "b"]))
                self.assertIs(caught.exception, fs.original)
                self.assertEqual(fs.events[-1], "abort")

    async def test_first_cancellation_drains_and_attempts_abort(self):
        fs = FS(fail="cancel")
        task = asyncio.create_task(fs._merge("dest/key", ["a", "b"]))
        await self.run_bounded(fs.started.wait())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.run_bounded(task)
        self.assertEqual(fs.active, 0)
        self.assertEqual(fs.events[-1], "abort")

    async def test_cancellation_during_drain_preserves_primary(self):
        fs = FS(fail="copy", slow_cleanup=True)
        task = asyncio.create_task(fs._merge("dest/key", ["a", "b"]))
        await self.run_bounded(fs.cleaning.wait())
        task.cancel()
        with self.assertRaises(RuntimeError) as caught:
            await self.run_bounded(task)
        self.assertIs(caught.exception, fs.original)
        self.assertNotIn("abort", fs.events)
        self.assertEqual(fs.active, 0)

    async def test_create_failure_no_known_upload_no_abort(self):
        fs = FS(fail="create")
        with self.assertRaises(RuntimeError) as caught:
            await self.run_bounded(fs._merge("dest/key", ["a", "b"]))
        self.assertIs(caught.exception, fs.original)
        self.assertEqual([c[0] for c in fs.calls], ["create_multipart_upload"])

    async def test_source_iterator_failure_cleans_known_upload(self):
        fs = FS()

        def broken():
            yield "a"
            raise fs.original

        with self.assertRaises(RuntimeError) as caught:
            await self.run_bounded(fs._merge("dest/key", broken()))
        self.assertIs(caught.exception, fs.original)
        self.assertEqual(fs.events[-1], "abort")
        self.assertEqual(fs.active, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
