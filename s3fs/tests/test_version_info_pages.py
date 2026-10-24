import asyncio
from unittest.mock import AsyncMock

import pytest

from s3fs import S3FileSystem


@pytest.mark.parametrize(
    "page",
    [
        {"IsTruncated": False},
        {"IsTruncated": False, "DeleteMarkers": [{"Key": "deleted"}]},
    ],
)
def test_version_info_without_versions(page):
    fs = S3FileSystem(version_aware=True, asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = AsyncMock(return_value=page)
    assert asyncio.run(fs._object_version_info("bucket/key")) == []
    assert fs._call_s3.await_count == 1


def test_version_info_continues_after_delete_marker_page():
    version = {"Key": "key", "VersionId": "live", "Size": 1}
    pages = [
        {
            "IsTruncated": True,
            "DeleteMarkers": [{"Key": "key", "VersionId": "deleted"}],
            "NextKeyMarker": "key",
            "NextVersionIdMarker": "deleted",
        },
        {
            "IsTruncated": True,
            "Versions": [version],
            "NextKeyMarker": "key",
            "NextVersionIdMarker": "live",
        },
        {"IsTruncated": False},
    ]
    fs = S3FileSystem(version_aware=True, asynchronous=True, skip_instance_cache=True)
    markers = []
    responses = iter(pages)

    async def call_s3(method, page_markers, **kwargs):
        markers.append(page_markers.copy())
        return next(responses)

    fs._call_s3 = AsyncMock(side_effect=call_s3)
    assert asyncio.run(fs._object_version_info("bucket/key")) == [version]
    assert fs._call_s3.await_count == 3
    assert markers[1] == {
        "KeyMarker": "key",
        "VersionIdMarker": "deleted",
    }


def test_version_info_requires_version_aware_filesystem():
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = AsyncMock()
    with pytest.raises(ValueError, match="version specific functionality"):
        asyncio.run(fs._object_version_info("bucket/key"))
    fs._call_s3.assert_not_awaited()
