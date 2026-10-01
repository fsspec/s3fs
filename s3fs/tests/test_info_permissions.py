import pytest

from s3fs import S3FileSystem


@pytest.mark.parametrize("operation", ["info", "exists"])
@pytest.mark.parametrize(
    "listing",
    [
        {"KeyCount": 1, "Contents": [{"Key": "allowed/log.eval"}]},
        {"CommonPrefixes": [{"Prefix": "allowed/nested/"}]},
    ],
)
def test_directory_probe_after_denied_head(monkeypatch, operation, listing):
    fs = S3FileSystem(anon=True, skip_instance_cache=True)
    calls = []

    async def call(method, *args, **kwargs):
        calls.append(method)
        assert kwargs["Bucket"] == "bucket"
        if method == "head_object":
            assert kwargs["Key"] == "allowed"
            raise PermissionError("HeadObject denied")
        assert method == "list_objects_v2"
        assert kwargs["Prefix"] == "allowed/"
        return listing

    monkeypatch.setattr(fs, "_call_s3", call)
    result = getattr(fs, operation)("s3://bucket/allowed")
    if operation == "info":
        assert result["type"] == "directory"
        assert result["name"] == "bucket/allowed"
    else:
        assert result is True
    assert calls == ["head_object", "list_objects_v2"]


@pytest.mark.parametrize("operation", ["info", "exists"])
@pytest.mark.parametrize("listing_error", [PermissionError, FileNotFoundError, None])
def test_denied_object_without_directory_stays_denied(
    monkeypatch, operation, listing_error
):
    fs = S3FileSystem(anon=True, skip_instance_cache=True)

    async def call(method, *args, **kwargs):
        if method == "head_object":
            raise PermissionError("HeadObject denied")
        assert method == "list_objects_v2"
        if listing_error:
            raise listing_error("ListObjectsV2 failed")
        return {"KeyCount": 0}

    monkeypatch.setattr(fs, "_call_s3", call)
    with pytest.raises(PermissionError):
        getattr(fs, operation)("s3://bucket/denied")


def test_denied_version_does_not_resolve_to_directory(monkeypatch):
    fs = S3FileSystem(anon=True, skip_instance_cache=True, version_aware=True)

    async def call(method, *args, **kwargs):
        assert method == "head_object"
        assert kwargs["VersionId"] == "version"
        raise PermissionError("HeadObject denied")

    monkeypatch.setattr(fs, "_call_s3", call)
    with pytest.raises(PermissionError, match="HeadObject denied"):
        fs.info("s3://bucket/allowed?versionId=version")
