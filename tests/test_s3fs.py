# -*- coding: utf-8 -*-
import asyncio
import errno
import datetime
import uuid
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor
import io
import os
import random
import time
import sys
import pytest
import moto
from itertools import chain
import fsspec.core
import s3fs.core
from s3fs.core import S3FileSystem
from s3fs.utils import ignoring, SSEParams
from botocore.exceptions import NoCredentialsError
from fsspec.asyn import sync
from packaging import version


@contextmanager
def expect_errno(expected_errno):
    """Expect an OSError and validate its errno code."""
    with pytest.raises(OSError) as error:
        yield
    assert error.value.errno == expected_errno, "OSError has wrong error code."


def test_simple(s3, file_a):
    data = b"a" * (10 * 2 ** 20)

    with s3.open(file_a, "wb") as f:
        f.write(data)

    with s3.open(file_a, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "mmap"])
def test_default_cache_type(s3, default_cache_type, endpoint_uri, file_a):
    data = b"a" * (10 * 2 ** 20)
    s3 = S3FileSystem(
        anon=False,
        default_cache_type=default_cache_type,
        client_kwargs={"endpoint_url": endpoint_uri},
    )

    with s3.open(file_a, "wb") as f:
        f.write(data)

    with s3.open(file_a, "rb") as f:
        assert isinstance(f.cache, fsspec.core.caches[default_cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_ssl_off(endpoint_uri):
    s3 = S3FileSystem(use_ssl=False, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.s3.meta.endpoint_url.startswith("http://")


def test_client_kwargs():
    s3 = S3FileSystem(client_kwargs={"endpoint_url": "http://foo"})
    assert s3.s3.meta.endpoint_url.startswith("http://foo")


def test_config_kwargs(endpoint_uri):
    s3 = S3FileSystem(
        config_kwargs={"signature_version": "s3v4"},
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    assert s3.connect().meta.config.signature_version == "s3v4"


def test_config_kwargs_class_attributes_default(endpoint_uri):
    s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.connect().meta.config.connect_timeout == 5
    assert s3.connect().meta.config.read_timeout == 15


def test_config_kwargs_class_attributes_override(endpoint_uri):
    s3 = S3FileSystem(
        config_kwargs={
            "connect_timeout": 60,
            "read_timeout": 120,
        },
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    assert s3.connect().meta.config.connect_timeout == 60
    assert s3.connect().meta.config.read_timeout == 120


def test_user_session_is_preserved():
    from aiobotocore.session import get_session

    session = get_session()
    s3 = S3FileSystem(session=session)
    s3.connect()
    assert s3.session == session


def test_idempotent_connect(s3):
    first = s3.s3
    assert s3.connect(refresh=True) is not first


def test_multiple_objects(s3, endpoint_uri, bucket_names):
    s3.connect()
    s3.ls(bucket_names["test"])
    s32 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
    assert s32.session
    assert s3.ls(bucket_names["test"]) == s32.ls(bucket_names["test"])


def test_info(s3, bucket_names, file_a, file_b):
    s3.touch(file_a)
    s3.touch(file_b)
    info = s3.info(file_a)
    linfo = s3.ls(file_a, detail=True)[0]
    assert abs(info.pop("LastModified") - linfo.pop("LastModified")).seconds < 1
    info.pop("VersionId")
    assert info == linfo
    parent = file_a.rsplit("/", 1)[0]
    s3.invalidate_cache()  # remove full path from the cache
    s3.ls(parent)  # fill the cache with parent dir
    assert s3.info(file_a) == s3.dircache[parent][0]  # correct value
    assert id(s3.info(file_a)) == id(s3.dircache[parent][0])  # is object from cache

    new_parent = bucket_names["test"] + "/foo"
    s3.mkdir(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)
    s3.ls(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)


def test_info_cached(s3, bucket_names):
    path = bucket_names["test"] + "/tmp/"
    fqpath = "s3://" + path
    s3.touch(path + "test")
    info = s3.info(fqpath)
    assert info == s3.info(fqpath)
    assert info == s3.info(path)


def test_checksum(s3, bucket_names):
    bucket = bucket_names["test"]
    d = "checksum"
    prefix = d + "/e"
    o1 = prefix + "1"
    o2 = prefix + "2"
    path1 = bucket + "/" + o1
    path2 = bucket + "/" + o2

    client = s3.s3

    # init client and files
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="")
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o2, Body="")

    # change one file, using cache
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="foo")
    checksum = s3.checksum(path1)
    s3.ls(path1)  # force caching
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="bar")
    # refresh == False => checksum doesn't change
    assert checksum == s3.checksum(path1)

    # change one file, without cache
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="foo")
    checksum = s3.checksum(path1, refresh=True)
    s3.ls(path1)  # force caching
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="bar")
    # refresh == True => checksum changes
    assert checksum != s3.checksum(path1, refresh=True)

    # Test for nonexistent file
    sync(s3.loop, client.put_object, Bucket=bucket, Key=o1, Body="bar")
    s3.ls(path1)  # force caching
    sync(s3.loop, client.delete_object, Bucket=bucket, Key=o1)
    with pytest.raises(FileNotFoundError):
        s3.checksum(o1, refresh=True)

    # Test multipart upload
    upload_id = sync(
        s3.loop,
        client.create_multipart_upload,
        Bucket=bucket,
        Key=o1,
    )["UploadId"]
    etag1 = sync(
        s3.loop,
        client.upload_part,
        Bucket=bucket,
        Key=o1,
        UploadId=upload_id,
        PartNumber=1,
        Body="0" * (5 * 1024 * 1024),
    )["ETag"]
    etag2 = sync(
        s3.loop,
        client.upload_part,
        Bucket=bucket,
        Key=o1,
        UploadId=upload_id,
        PartNumber=2,
        Body="0",
    )["ETag"]
    sync(
        s3.loop,
        client.complete_multipart_upload,
        Bucket=bucket,
        Key=o1,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": etag1},
                {"PartNumber": 2, "ETag": etag2},
            ]
        },
    )
    s3.checksum(path1, refresh=True)


test_xattr_sample_metadata = {"test_xattr": "1"}


def test_xattr(s3, bucket_names):
    bucket, key = (bucket_names["test"], "tmp/test/xattr")
    filename = bucket + "/" + key
    body = b"aaaa"
    public_read_acl = {
        "Permission": "READ",
        "Grantee": {
            "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
            "Type": "Group",
        },
    }

    sync(
        s3.loop,
        s3.s3.put_object,
        Bucket=bucket,
        Key=key,
        ACL="public-read",
        Metadata=test_xattr_sample_metadata,
        Body=body,
    )

    # save etag for later
    etag = s3.info(filename)["ETag"]
    assert (
        public_read_acl
        in sync(s3.loop, s3.s3.get_object_acl, Bucket=bucket, Key=key)["Grants"]
    )

    assert (
        s3.getxattr(filename, "test_xattr") == test_xattr_sample_metadata["test_xattr"]
    )
    assert s3.metadata(filename) == {"test-xattr": "1"}  # note _ became -

    s3file = s3.open(filename)
    assert s3file.getxattr("test_xattr") == test_xattr_sample_metadata["test_xattr"]
    assert s3file.metadata() == {"test-xattr": "1"}  # note _ became -

    s3file.setxattr(test_xattr="2")
    assert s3file.getxattr("test_xattr") == "2"
    s3file.setxattr(**{"test_xattr": None})
    assert s3file.metadata() == {}
    assert s3.cat(filename) == body

    # check that ACL and ETag are preserved after updating metadata
    assert (
        public_read_acl
        in sync(s3.loop, s3.s3.get_object_acl, Bucket=bucket, Key=key)["Grants"]
    )
    assert s3.info(filename)["ETag"] == etag


def test_xattr_setxattr_in_write_mode(s3, file_a):
    s3file = s3.open(file_a, "wb")
    with pytest.raises(NotImplementedError):
        s3file.setxattr(test_xattr="1")


@pytest.mark.xfail()
def test_delegate(s3, endpoint_uri):
    out = s3.get_delegated_s3pars()
    assert out
    assert out["token"]
    s32 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri}, **out)
    assert not s32.anon
    assert out == s32.get_delegated_s3pars()


def test_not_delegate(endpoint_uri):
    s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    out = s3.get_delegated_s3pars()
    assert out == {"anon": True}
    s3 = S3FileSystem(
        anon=False, client_kwargs={"endpoint_url": endpoint_uri}
    )  # auto credentials
    out = s3.get_delegated_s3pars()
    assert out == {"anon": False}


def test_ls(s3, bucket_names):
    assert set(s3.ls("", detail=False)) == {
        bucket_names["test"],
        bucket_names["secure"],
        bucket_names["versioned"],
    }
    with pytest.raises(FileNotFoundError):
        s3.ls("nonexistent")
    fn = bucket_names["test"] + "/test/accounts.1.json"
    assert fn in s3.ls(bucket_names["test"] + "/test", detail=False)


def test_pickle(s3, bucket_names):
    import pickle

    s32 = pickle.loads(pickle.dumps(s3))
    assert s3.ls(bucket_names["test"]) == s32.ls(bucket_names["test"])
    s33 = pickle.loads(pickle.dumps(s32))
    assert s3.ls(bucket_names["test"]) == s33.ls(bucket_names["test"])


def test_ls_touch(s3, bucket_names, file_a, file_b):
    assert not s3.exists(bucket_names["test"] + "/tmp/test")
    s3.touch(file_a)
    s3.touch(file_b)
    L = s3.ls(bucket_names["test"] + "/tmp/test", True)
    assert {d["Key"] for d in L} == {file_a, file_b}
    L = s3.ls(bucket_names["test"] + "/tmp/test", False)
    assert set(L) == {file_a, file_b}


@pytest.mark.parametrize("version_aware", [True, False])
def test_exists_versioned(s3, version_aware, endpoint_uri, bucket_names):
    """Test to ensure that a prefix exists when using a versioned bucket"""
    import uuid

    n = 3
    s3 = S3FileSystem(
        anon=False,
        version_aware=version_aware,
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    segments = [bucket_names["versioned"]] + [str(uuid.uuid4()) for _ in range(n)]
    path = "/".join(segments)
    for i in range(2, n + 1):
        assert not s3.exists("/".join(segments[:i]))
    s3.touch(path)
    for i in range(2, n + 1):
        assert s3.exists("/".join(segments[:i]))


def test_isfile(s3, bucket_names, file_a, file_b, file_c):
    assert not s3.isfile("")
    assert not s3.isfile("/")
    assert not s3.isfile(bucket_names["test"])
    assert not s3.isfile(bucket_names["test"] + "/test")

    assert not s3.isfile(bucket_names["test"] + "/test/foo")
    assert s3.isfile(bucket_names["test"] + "/test/accounts.1.json")
    assert s3.isfile(bucket_names["test"] + "/test/accounts.2.json")

    assert not s3.isfile(file_a)
    s3.touch(file_a)
    assert s3.isfile(file_a)

    assert not s3.isfile(file_b)
    assert not s3.isfile(file_b + "/")
    s3.mkdir(file_b)
    assert not s3.isfile(file_b)
    assert not s3.isfile(file_b + "/")

    assert not s3.isfile(file_c)
    assert not s3.isfile(file_c + "/")
    s3.mkdir(file_c + "/")
    assert not s3.isfile(file_c)
    assert not s3.isfile(file_c + "/")


def test_isdir(s3, bucket_names, file_a, file_b, file_c):
    assert s3.isdir("")
    assert s3.isdir("/")
    assert s3.isdir(bucket_names["test"])
    assert s3.isdir(bucket_names["test"] + "/test")

    assert not s3.isdir(bucket_names["test"] + "/test/foo")
    assert not s3.isdir(bucket_names["test"] + "/test/accounts.1.json")
    assert not s3.isdir(bucket_names["test"] + "/test/accounts.2.json")

    assert not s3.isdir(file_a)
    s3.touch(file_a)
    assert not s3.isdir(file_a)

    assert not s3.isdir(file_b)
    assert not s3.isdir(file_b + "/")

    assert not s3.isdir(file_c)
    assert not s3.isdir(file_c + "/")

    # test cache
    s3.invalidate_cache()
    assert not s3.dircache
    s3.ls(bucket_names["test"] + "/nested")
    assert bucket_names["test"] + "/nested" in s3.dircache
    assert not s3.isdir(bucket_names["test"] + "/nested/file1")
    assert not s3.isdir(bucket_names["test"] + "/nested/file2")
    assert s3.isdir(bucket_names["test"] + "/nested/nested2")
    assert s3.isdir(bucket_names["test"] + "/nested/nested2/")


def test_rm(s3, bucket_names, file_a):
    assert not s3.exists(file_a)
    s3.touch(file_a)
    assert s3.exists(file_a)
    s3.rm(file_a)
    assert not s3.exists(file_a)
    # the API is OK with deleting non-files; maybe this is an effect of using bulk
    # with pytest.raises(FileNotFoundError):
    #    s3.rm(bucket_names['test'] + '/nonexistent')
    with pytest.raises(FileNotFoundError):
        s3.rm("nonexistent")
    s3.rm(bucket_names["test"] + "/nested", recursive=True)
    assert not s3.exists(bucket_names["test"] + "/nested/nested2/file1")

    # whole bucket
    s3.rm(bucket_names["test"], recursive=True)
    assert not s3.exists(bucket_names["test"] + "/2014-01-01.csv")
    assert not s3.exists(bucket_names["test"])


def test_rmdir(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    s3.rmdir(bucket)
    assert bucket not in s3.ls("/")


def test_mkdir(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")


def test_mkdir_existing_bucket(s3, bucket_names):
    # mkdir called on existing bucket should be no-op and not calling create_bucket
    # creating a s3 bucket
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")
    # a second call.
    with pytest.raises(FileExistsError):
        s3.mkdir(bucket)


def test_mkdir_bucket_and_key_1(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    file = bucket + "/a/b/c"
    s3.mkdir(file, create_parents=True)
    assert bucket in s3.ls("/")


def test_mkdir_bucket_and_key_2(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    file = bucket + "/a/b/c"
    with pytest.raises(FileNotFoundError):
        s3.mkdir(file, create_parents=False)
    assert bucket not in s3.ls("/")


def test_mkdir_region_name(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket, region_name="eu-central-1")
    assert bucket in s3.ls("/")


def test_mkdir_client_region_name(s3, endpoint_uri):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3 = S3FileSystem(
        anon=False,
        client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
    )
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")


def test_makedirs(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    test_file = bucket + "/a/b/c/file"
    s3.makedirs(test_file)
    assert bucket in s3.ls("/")


def test_makedirs_existing_bucket(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")
    test_file = bucket + "/a/b/c/file"
    # no-op, and no error.
    s3.makedirs(test_file)


def test_makedirs_pure_bucket_exist_ok(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    s3.makedirs(bucket, exist_ok=True)


def test_makedirs_pure_bucket_error_on_exist(s3):
    bucket = f"test1_bucket-{uuid.uuid4()}"
    s3.mkdir(bucket)
    with pytest.raises(FileExistsError):
        s3.makedirs(bucket, exist_ok=False)


def test_bulk_delete(s3, bucket_names):
    with pytest.raises(FileNotFoundError):
        s3.rm(["nonexistent/file"])
    filelist = s3.find(bucket_names["test"] + "/nested")
    s3.rm(filelist)
    assert not s3.exists(bucket_names["test"] + "/nested/nested2/file1")


@pytest.mark.xfail(reason="anon user is still priviliged on moto")
def test_anonymous_access(s3, endpoint_uri):
    with ignoring(NoCredentialsError):
        s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
        assert s3.ls("") == []
        # TODO: public bucket doesn't work through moto

    with pytest.raises(PermissionError):
        s3.mkdir("newbucket")


def test_s3_file_access(s3, bucket_names):
    fn = bucket_names["test"] + "/nested/file1"
    data = b"hello\n"
    assert s3.cat(fn) == data
    assert s3.head(fn, 3) == data[:3]
    assert s3.tail(fn, 3) == data[-3:]
    assert s3.tail(fn, 10000) == data


def test_s3_file_info(s3, bucket_names):
    fn = bucket_names["test"] + "/nested/file1"
    data = b"hello\n"
    assert fn in s3.find(bucket_names["test"])
    assert s3.exists(fn)
    assert not s3.exists(fn + "another")
    assert s3.info(fn)["Size"] == len(data)
    with pytest.raises(FileNotFoundError):
        s3.info(fn + "another")


def test_content_type_is_set(s3, tmpdir, bucket_names):
    test_file = str(tmpdir) + "/test.json"
    destination = bucket_names["test"] + "/test.json"
    open(test_file, "w").write("text")
    s3.put(test_file, destination)
    assert s3.info(destination)["ContentType"] == "application/json"


def test_content_type_is_not_overrided(s3, tmpdir, bucket_names):
    test_file = os.path.join(str(tmpdir), "test.json")
    destination = os.path.join(bucket_names["test"], "test.json")
    open(test_file, "w").write("text")
    s3.put(test_file, destination, ContentType="text/css")
    assert s3.info(destination)["ContentType"] == "text/css"


def test_bucket_exists(s3, endpoint_uri, bucket_names):
    assert s3.exists(bucket_names["test"])
    assert not s3.exists(bucket_names["test"] + "x")
    s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.exists(bucket_names["test"])
    assert not s3.exists(bucket_names["test"] + "x")


def test_du(s3, bucket_names, s3_files):
    d = s3.du(bucket_names["test"], total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert bucket_names["test"] + "/nested/file1" in d

    assert s3.du(bucket_names["test"] + "/test/", total=True) == sum(
        map(len, s3_files["files"].values())
    )
    assert s3.du(bucket_names["test"]) == s3.du("s3://" + bucket_names["test"])


def test_s3_ls(s3, bucket_names):
    fn = bucket_names["test"] + "/nested/file1"
    assert fn not in s3.ls(bucket_names["test"] + "/")
    assert fn in s3.ls(bucket_names["test"] + "/nested/")
    assert fn in s3.ls(bucket_names["test"] + "/nested")
    assert s3.ls("s3://" + bucket_names["test"] + "/nested/") == s3.ls(
        bucket_names["test"] + "/nested"
    )


def test_s3_big_ls(s3, bucket_names):
    for x in range(1200):
        s3.touch(bucket_names["test"] + "/thousand/%i.part" % x)
    assert len(s3.find(bucket_names["test"])) > 1200
    s3.rm(bucket_names["test"] + "/thousand/", recursive=True)
    assert len(s3.find(bucket_names["test"] + "/thousand/")) == 0


def test_s3_ls_detail(s3, bucket_names):
    L = s3.ls(bucket_names["test"] + "/nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_s3_glob(s3, bucket_names):
    fn = bucket_names["test"] + "/nested/file1"
    assert fn not in s3.glob(bucket_names["test"] + "/")
    assert fn not in s3.glob(bucket_names["test"] + "/*")
    assert fn not in s3.glob(bucket_names["test"] + "/nested")
    assert fn in s3.glob(bucket_names["test"] + "/nested/*")
    assert fn in s3.glob(bucket_names["test"] + "/nested/file*")
    assert fn in s3.glob(bucket_names["test"] + "/*/*")
    assert all(
        any(p.startswith(f + "/") or p == f for p in s3.find(bucket_names["test"]))
        for f in s3.glob(bucket_names["test"] + "/nested/*")
    )
    assert [bucket_names["test"] + "/nested/nested2"] == s3.glob(
        bucket_names["test"] + "/nested/nested2"
    )
    out = s3.glob(bucket_names["test"] + "/nested/nested2/*")
    assert {
        bucket_names["test"] + "/nested/nested2/file1",
        bucket_names["test"] + "/nested/nested2/file2",
    } == set(out)

    with pytest.raises(ValueError):
        s3.glob("*")

    # Make sure glob() deals with the dot character (.) correctly.
    assert bucket_names["test"] + "/file.dat" in s3.glob(
        bucket_names["test"] + "/file.*"
    )
    assert bucket_names["test"] + "/filexdat" not in s3.glob(
        bucket_names["test"] + "/file.*"
    )


def test_get_list_of_summary_objects(s3, bucket_names, s3_files):
    L = s3.ls(bucket_names["test"] + "/test")

    assert len(L) == 2
    assert [l.lstrip(bucket_names["test"]).lstrip("/") for l in sorted(L)] == sorted(
        list(s3_files["files"])
    )

    L2 = s3.ls("s3://" + bucket_names["test"] + "/test")

    assert L == L2


def test_read_keys_from_bucket(s3, bucket_names, s3_files):
    for k, data in s3_files["files"].items():
        file_contents = s3.cat("/".join([bucket_names["test"], k]))
        assert file_contents == data

        assert s3.cat("/".join([bucket_names["test"], k])) == s3.cat(
            "s3://" + "/".join([bucket_names["test"], k])
        )


def test_url(s3, bucket_names):
    fn = bucket_names["test"] + "/nested/file1"
    url = s3.url(fn, expires=100)
    assert "http" in url
    import urllib.parse

    components = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(components.query)
    exp = int(query["Expires"][0])

    delta = abs(exp - time.time() - 100)
    assert delta < 5

    with s3.open(fn) as f:
        assert "http" in f.url()


def test_seek(s3, file_a):
    with s3.open(file_a, "wb") as f:
        f.write(b"123")

    with s3.open(file_a) as f:
        f.seek(1000)
        with pytest.raises(ValueError):
            f.seek(-1)
        with pytest.raises(ValueError):
            f.seek(-5, 2)
        with pytest.raises(ValueError):
            f.seek(0, 10)
        f.seek(0)
        assert f.read(1) == b"1"
        f.seek(0)
        assert f.read(1) == b"1"
        f.seek(3)
        assert f.read(1) == b""
        f.seek(-1, 2)
        assert f.read(1) == b"3"
        f.seek(-1, 1)
        f.seek(-1, 1)
        assert f.read(1) == b"2"
        for i in range(4):
            assert f.seek(i) == i


def test_bad_open(s3):
    with pytest.raises(ValueError):
        s3.open("")


def test_copy(s3, bucket_names):
    fn = bucket_names["test"] + "/test/accounts.1.json"
    s3.copy(fn, fn + "2")
    assert s3.cat(fn) == s3.cat(fn + "2")


def test_copy_managed(s3, bucket_names):
    data = b"abc" * 12 * 2 ** 20
    fn = bucket_names["test"] + "/test/biggerfile"
    with s3.open(fn, "wb") as f:
        f.write(data)
    sync(s3.loop, s3._copy_managed, fn, fn + "2", size=len(data), block=5 * 2 ** 20)
    assert s3.cat(fn) == s3.cat(fn + "2")
    with pytest.raises(ValueError):
        sync(s3.loop, s3._copy_managed, fn, fn + "3", size=len(data), block=4 * 2 ** 20)
    with pytest.raises(ValueError):
        sync(s3.loop, s3._copy_managed, fn, fn + "3", size=len(data), block=6 * 2 ** 30)


@pytest.mark.parametrize("recursive", [True, False])
def test_move(s3, recursive, bucket_names):
    fn = bucket_names["test"] + "/test/accounts.1.json"
    data = s3.cat(fn)
    s3.mv(fn, fn + "2", recursive=recursive)
    assert s3.cat(fn + "2") == data
    assert not s3.exists(fn)


def test_get_put(s3, tmpdir, bucket_names, s3_files):
    test_file = str(tmpdir.join("test.json"))

    s3.get(bucket_names["test"] + "/test/accounts.1.json", test_file)
    data = s3_files["files"]["test/accounts.1.json"]
    assert open(test_file, "rb").read() == data
    s3.put(test_file, bucket_names["test"] + "/temp")
    assert s3.du(bucket_names["test"] + "/temp", total=False)[
        bucket_names["test"] + "/temp"
    ] == len(data)
    assert s3.cat(bucket_names["test"] + "/temp") == data


def test_get_put_big(s3, tmpdir, bucket_names):
    test_file = str(tmpdir.join("test"))
    data = b"1234567890A" * 2 ** 20
    open(test_file, "wb").write(data)

    s3.put(test_file, bucket_names["test"] + "/bigfile")
    test_file = str(tmpdir.join("test2"))
    s3.get(bucket_names["test"] + "/bigfile", test_file)
    assert open(test_file, "rb").read() == data


def test_get_put_with_callback(s3, tmpdir, bucket_names):
    test_file = str(tmpdir.join("test.json"))

    class BranchingCallback(Callback):
        def branch(self, path_1, path_2, kwargs):
            kwargs["callback"] = BranchingCallback()

    cb = BranchingCallback()
    s3.get(bucket_names["test"] + "/test/accounts.1.json", test_file, callback=cb)
    assert cb.size == 1
    assert cb.value == 1

    cb = BranchingCallback()
    s3.put(test_file, bucket_names["test"] + "/temp", callback=cb)
    assert cb.size == 1
    assert cb.value == 1


def test_get_file_with_callback(s3, tmpdir, bucket_names):
    test_file = str(tmpdir.join("test.json"))

    cb = Callback()
    s3.get_file(bucket_names["test"] + "/test/accounts.1.json", test_file, callback=cb)
    assert cb.size == os.stat(test_file).st_size
    assert cb.value == cb.size


@pytest.mark.parametrize("size", [2**10, 10 * 2**20])
def test_put_file_with_callback(s3, tmpdir, size, bucket_names):
    test_file = str(tmpdir.join("test.json"))
    with open(test_file, "wb") as f:
        f.write(b"1234567890A" * size)

    cb = Callback()
    s3.put_file(test_file, bucket_names["test"] + "/temp", callback=cb)
    assert cb.size == os.stat(test_file).st_size
    assert cb.value == cb.size


@pytest.mark.parametrize("size", [2 ** 10, 2 ** 20, 10 * 2 ** 20])
def test_pipe_cat_big(s3, size, bucket_names):
    data = b"1234567890A" * size
    s3.pipe(bucket_names["test"] + "/bigfile", data)
    assert s3.cat(bucket_names["test"] + "/bigfile") == data


def test_errors(s3, bucket_names):
    with pytest.raises(FileNotFoundError):
        s3.open(bucket_names["test"] + "/tmp/test/shfoshf", "rb")

    # This is fine, no need for interleaving directories on S3
    # with pytest.raises((IOError, OSError)):
    #    s3.touch('tmp/test/shfoshf/x')

    # Deleting nonexistent or zero paths is allowed for now
    # with pytest.raises(FileNotFoundError):
    #    s3.rm(bucket_names['test'] + '/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        s3.mv(bucket_names["test"] + "/tmp/test/shfoshf/x", "tmp/test/shfoshf/y")

    with pytest.raises(ValueError):
        s3.open("x", "rb")

    with pytest.raises(FileNotFoundError):
        s3.rm("unknown")

    with pytest.raises(ValueError):
        with s3.open(bucket_names["test"] + "/temp", "wb") as f:
            f.read()

    with pytest.raises(ValueError):
        f = s3.open(bucket_names["test"] + "/temp", "rb")
        f.close()
        f.read()

    with pytest.raises(ValueError):
        s3.mkdir("/")

    with pytest.raises(ValueError):
        s3.find("")

    with pytest.raises(ValueError):
        s3.find("s3://")


def test_errors_cause_preservings(monkeypatch, s3):
    # We translate the error, and preserve the original one
    with pytest.raises(FileNotFoundError) as exc:
        s3.rm("unknown")

    assert type(exc.value.__cause__).__name__ == "NoSuchBucket"

    async def list_objects_v2(*args, **kwargs):
        raise NoCredentialsError

    monkeypatch.setattr(type(s3.s3), "list_objects_v2", list_objects_v2)

    # Since the error is not translate, the __cause__ would
    # be None
    with pytest.raises(NoCredentialsError) as exc:
        s3.info("test/a.txt")

    assert exc.value.__cause__ is None


def test_read_small(s3, bucket_names):
    fn = bucket_names["test"] + "/2014-01-01.csv"
    with s3.open(fn, "rb", block_size=10) as f:
        out = []
        while True:
            data = f.read(3)
            if data == b"":
                break
            out.append(data)
        assert s3.cat(fn) == b"".join(out)
        # cache drop
        assert len(f.cache) < len(out)


def test_read_s3_block(s3, bucket_names, s3_files):
    data = s3_files["files"]["test/accounts.1.json"]
    lines = io.BytesIO(data).readlines()
    path = bucket_names["test"] + "/test/accounts.1.json"
    assert s3.read_block(path, 1, 35, b"\n") == lines[1]
    assert s3.read_block(path, 0, 30, b"\n") == lines[0]
    assert s3.read_block(path, 0, 35, b"\n") == lines[0] + lines[1]
    assert s3.read_block(path, 0, 5000, b"\n") == data
    assert len(s3.read_block(path, 0, 5)) == 5
    assert len(s3.read_block(path, 4, 5000)) == len(data) - 4
    assert s3.read_block(path, 5000, 5010) == b""

    assert s3.read_block(path, 5, None) == s3.read_block(path, 5, 1000)


def test_new_bucket(s3):
    assert not s3.exists("new")
    s3.mkdir("new")
    assert s3.exists("new")
    with s3.open("new/temp", "wb") as f:
        f.write(b"hello")
    with pytest.raises(OSError):
        s3.rmdir("new")

    s3.rm("new/temp")
    s3.rmdir("new")
    assert "new" not in s3.ls("")
    assert not s3.exists("new")
    with pytest.raises(FileNotFoundError):
        s3.ls("new")


def test_new_bucket_auto(s3):
    assert not s3.exists("new")
    with pytest.raises(Exception):
        s3.mkdir("new/other", create_parents=False)
    s3.mkdir("new/other", create_parents=True)
    assert s3.exists("new")
    s3.touch("new/afile")
    with pytest.raises(Exception):
        s3.rm("new")
    with pytest.raises(Exception):
        s3.rmdir("new")
    s3.rm("new", recursive=True)
    assert not s3.exists("new")


def test_dynamic_add_rm(s3):
    s3.mkdir("one")
    s3.mkdir("one/two")
    assert s3.exists("one")
    s3.ls("one")
    s3.touch("one/two/file_a")
    assert s3.exists("one/two/file_a")
    s3.rm("one", recursive=True)
    assert not s3.exists("one")


def test_write_small(s3, bucket_names):
    with s3.open(bucket_names["test"] + "/test", "wb") as f:
        f.write(b"hello")
    assert s3.cat(bucket_names["test"] + "/test") == b"hello"
    s3.open(bucket_names["test"] + "/test", "wb").close()
    assert s3.info(bucket_names["test"] + "/test")["size"] == 0


def test_write_small_with_acl(s3, bucket_names):
    bucket, key = (bucket_names["test"], "test-acl")
    filename = bucket + "/" + key
    body = b"hello"
    public_read_acl = {
        "Permission": "READ",
        "Grantee": {
            "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
            "Type": "Group",
        },
    }

    with s3.open(filename, "wb", acl="public-read") as f:
        f.write(body)
    assert s3.cat(filename) == body

    assert (
        public_read_acl
        in sync(s3.loop, s3.s3.get_object_acl, Bucket=bucket, Key=key)["Grants"]
    )


def test_write_large(s3, bucket_names):
    "flush() chunks buffer when processing large singular payload"
    mb = 2 ** 20
    payload_size = int(2.5 * 5 * mb)
    payload = b"0" * payload_size

    with s3.open(bucket_names["test"] + "/test", "wb") as fd:
        fd.write(payload)

    assert s3.cat(bucket_names["test"] + "/test") == payload
    assert s3.info(bucket_names["test"] + "/test")["Size"] == payload_size


def test_write_limit(s3, bucket_names):
    "flush() respects part_max when processing large singular payload"
    mb = 2 ** 20
    block_size = 15 * mb
    payload_size = 44 * mb
    payload = b"0" * payload_size

    with s3.open(bucket_names["test"] + "/test", "wb", blocksize=block_size) as fd:
        fd.write(payload)

    assert s3.cat(bucket_names["test"] + "/test") == payload

    assert s3.info(bucket_names["test"] + "/test")["Size"] == payload_size


def test_write_small_secure(s3, bucket_names):
    # Unfortunately moto does not yet support enforcing SSE policies.  It also
    # does not return the correct objects that can be used to test the results
    # effectively.
    # This test is left as a placeholder in case moto eventually supports this.
    sse_params = SSEParams(server_side_encryption="aws:kms")
    with s3.open(bucket_names["secure"] + "/test", "wb", writer_kwargs=sse_params) as f:
        f.write(b"hello")
    assert s3.cat(bucket_names["secure"] + "/test") == b"hello"
    sync(s3.loop, s3.s3.head_object, Bucket=bucket_names["secure"], Key="test")


def test_write_large_secure(s3, endpoint_uri):
    # build our own s3fs with the relevant additional kwarg
    s3 = S3FileSystem(
        s3_additional_kwargs={"ServerSideEncryption": "AES256"},
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    s3.mkdir("mybucket")

    with s3.open("mybucket/myfile", "wb") as f:
        f.write(b"hello hello" * 10 ** 6)

    assert s3.cat("mybucket/myfile") == b"hello hello" * 10 ** 6


def test_write_fails(s3, bucket_names):
    with pytest.raises(ValueError):
        s3.touch(bucket_names["test"] + "/temp")
        s3.open(bucket_names["test"] + "/temp", "rb").write(b"hello")
    with pytest.raises(ValueError):
        s3.open(bucket_names["test"] + "/temp", "wb", block_size=10)
    f = s3.open(bucket_names["test"] + "/temp", "wb")
    f.close()
    with pytest.raises(ValueError):
        f.write(b"hello")
    with pytest.raises(FileNotFoundError):
        s3.open("nonexistentbucket/temp", "wb").close()


def test_write_blocks(s3, bucket_names):
    with s3.open(bucket_names["test"] + "/temp", "wb") as f:
        f.write(b"a" * 2 * 2 ** 20)
        assert f.buffer.tell() == 2 * 2 ** 20
        assert not (f.parts)
        f.flush()
        assert f.buffer.tell() == 2 * 2 ** 20
        assert not (f.parts)
        f.write(b"a" * 2 * 2 ** 20)
        f.write(b"a" * 2 * 2 ** 20)
        assert f.mpu
        assert f.parts
    assert s3.info(bucket_names["test"] + "/temp")["Size"] == 6 * 2 ** 20
    with s3.open(bucket_names["test"] + "/temp", "wb", block_size=10 * 2 ** 20) as f:
        f.write(b"a" * 15 * 2 ** 20)
        assert f.buffer.tell() == 0
    assert s3.info(bucket_names["test"] + "/temp")["Size"] == 15 * 2 ** 20


def test_readline(s3, bucket_names, s3_files):
    all_items = chain.from_iterable(
        [s3_files["files"].items(), s3_files["csv"].items(), s3_files["text"].items()]
    )
    for k, data in all_items:
        with s3.open("/".join([bucket_names["test"], k]), "rb") as f:
            result = f.readline()
            expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


def test_readline_empty(s3, file_a):
    data = b""
    with s3.open(file_a, "wb") as f:
        f.write(data)
    with s3.open(file_a, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(s3, file_a):
    data = b"ab\n" + b"a" * (10 * 2 ** 20) + b"\nab"
    with s3.open(file_a, "wb") as f:
        f.write(data)
    with s3.open(file_a, "rb") as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (10 * 2 ** 20) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(s3, bucket_names, s3_files):
    expected = s3_files["csv"]["2014-01-01.csv"].split(b"\n")[0] + b"\n"
    with s3.open(bucket_names["test"] + "/2014-01-01.csv") as f:
        result = next(f)
        assert result == expected


def test_iterable(s3, file_a):
    data = b"abc\n123"
    with s3.open(file_a, "wb") as f:
        f.write(data)
    with s3.open(file_a) as f, io.BytesIO(data) as g:
        for froms3, fromio in zip(f, g):
            assert froms3 == fromio
        f.seek(0)
        assert f.readline() == b"abc\n"
        assert f.readline() == b"123"
        f.seek(1)
        assert f.readline() == b"bc\n"

    with s3.open(file_a) as f:
        out = list(f)
    with s3.open(file_a) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_readable(s3, file_a):
    with s3.open(file_a, "wb") as f:
        assert not f.readable()

    with s3.open(file_a, "rb") as f:
        assert f.readable()


def test_seekable(s3, file_a):
    with s3.open(file_a, "wb") as f:
        assert not f.seekable()

    with s3.open(file_a, "rb") as f:
        assert f.seekable()


def test_writable(s3, file_a):
    with s3.open(file_a, "wb") as f:
        assert f.writable()

    with s3.open(file_a, "rb") as f:
        assert not f.writable()


def test_merge(s3, bucket_names, file_a, file_b):
    with s3.open(file_a, "wb") as f:
        f.write(b"a" * 10 * 2 ** 20)

    with s3.open(file_b, "wb") as f:
        f.write(b"a" * 10 * 2 ** 20)
    s3.merge(bucket_names["test"] + "/joined", [file_a, file_b])
    assert s3.info(bucket_names["test"] + "/joined")["Size"] == 2 * 10 * 2 ** 20


def test_append(s3, bucket_names, s3_files, file_a):
    data = s3_files["text"]["nested/file1"]
    with s3.open(bucket_names["test"] + "/nested/file1", "ab") as f:
        assert f.tell() == len(data)  # append, no write, small file
    assert s3.cat(bucket_names["test"] + "/nested/file1") == data
    with s3.open(bucket_names["test"] + "/nested/file1", "ab") as f:
        f.write(b"extra")  # append, write, small file
    assert s3.cat(bucket_names["test"] + "/nested/file1") == data + b"extra"

    with s3.open(file_a, "wb") as f:
        f.write(b"a" * 10 * 2 ** 20)
    with s3.open(file_a, "ab") as f:
        pass  # append, no write, big file
    assert s3.cat(file_a) == b"a" * 10 * 2 ** 20

    with s3.open(file_a, "ab") as f:
        assert f.parts is None
        f._initiate_upload()
        assert f.parts
        assert f.tell() == 10 * 2 ** 20
        f.write(b"extra")  # append, small write, big file
    assert s3.cat(file_a) == b"a" * 10 * 2 ** 20 + b"extra"

    with s3.open(file_a, "ab") as f:
        assert f.tell() == 10 * 2 ** 20 + 5
        f.write(b"b" * 10 * 2 ** 20)  # append, big write, big file
        assert f.tell() == 20 * 2 ** 20 + 5
    assert s3.cat(file_a) == b"a" * 10 * 2 ** 20 + b"extra" + b"b" * 10 * 2 ** 20

    # Keep Head Metadata
    head = dict(
        CacheControl="public",
        ContentDisposition="string",
        ContentEncoding="gzip",
        ContentLanguage="ru-RU",
        ContentType="text/csv",
        Expires=datetime.datetime(2015, 1, 1, 0, 0, tzinfo=tzutc()),
        Metadata={"string": "string"},
        ServerSideEncryption="AES256",
        StorageClass="REDUCED_REDUNDANCY",
        WebsiteRedirectLocation="https://www.example.com/",
        BucketKeyEnabled=False,
    )
    with s3.open(file_a, "wb", **head) as f:
        f.write(b"data")

    with s3.open(file_a, "ab") as f:
        f.write(b"other")

    with s3.open(file_a) as f:
        filehead = {
            k: v
            for k, v in f._call_s3(
                "head_object", f.kwargs, Bucket=f.bucket, Key=f.key
            ).items()
            if k in head
        }
        assert filehead == head


def test_bigger_than_block_read(s3, bucket_names, s3_files):
    with s3.open(bucket_names["test"] + "/2014-01-01.csv", "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(20)
            out.append(data)
            if len(data) == 0:
                break
    assert b"".join(out) == s3_files["csv"]["2014-01-01.csv"]


def test_current(s3, endpoint_uri):
    s3._cache.clear()
    s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.current() is s3
    assert S3FileSystem.current() is s3


def test_array(s3, file_a):
    from array import array

    data = array("B", [65] * 1000)

    with s3.open(file_a, "wb") as f:
        f.write(data)

    with s3.open(file_a, "rb") as f:
        out = f.read()
        assert out == b"A" * 1000


def _get_s3_id(s3):
    return id(s3.s3)


@pytest.mark.skipif(sys.version_info[:2] < (3, 7), reason="ctx method only >py37")
@pytest.mark.parametrize(
    "method",
    [
        "spawn",
        pytest.param(
            "forkserver",
            marks=pytest.mark.skipif(
                sys.platform.startswith("win"),
                reason="'forserver' not available on windows",
            ),
        ),
    ],
)
def test_no_connection_sharing_among_processes(s3, method):
    import multiprocessing as mp

    ctx = mp.get_context(method)
    executor = ProcessPoolExecutor(mp_context=ctx)
    conn_id = executor.submit(_get_s3_id, s3).result()
    assert id(s3.connect()) != conn_id, "Processes should not share S3 connections."


@pytest.mark.xfail()
def test_public_file(s3, endpoint_uri, bucket_names):
    # works on real s3, not on moto
    bucket_names["test"] = "s3fs_public_test"
    other_bucket_name = "s3fs_private_test"

    s3.touch(bucket_names["test"])
    s3.touch(bucket_names["test"] + "/afile")
    s3.touch(other_bucket_name, acl="public-read")
    s3.touch(other_bucket_name + "/afile", acl="public-read")

    s = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    with pytest.raises(PermissionError):
        s.ls(bucket_names["test"])
    s.ls(other_bucket_name)

    s3.chmod(bucket_names["test"], acl="public-read")
    s3.chmod(other_bucket_name, acl="private")
    with pytest.raises(PermissionError):
        s.ls(other_bucket_name, refresh=True)
    assert s.ls(bucket_names["test"], refresh=True)

    # public file in private bucket
    with s3.open(other_bucket_name + "/see_me", "wb", acl="public-read") as f:
        f.write(b"hello")
    assert s.cat(other_bucket_name + "/see_me") == b"hello"


def test_upload_with_s3fs_prefix(s3, bucket_names):
    path = f"s3://{bucket_names['test']}/prefix/key"

    with s3.open(path, "wb") as f:
        f.write(b"a" * (10 * 2 ** 20))

    with s3.open(path, "ab") as f:
        f.write(b"b" * (10 * 2 ** 20))


def test_multipart_upload_blocksize(s3, file_a):
    blocksize = 5 * (2 ** 20)
    expected_parts = 3

    s3f = s3.open(file_a, "wb", block_size=blocksize)
    for _ in range(3):
        data = b"b" * blocksize
        s3f.write(data)

    # Ensure that the multipart upload consists of only 3 parts
    assert len(s3f.parts) == expected_parts
    s3f.close()


def test_default_pars(s3, endpoint_uri, bucket_names, s3_files):
    s3 = S3FileSystem(
        default_block_size=20,
        default_fill_cache=False,
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    fn = bucket_names["test"] + "/" + list(s3_files["files"])[0]
    with s3.open(fn) as f:
        assert f.blocksize == 20
        assert f.fill_cache is False
    with s3.open(fn, block_size=40, fill_cache=True) as f:
        assert f.blocksize == 40
        assert f.fill_cache is True


def test_tags(s3, s3_files, bucket_names):
    tagset = {"tag1": "value1", "tag2": "value2"}
    fname = bucket_names["test"] + "/" + list(s3_files["files"])[0]
    s3.touch(fname)
    s3.put_tags(fname, tagset)
    assert s3.get_tags(fname) == tagset

    # Ensure merge mode updates value of existing key and adds new one
    new_tagset = {"tag2": "updatedvalue2", "tag3": "value3"}
    s3.put_tags(fname, new_tagset, mode="m")
    tagset.update(new_tagset)
    assert s3.get_tags(fname) == tagset


def test_versions(s3, endpoint_uri, bucket_names):
    versioned_file = bucket_names["versioned"] + "/versioned_file"
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"1")
    first_version = fo.version_id

    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"2")
    second_version = fo.version_id

    assert s3.isfile(versioned_file)
    versions = s3.object_version_info(versioned_file)
    assert len(versions) == 2
    assert {version["VersionId"] for version in versions} == {
        first_version,
        second_version,
    }

    with s3.open(versioned_file) as fo:
        assert fo.version_id == second_version
        assert fo.read() == b"2"

    with s3.open(versioned_file, version_id=first_version) as fo:
        assert fo.version_id == first_version
        assert fo.read() == b"1"


def test_list_versions_many(s3, endpoint_uri, bucket_names):
    # moto doesn't actually behave in the same way that s3 does here so this doesn't test
    # anything really in moto 1.2
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    versioned_file = bucket_names["versioned"] + "/versioned_file2"
    for i in range(1200):
        with s3.open(versioned_file, "wb") as fo:
            fo.write(b"1")
    versions = s3.object_version_info(versioned_file)
    assert len(versions) == 1200


def test_fsspec_versions_multiple(s3, endpoint_uri, bucket_names):
    """Test that the standard fsspec.core.get_fs_token_paths behaves as expected for versionId urls"""
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    versioned_file = bucket_names["versioned"] + "/versioned_file3"
    version_lookup = {}
    for i in range(20):
        contents = str(i).encode()
        with s3.open(versioned_file, "wb") as fo:
            fo.write(contents)
        version_lookup[fo.version_id] = contents
    urls = [
        "s3://{}?versionId={}".format(versioned_file, version)
        for version in version_lookup.keys()
    ]
    fs, token, paths = fsspec.core.get_fs_token_paths(
        urls, storage_options=dict(client_kwargs={"endpoint_url": endpoint_uri})
    )
    assert isinstance(fs, S3FileSystem)
    assert fs.version_aware
    for path in paths:
        with fs.open(path, "rb") as fo:
            contents = fo.read()
            assert contents == version_lookup[fo.version_id]


def test_versioned_file_fullpath(s3, endpoint_uri, bucket_names):
    versioned_file = bucket_names["versioned"] + "/versioned_file_fullpath"
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"1")
    # moto doesn't correctly return a versionId for a multipart upload. So we resort to this.
    # version_id = fo.version_id
    versions = s3.object_version_info(versioned_file)
    version_ids = [version["VersionId"] for version in versions]
    version_id = version_ids[0]

    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"2")

    file_with_version = "{}?versionId={}".format(versioned_file, version_id)

    with s3.open(file_with_version, "rb") as fo:
        assert fo.version_id == version_id
        assert fo.read() == b"1"


def test_versions_unaware(s3, endpoint_uri, bucket_names):
    versioned_file = bucket_names["versioned"] + "/versioned_file3"
    s3 = S3FileSystem(
        anon=False, version_aware=False, client_kwargs={"endpoint_url": endpoint_uri}
    )
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"1")
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"2")

    with s3.open(versioned_file) as fo:
        assert fo.version_id is None
        assert fo.read() == b"2"

    with pytest.raises(ValueError):
        with s3.open(versioned_file, version_id="0"):
            fo.read()


def test_text_io__stream_wrapper_works(s3):
    """Ensure using TextIOWrapper works."""
    s3.mkdir("bucket")

    with s3.open("bucket/file.txt", "wb") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af".encode("utf-16-le"))

    with s3.open("bucket/file.txt", "rb") as fd:
        with io.TextIOWrapper(fd, "utf-16-le") as stream:
            assert stream.readline() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__basic(s3):
    """Text mode is now allowed."""
    s3.mkdir("bucket")

    with s3.open("bucket/file.txt", "w", encoding="utf-8") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af")

    with s3.open("bucket/file.txt", "r", encoding="utf-8") as fd:
        assert fd.read() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__override_encoding(s3):
    """Allow overriding the default text encoding."""
    s3.mkdir("bucket")

    with s3.open("bucket/file.txt", "w", encoding="ibm500") as fd:
        fd.write("Hello, World!")

    with s3.open("bucket/file.txt", "r", encoding="ibm500") as fd:
        assert fd.read() == "Hello, World!"


def test_readinto(s3):
    s3.mkdir("bucket")

    with s3.open("bucket/file.txt", "wb") as fd:
        fd.write(b"Hello, World!")

    contents = bytearray(15)

    with s3.open("bucket/file.txt", "rb") as fd:
        assert fd.readinto(contents) == 13

    assert contents.startswith(b"Hello, World!")


def test_change_defaults_only_subsequent(endpoint_uri):
    """Test for Issue #135

    Ensure that changing the default block size doesn't affect existing file
    systems that were created using that default. It should only affect file
    systems created after the change.
    """
    try:
        S3FileSystem.cachable = False  # don't reuse instances with same pars

        fs_default = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
        assert fs_default.default_block_size == 5 * (1024 ** 2)

        fs_overridden = S3FileSystem(
            default_block_size=64 * (1024 ** 2),
            client_kwargs={"endpoint_url": endpoint_uri},
        )
        assert fs_overridden.default_block_size == 64 * (1024 ** 2)

        # Suppose I want all subsequent file systems to have a block size of 1 GiB
        # instead of 5 MiB:
        S3FileSystem.default_block_size = 1024 ** 3

        fs_big = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
        assert fs_big.default_block_size == 1024 ** 3

        # Test the other file systems created to see if their block sizes changed
        assert fs_overridden.default_block_size == 64 * (1024 ** 2)
        assert fs_default.default_block_size == 5 * (1024 ** 2)
    finally:
        S3FileSystem.default_block_size = 5 * (1024 ** 2)
        S3FileSystem.cachable = True


def test_cache_after_copy(s3, bucket_names):
    # https://github.com/dask/dask/issues/5134
    s3.touch(bucket_names["test"] + "/afile")
    assert bucket_names["test"] + "/afile" in s3.ls(
        "s3://" + bucket_names["test"], False
    )
    s3.cp(bucket_names["test"] + "/afile", bucket_names["test"] + "/bfile")
    assert bucket_names["test"] + "/bfile" in s3.ls(
        "s3://" + bucket_names["test"], False
    )


def test_autocommit(s3, endpoint_uri, bucket_names):
    auto_file = bucket_names["test"] + "/auto_file"
    committed_file = bucket_names["test"] + "/commit_file"
    aborted_file = bucket_names["test"] + "/aborted_file"
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )

    def write_and_flush(path, autocommit):
        with s3.open(path, "wb", autocommit=autocommit) as fo:
            fo.write(b"1")
        return fo

    # regular behavior
    fo = write_and_flush(auto_file, autocommit=True)
    assert fo.autocommit
    assert s3.exists(auto_file)

    fo = write_and_flush(committed_file, autocommit=False)
    assert not fo.autocommit
    assert not s3.exists(committed_file)
    fo.commit()
    assert s3.exists(committed_file)

    fo = write_and_flush(aborted_file, autocommit=False)
    assert not s3.exists(aborted_file)
    fo.discard()
    assert not s3.exists(aborted_file)
    # Cannot commit a file that was discarded
    with pytest.raises(Exception):
        fo.commit()


def test_autocommit_mpu(s3, bucket_names):
    """When not autocommitting we always want to use multipart uploads"""
    path = bucket_names["test"] + "/auto_commit_with_mpu"
    with s3.open(path, "wb", autocommit=False) as fo:
        fo.write(b"1")
    assert fo.mpu is not None
    assert len(fo.parts) == 1


def test_touch(s3, bucket_names):
    # create
    fn = bucket_names["test"] + "/touched"
    assert not s3.exists(fn)
    s3.touch(fn)
    assert s3.exists(fn)
    assert s3.size(fn) == 0

    # truncates
    with s3.open(fn, "wb") as f:
        f.write(b"data")
    assert s3.size(fn) == 4
    s3.touch(fn, truncate=True)
    assert s3.size(fn) == 0

    # exists error
    with s3.open(fn, "wb") as f:
        f.write(b"data")
    assert s3.size(fn) == 4
    with pytest.raises(ValueError):
        s3.touch(fn, truncate=False)
    assert s3.size(fn) == 4


def test_touch_versions(s3, endpoint_uri, bucket_names):
    versioned_file = bucket_names["versioned"] + "/versioned_file"
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )

    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"1")
    first_version = fo.version_id
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"")
    second_version = fo.version_id

    assert s3.isfile(versioned_file)
    versions = s3.object_version_info(versioned_file)
    assert len(versions) == 2
    assert {version["VersionId"] for version in versions} == {
        first_version,
        second_version,
    }

    with s3.open(versioned_file) as fo:
        assert fo.version_id == second_version
        assert fo.read() == b""

    with s3.open(versioned_file, version_id=first_version) as fo:
        assert fo.version_id == first_version
        assert fo.read() == b"1"


def test_cat_missing(s3, bucket_names):
    fn0 = bucket_names["test"] + "/file0"
    fn1 = bucket_names["test"] + "/file1"
    s3.touch(fn0)
    with pytest.raises(FileNotFoundError):
        s3.cat([fn0, fn1], on_error="raise")
    out = s3.cat([fn0, fn1], on_error="omit")
    assert list(out) == [fn0]
    out = s3.cat([fn0, fn1], on_error="return")
    assert fn1 in out
    assert isinstance(out[fn1], FileNotFoundError)


def test_get_directories(s3, tmpdir, bucket_names):
    s3.touch(bucket_names["test"] + "/dir/dirkey/key0")
    s3.touch(bucket_names["test"] + "/dir/dirkey/key1")
    s3.touch(bucket_names["test"] + "/dir/dirkey")
    s3.touch(bucket_names["test"] + "/dir/dir/key")
    d = str(tmpdir)
    s3.get(bucket_names["test"] + "/dir", d, recursive=True)
    assert {"dirkey", "dir"} == set(os.listdir(d))
    assert ["key"] == os.listdir(os.path.join(d, "dir"))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dirkey")))


def test_seek_reads(s3, bucket_names):
    fn = bucket_names["test"] + "/myfile"
    with s3.open(fn, "wb") as f:
        f.write(b"a" * 175627146)
    with s3.open(fn, "rb", blocksize=100) as f:
        f.seek(175561610)
        d1 = f.read(65536)

        f.seek(4)
        size = 17562198
        d2 = f.read(size)
        assert len(d2) == size

        f.seek(17562288)
        size = 17562187
        d3 = f.read(size)
        assert len(d3) == size


def test_connect_many(s3, endpoint_uri):
    from multiprocessing.pool import ThreadPool

    def task(i):
        S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri}).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


def test_requester_pays(s3, endpoint_uri, bucket_names):
    fn = bucket_names["test"] + "/myfile"
    s3 = S3FileSystem(requester_pays=True, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.req_kw["RequestPayer"] == "requester"
    s3.touch(fn)
    with s3.open(fn, "rb") as f:
        assert f.req_kw["RequestPayer"] == "requester"


def test_credentials(endpoint_uri):
    s3 = S3FileSystem(
        key="foo", secret="foo", client_kwargs={"endpoint_url": endpoint_uri}
    )
    assert s3.s3._request_signer._credentials.access_key == "foo"
    assert s3.s3._request_signer._credentials.secret_key == "foo"
    s3 = S3FileSystem(
        client_kwargs={
            "aws_access_key_id": "bar",
            "aws_secret_access_key": "bar",
            "endpoint_url": endpoint_uri,
        }
    )
    assert s3.s3._request_signer._credentials.access_key == "bar"
    assert s3.s3._request_signer._credentials.secret_key == "bar"
    s3 = S3FileSystem(
        key="foo",
        client_kwargs={"aws_secret_access_key": "bar", "endpoint_url": endpoint_uri},
    )
    assert s3.s3._request_signer._credentials.access_key == "foo"
    assert s3.s3._request_signer._credentials.secret_key == "bar"
    s3 = S3FileSystem(
        key="foobar",
        secret="foobar",
        client_kwargs={
            "aws_access_key_id": "foobar",
            "aws_secret_access_key": "foobar",
            "endpoint_url": endpoint_uri,
        },
    )
    assert s3.s3._request_signer._credentials.access_key == "foobar"
    assert s3.s3._request_signer._credentials.secret_key == "foobar"
    with pytest.raises((TypeError, KeyError)):
        # should be TypeError: arg passed twice; but in moto can be KeyError
        S3FileSystem(
            key="foo",
            secret="foo",
            client_kwargs={
                "aws_access_key_id": "bar",
                "aws_secret_access_key": "bar",
                "endpoint_url": endpoint_uri,
            },
        ).s3


def test_modified(s3, bucket_names):
    dir_path = bucket_names["test"] + "/modified"
    file_path = dir_path + "/file"

    # Test file
    s3.touch(file_path)
    modified = s3.modified(path=file_path)
    assert isinstance(modified, datetime.datetime)

    # Test directory
    with pytest.raises(IsADirectoryError):
        modified = s3.modified(path=dir_path)

    # Test bucket
    with pytest.raises(IsADirectoryError):
        s3.modified(path=bucket_names["test"])


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_async_s3(s3, endpoint_uri, bucket_names):
    async def _():
        s3 = S3FileSystem(
            anon=False,
            asynchronous=True,
            loop=asyncio.get_running_loop(),
            client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
        )

        fn = bucket_names["test"] + "/nested/file1"
        data = b"hello\n"

        # Is good with or without connect()
        await s3._cat_file(fn)

        session = await s3.set_session()  # creates client

        assert await s3._cat_file(fn) == data

        assert await s3._cat_file(fn, start=0, end=3) == data[:3]

        # TODO: file IO is *not* async
        # with s3.open(fn, "rb") as f:
        #     assert f.read() == data

        try:
            await session.close()
        except AttributeError:
            # bug in aiobotocore 1.4.1
            await session._endpoint.http_session._session.close()

    asyncio.run(_())


def test_cat_ranges(s3, bucket_names):
    data = b"a string to select from"
    fn = bucket_names["test"] + "/parts"
    s3.pipe(fn, data)

    assert s3.cat_file(fn) == data
    assert s3.cat_file(fn, start=5) == data[5:]
    assert s3.cat_file(fn, end=5) == data[:5]
    assert s3.cat_file(fn, start=1, end=-1) == data[1:-1]
    assert s3.cat_file(fn, start=-5) == data[-5:]


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_async_s3_old(s3, endpoint_uri, bucket_names):
    async def _():
        s3 = S3FileSystem(
            anon=False,
            asynchronous=True,
            loop=asyncio.get_running_loop(),
            client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
        )

        fn = bucket_names["test"] + "/nested/file1"
        data = b"hello\n"

        # Check old API
        session = await s3._connect()
        assert await s3._cat_file(fn, start=0, end=3) == data[:3]
        try:
            await session.close()
        except AttributeError:
            # bug in aiobotocore 1.4.1
            await session._endpoint.http_session._session.close()

    asyncio.run(_())


def test_via_fsspec(s3):
    import fsspec

    s3.mkdir("mine")
    with fsspec.open("mine/oi", "wb") as f:
        f.write(b"hello")
    with fsspec.open("mine/oi", "rb") as f:
        assert f.read() == b"hello"


def test_repeat_exists(s3, bucket_names):
    fn = "s3://" + bucket_names["test"] + "/file1"
    s3.touch(fn)

    assert s3.exists(fn)
    assert s3.exists(fn)


def test_with_xzarr(s3, bucket_names):
    da = pytest.importorskip("dask.array")
    xr = pytest.importorskip("xarray")
    name = "sample"

    nana = xr.DataArray(da.random.random((1024, 1024, 10, 9, 1)))

    s3_path = f"{bucket_names['test']}/{name}"
    s3store = s3.get_mapper(s3_path)

    s3.ls("")
    nana.to_dataset().to_zarr(store=s3store, mode="w", consolidated=True, compute=True)


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_async_close(bucket_names):
    async def _():
        loop = asyncio.get_event_loop()
        s3 = S3FileSystem(anon=False, asynchronous=True, loop=loop)
        await s3._connect()

        fn = bucket_names["test"] + "/afile"

        async def async_wrapper():
            coros = [
                asyncio.ensure_future(s3._get_file(fn, "/nonexistent/a/b/c"), loop=loop)
                for _ in range(3)
            ]
            completed, pending = await asyncio.wait(coros)
            for future in completed:
                with pytest.raises(OSError):
                    future.result()

        await asyncio.gather(*[async_wrapper() for __ in range(2)])
        try:
            await s3._s3.close()
        except AttributeError:
            # bug in aiobotocore 1.4.1
            await s3._s3._endpoint.http_session._session.close()

    asyncio.run(_())


def test_put_single(s3, tmpdir, bucket_names):
    fn = os.path.join(str(tmpdir), "dir")
    os.mkdir(fn)
    open(os.path.join(fn, "abc"), "w").write("text")
    s3.put(fn + "/", bucket_names["test"])  # no-op, no files
    assert not s3.exists(bucket_names["test"] + "/abc")
    assert not s3.exists(bucket_names["test"] + "/dir")
    s3.put(fn + "/", bucket_names["test"], recursive=True)  # no-op, no files
    assert s3.cat(bucket_names["test"] + "/dir/abc") == b"text"


def test_shallow_find(s3, bucket_names):
    """Test that find method respects maxdepth.

    Verify that the ``find`` method respects the ``maxdepth`` parameter.  With
    ``maxdepth=1``, the results of ``find`` should be the same as those of
    ``ls``, without returning subdirectories.  See also issue 378.
    """

    assert s3.ls(bucket_names["test"]) == s3.find(
        bucket_names["test"], maxdepth=1, withdirs=True
    )
    assert s3.ls(bucket_names["test"]) == s3.glob(bucket_names["test"] + "/*")


def test_version_sizes(s3, endpoint_uri, bucket_names):
    # protect against caching of incorrect version details
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    import gzip

    path = f"s3://{bucket_names['versioned']}/test.txt.gz"
    versions = [
        s3.pipe_file(path, gzip.compress(text))
        for text in (
            b"good morning!",
            b"hello!",
            b"hi!",
            b"hello!",
        )
    ]
    for version in versions:
        version_id = version["VersionId"]
        with s3.open(path, version_id=version_id) as f:
            with gzip.GzipFile(fileobj=f) as zfp:
                zfp.read()


def test_find_no_side_effect(s3, bucket_names):
    infos1 = s3.find(bucket_names["test"], maxdepth=1, withdirs=True, detail=True)
    s3.find(bucket_names["test"], maxdepth=None, withdirs=True, detail=True)
    infos3 = s3.find(bucket_names["test"], maxdepth=1, withdirs=True, detail=True)
    assert infos1.keys() == infos3.keys()


def test_get_file_info_with_selector(s3):
    fs = s3
    base_dir = "selector-dir/"
    file_a = "selector-dir/test_file_a"
    file_b = "selector-dir/test_file_b"
    dir_a = "selector-dir/test_dir_a"
    file_c = "selector-dir/test_dir_a/test_file_c"

    try:
        fs.mkdir(base_dir)
        with fs.open(file_a, mode="wb"):
            pass
        with fs.open(file_b, mode="wb"):
            pass
        fs.mkdir(dir_a)
        with fs.open(file_c, mode="wb"):
            pass

        infos = fs.find(base_dir, maxdepth=None, withdirs=True, detail=True)
        assert len(infos) == 5  # includes base_dir directory

        for info in infos.values():
            if info["name"].endswith(file_a):
                assert info["type"] == "file"
            elif info["name"].endswith(file_b):
                assert info["type"] == "file"
            elif info["name"].endswith(file_c):
                assert info["type"] == "file"
            elif info["name"].rstrip("/").endswith(dir_a):
                assert info["type"] == "directory"
    finally:
        fs.rm(base_dir, recursive=True)


@pytest.mark.xfail(
    condition=version.parse(moto.__version__) <= version.parse("1.3.16"),
    reason="Moto 1.3.16 is not supporting pre-conditions.",
)
def test_raise_exception_when_file_has_changed_during_reading(
    s3, bucket_names, boto3_client
):
    test_file_name = "file1"
    test_file = "s3://" + bucket_names["test"] + "/" + test_file_name
    content1 = b"123"
    content2 = b"ABCDEFG"

    def create_file(content: bytes):
        boto3_client.put_object(
            Bucket=bucket_names["test"], Key=test_file_name, Body=content
        )

    create_file(b"123")

    with s3.open(test_file, "rb") as f:
        content = f.read()
        assert content == content1

    with s3.open(test_file, "rb") as f:
        create_file(content2)
        with expect_errno(errno.EBUSY):
            f.read()


def test_s3fs_etag_preserving_multipart_copy(monkeypatch, s3, bucket_names):
    # Set this to a lower value so that we can actually
    # test this without creating giant objects in memory
    monkeypatch.setattr(s3fs.core, "MANAGED_COPY_THRESHOLD", 5 * 2 ** 20)

    test_file1 = bucket_names["test"] + "/test/multipart-upload.txt"
    test_file2 = bucket_names["test"] + "/test/multipart-upload-copy.txt"

    with s3.open(test_file1, "wb", block_size=5 * 2 ** 21) as stream:
        for _ in range(5):
            stream.write(b"b" * (stream.blocksize + random.randrange(200)))

    file_1 = s3.info(test_file1)

    s3.copy(test_file1, test_file2)
    file_2 = s3.info(test_file2)
    s3.rm(test_file2)

    # normal copy() uses a block size of 5GB
    assert file_1["ETag"] != file_2["ETag"]

    s3.copy(test_file1, test_file2, preserve_etag=True)
    file_2 = s3.info(test_file2)
    s3.rm(test_file2)

    # etag preserving copy() determines each part size for the destination
    # by checking out the matching part's size on the source
    assert file_1["ETag"] == file_2["ETag"]

    s3.rm(test_file1)


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_sync_from_wihin_async(s3, endpoint_uri, bucket_names):
    # if treating as sync but within an even loop, e.g., calling from jupyter;
    # IO happens on dedicated thread.
    async def f():
        S3FileSystem.clear_instance_cache()
        s3 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
        assert s3.ls(bucket_names["test"])

    asyncio.run(f())


def test_token_paths(s3, endpoint_uri, bucket_names):
    fs, tok, files = fsspec.get_fs_token_paths(
        "s3://" + bucket_names["test"] + "/*.csv",
        storage_options={"client_kwargs": {"endpoint_url": endpoint_uri}},
    )
    assert files


def test_same_name_but_no_exact(s3, bucket_names):
    s3.touch(bucket_names["test"] + "/very/similiar/prefix1")
    s3.touch(bucket_names["test"] + "/very/similiar/prefix2")
    s3.touch(bucket_names["test"] + "/very/similiar/prefix3/something")
    assert not s3.exists(bucket_names["test"] + "/very/similiar/prefix")
    assert not s3.exists(bucket_names["test"] + "/very/similiar/prefi")
    assert not s3.exists(bucket_names["test"] + "/very/similiar/pref")

    assert s3.exists(test_bucket_name + "/very/similiar/")
    assert s3.exists(test_bucket_name + "/very/similiar/prefix1")
    assert s3.exists(test_bucket_name + "/very/similiar/prefix2")
    assert s3.exists(test_bucket_name + "/very/similiar/prefix3")
    assert s3.exists(test_bucket_name + "/very/similiar/prefix3/")
    assert s3.exists(test_bucket_name + "/very/similiar/prefix3/something")

    assert not s3.exists(test_bucket_name + "/very/similiar/prefix3/some")

    s3.touch(test_bucket_name + "/starting/very/similiar/prefix")

    assert s3.exists(bucket_names["test"] + "/very/similar/")
    assert s3.exists(bucket_names["test"] + "/very/similar/prefix1")
    assert s3.exists(bucket_names["test"] + "/very/similar/prefix2")
    assert s3.exists(bucket_names["test"] + "/very/similar/prefix3")
    assert s3.exists(bucket_names["test"] + "/very/similar/prefix3/")
    assert s3.exists(bucket_names["test"] + "/very/similar/prefix3/something")

    assert s3.exists(test_bucket_name + "/starting/very/similiar/prefix")
    assert s3.exists(bucket_names["test"] + "/starting/very/similiar/prefix/")

    s3.touch(bucket_names["test"] + "/starting/very/similar/prefix")

    assert not s3.exists(bucket_names["test"] + "/starting/very/similar/prefix1")
    assert not s3.exists(bucket_names["test"] + "/starting/very/similar/prefix2")
    assert not s3.exists(bucket_names["test"] + "/starting/very/similar/prefix3")
    assert not s3.exists(bucket_names["test"] + "/starting/very/similar/prefix3/")
    assert not s3.exists(
        bucket_names["test"] + "/starting/very/similar/prefix3/something"
    )

    async def list_objects_v2(*args, **kwargs):
        if kwargs.pop("Prefix").endswith("/"):
            return {}
        else:
            raise PermissionError

    monkeypatch.setattr(type(s3.s3), "list_objects_v2", list_objects_v2)
    assert not s3.exists(bucket_names["test"] + "/very/similiar/prefix1")
    assert s3.exists(bucket_names["test"] + "/very/similiar/prefix")
    assert s3.info(test_bucket_name + "/very/similiar/prefix")["type"] == "file"


def test_info_with_permission_error_for_list_objects(monkeypatch, s3, bucket_names):
    s3.touch(bucket_names['test'] + "/very/similiar/prefix")

    async def list_objects_v2(*args, **kwargs):
        if kwargs.pop("Prefix").endswith("/"):
            return {}
        else:
            raise PermissionError

    monkeypatch.setattr(type(s3.s3), "list_objects_v2", list_objects_v2)
    assert not s3.exists(bucket_names['test'] + "/very/similiar/prefix1")
    assert s3.exists(bucket_names['test'] + "/very/similiar/prefix")
    assert s3.info(bucket_names['test'] + "/very/similiar/prefix")["type"] == "file"


def test_leading_forward_slash(s3, bucket_names):
    s3.touch(bucket_names["test"] + "/some/file")
    assert s3.ls(bucket_names["test"] + "/some/")
    assert s3.exists(bucket_names["test"] + "/some/file")
    assert s3.exists("s3://" + bucket_names["test"] + "/some/file")


def test_lsdir(s3, bucket_names):
    # https://github.com/dask/s3fs/issues/475
    s3.find(bucket_names["test"])

    d = bucket_names["test"] + "/test"
    assert d in s3.ls(bucket_names["test"])


def test_copy_file_without_etag(s3, monkeypatch, bucket_names):

    s3.touch(bucket_names["test"] + "/copy_tests/file")
    s3.ls(bucket_names["test"] + "/copy_tests/")

    [file] = s3.dircache[bucket_names["test"] + "/copy_tests"]

    assert file["name"] == bucket_names["test"] + "/copy_tests/file"
    file.pop("ETag")

    assert s3.info(file["name"]).get("ETag", None) is None

    s3.cp_file(file["name"], bucket_names["test"] + "/copy_tests/file2")
    assert s3.info(bucket_names["test"] + "/copy_tests/file2")["ETag"] is not None


def test_find_with_prefix(s3, bucket_names):
    for cursor in range(100):
        s3.touch(bucket_names["test"] + f"/prefixes/test_{cursor}")

    s3.touch(bucket_names["test"] + "/prefixes2")
    assert len(s3.find(bucket_names["test"] + "/prefixes")) == 100
    assert len(s3.find(bucket_names["test"], prefix="prefixes")) == 101

    assert len(s3.find(bucket_names["test"] + "/prefixes/test_")) == 0
    assert len(s3.find(bucket_names["test"] + "/prefixes", prefix="test_")) == 100
    assert len(s3.find(bucket_names["test"] + "/prefixes/", prefix="test_")) == 100

    test_1s = s3.find(bucket_names["test"] + "/prefixes/test_1")
    assert len(test_1s) == 1
    assert test_1s[0] == bucket_names["test"] + "/prefixes/test_1"

    test_1s = s3.find(bucket_names["test"] + "/prefixes/", prefix="test_1")
    assert len(test_1s) == 11
    assert test_1s == [bucket_names["test"] + "/prefixes/test_1"] + [
        bucket_names["test"] + f"/prefixes/test_{cursor}" for cursor in range(10, 20)
    ]
    assert s3.find(bucket_names["test"] + "/prefixes/") == s3.find(
        bucket_names["test"] + "/prefixes/", prefix=None
    )


def test_list_after_find(s3, bucket_names):
    before = s3.ls(f"s3://{bucket_names['test']}")
    s3.invalidate_cache(f"s3://{bucket_names['test']}/2014-01-01.csv")
    s3.find(f"s3://{bucket_names['test']}/2014-01-01.csv")
    after = s3.ls(f"s3://{bucket_names['test']}")
    assert before == after


def test_upload_recursive_to_bucket(s3, tmpdir):
    # GH#491
    folders = [os.path.join(tmpdir, d) for d in ["outer", "outer/inner"]]
    files = [os.path.join(tmpdir, f) for f in ["outer/afile", "outer/inner/bfile"]]
    for d in folders:
        os.mkdir(d)
    for f in files:
        open(f, "w").write("hello")
    s3.put(folders[0], "newbucket", recursive=True)


def test_rm_file(s3, bucket_names):
    target = bucket_names["test"] + "/to_be_removed/file"
    s3.touch(target)
    s3.rm_file(target)
    assert not s3.exists(target)
    assert not s3.exists(bucket_names["test"] + "/to_be_removed")


def test_exists_isdir(s3):
    bad_path = "s3://nyc-tlc-asdfasdf/trip data/"
    assert not s3.exists(bad_path)
    assert not s3.isdir(bad_path)
