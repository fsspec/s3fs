# -*- coding: utf-8 -*-
import asyncio
import errno
import datetime
from contextlib import contextmanager
import json
from concurrent.futures import ProcessPoolExecutor
import io
import os
import random
import requests
import time
import sys
import pytest
import moto
from itertools import chain
import fsspec.core
from dateutil.tz import tzutc

import s3fs.core
from s3fs.core import S3FileSystem
from s3fs.utils import ignoring, SSEParams
from botocore.exceptions import NoCredentialsError
from fsspec.asyn import sync
from fsspec.callbacks import Callback
from packaging import version

test_bucket_name = "test"
secure_bucket_name = "test-secure"
versioned_bucket_name = "test-versioned"
files = {
    "test/accounts.1.json": (
        b'{"amount": 100, "name": "Alice"}\n'
        b'{"amount": 200, "name": "Bob"}\n'
        b'{"amount": 300, "name": "Charlie"}\n'
        b'{"amount": 400, "name": "Dennis"}\n'
    ),
    "test/accounts.2.json": (
        b'{"amount": 500, "name": "Alice"}\n'
        b'{"amount": 600, "name": "Bob"}\n'
        b'{"amount": 700, "name": "Charlie"}\n'
        b'{"amount": 800, "name": "Dennis"}\n'
    ),
}

csv_files = {
    "2014-01-01.csv": (
        b"name,amount,id\n" b"Alice,100,1\n" b"Bob,200,2\n" b"Charlie,300,3\n"
    ),
    "2014-01-02.csv": (b"name,amount,id\n"),
    "2014-01-03.csv": (
        b"name,amount,id\n" b"Dennis,400,4\n" b"Edith,500,5\n" b"Frank,600,6\n"
    ),
}
text_files = {
    "nested/file1": b"hello\n",
    "nested/file2": b"world",
    "nested/nested2/file1": b"hello\n",
    "nested/nested2/file2": b"world",
}
glob_files = {"file.dat": b"", "filexdat": b""}
a = test_bucket_name + "/tmp/test/a"
b = test_bucket_name + "/tmp/test/b"
c = test_bucket_name + "/tmp/test/c"
d = test_bucket_name + "/tmp/test/d"
port = 5555
endpoint_uri = "http://127.0.0.1:%s/" % port


@pytest.fixture()
def s3_base():
    # writable local S3 system
    import shlex
    import subprocess

    try:
        # should fail since we didn't start server yet
        r = requests.get(endpoint_uri)
    except:
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(
        shlex.split("moto_server s3 -p %s" % port),
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )

    timeout = 5
    while timeout > 0:
        try:
            print("polling for moto server")
            r = requests.get(endpoint_uri)
            if r.ok:
                break
        except:
            pass
        timeout -= 0.1
        time.sleep(0.1)
    print("server up")
    yield
    print("moto done")
    proc.terminate()
    proc.wait()


def get_boto3_client():
    from botocore.session import Session

    # NB: we use the sync botocore client for setup
    session = Session()
    return session.create_client("s3", endpoint_url=endpoint_uri)


@pytest.fixture()
def s3(s3_base):
    client = get_boto3_client()
    client.create_bucket(Bucket=test_bucket_name, ACL="public-read")

    client.create_bucket(Bucket=versioned_bucket_name, ACL="public-read")
    client.put_bucket_versioning(
        Bucket=versioned_bucket_name, VersioningConfiguration={"Status": "Enabled"}
    )

    # initialize secure bucket
    client.create_bucket(Bucket=secure_bucket_name, ACL="public-read")
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Id": "PutObjPolicy",
            "Statement": [
                {
                    "Sid": "DenyUnEncryptedObjectUploads",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::{bucket_name}/*".format(
                        bucket_name=secure_bucket_name
                    ),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-server-side-encryption": "aws:kms"
                        }
                    },
                }
            ],
        }
    )
    client.put_bucket_policy(Bucket=secure_bucket_name, Policy=policy)
    for flist in [files, csv_files, text_files, glob_files]:
        for f, data in flist.items():
            client.put_object(Bucket=test_bucket_name, Key=f, Body=data)

    S3FileSystem.clear_instance_cache()
    s3 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
    s3.invalidate_cache()
    yield s3


@contextmanager
def expect_errno(expected_errno):
    """Expect an OSError and validate its errno code."""
    with pytest.raises(OSError) as error:
        yield
    assert error.value.errno == expected_errno, "OSError has wrong error code."


def test_simple(s3):
    data = b"a" * (10 * 2**20)

    with s3.open(a, "wb") as f:
        f.write(data)

    with s3.open(a, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "mmap", "readahead"])
def test_default_cache_type(s3, default_cache_type):
    data = b"a" * (10 * 2**20)
    s3 = S3FileSystem(
        anon=False,
        default_cache_type=default_cache_type,
        client_kwargs={"endpoint_url": endpoint_uri},
    )

    with s3.open(a, "wb") as f:
        f.write(data)

    with s3.open(a, "rb") as f:
        assert isinstance(f.cache, fsspec.core.caches[default_cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_ssl_off():
    s3 = S3FileSystem(use_ssl=False, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.s3.meta.endpoint_url.startswith("http://")


def test_client_kwargs():
    s3 = S3FileSystem(client_kwargs={"endpoint_url": "http://foo"})
    assert s3.s3.meta.endpoint_url.startswith("http://foo")


def test_config_kwargs():
    s3 = S3FileSystem(
        config_kwargs={"signature_version": "s3v4"},
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    assert s3.connect().meta.config.signature_version == "s3v4"


def test_config_kwargs_class_attributes_default():
    s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.connect().meta.config.connect_timeout == 5
    assert s3.connect().meta.config.read_timeout == 15


def test_config_kwargs_class_attributes_override():
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


def test_multiple_objects(s3):
    s3.connect()
    s3.ls("test")
    s32 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
    assert s32.session
    assert s3.ls("test") == s32.ls("test")


def test_info(s3):
    s3.touch(a)
    s3.touch(b)
    info = s3.info(a)
    linfo = s3.ls(a, detail=True)[0]
    assert abs(info.pop("LastModified") - linfo.pop("LastModified")).seconds < 1
    info.pop("VersionId")
    info.pop("ContentType")
    linfo.pop("Key")
    linfo.pop("Size")
    assert info == linfo
    parent = a.rsplit("/", 1)[0]
    s3.invalidate_cache()  # remove full path from the cache
    s3.ls(parent)  # fill the cache with parent dir
    assert s3.info(a) == s3.dircache[parent][0]  # correct value
    assert id(s3.info(a)) == id(s3.dircache[parent][0])  # is object from cache

    new_parent = test_bucket_name + "/foo"
    s3.mkdir(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.ls(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)


def test_info_cached(s3):
    path = test_bucket_name + "/tmp/"
    fqpath = "s3://" + path
    s3.touch(path + "test")
    info = s3.info(fqpath)
    assert info == s3.info(fqpath)
    assert info == s3.info(path)


def test_checksum(s3):
    bucket = test_bucket_name
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


test_xattr_sample_metadata = {"testxattr": "1"}


def test_xattr(s3):
    bucket, key = (test_bucket_name, "tmp/test/xattr")
    filename = bucket + "/" + key
    body = b"aaaa"
    public_read_acl = {
        "Permission": "READ",
        "Grantee": {
            "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
            "Type": "Group",
        },
    }

    resp = sync(
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

    assert s3.getxattr(filename, "testxattr") == test_xattr_sample_metadata["testxattr"]
    assert s3.metadata(filename) == {"testxattr": "1"}  # note _ became -

    s3file = s3.open(filename)
    assert s3file.getxattr("testxattr") == test_xattr_sample_metadata["testxattr"]
    assert s3file.metadata() == {"testxattr": "1"}  # note _ became -

    s3file.setxattr(testxattr="2")
    assert s3file.getxattr("testxattr") == "2"
    s3file.setxattr(**{"testxattr": None})
    assert s3file.metadata() == {}
    assert s3.cat(filename) == body

    # check that ACL and ETag are preserved after updating metadata
    assert (
        public_read_acl
        in sync(s3.loop, s3.s3.get_object_acl, Bucket=bucket, Key=key)["Grants"]
    )
    assert s3.info(filename)["ETag"] == etag


def test_xattr_setxattr_in_write_mode(s3):
    s3file = s3.open(a, "wb")
    with pytest.raises(NotImplementedError):
        s3file.setxattr(test_xattr="1")


@pytest.mark.xfail()
def test_delegate(s3):
    out = s3.get_delegated_s3pars()
    assert out
    assert out["token"]
    s32 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri}, **out)
    assert not s32.anon
    assert out == s32.get_delegated_s3pars()


def test_not_delegate():
    s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    out = s3.get_delegated_s3pars()
    assert out == {"anon": True}
    s3 = S3FileSystem(
        anon=False, client_kwargs={"endpoint_url": endpoint_uri}
    )  # auto credentials
    out = s3.get_delegated_s3pars()
    assert out == {"anon": False}


def test_ls(s3):
    assert set(s3.ls("", detail=False)) == {
        test_bucket_name,
        secure_bucket_name,
        versioned_bucket_name,
    }
    with pytest.raises(FileNotFoundError):
        s3.ls("nonexistent")
    fn = test_bucket_name + "/test/accounts.1.json"
    assert fn in s3.ls(test_bucket_name + "/test", detail=False)


def test_pickle(s3):
    import pickle

    s32 = pickle.loads(pickle.dumps(s3))
    assert s3.ls("test") == s32.ls("test")
    s33 = pickle.loads(pickle.dumps(s32))
    assert s3.ls("test") == s33.ls("test")


def test_ls_touch(s3):
    assert not s3.exists(test_bucket_name + "/tmp/test")
    s3.touch(a)
    s3.touch(b)
    L = s3.ls(test_bucket_name + "/tmp/test", True)
    assert {d["Key"] for d in L} == {a, b}
    L = s3.ls(test_bucket_name + "/tmp/test", False)
    assert set(L) == {a, b}


@pytest.mark.parametrize("version_aware", [True, False])
def test_exists_versioned(s3, version_aware):
    """Test to ensure that a prefix exists when using a versioned bucket"""
    import uuid

    n = 3
    s3 = S3FileSystem(
        anon=False,
        version_aware=version_aware,
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    segments = [versioned_bucket_name] + [str(uuid.uuid4()) for _ in range(n)]
    path = "/".join(segments)
    for i in range(2, n + 1):
        assert not s3.exists("/".join(segments[:i]))
    s3.touch(path)
    for i in range(2, n + 1):
        assert s3.exists("/".join(segments[:i]))


def test_isfile(s3):
    assert not s3.isfile("")
    assert not s3.isfile("/")
    assert not s3.isfile(test_bucket_name)
    assert not s3.isfile(test_bucket_name + "/test")

    assert not s3.isfile(test_bucket_name + "/test/foo")
    assert s3.isfile(test_bucket_name + "/test/accounts.1.json")
    assert s3.isfile(test_bucket_name + "/test/accounts.2.json")

    assert not s3.isfile(a)
    s3.touch(a)
    assert s3.isfile(a)

    assert not s3.isfile(b)
    assert not s3.isfile(b + "/")
    s3.mkdir(b)
    assert not s3.isfile(b)
    assert not s3.isfile(b + "/")

    assert not s3.isfile(c)
    assert not s3.isfile(c + "/")
    s3.mkdir(c + "/")
    assert not s3.isfile(c)
    assert not s3.isfile(c + "/")


def test_isdir(s3):
    assert s3.isdir("")
    assert s3.isdir("/")
    assert s3.isdir(test_bucket_name)
    assert s3.isdir(test_bucket_name + "/test")

    assert not s3.isdir(test_bucket_name + "/test/foo")
    assert not s3.isdir(test_bucket_name + "/test/accounts.1.json")
    assert not s3.isdir(test_bucket_name + "/test/accounts.2.json")

    assert not s3.isdir(a)
    s3.touch(a)
    assert not s3.isdir(a)

    assert not s3.isdir(b)
    assert not s3.isdir(b + "/")

    assert not s3.isdir(c)
    assert not s3.isdir(c + "/")

    # test cache
    s3.invalidate_cache()
    assert not s3.dircache
    s3.ls(test_bucket_name + "/nested")
    assert test_bucket_name + "/nested" in s3.dircache
    assert not s3.isdir(test_bucket_name + "/nested/file1")
    assert not s3.isdir(test_bucket_name + "/nested/file2")
    assert s3.isdir(test_bucket_name + "/nested/nested2")
    assert s3.isdir(test_bucket_name + "/nested/nested2/")


def test_rm(s3):
    assert not s3.exists(a)
    s3.touch(a)
    assert s3.exists(a)
    s3.rm(a)
    assert not s3.exists(a)
    # the API is OK with deleting non-files; maybe this is an effect of using bulk
    # with pytest.raises(FileNotFoundError):
    #    s3.rm(test_bucket_name + '/nonexistent')
    with pytest.raises(FileNotFoundError):
        s3.rm("nonexistent")
    out = s3.rm(test_bucket_name + "/nested", recursive=True)
    assert test_bucket_name + "/nested/nested2/file1" in out
    assert not s3.exists(test_bucket_name + "/nested/nested2/file1")

    # whole bucket
    out = s3.rm(test_bucket_name, recursive=True)
    assert test_bucket_name + "/2014-01-01.csv" in out
    assert not s3.exists(test_bucket_name + "/2014-01-01.csv")
    assert not s3.exists(test_bucket_name)


def test_rmdir(s3):
    bucket = "test1_bucket"
    s3.mkdir(bucket)
    s3.rmdir(bucket)
    assert bucket not in s3.ls("/")


def test_mkdir(s3):
    bucket = "test1_bucket"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")


def test_mkdir_existing_bucket(s3):
    # mkdir called on existing bucket should be no-op and not calling create_bucket
    # creating a s3 bucket
    bucket = "test1_bucket"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")
    # a second call.
    with pytest.raises(FileExistsError):
        s3.mkdir(bucket)


def test_mkdir_bucket_and_key_1(s3):
    bucket = "test1_bucket"
    file = bucket + "/a/b/c"
    s3.mkdir(file, create_parents=True)
    assert bucket in s3.ls("/")


def test_mkdir_bucket_and_key_2(s3):
    bucket = "test1_bucket"
    file = bucket + "/a/b/c"
    with pytest.raises(FileNotFoundError):
        s3.mkdir(file, create_parents=False)
    assert bucket not in s3.ls("/")


def test_mkdir_region_name(s3):
    bucket = "test2_bucket"
    s3.mkdir(bucket, region_name="eu-central-1")
    assert bucket in s3.ls("/")


def test_mkdir_client_region_name(s3):
    bucket = "test3_bucket"
    s3 = S3FileSystem(
        anon=False,
        client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
    )
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")


def test_makedirs(s3):
    bucket = "test_makedirs_bucket"
    test_file = bucket + "/a/b/c/file"
    s3.makedirs(test_file)
    assert bucket in s3.ls("/")


def test_makedirs_existing_bucket(s3):
    bucket = "test_makedirs_bucket"
    s3.mkdir(bucket)
    assert bucket in s3.ls("/")
    test_file = bucket + "/a/b/c/file"
    # no-op, and no error.
    s3.makedirs(test_file)


def test_makedirs_pure_bucket_exist_ok(s3):
    bucket = "test1_bucket"
    s3.mkdir(bucket)
    s3.makedirs(bucket, exist_ok=True)


def test_makedirs_pure_bucket_error_on_exist(s3):
    bucket = "test1_bucket"
    s3.mkdir(bucket)
    with pytest.raises(FileExistsError):
        s3.makedirs(bucket, exist_ok=False)


def test_bulk_delete(s3):
    with pytest.raises(FileNotFoundError):
        s3.rm(["nonexistent/file"])
    filelist = s3.find(test_bucket_name + "/nested")
    s3.rm(filelist)
    assert not s3.exists(test_bucket_name + "/nested/nested2/file1")


@pytest.mark.xfail(reason="anon user is still privileged on moto")
def test_anonymous_access(s3):
    with ignoring(NoCredentialsError):
        s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
        assert s3.ls("") == []
        # TODO: public bucket doesn't work through moto

    with pytest.raises(PermissionError):
        s3.mkdir("newbucket")


def test_s3_file_access(s3):
    fn = test_bucket_name + "/nested/file1"
    data = b"hello\n"
    assert s3.cat(fn) == data
    assert s3.head(fn, 3) == data[:3]
    assert s3.tail(fn, 3) == data[-3:]
    assert s3.tail(fn, 10000) == data


def test_s3_file_info(s3):
    fn = test_bucket_name + "/nested/file1"
    data = b"hello\n"
    assert fn in s3.find(test_bucket_name)
    assert s3.exists(fn)
    assert not s3.exists(fn + "another")
    assert s3.info(fn)["Size"] == len(data)
    with pytest.raises(FileNotFoundError):
        s3.info(fn + "another")


def test_content_type_is_set(s3, tmpdir):
    test_file = str(tmpdir) + "/test.json"
    destination = test_bucket_name + "/test.json"
    open(test_file, "w").write("text")
    s3.put(test_file, destination)
    assert s3.info(destination)["ContentType"] == "application/json"


def test_content_type_is_not_overrided(s3, tmpdir):
    test_file = os.path.join(str(tmpdir), "test.json")
    destination = os.path.join(test_bucket_name, "test.json")
    open(test_file, "w").write("text")
    s3.put(test_file, destination, ContentType="text/css")
    assert s3.info(destination)["ContentType"] == "text/css"


def test_bucket_exists(s3):
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name + "x")
    s3 = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name + "x")


def test_du(s3):
    d = s3.du(test_bucket_name, total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert test_bucket_name + "/nested/file1" in d

    assert s3.du(test_bucket_name + "/test/", total=True) == sum(
        map(len, files.values())
    )
    assert s3.du(test_bucket_name) == s3.du("s3://" + test_bucket_name)


def test_s3_ls(s3):
    fn = test_bucket_name + "/nested/file1"
    assert fn not in s3.ls(test_bucket_name + "/")
    assert fn in s3.ls(test_bucket_name + "/nested/")
    assert fn in s3.ls(test_bucket_name + "/nested")
    assert s3.ls("s3://" + test_bucket_name + "/nested/") == s3.ls(
        test_bucket_name + "/nested"
    )


def test_s3_big_ls(s3):
    for x in range(1200):
        s3.touch(test_bucket_name + "/thousand/%i.part" % x)
    assert len(s3.find(test_bucket_name)) > 1200
    s3.rm(test_bucket_name + "/thousand/", recursive=True)
    assert len(s3.find(test_bucket_name + "/thousand/")) == 0


def test_s3_ls_detail(s3):
    L = s3.ls(test_bucket_name + "/nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_s3_glob(s3):
    fn = test_bucket_name + "/nested/file1"
    assert fn not in s3.glob(test_bucket_name + "/")
    assert fn not in s3.glob(test_bucket_name + "/*")
    assert fn not in s3.glob(test_bucket_name + "/nested")
    assert fn in s3.glob(test_bucket_name + "/nested/*")
    assert fn in s3.glob(test_bucket_name + "/nested/file*")
    assert fn in s3.glob(test_bucket_name + "/*/*")
    assert all(
        any(p.startswith(f + "/") or p == f for p in s3.find(test_bucket_name))
        for f in s3.glob(test_bucket_name + "/nested/*")
    )
    assert [test_bucket_name + "/nested/nested2"] == s3.glob(
        test_bucket_name + "/nested/nested2"
    )
    out = s3.glob(test_bucket_name + "/nested/nested2/*")
    assert {"test/nested/nested2/file1", "test/nested/nested2/file2"} == set(out)

    with pytest.raises(ValueError):
        s3.glob("*")

    # Make sure glob() deals with the dot character (.) correctly.
    assert test_bucket_name + "/file.dat" in s3.glob(test_bucket_name + "/file.*")
    assert test_bucket_name + "/filexdat" not in s3.glob(test_bucket_name + "/file.*")


def test_get_list_of_summary_objects(s3):
    L = s3.ls(test_bucket_name + "/test")

    assert len(L) == 2
    assert [l.lstrip(test_bucket_name).lstrip("/") for l in sorted(L)] == sorted(
        list(files)
    )

    L2 = s3.ls("s3://" + test_bucket_name + "/test")

    assert L == L2


def test_read_keys_from_bucket(s3):
    for k, data in files.items():
        file_contents = s3.cat("/".join([test_bucket_name, k]))
        assert file_contents == data

        assert s3.cat("/".join([test_bucket_name, k])) == s3.cat(
            "s3://" + "/".join([test_bucket_name, k])
        )


def test_url(s3):
    fn = test_bucket_name + "/nested/file1"
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


def test_seek(s3):
    with s3.open(a, "wb") as f:
        f.write(b"123")

    with s3.open(a) as f:
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


def test_copy(s3):
    fn = test_bucket_name + "/test/accounts.1.json"
    s3.copy(fn, fn + "2")
    assert s3.cat(fn) == s3.cat(fn + "2")


def test_copy_managed(s3):
    data = b"abc" * 12 * 2**20
    fn = test_bucket_name + "/test/biggerfile"
    with s3.open(fn, "wb") as f:
        f.write(data)
    sync(s3.loop, s3._copy_managed, fn, fn + "2", size=len(data), block=5 * 2**20)
    assert s3.cat(fn) == s3.cat(fn + "2")
    with pytest.raises(ValueError):
        sync(s3.loop, s3._copy_managed, fn, fn + "3", size=len(data), block=4 * 2**20)
    with pytest.raises(ValueError):
        sync(s3.loop, s3._copy_managed, fn, fn + "3", size=len(data), block=6 * 2**30)


@pytest.mark.parametrize("recursive", [True, False])
def test_move(s3, recursive):
    fn = test_bucket_name + "/test/accounts.1.json"
    data = s3.cat(fn)
    s3.mv(fn, fn + "2", recursive=recursive)
    assert s3.cat(fn + "2") == data
    assert not s3.exists(fn)


def test_get_put(s3, tmpdir):
    test_file = str(tmpdir.join("test.json"))

    s3.get(test_bucket_name + "/test/accounts.1.json", test_file)
    data = files["test/accounts.1.json"]
    assert open(test_file, "rb").read() == data
    s3.put(test_file, test_bucket_name + "/temp")
    assert s3.du(test_bucket_name + "/temp", total=False)[
        test_bucket_name + "/temp"
    ] == len(data)
    assert s3.cat(test_bucket_name + "/temp") == data


def test_get_put_big(s3, tmpdir):
    test_file = str(tmpdir.join("test"))
    data = b"1234567890A" * 2**20
    open(test_file, "wb").write(data)

    s3.put(test_file, test_bucket_name + "/bigfile")
    test_file = str(tmpdir.join("test2"))
    s3.get(test_bucket_name + "/bigfile", test_file)
    assert open(test_file, "rb").read() == data


def test_get_put_with_callback(s3, tmpdir):
    test_file = str(tmpdir.join("test.json"))

    class BranchingCallback(Callback):
        def branch(self, path_1, path_2, kwargs):
            kwargs["callback"] = BranchingCallback()

    cb = BranchingCallback()
    s3.get(test_bucket_name + "/test/accounts.1.json", test_file, callback=cb)
    assert cb.size == 1
    assert cb.value == 1

    cb = BranchingCallback()
    s3.put(test_file, test_bucket_name + "/temp", callback=cb)
    assert cb.size == 1
    assert cb.value == 1


def test_get_file_with_callback(s3, tmpdir):
    test_file = str(tmpdir.join("test.json"))

    cb = Callback()
    s3.get_file(test_bucket_name + "/test/accounts.1.json", test_file, callback=cb)
    assert cb.size == os.stat(test_file).st_size
    assert cb.value == cb.size


@pytest.mark.parametrize("size", [2**10, 10 * 2**20])
def test_put_file_with_callback(s3, tmpdir, size):
    test_file = str(tmpdir.join("test.json"))
    with open(test_file, "wb") as f:
        f.write(b"1234567890A" * size)

    cb = Callback()
    s3.put_file(test_file, test_bucket_name + "/temp", callback=cb)
    assert cb.size == os.stat(test_file).st_size
    assert cb.value == cb.size


@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_pipe_cat_big(s3, size):
    data = b"1234567890A" * size
    s3.pipe(test_bucket_name + "/bigfile", data)
    assert s3.cat(test_bucket_name + "/bigfile") == data


def test_errors(s3):
    with pytest.raises(FileNotFoundError):
        s3.open(test_bucket_name + "/tmp/test/shfoshf", "rb")

    # This is fine, no need for interleaving directories on S3
    # with pytest.raises((IOError, OSError)):
    #    s3.touch('tmp/test/shfoshf/x')

    # Deleting nonexistent or zero paths is allowed for now
    # with pytest.raises(FileNotFoundError):
    #    s3.rm(test_bucket_name + '/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        s3.mv(test_bucket_name + "/tmp/test/shfoshf/x", "tmp/test/shfoshf/y")

    with pytest.raises(ValueError):
        s3.open("x", "rb")

    with pytest.raises(FileNotFoundError):
        s3.rm("unknown")

    with pytest.raises(ValueError):
        with s3.open(test_bucket_name + "/temp", "wb") as f:
            f.read()

    with pytest.raises(ValueError):
        f = s3.open(test_bucket_name + "/temp", "rb")
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

    async def head_object(*args, **kwargs):
        raise NoCredentialsError

    monkeypatch.setattr(type(s3.s3), "head_object", head_object)

    # Since the error is not translate, the __cause__ would
    # be None
    with pytest.raises(NoCredentialsError) as exc:
        s3.info("test/a.txt")

    assert exc.value.__cause__ is None


def test_read_small(s3):
    fn = test_bucket_name + "/2014-01-01.csv"
    with s3.open(fn, "rb", block_size=10, cache_type="bytes") as f:
        out = []
        while True:
            data = f.read(3)
            if data == b"":
                break
            out.append(data)
        assert s3.cat(fn) == b"".join(out)
        # cache drop
        assert len(f.cache) < len(out)


def test_read_s3_block(s3):
    data = files["test/accounts.1.json"]
    lines = io.BytesIO(data).readlines()
    path = test_bucket_name + "/test/accounts.1.json"
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


def test_write_small(s3):
    with s3.open(test_bucket_name + "/test", "wb") as f:
        f.write(b"hello")
    assert s3.cat(test_bucket_name + "/test") == b"hello"
    s3.open(test_bucket_name + "/test", "wb").close()
    assert s3.info(test_bucket_name + "/test")["size"] == 0


def test_write_small_with_acl(s3):
    bucket, key = (test_bucket_name, "test-acl")
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


def test_write_large(s3):
    "flush() chunks buffer when processing large singular payload"
    mb = 2**20
    payload_size = int(2.5 * 5 * mb)
    payload = b"0" * payload_size

    with s3.open(test_bucket_name + "/test", "wb") as fd:
        fd.write(payload)

    assert s3.cat(test_bucket_name + "/test") == payload
    assert s3.info(test_bucket_name + "/test")["size"] == payload_size


def test_write_limit(s3):
    "flush() respects part_max when processing large singular payload"
    mb = 2**20
    block_size = 15 * mb
    payload_size = 44 * mb
    payload = b"0" * payload_size

    with s3.open(test_bucket_name + "/test", "wb", blocksize=block_size) as fd:
        fd.write(payload)

    assert s3.cat(test_bucket_name + "/test") == payload

    assert s3.info(test_bucket_name + "/test")["size"] == payload_size


def test_write_small_secure(s3):
    # Unfortunately moto does not yet support enforcing SSE policies.  It also
    # does not return the correct objects that can be used to test the results
    # effectively.
    # This test is left as a placeholder in case moto eventually supports this.
    sse_params = SSEParams(server_side_encryption="aws:kms")
    with s3.open(secure_bucket_name + "/test", "wb", writer_kwargs=sse_params) as f:
        f.write(b"hello")
    assert s3.cat(secure_bucket_name + "/test") == b"hello"
    sync(s3.loop, s3.s3.head_object, Bucket=secure_bucket_name, Key="test")


def test_write_large_secure(s3):
    # build our own s3fs with the relevant additional kwarg
    s3 = S3FileSystem(
        s3_additional_kwargs={"ServerSideEncryption": "AES256"},
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    s3.mkdir("mybucket")

    with s3.open("mybucket/myfile", "wb") as f:
        f.write(b"hello hello" * 10**6)

    assert s3.cat("mybucket/myfile") == b"hello hello" * 10**6


def test_write_fails(s3):
    with pytest.raises(ValueError):
        s3.touch(test_bucket_name + "/temp")
        s3.open(test_bucket_name + "/temp", "rb").write(b"hello")
    with pytest.raises(ValueError):
        s3.open(test_bucket_name + "/temp", "wb", block_size=10)
    f = s3.open(test_bucket_name + "/temp", "wb")
    f.close()
    with pytest.raises(ValueError):
        f.write(b"hello")
    with pytest.raises(FileNotFoundError):
        s3.open("nonexistentbucket/temp", "wb").close()


def test_write_blocks(s3):
    with s3.open(test_bucket_name + "/temp", "wb") as f:
        f.write(b"a" * 2 * 2**20)
        assert f.buffer.tell() == 2 * 2**20
        assert not (f.parts)
        f.flush()
        assert f.buffer.tell() == 2 * 2**20
        assert not (f.parts)
        f.write(b"a" * 2 * 2**20)
        f.write(b"a" * 2 * 2**20)
        assert f.mpu
        assert f.parts
    assert s3.info(test_bucket_name + "/temp")["size"] == 6 * 2**20
    with s3.open(test_bucket_name + "/temp", "wb", block_size=10 * 2**20) as f:
        f.write(b"a" * 15 * 2**20)
        assert f.buffer.tell() == 0
    assert s3.info(test_bucket_name + "/temp")["size"] == 15 * 2**20


def test_readline(s3):
    all_items = chain.from_iterable(
        [files.items(), csv_files.items(), text_files.items()]
    )
    for k, data in all_items:
        with s3.open("/".join([test_bucket_name, k]), "rb") as f:
            result = f.readline()
            expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


def test_readline_empty(s3):
    data = b""
    with s3.open(a, "wb") as f:
        f.write(data)
    with s3.open(a, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(s3):
    data = b"ab\n" + b"a" * (10 * 2**20) + b"\nab"
    with s3.open(a, "wb") as f:
        f.write(data)
    with s3.open(a, "rb") as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (10 * 2**20) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(s3):
    expected = csv_files["2014-01-01.csv"].split(b"\n")[0] + b"\n"
    with s3.open(test_bucket_name + "/2014-01-01.csv") as f:
        result = next(f)
        assert result == expected


def test_iterable(s3):
    data = b"abc\n123"
    with s3.open(a, "wb") as f:
        f.write(data)
    with s3.open(a) as f, io.BytesIO(data) as g:
        for froms3, fromio in zip(f, g):
            assert froms3 == fromio
        f.seek(0)
        assert f.readline() == b"abc\n"
        assert f.readline() == b"123"
        f.seek(1)
        assert f.readline() == b"bc\n"

    with s3.open(a) as f:
        out = list(f)
    with s3.open(a) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_readable(s3):
    with s3.open(a, "wb") as f:
        assert not f.readable()

    with s3.open(a, "rb") as f:
        assert f.readable()


def test_seekable(s3):
    with s3.open(a, "wb") as f:
        assert not f.seekable()

    with s3.open(a, "rb") as f:
        assert f.seekable()


def test_writable(s3):
    with s3.open(a, "wb") as f:
        assert f.writable()

    with s3.open(a, "rb") as f:
        assert not f.writable()


def test_merge(s3):
    with s3.open(a, "wb") as f:
        f.write(b"a" * 10 * 2**20)

    with s3.open(b, "wb") as f:
        f.write(b"a" * 10 * 2**20)
    s3.merge(test_bucket_name + "/joined", [a, b])
    assert s3.info(test_bucket_name + "/joined")["size"] == 2 * 10 * 2**20


def test_append(s3):
    data = text_files["nested/file1"]
    with s3.open(test_bucket_name + "/nested/file1", "ab") as f:
        assert f.tell() == len(data)  # append, no write, small file
    assert s3.cat(test_bucket_name + "/nested/file1") == data
    with s3.open(test_bucket_name + "/nested/file1", "ab") as f:
        f.write(b"extra")  # append, write, small file
    assert s3.cat(test_bucket_name + "/nested/file1") == data + b"extra"

    with s3.open(a, "wb") as f:
        f.write(b"a" * 10 * 2**20)
    with s3.open(a, "ab") as f:
        pass  # append, no write, big file
    assert s3.cat(a) == b"a" * 10 * 2**20

    with s3.open(a, "ab") as f:
        assert f.parts is None
        f._initiate_upload()
        assert f.parts
        assert f.tell() == 10 * 2**20
        f.write(b"extra")  # append, small write, big file
    assert s3.cat(a) == b"a" * 10 * 2**20 + b"extra"

    with s3.open(a, "ab") as f:
        assert f.tell() == 10 * 2**20 + 5
        f.write(b"b" * 10 * 2**20)  # append, big write, big file
        assert f.tell() == 20 * 2**20 + 5
    assert s3.cat(a) == b"a" * 10 * 2**20 + b"extra" + b"b" * 10 * 2**20

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
    )
    with s3.open(a, "wb", **head) as f:
        f.write(b"data")

    with s3.open(a, "ab") as f:
        f.write(b"other")

    with s3.open(a) as f:
        filehead = {
            k: v
            for k, v in f._call_s3(
                "head_object", f.kwargs, Bucket=f.bucket, Key=f.key
            ).items()
            if k in head
        }
        assert filehead == head


def test_bigger_than_block_read(s3):
    with s3.open(test_bucket_name + "/2014-01-01.csv", "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(20)
            out.append(data)
            if len(data) == 0:
                break
    assert b"".join(out) == csv_files["2014-01-01.csv"]


def test_current(s3):
    s3._cache.clear()
    s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.current() is s3
    assert S3FileSystem.current() is s3


def test_array(s3):
    from array import array

    data = array("B", [65] * 1000)

    with s3.open(a, "wb") as f:
        f.write(data)

    with s3.open(a, "rb") as f:
        out = f.read()
        assert out == b"A" * 1000


def _get_s3_id(s3):
    return id(s3.s3)


@pytest.mark.parametrize(
    "method",
    [
        "spawn",
        pytest.param(
            "forkserver",
            marks=pytest.mark.skipif(
                sys.platform.startswith("win"),
                reason="'forkserver' not available on windows",
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
def test_public_file(s3):
    # works on real s3, not on moto
    test_bucket_name = "s3fs_public_test"
    other_bucket_name = "s3fs_private_test"

    s3.touch(test_bucket_name)
    s3.touch(test_bucket_name + "/afile")
    s3.touch(other_bucket_name, acl="public-read")
    s3.touch(other_bucket_name + "/afile", acl="public-read")

    s = S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint_uri})
    with pytest.raises(PermissionError):
        s.ls(test_bucket_name)
    s.ls(other_bucket_name)

    s3.chmod(test_bucket_name, acl="public-read")
    s3.chmod(other_bucket_name, acl="private")
    with pytest.raises(PermissionError):
        s.ls(other_bucket_name, refresh=True)
    assert s.ls(test_bucket_name, refresh=True)

    # public file in private bucket
    with s3.open(other_bucket_name + "/see_me", "wb", acl="public-read") as f:
        f.write(b"hello")
    assert s.cat(other_bucket_name + "/see_me") == b"hello"


def test_upload_with_s3fs_prefix(s3):
    path = "s3://test/prefix/key"

    with s3.open(path, "wb") as f:
        f.write(b"a" * (10 * 2**20))

    with s3.open(path, "ab") as f:
        f.write(b"b" * (10 * 2**20))


def test_multipart_upload_blocksize(s3):
    blocksize = 5 * (2**20)
    expected_parts = 3

    s3f = s3.open(a, "wb", block_size=blocksize)
    for _ in range(3):
        data = b"b" * blocksize
        s3f.write(data)

    # Ensure that the multipart upload consists of only 3 parts
    assert len(s3f.parts) == expected_parts
    s3f.close()


def test_default_pars(s3):
    s3 = S3FileSystem(
        default_block_size=20,
        default_fill_cache=False,
        client_kwargs={"endpoint_url": endpoint_uri},
    )
    fn = test_bucket_name + "/" + list(files)[0]
    with s3.open(fn) as f:
        assert f.blocksize == 20
        assert f.fill_cache is False
    with s3.open(fn, block_size=40, fill_cache=True) as f:
        assert f.blocksize == 40
        assert f.fill_cache is True


def test_tags(s3):
    tagset = {"tag1": "value1", "tag2": "value2"}
    fname = list(files)[0]
    s3.touch(fname)
    s3.put_tags(fname, tagset)
    assert s3.get_tags(fname) == tagset

    # Ensure merge mode updates value of existing key and adds new one
    new_tagset = {"tag2": "updatedvalue2", "tag3": "value3"}
    s3.put_tags(fname, new_tagset, mode="m")
    tagset.update(new_tagset)
    assert s3.get_tags(fname) == tagset


@pytest.mark.parametrize("prefix", ["", "/dir", "/dir/subdir"])
def test_versions(s3, prefix):
    parent = versioned_bucket_name + prefix
    versioned_file = parent + "/versioned_file"

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

    versioned_file_v1 = f"{versioned_file}?versionId={first_version}"
    versioned_file_v2 = f"{versioned_file}?versionId={second_version}"

    assert s3.ls(parent) == [versioned_file]
    assert set(s3.ls(parent, versions=True)) == {versioned_file_v1, versioned_file_v2}

    assert s3.exists(versioned_file_v1)
    assert s3.info(versioned_file_v1)
    assert s3.exists(versioned_file_v2)
    assert s3.info(versioned_file_v2)


def test_list_versions_many(s3):
    # moto doesn't actually behave in the same way that s3 does here so this doesn't test
    # anything really in moto 1.2
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    versioned_file = versioned_bucket_name + "/versioned_file2"
    for i in range(1200):
        with s3.open(versioned_file, "wb") as fo:
            fo.write(b"1")
    versions = s3.object_version_info(versioned_file)
    assert len(versions) == 1200


def test_fsspec_versions_multiple(s3):
    """Test that the standard fsspec.core.get_fs_token_paths behaves as expected for versionId urls"""
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    versioned_file = versioned_bucket_name + "/versioned_file3"
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


def test_versioned_file_fullpath(s3):
    versioned_file = versioned_bucket_name + "/versioned_file_fullpath"
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

    versions = s3.object_version_info(versioned_file)
    version_ids = [version["VersionId"] for version in versions]
    assert set(s3.ls(versioned_bucket_name, versions=True)) == {
        f"{versioned_file}?versionId={vid}" for vid in version_ids
    }


def test_versions_unaware(s3):
    versioned_file = versioned_bucket_name + "/versioned_file3"
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


def test_versions_dircached(s3):
    versioned_file = versioned_bucket_name + "/dir/versioned_file"
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"1")
    first_version = fo.version_id
    with s3.open(versioned_file, "wb") as fo:
        fo.write(b"2")
    second_version = fo.version_id
    s3.find(versioned_bucket_name)
    cached = s3.dircache[versioned_bucket_name + "/dir"][0]

    assert cached.get("VersionId") == second_version
    assert s3.info(versioned_file) == cached

    assert (
        s3.info(versioned_file, version_id=first_version).get("VersionId")
        == first_version
    )
    assert (
        s3.info(versioned_file, version_id=second_version).get("VersionId")
        == second_version
    )


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


def test_change_defaults_only_subsequent():
    """Test for Issue #135

    Ensure that changing the default block size doesn't affect existing file
    systems that were created using that default. It should only affect file
    systems created after the change.
    """
    try:
        S3FileSystem.cachable = False  # don't reuse instances with same pars

        fs_default = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
        assert fs_default.default_block_size == 5 * (1024**2)

        fs_overridden = S3FileSystem(
            default_block_size=64 * (1024**2),
            client_kwargs={"endpoint_url": endpoint_uri},
        )
        assert fs_overridden.default_block_size == 64 * (1024**2)

        # Suppose I want all subsequent file systems to have a block size of 1 GiB
        # instead of 5 MiB:
        S3FileSystem.default_block_size = 1024**3

        fs_big = S3FileSystem(client_kwargs={"endpoint_url": endpoint_uri})
        assert fs_big.default_block_size == 1024**3

        # Test the other file systems created to see if their block sizes changed
        assert fs_overridden.default_block_size == 64 * (1024**2)
        assert fs_default.default_block_size == 5 * (1024**2)
    finally:
        S3FileSystem.default_block_size = 5 * (1024**2)
        S3FileSystem.cachable = True


def test_cache_after_copy(s3):
    # https://github.com/dask/dask/issues/5134
    s3.touch("test/afile")
    assert "test/afile" in s3.ls("s3://test", False)
    s3.cp("test/afile", "test/bfile")
    assert "test/bfile" in s3.ls("s3://test", False)


def test_autocommit(s3):
    auto_file = test_bucket_name + "/auto_file"
    committed_file = test_bucket_name + "/commit_file"
    aborted_file = test_bucket_name + "/aborted_file"
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


def test_autocommit_mpu(s3):
    """When not autocommitting we always want to use multipart uploads"""
    path = test_bucket_name + "/auto_commit_with_mpu"
    with s3.open(path, "wb", autocommit=False) as fo:
        fo.write(b"1")
    assert fo.mpu is not None
    assert len(fo.parts) == 1


def test_touch(s3):
    # create
    fn = test_bucket_name + "/touched"
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


def test_touch_versions(s3):
    versioned_file = versioned_bucket_name + "/versioned_file"
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


def test_cat_missing(s3):
    fn0 = test_bucket_name + "/file0"
    fn1 = test_bucket_name + "/file1"
    s3.touch(fn0)
    with pytest.raises(FileNotFoundError):
        s3.cat([fn0, fn1], on_error="raise")
    out = s3.cat([fn0, fn1], on_error="omit")
    assert list(out) == [fn0]
    out = s3.cat([fn0, fn1], on_error="return")
    assert fn1 in out
    assert isinstance(out[fn1], FileNotFoundError)


def test_get_directories(s3, tmpdir):
    s3.touch(test_bucket_name + "/dir/dirkey/key0")
    s3.touch(test_bucket_name + "/dir/dirkey/key1")
    s3.touch(test_bucket_name + "/dir/dirkey")
    s3.touch(test_bucket_name + "/dir/dir/key")
    d = str(tmpdir)

    # Target directory with trailing slash
    s3.get(test_bucket_name + "/dir/", d, recursive=True)
    assert {"dirkey", "dir"} == set(os.listdir(d))
    assert ["key"] == os.listdir(os.path.join(d, "dir"))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dirkey")))

    local_fs = fsspec.filesystem("file")
    local_fs.rm(os.path.join(d, "dir"), recursive=True)
    local_fs.rm(os.path.join(d, "dirkey"), recursive=True)

    # Target directory without trailing slash
    s3.get(test_bucket_name + "/dir", d, recursive=True)
    assert ["dir"] == os.listdir(d)
    assert {"dirkey", "dir"} == set(os.listdir(os.path.join(d, "dir")))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dir", "dirkey")))


def test_seek_reads(s3):
    fn = test_bucket_name + "/myfile"
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


def test_connect_many(s3):
    from multiprocessing.pool import ThreadPool

    def task(i):
        S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri}).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


def test_requester_pays(s3):
    fn = test_bucket_name + "/myfile"
    s3 = S3FileSystem(requester_pays=True, client_kwargs={"endpoint_url": endpoint_uri})
    assert s3.req_kw["RequestPayer"] == "requester"
    s3.touch(fn)
    with s3.open(fn, "rb") as f:
        assert f.req_kw["RequestPayer"] == "requester"


def test_credentials():
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


def test_modified(s3):
    dir_path = test_bucket_name + "/modified"
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
        s3.modified(path=test_bucket_name)


def test_async_s3(s3):
    async def _():
        s3 = S3FileSystem(
            anon=False,
            asynchronous=True,
            loop=asyncio.get_running_loop(),
            client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
        )

        fn = test_bucket_name + "/nested/file1"
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


def test_cat_ranges(s3):
    data = b"a string to select from"
    fn = test_bucket_name + "/parts"
    s3.pipe(fn, data)

    assert s3.cat_file(fn) == data
    assert s3.cat_file(fn, start=5) == data[5:]
    assert s3.cat_file(fn, end=5) == data[:5]
    assert s3.cat_file(fn, start=1, end=-1) == data[1:-1]
    assert s3.cat_file(fn, start=-5) == data[-5:]


def test_async_s3_old(s3):
    async def _():
        s3 = S3FileSystem(
            anon=False,
            asynchronous=True,
            loop=asyncio.get_running_loop(),
            client_kwargs={"region_name": "eu-central-1", "endpoint_url": endpoint_uri},
        )

        fn = test_bucket_name + "/nested/file1"
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
    with fsspec.open(
        "s3://mine/oi", "wb", client_kwargs={"endpoint_url": endpoint_uri}
    ) as f:
        f.write(b"hello")
    with fsspec.open(
        "s3://mine/oi", "rb", client_kwargs={"endpoint_url": endpoint_uri}
    ) as f:
        assert f.read() == b"hello"


def test_repeat_exists(s3):
    fn = "s3://" + test_bucket_name + "/file1"
    s3.touch(fn)

    assert s3.exists(fn)
    assert s3.exists(fn)


def test_with_xzarr(s3):
    da = pytest.importorskip("dask.array")
    xr = pytest.importorskip("xarray")
    name = "sample"

    nana = xr.DataArray(da.random.random((1024, 1024, 10, 9, 1)))

    s3_path = f"{test_bucket_name}/{name}"
    s3store = s3.get_mapper(s3_path)

    s3.ls("")
    nana.to_dataset().to_zarr(store=s3store, mode="w", consolidated=True, compute=True)


def test_async_close():
    async def _():
        loop = asyncio.get_event_loop()
        s3 = S3FileSystem(anon=False, asynchronous=True, loop=loop)
        await s3._connect()

        fn = test_bucket_name + "/afile"

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


def test_put_single(s3, tmpdir):
    fn = os.path.join(str(tmpdir), "dir")
    os.mkdir(fn)
    open(os.path.join(fn, "abc"), "w").write("text")

    # Put with trailing slash
    s3.put(fn + "/", test_bucket_name)  # no-op, no files
    assert not s3.exists(test_bucket_name + "/abc")
    assert not s3.exists(test_bucket_name + "/dir")

    s3.put(fn + "/", test_bucket_name, recursive=True)
    assert s3.cat(test_bucket_name + "/abc") == b"text"

    # Put without trailing slash
    s3.put(fn, test_bucket_name, recursive=True)
    assert s3.cat(test_bucket_name + "/dir/abc") == b"text"


def test_shallow_find(s3):
    """Test that find method respects maxdepth.

    Verify that the ``find`` method respects the ``maxdepth`` parameter.  With
    ``maxdepth=1``, the results of ``find`` should be the same as those of
    ``ls``, without returning subdirectories.  See also issue 378.
    """
    ls_output = s3.ls(test_bucket_name)
    assert sorted(ls_output + [test_bucket_name]) == s3.find(
        test_bucket_name, maxdepth=1, withdirs=True
    )
    assert ls_output == s3.glob(test_bucket_name + "/*")


def test_multi_find(s3):
    s3.mkdir("bucket/test")
    s3.mkdir("bucket/test/sub")
    s3.write_text("bucket/test/file.txt", "some_text")
    s3.write_text("bucket/test/sub/file.txt", "some_text")

    out1 = s3.find("bucket", withdirs=True)
    out2 = s3.find("bucket", withdirs=True)
    assert (
        out1
        == out2
        == [
            "bucket/test",
            "bucket/test/file.txt",
            "bucket/test/sub",
            "bucket/test/sub/file.txt",
        ]
    )
    out1 = s3.find("bucket", withdirs=False)
    out2 = s3.find("bucket", withdirs=False)
    assert out1 == out2 == ["bucket/test/file.txt", "bucket/test/sub/file.txt"]


def test_version_sizes(s3):
    # protect against caching of incorrect version details
    s3 = S3FileSystem(
        anon=False, version_aware=True, client_kwargs={"endpoint_url": endpoint_uri}
    )
    import gzip

    path = f"s3://{versioned_bucket_name}/test.txt.gz"
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


def test_find_no_side_effect(s3):
    infos1 = s3.find(test_bucket_name, maxdepth=1, withdirs=True, detail=True)
    s3.find(test_bucket_name, maxdepth=None, withdirs=True, detail=True)
    infos3 = s3.find(test_bucket_name, maxdepth=1, withdirs=True, detail=True)
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
def test_raise_exception_when_file_has_changed_during_reading(s3):
    test_file_name = "file1"
    test_file = "s3://" + test_bucket_name + "/" + test_file_name
    content1 = b"123"
    content2 = b"ABCDEFG"

    boto3_client = get_boto3_client()

    def create_file(content: bytes):
        boto3_client.put_object(
            Bucket=test_bucket_name, Key=test_file_name, Body=content
        )

    create_file(b"123")

    with s3.open(test_file, "rb") as f:
        content = f.read()
        assert content == content1

    with s3.open(test_file, "rb") as f:
        create_file(content2)
        with expect_errno(errno.EBUSY):
            f.read()


def test_s3fs_etag_preserving_multipart_copy(monkeypatch, s3):
    # Set this to a lower value so that we can actually
    # test this without creating giant objects in memory
    monkeypatch.setattr(s3fs.core, "MANAGED_COPY_THRESHOLD", 5 * 2**20)

    test_file1 = test_bucket_name + "/test/multipart-upload.txt"
    test_file2 = test_bucket_name + "/test/multipart-upload-copy.txt"

    with s3.open(test_file1, "wb", block_size=5 * 2**21) as stream:
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


def test_sync_from_wihin_async(s3):
    # if treating as sync but within an even loop, e.g., calling from jupyter;
    # IO happens on dedicated thread.
    async def f():
        S3FileSystem.clear_instance_cache()
        s3 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
        assert s3.ls(test_bucket_name)

    asyncio.run(f())


def test_token_paths(s3):
    fs, tok, files = fsspec.get_fs_token_paths(
        "s3://" + test_bucket_name + "/*.csv",
        storage_options={"client_kwargs": {"endpoint_url": endpoint_uri}},
    )
    assert files


def test_same_name_but_no_exact(s3):
    s3.touch(test_bucket_name + "/very/similar/prefix1")
    s3.touch(test_bucket_name + "/very/similar/prefix2")
    s3.touch(test_bucket_name + "/very/similar/prefix3/something")
    assert not s3.exists(test_bucket_name + "/very/similar/prefix")
    assert not s3.exists(test_bucket_name + "/very/similar/prefi")
    assert not s3.exists(test_bucket_name + "/very/similar/pref")

    assert s3.exists(test_bucket_name + "/very/similar/")
    assert s3.exists(test_bucket_name + "/very/similar/prefix1")
    assert s3.exists(test_bucket_name + "/very/similar/prefix2")
    assert s3.exists(test_bucket_name + "/very/similar/prefix3")
    assert s3.exists(test_bucket_name + "/very/similar/prefix3/")
    assert s3.exists(test_bucket_name + "/very/similar/prefix3/something")

    assert not s3.exists(test_bucket_name + "/very/similar/prefix3/some")

    s3.touch(test_bucket_name + "/starting/very/similar/prefix")

    assert not s3.exists(test_bucket_name + "/starting/very/similar/prefix1")
    assert not s3.exists(test_bucket_name + "/starting/very/similar/prefix2")
    assert not s3.exists(test_bucket_name + "/starting/very/similar/prefix3")
    assert not s3.exists(test_bucket_name + "/starting/very/similar/prefix3/")
    assert not s3.exists(test_bucket_name + "/starting/very/similar/prefix3/something")

    assert s3.exists(test_bucket_name + "/starting/very/similar/prefix")
    assert s3.exists(test_bucket_name + "/starting/very/similar/prefix/")


def test_leading_forward_slash(s3):
    s3.touch(test_bucket_name + "/some/file")
    assert s3.ls(test_bucket_name + "/some/")
    assert s3.exists(test_bucket_name + "/some/file")
    assert s3.exists("s3://" + test_bucket_name + "/some/file")


def test_lsdir(s3):
    # https://github.com/fsspec/s3fs/issues/475
    s3.find(test_bucket_name)

    d = test_bucket_name + "/test"
    assert d in s3.ls(test_bucket_name)


def test_rm_recursive_folder(s3):
    s3.touch(test_bucket_name + "/sub/file")
    s3.rm(test_bucket_name + "/sub", recursive=True)
    assert not s3.exists(test_bucket_name + "/sub/file")
    assert not s3.exists(test_bucket_name + "/sub")

    s3.touch(test_bucket_name + "/sub/file")
    s3.touch(test_bucket_name + "/sub/")  # placeholder
    s3.rm(test_bucket_name + "/sub", recursive=True)
    assert not s3.exists(test_bucket_name + "/sub/file")
    assert not s3.exists(test_bucket_name + "/sub")

    s3.touch(test_bucket_name + "/sub/file")
    s3.rm(test_bucket_name, recursive=True)
    assert not s3.exists(test_bucket_name + "/sub/file")
    assert not s3.exists(test_bucket_name + "/sub")
    assert not s3.exists(test_bucket_name)


def test_copy_file_without_etag(s3, monkeypatch):

    s3.touch(test_bucket_name + "/copy_tests/file")
    s3.ls(test_bucket_name + "/copy_tests/")

    [file] = s3.dircache[test_bucket_name + "/copy_tests"]

    assert file["name"] == test_bucket_name + "/copy_tests/file"
    file.pop("ETag")

    assert s3.info(file["name"]).get("ETag", None) is None

    s3.cp_file(file["name"], test_bucket_name + "/copy_tests/file2")
    assert s3.info(test_bucket_name + "/copy_tests/file2")["ETag"] is not None


def test_find_with_prefix(s3):
    for cursor in range(100):
        s3.touch(test_bucket_name + f"/prefixes/test_{cursor}")

    s3.touch(test_bucket_name + "/prefixes2")
    assert len(s3.find(test_bucket_name + "/prefixes")) == 100
    assert len(s3.find(test_bucket_name, prefix="prefixes")) == 101

    assert len(s3.find(test_bucket_name + "/prefixes/test_")) == 0
    assert len(s3.find(test_bucket_name + "/prefixes", prefix="test_")) == 100
    assert len(s3.find(test_bucket_name + "/prefixes/", prefix="test_")) == 100

    test_1s = s3.find(test_bucket_name + "/prefixes/test_1")
    assert len(test_1s) == 1
    assert test_1s[0] == test_bucket_name + "/prefixes/test_1"

    test_1s = s3.find(test_bucket_name + "/prefixes/", prefix="test_1")
    assert len(test_1s) == 11
    assert test_1s == [test_bucket_name + "/prefixes/test_1"] + [
        test_bucket_name + f"/prefixes/test_{cursor}" for cursor in range(10, 20)
    ]
    assert s3.find(test_bucket_name + "/prefixes/") == s3.find(
        test_bucket_name + "/prefixes/", prefix=None
    )


def test_list_after_find(s3):
    before = s3.ls("s3://test")
    s3.invalidate_cache("s3://test/2014-01-01.csv")
    s3.find("s3://test/2014-01-01.csv")
    after = s3.ls("s3://test")
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


def test_rm_file(s3):
    target = test_bucket_name + "/to_be_removed/file"
    s3.touch(target)
    s3.rm_file(target)
    assert not s3.exists(target)
    assert not s3.exists(test_bucket_name + "/to_be_removed")


def test_exists_isdir(s3):
    bad_path = "s3://nyc-tlc-asdfasdf/trip data/"
    assert not s3.exists(bad_path)
    assert not s3.isdir(bad_path)


def test_list_del_multipart(s3):
    path = test_bucket_name + "/afile"
    f = s3.open(path, "wb")
    f.write(b"0" * 6 * 2**20)

    out = s3.list_multipart_uploads(test_bucket_name)
    assert [_ for _ in out if _["Key"] == "afile"]

    s3.clear_multipart_uploads(test_bucket_name)
    out = s3.list_multipart_uploads(test_bucket_name)
    assert not [_ for _ in out if _["Key"] == "afile"]

    try:
        f.close()  # may error
    except Exception:
        pass


def test_split_path(s3):
    buckets = [
        "my-test-bucket",
        "arn:aws:s3:region:123456789012:accesspoint/my-access-point-name",
        "arn:aws:s3-outposts:region:123456789012:outpost/outpost-id/bucket/my-test-bucket",
        "arn:aws:s3-outposts:region:123456789012:outpost/outpost-id/accesspoint/my-accesspoint-name",
        "arn:aws:s3-object-lambda:region:123456789012:accesspoint/my-lambda-object-name",
    ]
    test_key = "my/test/path"
    for test_bucket in buckets:
        bucket, key, _ = s3.split_path("s3://" + test_bucket + "/" + test_key)
        assert bucket == test_bucket
        assert key == test_key


def test_cp_directory_recursive(s3):
    src = test_bucket_name + "/src"
    src_file = src + "/file"
    s3.mkdir(src)
    s3.touch(src_file)

    target = test_bucket_name + "/target"

    # cp without slash
    assert not s3.exists(target)
    for loop in range(2):
        s3.cp(src, target, recursive=True)
        assert s3.isdir(target)

        if loop == 0:
            correct = [target + "/file"]
            assert s3.find(target) == correct
        else:
            correct = [target + "/file", target + "/src/file"]
            assert sorted(s3.find(target)) == correct

    s3.rm(target, recursive=True)

    # cp with slash
    assert not s3.exists(target)
    for loop in range(2):
        s3.cp(src + "/", target, recursive=True)
        assert s3.isdir(target)
        correct = [target + "/file"]
        assert s3.find(target) == correct


def test_get_directory_recursive(s3, tmpdir):
    src = test_bucket_name + "/src"
    src_file = src + "/file"
    s3.mkdir(src)
    s3.touch(src_file)

    target = os.path.join(tmpdir, "target")
    target_fs = fsspec.filesystem("file")

    # get without slash
    assert not target_fs.exists(target)
    for loop in range(2):
        s3.get(src, target, recursive=True)
        assert target_fs.isdir(target)

        if loop == 0:
            assert target_fs.find(target) == [os.path.join(target, "file")]
        else:
            assert sorted(target_fs.find(target)) == [
                os.path.join(target, "file"),
                os.path.join(target, "src", "file"),
            ]

    target_fs.rm(target, recursive=True)

    # get with slash
    assert not target_fs.exists(target)
    for loop in range(2):
        s3.get(src + "/", target, recursive=True)
        assert target_fs.isdir(target)
        assert target_fs.find(target) == [os.path.join(target, "file")]


def test_put_directory_recursive(s3, tmpdir):
    src = os.path.join(tmpdir, "src")
    src_file = os.path.join(src, "file")
    source_fs = fsspec.filesystem("file")
    source_fs.mkdir(src)
    source_fs.touch(src_file)

    target = test_bucket_name + "/target"

    # put without slash
    assert not s3.exists(target)
    for loop in range(2):
        s3.put(src, target, recursive=True)
        assert s3.isdir(target)

        if loop == 0:
            assert s3.find(target) == [target + "/file"]
        else:
            assert sorted(s3.find(target)) == [target + "/file", target + "/src/file"]

    s3.rm(target, recursive=True)

    # put with slash
    assert not s3.exists(target)
    for loop in range(2):
        s3.put(src + "/", target, recursive=True)
        assert s3.isdir(target)
        assert s3.find(target) == [target + "/file"]


def test_cp_two_files(s3):
    src = test_bucket_name + "/src"
    file0 = src + "/file0"
    file1 = src + "/file1"
    s3.mkdir(src)
    s3.touch(file0)
    s3.touch(file1)

    target = test_bucket_name + "/target"
    assert not s3.exists(target)

    s3.cp([file0, file1], target)

    assert s3.isdir(target)
    assert sorted(s3.find(target)) == [
        target + "/file0",
        target + "/file1",
    ]


def test_async_stream(s3_base):
    fn = test_bucket_name + "/target"
    data = b"hello world" * 1000
    out = []

    async def read_stream():
        fs = S3FileSystem(
            anon=False,
            client_kwargs={"endpoint_url": endpoint_uri},
            skip_instance_cache=True,
        )
        await fs._mkdir(test_bucket_name)
        await fs._pipe(fn, data)
        f = await fs.open_async(fn, mode="rb", block_seze=1000)
        while True:
            got = await f.read(1000)
            assert f.size == len(data)
            assert f.tell()
            if not got:
                break
            out.append(got)

    asyncio.run(read_stream())
    assert b"".join(out) == data


def test_rm_invalidates_cache(s3):
    # Issue 761: rm_file does not invalidate cache
    fn = test_bucket_name + "/2014-01-01.csv"
    assert s3.exists(fn)
    assert fn in s3.ls(test_bucket_name)
    s3.rm(fn)
    assert not s3.exists(fn)
    assert fn not in s3.ls(test_bucket_name)

    fn = test_bucket_name + "/2014-01-02.csv"
    assert s3.exists(fn)
    assert fn in s3.ls(test_bucket_name)
    s3.rm_file(fn)
    assert not s3.exists(fn)
    assert fn not in s3.ls(test_bucket_name)
