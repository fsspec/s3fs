import json
import os
import time

import pytest
import requests

from s3fs import S3FileSystem
import uuid
import socket
from contextlib import closing


@pytest.fixture()
def file_a(bucket_names):
    return bucket_names["test"] + "/tmp/test/a"


@pytest.fixture()
def file_b(bucket_names):
    return bucket_names["test"] + "/tmp/test/b"


@pytest.fixture()
def file_c(bucket_names):
    return bucket_names["test"] + "/tmp/test/c"


@pytest.fixture()
def file_d(bucket_names):
    return bucket_names["test"] + "/tmp/test/d"


@pytest.fixture()
def s3_files():
    return {
        "files": {
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
        },
        "csv": {
            "2014-01-01.csv": (
                b"name,amount,id\n" b"Alice,100,1\n" b"Bob,200,2\n" b"Charlie,300,3\n"
            ),
            "2014-01-02.csv": (b"name,amount,id\n"),
            "2014-01-03.csv": (
                b"name,amount,id\n" b"Dennis,400,4\n" b"Edith,500,5\n" b"Frank,600,6\n"
            ),
        },
        "text": {
            "nested/file1": b"hello\n",
            "nested/file2": b"world",
            "nested/nested2/file1": b"hello\n",
            "nested/nested2/file2": b"world",
        },
        "glob": {"file.dat": b"", "filexdat": b""},
    }


@pytest.fixture()
def bucket_names():
    return {
        "test": f"test-{uuid.uuid4()}",
        "secure": f"test-secure-{uuid.uuid4()}",
        "versioned": f"test-versioned-{uuid.uuid4()}",
    }


@pytest.fixture(scope="session")
def endpoint_port():
    # Find a free port.
    # From https://stackoverflow.com/questions/1365265/on-localhost-how-do-i-pick-a-free-port-number
    # Not perfect, but good enough
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def endpoint_uri(endpoint_port):
    return f"http://127.0.0.1:{endpoint_port}"


@pytest.fixture(scope="session")
def s3_base(endpoint_uri, endpoint_port):
    # writable local S3 system
    import shlex
    import subprocess

    try:
        # should fail since we didn't start server yet
        r = requests.get(endpoint_uri)
    except Exception:
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"

    with subprocess.Popen(shlex.split("moto_server s3 -p %s" % endpoint_port)) as proc:
        timeout = 5
        while timeout > 0:
            exit_code = proc.poll()
            if exit_code is not None:
                raise RuntimeError('Server failed to start: %s' % exit_code)
            try:
                r = requests.get(endpoint_uri)
                if r.ok:
                    break
            except Exception:
                pass
            timeout -= 0.1
            time.sleep(0.1)
        yield
        proc.terminate()
        proc.wait()


@pytest.fixture()
def boto3_client(endpoint_uri):
    from botocore.session import Session

    # NB: we use the sync botocore client for setup
    session = Session()
    return session.create_client("s3", endpoint_url=endpoint_uri)


@pytest.fixture()
def s3(s3_base, boto3_client, bucket_names, s3_files, endpoint_uri):
    boto3_client.create_bucket(Bucket=bucket_names["test"], ACL="public-read")

    boto3_client.create_bucket(Bucket=bucket_names["versioned"], ACL="public-read")
    boto3_client.put_bucket_versioning(
        Bucket=bucket_names["versioned"], VersioningConfiguration={"Status": "Enabled"}
    )

    # initialize secure bucket
    boto3_client.create_bucket(Bucket=bucket_names["secure"], ACL="public-read")
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
                        bucket_name=bucket_names["secure"]
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
    boto3_client.put_bucket_policy(Bucket=bucket_names["secure"], Policy=policy)
    for flist in s3_files.values():
        for f, data in flist.items():
            boto3_client.put_object(Bucket=bucket_names["test"], Key=f, Body=data)

    S3FileSystem.clear_instance_cache()
    s3 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
    s3.invalidate_cache()
    yield s3
    # Clean up all buckets and all items within those buckets
    for bucket in boto3_client.list_buckets()["Buckets"]:
        paginator = boto3_client.get_paginator("list_object_versions")

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=bucket["Name"], Delimiter="")

        for page in page_iterator:
            versions = page.get("Versions", [])
            delete_markers = page.get("DeleteMarkers", [])
            if not versions and not delete_markers:
                break
            boto3_client.delete_objects(
                Bucket=bucket["Name"],
                Delete={
                    "Objects": [
                        {"Key": v["Key"], "VersionId": v["VersionId"]}
                        for v in versions + delete_markers
                    ]
                },
            )

        boto3_client.delete_bucket(Bucket=bucket["Name"])
