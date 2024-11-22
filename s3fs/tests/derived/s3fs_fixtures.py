import json
import os
import pytest
import requests
import time

from fsspec.tests.abstract import AbstractFixtures
from s3fs.core import S3FileSystem


test_bucket_name = "test"
secure_bucket_name = "test-secure"
versioned_bucket_name = "test-versioned"
port = 5556
endpoint_uri = "http://127.0.0.1:%s/" % port


class S3fsFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self, _s3_base, _get_boto3_client):
        client = _get_boto3_client
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

        S3FileSystem.clear_instance_cache()
        s3 = S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint_uri})
        s3.invalidate_cache()
        yield s3

    @pytest.fixture
    def fs_path(self):
        return test_bucket_name

    @pytest.fixture
    def supports_empty_directories(self):
        return False

    @pytest.fixture(scope="class")
    def _get_boto3_client(self):
        from botocore.session import Session

        # NB: we use the sync botocore client for setup
        session = Session()
        return session.create_client("s3", endpoint_url=endpoint_uri)

    @pytest.fixture(scope="class")
    def _s3_base(self):
        # copy of s3_base in test_s3fs
        from moto.moto_server.threaded_moto_server import ThreadedMotoServer

        server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
        server.start()
        if "AWS_SECRET_ACCESS_KEY" not in os.environ:
            os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
        if "AWS_ACCESS_KEY_ID" not in os.environ:
            os.environ["AWS_ACCESS_KEY_ID"] = "foo"

        print("server up")
        yield
        print("moto done")
        server.stop()
