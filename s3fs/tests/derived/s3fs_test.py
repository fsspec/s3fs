import pytest

import fsspec.tests.abstract as abstract
from s3fs.tests.derived.s3fs_fixtures import S3fsFixtures


class TestS3fsCopy(abstract.AbstractCopyTests, S3fsFixtures):
    pass


class TestS3fsGet(abstract.AbstractGetTests, S3fsFixtures):
    pass


class TestS3fsPut(abstract.AbstractPutTests, S3fsFixtures):
    pass


class TestS3fsPipe(abstract.AbstractPipeTests, S3fsFixtures):
    pass


class TestS3fsOpen(abstract.AbstractOpenTests, S3fsFixtures):

    test_open_exclusive = pytest.mark.xfail(
        reason="complete_multipart_upload doesn't implement condition in moto"
    )(abstract.AbstractOpenTests.test_open_exclusive)
