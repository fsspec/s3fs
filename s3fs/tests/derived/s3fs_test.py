import pytest

import fsspec.tests.abstract as abstract
from s3fs.tests.derived.s3fs_fixtures import S3fsFixtures


class TestS3fsCopy(abstract.AbstractCopyTests, S3fsFixtures):
    pass


class TestS3fsGet(abstract.AbstractGetTests, S3fsFixtures):
    pass


class TestS3fsPut(abstract.AbstractPutTests, S3fsFixtures):
    pass


def botocore_too_old():
    import botocore
    from packaging.version import parse

    MIN_BOTOCORE_VERSION = "1.33.2"

    return parse(botocore.__version__) < parse(MIN_BOTOCORE_VERSION)


class TestS3fsPipe(abstract.AbstractPipeTests, S3fsFixtures):

    test_pipe_exclusive = pytest.mark.skipif(
        botocore_too_old(), reason="Older botocore doesn't support exclusive writes"
    )(abstract.AbstractPipeTests.test_pipe_exclusive)


class TestS3fsOpen(abstract.AbstractOpenTests, S3fsFixtures):

    test_open_exclusive = pytest.mark.xfail(
        reason="complete_multipart_upload doesn't implement condition in moto"
    )(abstract.AbstractOpenTests.test_open_exclusive)
