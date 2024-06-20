import pytest
from s3fs.tests.test_s3fs import s3_base, s3, test_bucket_name
from s3fs import S3Map, S3FileSystem

root = test_bucket_name + "/mapping"


def test_simple(s3):
    d = s3.get_mapper(root)
    assert not d

    assert list(d) == list(d.keys()) == []
    assert list(d.values()) == []
    assert list(d.items()) == []
    s3.get_mapper(root)

    try:
        # Make an operation that raises IOError or OSError
        f = d["nonexistent"]
        print(f)
    except OSError as e:
        # verify error details
        ...

