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


def test_default_s3filesystem(s3):
    d = s3.get_mapper(root)
    assert d.fs is s3


def test_errors(s3):
    d = s3.get_mapper(root)
    with pytest.raises(KeyError):
        d["nonexistent"]

    try:
        s3.get_mapper("does-not-exist", check=True)
    except Exception as e:
        assert "does-not-exist" in str(e)


def test_with_data(s3):
    d = s3.get_mapper(root)
    d["x"] = b"123"
    assert list(d) == list(d.keys()) == ["x"]
    assert list(d.values()) == [b"123"]
    assert list(d.items()) == [("x", b"123")]
    assert d["x"] == b"123"
    assert bool(d)

    assert s3.find(root) == [test_bucket_name + "/mapping/x"]
    d["x"] = b"000"
    assert d["x"] == b"000"

    d["y"] = b"456"
    assert d["y"] == b"456"
    assert set(d) == {"x", "y"}

    d.clear()
    assert list(d) == []


def test_complex_keys(s3):
    d = s3.get_mapper(root)
    d[1] = b"hello"
    assert d[1] == b"hello"
    del d[1]

    d[1, 2] = b"world"
    assert d[1, 2] == b"world"
    del d[1, 2]

    d["x", 1, 2] = b"hello world"
    assert d["x", 1, 2] == b"hello world"

    assert ("x", 1, 2) in d


def test_clear_empty(s3):
    d = s3.get_mapper(root)
    d.clear()
    assert list(d) == []
    d[1] = b"1"
    assert list(d) == ["1"]
    d.clear()
    assert list(d) == []


def test_no_dircache(s3):
    from s3fs.tests.test_s3fs import endpoint_uri
    import fsspec

    d = fsspec.get_mapper(
        "s3://" + root,
        anon=False,
        client_kwargs={"endpoint_url": endpoint_uri},
        use_listings_cache=False,
    )
    d.clear()
    assert list(d) == []
    d[1] = b"1"
    assert list(d) == ["1"]
    d.clear()
    assert list(d) == []


def test_pickle(s3):
    d = s3.get_mapper(root)
    d["x"] = b"1"

    import pickle

    d2 = pickle.loads(pickle.dumps(d))

    assert d2["x"] == b"1"


def test_array(s3):
    from array import array

    d = s3.get_mapper(root)
    d["x"] = array("B", [65] * 1000)

    assert d["x"] == b"A" * 1000


def test_bytearray(s3):
    d = s3.get_mapper(root)
    d["x"] = bytearray(b"123")

    assert d["x"] == b"123"


def test_new_bucket(s3):
    try:
        s3.get_mapper("new-bucket", check=True)
        assert False
    except ValueError as e:
        assert "create" in str(e)

    d = s3.get_mapper("new-bucket", create=True)
    assert not d

    d = s3.get_mapper("new-bucket/new-directory")
    assert not d


def test_old_api(s3):
    import fsspec.mapping

    assert isinstance(S3Map(root, s3), fsspec.mapping.FSMap)
