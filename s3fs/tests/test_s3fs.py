# -*- coding: utf-8 -*-
from contextlib import contextmanager
import errno
import json
from concurrent.futures import ProcessPoolExecutor
import io
import time
import pytest
from itertools import chain
import fsspec.core
from s3fs.core import S3FileSystem
from s3fs.utils import ignoring, SSEParams
import moto
import boto3
from unittest import mock
from botocore.exceptions import NoCredentialsError

test_bucket_name = 'test'
secure_bucket_name = 'test-secure'
versioned_bucket_name = 'test-versioned'
files = {'test/accounts.1.json': (b'{"amount": 100, "name": "Alice"}\n'
                                  b'{"amount": 200, "name": "Bob"}\n'
                                  b'{"amount": 300, "name": "Charlie"}\n'
                                  b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json': (b'{"amount": 500, "name": "Alice"}\n'
                                  b'{"amount": 600, "name": "Bob"}\n'
                                  b'{"amount": 700, "name": "Charlie"}\n'
                                  b'{"amount": 800, "name": "Dennis"}\n')}

csv_files = {'2014-01-01.csv': (b'name,amount,id\n'
                                b'Alice,100,1\n'
                                b'Bob,200,2\n'
                                b'Charlie,300,3\n'),
             '2014-01-02.csv': (b'name,amount,id\n'),
             '2014-01-03.csv': (b'name,amount,id\n'
                                b'Dennis,400,4\n'
                                b'Edith,500,5\n'
                                b'Frank,600,6\n')}
text_files = {'nested/file1': b'hello\n',
              'nested/file2': b'world',
              'nested/nested2/file1': b'hello\n',
              'nested/nested2/file2': b'world'}
glob_files = {'file.dat': b'',
              'filexdat': b''}
a = test_bucket_name + '/tmp/test/a'
b = test_bucket_name + '/tmp/test/b'
c = test_bucket_name + '/tmp/test/c'
d = test_bucket_name + '/tmp/test/d'


@pytest.yield_fixture
def s3():
    # writable local S3 system
    with moto.mock_s3():
        import boto3
        client = boto3.client('s3')
        client.create_bucket(Bucket=test_bucket_name, ACL='public-read')

        bucket = client.create_bucket(
            Bucket=versioned_bucket_name, ACL='public-read')
        bucket_versioning = boto3.resource(
            's3').BucketVersioning(versioned_bucket_name)
        bucket_versioning.enable()

        # initialize secure bucket
        bucket = client.create_bucket(
            Bucket=secure_bucket_name, ACL='public-read')
        policy = json.dumps({
            "Version": "2012-10-17",
            "Id": "PutObjPolicy",
            "Statement": [
                {
                    "Sid": "DenyUnEncryptedObjectUploads",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::{bucket_name}/*".format(
                        bucket_name=secure_bucket_name),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-server-side-encryption": "aws:kms"
                        }
                    }
                }
            ]
        })
        client.put_bucket_policy(Bucket=secure_bucket_name, Policy=policy)

        for k in [a, b, c, d]:
            try:
                client.delete_object(Bucket=test_bucket_name, Key=k)
            except:
                pass
        for flist in [files, csv_files, text_files, glob_files]:
            for f, data in flist.items():
                client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
        S3FileSystem.clear_instance_cache()
        s3 = S3FileSystem(anon=False)
        s3.invalidate_cache()
        yield s3
        for flist in [files, csv_files, text_files, glob_files]:
            for f, data in flist.items():
                try:
                    client.delete_object(
                        Bucket=test_bucket_name, Key=f, Body=data)
                    client.delete_object(
                        Bucket=secure_bucket_name, Key=f, Body=data)
                except:
                    pass
        for k in [a, b, c, d]:
            try:
                client.delete_object(Bucket=test_bucket_name, Key=k)
                client.delete_object(Bucket=secure_bucket_name, Key=k)
            except:
                pass


@contextmanager
def expect_errno(expected_errno):
    """Expect an OSError and validate its errno code."""
    with pytest.raises(OSError) as error:
        yield
    assert error.value.errno == expected_errno, 'OSError has wrong error code.'


def test_simple(s3):
    data = b'a' * (10 * 2 ** 20)

    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize('default_cache_type', ['none', 'bytes', 'mmap'])
def test_default_cache_type(s3, default_cache_type):
    data = b'a' * (10 * 2 ** 20)
    s3 = S3FileSystem(anon=False, default_cache_type=default_cache_type)

    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
        assert isinstance(f.cache, fsspec.core.caches[default_cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_ssl_off():
    s3 = S3FileSystem(use_ssl=False)
    assert s3.s3.meta.endpoint_url.startswith('http://')


def test_client_kwargs():
    s3 = S3FileSystem(client_kwargs={'endpoint_url': 'http://foo'})
    assert s3.s3.meta.endpoint_url.startswith('http://foo')


def test_config_kwargs():
    s3 = S3FileSystem(config_kwargs={'signature_version': 's3v4'})
    assert s3.connect(refresh=True).meta.config.signature_version == 's3v4'


def test_idempotent_connect(s3):
    con1 = s3.connect()
    con2 = s3.connect(refresh=False)
    con3 = s3.connect(refresh=True)
    assert con1 is con2
    assert con1 is not con3


def test_multiple_objects(s3):
    s3.connect()
    assert s3.ls('test')
    s32 = S3FileSystem(anon=False)
    assert s32.session
    assert s3.ls('test') == s32.ls('test')


def test_info(s3):
    s3.touch(a)
    s3.touch(b)
    assert s3.info(a) == s3.ls(a, detail=True)[0]
    parent = a.rsplit('/', 1)[0]
    s3.invalidate_cache()  # remove full path from the cache
    s3.ls(parent)  # fill the cache with parent dir
    assert s3.info(a) == s3.dircache[parent][0]  # correct value
    assert id(s3.info(a)) == id(s3.dircache[parent][0])  # is object from cache

    new_parent = test_bucket_name + '/foo'
    s3.mkdir(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)
    s3.ls(new_parent)
    with pytest.raises(FileNotFoundError):
        s3.info(new_parent)


test_xattr_sample_metadata = {'test_xattr': '1'}


def test_xattr(s3):
    bucket, key = (test_bucket_name, 'tmp/test/xattr')
    filename = bucket + '/' + key
    body = b'aaaa'
    public_read_acl = {'Permission': 'READ', 'Grantee': {
        'URI': 'http://acs.amazonaws.com/groups/global/AllUsers', 'Type': 'Group'}}

    s3.s3.put_object(Bucket=bucket, Key=key,
                     ACL='public-read',
                     Metadata=test_xattr_sample_metadata,
                     Body=body)

    # save etag for later
    etag = s3.info(filename)['ETag']
    assert public_read_acl in s3.s3.get_object_acl(
        Bucket=bucket, Key=key)['Grants']

    assert s3.getxattr(
        filename, 'test_xattr') == test_xattr_sample_metadata['test_xattr']
    assert s3.metadata(filename) == test_xattr_sample_metadata

    s3file = s3.open(filename)
    assert s3file.getxattr(
        'test_xattr') == test_xattr_sample_metadata['test_xattr']
    assert s3file.metadata() == test_xattr_sample_metadata

    s3file.setxattr(test_xattr='2')
    assert s3file.getxattr('test_xattr') == '2'
    s3file.setxattr(**{'test_xattr': None})
    assert s3file.metadata() == {}
    assert s3.cat(filename) == body

    # check that ACL and ETag are preserved after updating metadata
    assert public_read_acl in s3.s3.get_object_acl(
        Bucket=bucket, Key=key)['Grants']
    assert s3.info(filename)['ETag'] == etag


def test_xattr_setxattr_in_write_mode(s3):
    s3file = s3.open(a, 'wb')
    with pytest.raises(NotImplementedError):
        s3file.setxattr(test_xattr='1')


@pytest.mark.xfail()
def test_delegate(s3):
    out = s3.get_delegated_s3pars()
    assert out
    assert out['token']
    s32 = S3FileSystem(**out)
    assert not s32.anon
    assert out == s32.get_delegated_s3pars()


def test_not_delegate():
    s3 = S3FileSystem(anon=True)
    out = s3.get_delegated_s3pars()
    assert out == {'anon': True}
    s3 = S3FileSystem(anon=False)  # auto credentials
    out = s3.get_delegated_s3pars()
    assert out == {'anon': False}


def test_ls(s3):
    assert set(s3.ls('')) == {test_bucket_name,
                              secure_bucket_name, versioned_bucket_name}
    with pytest.raises(FileNotFoundError):
        s3.ls('nonexistent')
    fn = test_bucket_name + '/test/accounts.1.json'
    assert fn in s3.ls(test_bucket_name + '/test')


def test_pickle(s3):
    import pickle
    s32 = pickle.loads(pickle.dumps(s3))
    assert s3.ls('test') == s32.ls('test')
    s33 = pickle.loads(pickle.dumps(s32))
    assert s3.ls('test') == s33.ls('test')


def test_ls_touch(s3):
    assert not s3.exists(test_bucket_name + '/tmp/test')
    s3.touch(a)
    s3.touch(b)
    L = s3.ls(test_bucket_name + '/tmp/test', True)
    assert {d['Key'] for d in L} == {a, b}
    L = s3.ls(test_bucket_name + '/tmp/test', False)
    assert set(L) == {a, b}


@pytest.mark.parametrize('version_aware', [True, False])
def test_exists_versioned(s3, version_aware):
    """Test to ensure that a prefix exists when using a versioned bucket"""
    import uuid
    n = 3
    s3 = S3FileSystem(anon=False, version_aware=version_aware)
    segments = [versioned_bucket_name] + [str(uuid.uuid4()) for _ in range(n)]
    path = '/'.join(segments)
    for i in range(2, n+1):
        assert not s3.exists('/'.join(segments[:i]))
    s3.touch(path)
    for i in range(2, n+1):
        assert s3.exists('/'.join(segments[:i]))


def test_isfile(s3):
    assert not s3.isfile('')
    assert not s3.isfile('/')
    assert not s3.isfile(test_bucket_name)
    assert not s3.isfile(test_bucket_name + '/test')

    assert not s3.isfile(test_bucket_name + '/test/foo')
    assert s3.isfile(test_bucket_name + '/test/accounts.1.json')
    assert s3.isfile(test_bucket_name + '/test/accounts.2.json')

    assert not s3.isfile(a)
    s3.touch(a)
    assert s3.isfile(a)

    assert not s3.isfile(b)
    assert not s3.isfile(b + '/')
    s3.mkdir(b)
    assert not s3.isfile(b)
    assert not s3.isfile(b + '/')

    assert not s3.isfile(c)
    assert not s3.isfile(c + '/')
    s3.mkdir(c + '/')
    assert not s3.isfile(c)
    assert not s3.isfile(c + '/')


def test_isdir(s3):
    assert s3.isdir('')
    assert s3.isdir('/')
    assert s3.isdir(test_bucket_name)
    assert s3.isdir(test_bucket_name + '/test')

    assert not s3.isdir(test_bucket_name + '/test/foo')
    assert not s3.isdir(test_bucket_name + '/test/accounts.1.json')
    assert not s3.isdir(test_bucket_name + '/test/accounts.2.json')

    assert not s3.isdir(a)
    s3.touch(a)
    assert not s3.isdir(a)

    assert not s3.isdir(b)
    assert not s3.isdir(b + '/')

    assert not s3.isdir(c)
    assert not s3.isdir(c + '/')

    # test cache
    s3.invalidate_cache()
    assert not s3.dircache
    s3.ls(test_bucket_name + '/nested')
    assert test_bucket_name + '/nested' in s3.dircache
    assert not s3.isdir(test_bucket_name + '/nested/file1')
    assert not s3.isdir(test_bucket_name + '/nested/file2')
    assert s3.isdir(test_bucket_name + '/nested/nested2')
    assert s3.isdir(test_bucket_name + '/nested/nested2/')


def test_rm(s3):
    assert not s3.exists(a)
    s3.touch(a)
    assert s3.exists(a)
    s3.rm(a)
    assert not s3.exists(a)
    with pytest.raises(FileNotFoundError):
        s3.rm(test_bucket_name + '/nonexistent')
    with pytest.raises(FileNotFoundError):
        s3.rm('nonexistent')
    s3.rm(test_bucket_name + '/nested', recursive=True)
    assert not s3.exists(test_bucket_name + '/nested/nested2/file1')

    # whole bucket
    s3.rm(test_bucket_name, recursive=True)
    assert not s3.exists(test_bucket_name + '/2014-01-01.csv')
    assert not s3.exists(test_bucket_name)


def test_rmdir(s3):
    bucket = 'test1_bucket'
    s3.mkdir(bucket)
    s3.rmdir(bucket)
    assert bucket not in s3.ls('/')


def test_mkdir(s3):
    bucket = 'test1_bucket'
    s3.mkdir(bucket)
    assert bucket in s3.ls('/')


def test_mkdir_region_name(s3):
    bucket = 'test1_bucket'
    s3.mkdir(bucket, region_name="eu-central-1")
    assert bucket in s3.ls('/')


def test_mkdir_client_region_name():
    bucket = 'test1_bucket'
    try:
        m = moto.mock_s3()
        m.start()
        s3 = S3FileSystem(anon=False, client_kwargs={"region_name":
                                                         "eu-central-1"})
        s3.mkdir(bucket)
        assert bucket in s3.ls('/')
    finally:
        m.stop()


def test_bulk_delete(s3):
    with pytest.raises(FileNotFoundError):
        s3.bulk_delete(['nonexistent/file'])
    with pytest.raises(ValueError):
        s3.bulk_delete(['bucket1/file', 'bucket2/file'])
    filelist = s3.find(test_bucket_name+'/nested')
    s3.bulk_delete(filelist)
    assert not s3.exists(test_bucket_name + '/nested/nested2/file1')


def test_anonymous_access():
    with ignoring(NoCredentialsError):
        s3 = S3FileSystem(anon=True)
        assert s3.ls('') == []
        # TODO: public bucket doesn't work through moto

    with pytest.raises(PermissionError):
        s3.mkdir('newbucket')


def test_s3_file_access(s3):
    fn = test_bucket_name + '/nested/file1'
    data = b'hello\n'
    assert s3.cat(fn) == data
    assert s3.head(fn, 3) == data[:3]
    assert s3.tail(fn, 3) == data[-3:]
    assert s3.tail(fn, 10000) == data


def test_s3_file_info(s3):
    fn = test_bucket_name + '/nested/file1'
    data = b'hello\n'
    assert fn in s3.find(test_bucket_name)
    assert s3.exists(fn)
    assert not s3.exists(fn + 'another')
    assert s3.info(fn)['Size'] == len(data)
    with pytest.raises(FileNotFoundError):
        s3.info(fn + 'another')


def test_bucket_exists(s3):
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name + 'x')
    s3 = S3FileSystem(anon=True)
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name + 'x')


def test_du(s3):
    d = s3.du(test_bucket_name, total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert test_bucket_name + '/nested/file1' in d

    assert s3.du(test_bucket_name + '/test/', total=True) == \
           sum(map(len, files.values()))
    assert s3.du(test_bucket_name) == s3.du('s3://' + test_bucket_name)


def test_s3_ls(s3):
    fn = test_bucket_name + '/nested/file1'
    assert fn not in s3.ls(test_bucket_name + '/')
    assert fn in s3.ls(test_bucket_name + '/nested/')
    assert fn in s3.ls(test_bucket_name + '/nested')
    assert s3.ls('s3://' + test_bucket_name +
                 '/nested/') == s3.ls(test_bucket_name + '/nested')


def test_s3_big_ls(s3):
    for x in range(1200):
        s3.touch(test_bucket_name + '/thousand/%i.part' % x)
    assert len(s3.find(test_bucket_name)) > 1200
    s3.rm(test_bucket_name + '/thousand/', recursive=True)
    assert len(s3.find(test_bucket_name + '/thousand/')) == 0


def test_s3_ls_detail(s3):
    L = s3.ls(test_bucket_name + '/nested', detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_s3_glob(s3):
    fn = test_bucket_name + '/nested/file1'
    assert fn not in s3.glob(test_bucket_name + '/')
    assert fn not in s3.glob(test_bucket_name + '/*')
    assert fn not in s3.glob(test_bucket_name + '/nested')
    assert fn in s3.glob(test_bucket_name + '/nested/*')
    assert fn in s3.glob(test_bucket_name + '/nested/file*')
    assert fn in s3.glob(test_bucket_name + '/*/*')
    assert all(any(p.startswith(f + '/') or p == f
                   for p in s3.find(test_bucket_name))
               for f in s3.glob(test_bucket_name + '/nested/*'))
    assert [test_bucket_name +
            '/nested/nested2'] == s3.glob(test_bucket_name + '/nested/nested2')
    assert ['test/nested/nested2/file1',
            'test/nested/nested2/file2'] == s3.glob(test_bucket_name + '/nested/nested2/*')

    with pytest.raises(ValueError):
        s3.glob('*')

    # Make sure glob() deals with the dot character (.) correctly.
    assert test_bucket_name + '/file.dat' in s3.glob(test_bucket_name + '/file.*')
    assert test_bucket_name + \
           '/filexdat' not in s3.glob(test_bucket_name + '/file.*')


def test_get_list_of_summary_objects(s3):
    L = s3.ls(test_bucket_name + '/test')

    assert len(L) == 2
    assert [l.lstrip(test_bucket_name).lstrip('/')
            for l in sorted(L)] == sorted(list(files))

    L2 = s3.ls('s3://' + test_bucket_name + '/test')

    assert L == L2


def test_read_keys_from_bucket(s3):
    for k, data in files.items():
        file_contents = s3.cat('/'.join([test_bucket_name, k]))
        assert file_contents == data

        assert (s3.cat('/'.join([test_bucket_name, k])) ==
                s3.cat('s3://' + '/'.join([test_bucket_name, k])))


@pytest.mark.xfail(reason="misbehaves in modern versions of moto?")
def test_url(s3):
    fn = test_bucket_name + '/nested/file1'
    url = s3.url(fn, expires=100)
    assert 'http' in url
    import urllib.parse
    components = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(components.query)
    exp = int(query['Expires'][0])

    delta = abs(exp - time.time() - 100)
    assert delta < 5

    with s3.open(fn) as f:
        assert 'http' in f.url()


def test_seek(s3):
    with s3.open(a, 'wb') as f:
        f.write(b'123')

    with s3.open(a) as f:
        f.seek(1000)
        with pytest.raises(ValueError):
            f.seek(-1)
        with pytest.raises(ValueError):
            f.seek(-5, 2)
        with pytest.raises(ValueError):
            f.seek(0, 10)
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(3)
        assert f.read(1) == b''
        f.seek(-1, 2)
        assert f.read(1) == b'3'
        f.seek(-1, 1)
        f.seek(-1, 1)
        assert f.read(1) == b'2'
        for i in range(4):
            assert f.seek(i) == i


def test_bad_open(s3):
    with pytest.raises(ValueError):
        s3.open('')


def test_copy(s3):
    fn = test_bucket_name + '/test/accounts.1.json'
    s3.copy(fn, fn + '2')
    assert s3.cat(fn) == s3.cat(fn + '2')


def test_move(s3):
    fn = test_bucket_name + '/test/accounts.1.json'
    data = s3.cat(fn)
    s3.mv(fn, fn + '2')
    assert s3.cat(fn + '2') == data
    assert not s3.exists(fn)


def test_get_put(s3, tmpdir):
    test_file = str(tmpdir.join('test.json'))

    s3.get(test_bucket_name + '/test/accounts.1.json', test_file)
    data = files['test/accounts.1.json']
    assert open(test_file, 'rb').read() == data
    s3.put(test_file, test_bucket_name + '/temp')
    assert s3.du(test_bucket_name +
                 '/temp', total=False)[test_bucket_name + '/temp'] == len(data)
    assert s3.cat(test_bucket_name + '/temp') == data


def test_errors(s3):
    with pytest.raises(FileNotFoundError):
        s3.open(test_bucket_name + '/tmp/test/shfoshf', 'rb')

    # This is fine, no need for interleaving directories on S3
    # with pytest.raises((IOError, OSError)):
    #    s3.touch('tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        s3.rm(test_bucket_name + '/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        s3.mv(test_bucket_name + '/tmp/test/shfoshf/x', 'tmp/test/shfoshf/y')

    with pytest.raises(ValueError):
        s3.open('x', 'rb')

    with pytest.raises(FileNotFoundError):
        s3.rm('unknown')

    with pytest.raises(ValueError):
        with s3.open(test_bucket_name + '/temp', 'wb') as f:
            f.read()

    with pytest.raises(ValueError):
        f = s3.open(test_bucket_name + '/temp', 'rb')
        f.close()
        f.read()

    with pytest.raises(ValueError):
        s3.mkdir('/')

    with pytest.raises(ValueError):
        s3.find('')

    with pytest.raises(ValueError):
        s3.find('s3://')


def test_read_small(s3):
    fn = test_bucket_name + '/2014-01-01.csv'
    with s3.open(fn, 'rb', block_size=10) as f:
        out = []
        while True:
            data = f.read(3)
            if data == b'':
                break
            out.append(data)
        assert s3.cat(fn) == b''.join(out)
        # cache drop
        assert len(f.cache) < len(out)


def test_read_s3_block(s3):
    data = files['test/accounts.1.json']
    lines = io.BytesIO(data).readlines()
    path = test_bucket_name + '/test/accounts.1.json'
    assert s3.read_block(path, 1, 35, b'\n') == lines[1]
    assert s3.read_block(path, 0, 30, b'\n') == lines[0]
    assert s3.read_block(path, 0, 35, b'\n') == lines[0] + lines[1]
    assert s3.read_block(path, 0, 5000, b'\n') == data
    assert len(s3.read_block(path, 0, 5)) == 5
    assert len(s3.read_block(path, 4, 5000)) == len(data) - 4
    assert s3.read_block(path, 5000, 5010) == b''

    assert s3.read_block(path, 5, None) == s3.read_block(path, 5, 1000)


def test_new_bucket(s3):
    assert not s3.exists('new')
    s3.mkdir('new')
    assert s3.exists('new')
    with s3.open('new/temp', 'wb') as f:
        f.write(b'hello')
    with expect_errno(errno.ENOTEMPTY):
        s3.rmdir('new')

    s3.rm('new/temp')
    s3.rmdir('new')
    assert 'new' not in s3.ls('')
    assert not s3.exists('new')
    with pytest.raises(FileNotFoundError):
        s3.ls('new')


def test_dynamic_add_rm(s3):
    s3.mkdir('one')
    s3.mkdir('one/two')
    assert s3.exists('one')
    s3.ls('one')
    s3.touch("one/two/file_a")
    assert s3.exists('one/two/file_a')
    s3.rm('one', recursive=True)
    assert not s3.exists('one')


def test_write_small(s3):
    with s3.open(test_bucket_name + '/test', 'wb') as f:
        f.write(b'hello')
    assert s3.cat(test_bucket_name + '/test') == b'hello'
    s3.open(test_bucket_name + '/test', 'wb').close()
    assert s3.info(test_bucket_name + '/test')['Size'] == 0


def test_write_large(s3):
    "flush() chunks buffer when processing large singular payload"
    mb = 2 ** 20
    payload_size = int(2.5 * 5 * mb)
    payload = b'0' * payload_size

    with s3.open(test_bucket_name + '/test', 'wb') as fd, \
         mock.patch.object(s3, '_call_s3', side_effect=s3._call_s3) as s3_mock:
        fd.write(payload)

    upload_parts = s3_mock.mock_calls[1:]
    upload_sizes = [len(upload_part[2]['Body']) for upload_part in upload_parts]
    assert upload_sizes == [5 * mb, int(7.5 * mb)]

    assert s3.cat(test_bucket_name + '/test') == payload

    assert s3.info(test_bucket_name + '/test')['Size'] == payload_size


def test_write_limit(s3):
    "flush() respects part_max when processing large singular payload"
    mb = 2 ** 20
    block_size = 15 * mb
    part_max = 28 * mb
    payload_size = 44 * mb
    payload = b'0' * payload_size

    with s3.open(test_bucket_name + '/test', 'wb') as fd, \
         mock.patch('s3fs.core.S3File.part_max', new=part_max), \
         mock.patch.object(s3, '_call_s3', side_effect=s3._call_s3) as s3_mock:
        fd.blocksize = block_size
        fd.write(payload)

    upload_parts = s3_mock.mock_calls[1:]
    upload_sizes = [len(upload_part[2]['Body']) for upload_part in upload_parts]
    assert upload_sizes == [block_size, int(14.5 * mb), int(14.5 * mb)]

    assert s3.cat(test_bucket_name + '/test') == payload

    assert s3.info(test_bucket_name + '/test')['Size'] == payload_size


def test_write_small_secure(s3):
    # Unfortunately moto does not yet support enforcing SSE policies.  It also
    # does not return the correct objects that can be used to test the results
    # effectively.
    # This test is left as a placeholder in case moto eventually supports this.
    sse_params = SSEParams(server_side_encryption='aws:kms')
    with s3.open(secure_bucket_name + '/test', 'wb', writer_kwargs=sse_params) as f:
        f.write(b'hello')
    assert s3.cat(secure_bucket_name + '/test') == b'hello'
    head = s3.s3.head_object(Bucket=secure_bucket_name, Key='test')


def test_write_large_secure(s3):
    s3_mock = moto.mock_s3()
    s3_mock.start()

    # build our own s3fs with the relevant additional kwarg
    s3 = S3FileSystem(s3_additional_kwargs={'ServerSideEncryption': 'AES256'})
    s3.mkdir('mybucket')

    with s3.open('mybucket/myfile', 'wb') as f:
        f.write(b'hello hello' * 10 ** 6)

    assert s3.cat('mybucket/myfile') == b'hello hello' * 10 ** 6


def test_write_fails(s3):
    with pytest.raises(ValueError):
        s3.touch(test_bucket_name + '/temp')
        s3.open(test_bucket_name + '/temp', 'rb').write(b'hello')
    with pytest.raises(ValueError):
        s3.open(test_bucket_name + '/temp', 'wb', block_size=10)
    f = s3.open(test_bucket_name + '/temp', 'wb')
    f.close()
    with pytest.raises(ValueError):
        f.write(b'hello')
    with pytest.raises(FileNotFoundError):
        s3.open('nonexistentbucket/temp', 'wb').close()


def test_write_blocks(s3):
    with s3.open(test_bucket_name + '/temp', 'wb') as f:
        f.write(b'a' * 2 * 2 ** 20)
        assert f.buffer.tell() == 2 * 2 ** 20
        assert not (f.parts)
        f.flush()
        assert f.buffer.tell() == 2 * 2 ** 20
        assert not (f.parts)
        f.write(b'a' * 2 * 2 ** 20)
        f.write(b'a' * 2 * 2 ** 20)
        assert f.mpu
        assert f.parts
    assert s3.info(test_bucket_name + '/temp')['Size'] == 6 * 2 ** 20
    with s3.open(test_bucket_name + '/temp', 'wb', block_size=10 * 2 ** 20) as f:
        f.write(b'a' * 15 * 2 ** 20)
        assert f.buffer.tell() == 0
    assert s3.info(test_bucket_name + '/temp')['Size'] == 15 * 2 ** 20


def test_readline(s3):
    all_items = chain.from_iterable([
        files.items(), csv_files.items(), text_files.items()
    ])
    for k, data in all_items:
        with s3.open('/'.join([test_bucket_name, k]), 'rb') as f:
            result = f.readline()
            expected = data.split(b'\n')[0] + (b'\n' if data.count(b'\n')
                                               else b'')
            assert result == expected


def test_readline_empty(s3):
    data = b''
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a, 'rb') as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(s3):
    data = b'ab\n' + b'a' * (10 * 2 ** 20) + b'\nab'
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a, 'rb') as f:
        result = f.readline()
        expected = b'ab\n'
        assert result == expected

        result = f.readline()
        expected = b'a' * (10 * 2 ** 20) + b'\n'
        assert result == expected

        result = f.readline()
        expected = b'ab'
        assert result == expected


def test_next(s3):
    expected = csv_files['2014-01-01.csv'].split(b'\n')[0] + b'\n'
    with s3.open(test_bucket_name + '/2014-01-01.csv') as f:
        result = next(f)
        assert result == expected


def test_iterable(s3):
    data = b'abc\n123'
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a) as f, io.BytesIO(data) as g:
        for froms3, fromio in zip(f, g):
            assert froms3 == fromio
        f.seek(0)
        assert f.readline() == b'abc\n'
        assert f.readline() == b'123'
        f.seek(1)
        assert f.readline() == b'bc\n'

    with s3.open(a) as f:
        out = list(f)
    with s3.open(a) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_readable(s3):
    with s3.open(a, 'wb') as f:
        assert not f.readable()

    with s3.open(a, 'rb') as f:
        assert f.readable()


def test_seekable(s3):
    with s3.open(a, 'wb') as f:
        assert not f.seekable()

    with s3.open(a, 'rb') as f:
        assert f.seekable()


def test_writable(s3):
    with s3.open(a, 'wb') as f:
        assert f.writable()

    with s3.open(a, 'rb') as f:
        assert not f.writable()


def test_merge(s3):
    with s3.open(a, 'wb') as f:
        f.write(b'a' * 10 * 2 ** 20)

    with s3.open(b, 'wb') as f:
        f.write(b'a' * 10 * 2 ** 20)
    s3.merge(test_bucket_name + '/joined', [a, b])
    assert s3.info(test_bucket_name + '/joined')['Size'] == 2 * 10 * 2 ** 20


def test_append(s3):
    data = text_files['nested/file1']
    with s3.open(test_bucket_name + '/nested/file1', 'ab') as f:
        assert f.tell() == len(data)  # append, no write, small file
    assert s3.cat(test_bucket_name + '/nested/file1') == data
    with s3.open(test_bucket_name + '/nested/file1', 'ab') as f:
        f.write(b'extra')  # append, write, small file
    assert s3.cat(test_bucket_name + '/nested/file1') == data + b'extra'

    with s3.open(a, 'wb') as f:
        f.write(b'a' * 10 * 2 ** 20)
    with s3.open(a, 'ab') as f:
        pass  # append, no write, big file
    assert s3.cat(a) == b'a' * 10 * 2 ** 20

    with s3.open(a, 'ab') as f:
        assert f.parts is None
        f._initiate_upload()
        assert f.parts
        assert f.tell() == 10 * 2 ** 20
        f.write(b'extra')  # append, small write, big file
    assert s3.cat(a) == b'a' * 10 * 2 ** 20 + b'extra'

    with s3.open(a, 'ab') as f:
        assert f.tell() == 10 * 2 ** 20 + 5
        f.write(b'b' * 10 * 2 ** 20)  # append, big write, big file
        assert f.tell() == 20 * 2 ** 20 + 5
    assert s3.cat(a) == b'a' * 10 * 2 ** 20 + b'extra' + b'b' * 10 * 2 ** 20


def test_bigger_than_block_read(s3):
    with s3.open(test_bucket_name + '/2014-01-01.csv', 'rb', block_size=3) as f:
        out = []
        while True:
            data = f.read(20)
            out.append(data)
            if len(data) == 0:
                break
    assert b''.join(out) == csv_files['2014-01-01.csv']


def test_current(s3):
    s3._cache.clear()
    s3 = S3FileSystem()
    assert s3.current() is s3
    assert S3FileSystem.current() is s3


def test_array(s3):
    from array import array
    data = array('B', [65] * 1000)

    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
        out = f.read()
        assert out == b'A' * 1000


def _get_s3_id(s3):
    return id(s3.s3)


def test_no_connection_sharing_among_processes(s3):
    executor = ProcessPoolExecutor()
    conn_id = executor.submit(_get_s3_id, s3).result()
    assert id(s3.connect()) != conn_id, \
        "Processes should not share S3 connections."


@pytest.mark.xfail()
def test_public_file(s3):
    # works on real s3, not on moto
    try:
        test_bucket_name = 's3fs_public_test'
        other_bucket_name = 's3fs_private_test'

        s3.touch(test_bucket_name)
        s3.touch(test_bucket_name + '/afile')
        s3.touch(other_bucket_name, acl='public-read')
        s3.touch(other_bucket_name + '/afile', acl='public-read')

        s = S3FileSystem(anon=True)
        with pytest.raises(PermissionError):
            s.ls(test_bucket_name)
        s.ls(other_bucket_name)

        s3.chmod(test_bucket_name, acl='public-read')
        s3.chmod(other_bucket_name, acl='private')
        with pytest.raises(PermissionError):
            s.ls(other_bucket_name, refresh=True)
        assert s.ls(test_bucket_name, refresh=True)

        # public file in private bucket
        with s3.open(other_bucket_name + '/see_me', 'wb', acl='public-read') as f:
            f.write(b'hello')
        assert s.cat(other_bucket_name + '/see_me') == b'hello'
    finally:
        s3.rm(test_bucket_name, recursive=True)
        s3.rm(other_bucket_name, recursive=True)


def test_upload_with_s3fs_prefix(s3):
    path = 's3://test/prefix/key'

    with s3.open(path, 'wb') as f:
        f.write(b'a' * (10 * 2 ** 20))

    with s3.open(path, 'ab') as f:
        f.write(b'b' * (10 * 2 ** 20))


def test_multipart_upload_blocksize(s3):
    blocksize = 5 * (2 ** 20)
    expected_parts = 3

    s3f = s3.open(a, 'wb', block_size=blocksize)
    for _ in range(3):
        data = b'b' * blocksize
        s3f.write(data)

    # Ensure that the multipart upload consists of only 3 parts
    assert len(s3f.parts) == expected_parts
    s3f.close()


def test_default_pars(s3):
    s3 = S3FileSystem(default_block_size=20, default_fill_cache=False)
    fn = test_bucket_name + '/' + list(files)[0]
    with s3.open(fn) as f:
        assert f.blocksize == 20
        assert f.fill_cache is False
    with s3.open(fn, block_size=40, fill_cache=True) as f:
        assert f.blocksize == 40
        assert f.fill_cache is True


def test_tags(s3):
    tagset = {'tag1': 'value1', 'tag2': 'value2'}
    fname = list(files)[0]
    s3.touch(fname)
    s3.put_tags(fname, tagset)
    assert s3.get_tags(fname) == tagset

    # Ensure merge mode updates value of existing key and adds new one
    new_tagset = {'tag2': 'updatedvalue2', 'tag3': 'value3'}
    s3.put_tags(fname, new_tagset, mode='m')
    tagset.update(new_tagset)
    assert s3.get_tags(fname) == tagset


def test_versions(s3):
    versioned_file = versioned_bucket_name + '/versioned_file'
    s3 = S3FileSystem(anon=False, version_aware=True)
    with s3.open(versioned_file, 'wb') as fo:
        fo.write(b'1')
    with s3.open(versioned_file, 'wb') as fo:
        fo.write(b'2')
    versions = s3.object_version_info(versioned_file)
    version_ids = [version['VersionId'] for version in versions]
    assert len(version_ids) == 2

    with s3.open(versioned_file) as fo:
        assert fo.version_id == version_ids[1]
        assert fo.read() == b'2'

    with s3.open(versioned_file, version_id=version_ids[0]) as fo:
        assert fo.version_id == version_ids[0]
        assert fo.read() == b'1'


def test_list_versions_many(s3):
    # moto doesn't actually behave in the same way that s3 does here so this doesn't test
    # anything really in moto 1.2
    s3 = S3FileSystem(anon=False, version_aware=True)
    versioned_file = versioned_bucket_name + '/versioned_file2'
    for i in range(1200):
        with s3.open(versioned_file, 'wb') as fo:
            fo.write(b'1')
    versions = s3.object_version_info(versioned_file)
    assert len(versions) == 1200


def test_versions_unaware(s3):
    versioned_file = versioned_bucket_name + '/versioned_file3'
    s3 = S3FileSystem(anon=False, version_aware=False)
    with s3.open(versioned_file, 'wb') as fo:
        fo.write(b'1')
    with s3.open(versioned_file, 'wb') as fo:
        fo.write(b'2')

    with s3.open(versioned_file) as fo:
        assert fo.version_id is None
        assert fo.read() == b'2'

    with pytest.raises(ValueError):
        with s3.open(versioned_file, version_id='0'):
            fo.read()


def test_text_io__stream_wrapper_works(s3):
    """Ensure using TextIOWrapper works."""
    s3.mkdir('bucket')

    with s3.open('bucket/file.txt', 'wb') as fd:
        fd.write(u'\u00af\\_(\u30c4)_/\u00af'.encode('utf-16-le'))

    with s3.open('bucket/file.txt', 'rb') as fd:
        with io.TextIOWrapper(fd, 'utf-16-le') as stream:
            assert stream.readline() == u'\u00af\\_(\u30c4)_/\u00af'


def test_text_io__basic(s3):
    """Text mode is now allowed."""
    s3.mkdir('bucket')

    with s3.open('bucket/file.txt', 'w') as fd:
        fd.write(u'\u00af\\_(\u30c4)_/\u00af')

    with s3.open('bucket/file.txt', 'r') as fd:
        assert fd.read() == u'\u00af\\_(\u30c4)_/\u00af'


def test_text_io__override_encoding(s3):
    """Allow overriding the default text encoding."""
    s3.mkdir('bucket')

    with s3.open('bucket/file.txt', 'w', encoding='ibm500') as fd:
        fd.write(u'Hello, World!')

    with s3.open('bucket/file.txt', 'r', encoding='ibm500') as fd:
        assert fd.read() == u'Hello, World!'


def test_readinto(s3):
    s3.mkdir('bucket')

    with s3.open('bucket/file.txt', 'wb') as fd:
        fd.write(b'Hello, World!')

    contents = bytearray(15)

    with s3.open('bucket/file.txt', 'rb') as fd:
        assert fd.readinto(contents) == 13

    assert contents.startswith(b'Hello, World!')


def test_change_defaults_only_subsequent():
    """Test for Issue #135

    Ensure that changing the default block size doesn't affect existing file
    systems that were created using that default. It should only affect file
    systems created after the change.
    """
    try:
        S3FileSystem.cachable = False  # don't reuse instances with same pars

        fs_default = S3FileSystem()
        assert fs_default.default_block_size == 5 * (1024 ** 2)

        fs_overridden = S3FileSystem(default_block_size=64 * (1024 ** 2))
        assert fs_overridden.default_block_size == 64 * (1024 ** 2)

        # Suppose I want all subsequent file systems to have a block size of 1 GiB
        # instead of 5 MiB:
        S3FileSystem.default_block_size = 1024 ** 3

        fs_big = S3FileSystem()
        assert fs_big.default_block_size == 1024 ** 3

        # Test the other file systems created to see if their block sizes changed
        assert fs_overridden.default_block_size == 64 * (1024 ** 2)
        assert fs_default.default_block_size == 5 * (1024 ** 2)
    finally:
        S3FileSystem.default_block_size = 5 * (1024 ** 2)
        S3FileSystem.cachable = True


def test_passed_in_session_set_correctly(s3):
    session = boto3.session.Session()
    s3 = S3FileSystem(session=session)
    assert s3.passed_in_session is session
    client = s3.connect()
    assert s3.session is session


def test_without_passed_in_session_set_unique(s3):
    session = boto3.session.Session()
    s3 = S3FileSystem()
    assert s3.passed_in_session is None
    client = s3.connect()
    assert s3.session is not session


def test_pickle_without_passed_in_session(s3):
    import pickle
    s3 = S3FileSystem()
    pickle.dumps(s3)


def test_pickle_with_passed_in_session(s3):
    import pickle
    session = boto3.session.Session()
    s3 = S3FileSystem(session=session)
    with pytest.raises((AttributeError, NotImplementedError, TypeError)):
        pickle.dumps(s3)


def test_cache_after_copy(s3):
    # https://github.com/dask/dask/issues/5134
    s3.touch('test/afile')
    assert 'test/afile' in s3.ls('s3://test', False)
    s3.cp('test/afile', 'test/bfile')
    assert 'test/bfile' in s3.ls('s3://test', False)


def test_autocommit(s3):
    auto_file = test_bucket_name + '/auto_file'
    committed_file = test_bucket_name + '/commit_file'
    aborted_file = test_bucket_name + '/aborted_file'
    s3 = S3FileSystem(anon=False, version_aware=True)

    def write_and_flush(path, autocommit):
        with s3.open(path, 'wb', autocommit=autocommit) as fo:
            fo.write(b'1')
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

    fo = write_and_flush(aborted_file,autocommit=False)
    assert not s3.exists(aborted_file)
    fo.discard()
    assert not s3.exists(aborted_file)
    # Cannot commit a file that was discarded
    with pytest.raises(Exception):
        fo.commit()


def test_autocommit_mpu(s3):
    """When not autocommitting we always want to use multipart uploads"""
    path = test_bucket_name + '/auto_commit_with_mpu'
    with s3.open(path, 'wb', autocommit=True) as fo:
        fo.write(b'1')
    # fo.flush()
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
    with s3.open(fn, 'wb') as f:
        f.write(b'data')
    assert s3.size(fn) == 4
    s3.touch(fn, truncate=True)
    assert s3.size(fn) == 0

    # exists error
    with s3.open(fn, 'wb') as f:
        f.write(b'data')
    assert s3.size(fn) == 4
    with pytest.raises(ValueError):
        s3.touch(fn, truncate=False)
    assert s3.size(fn) == 4


def test_seek_reads(s3):
    fn = test_bucket_name + "/myfile"
    with s3.open(fn, 'wb') as f:
        f.write(b'a' * 175_627_146)
    with s3.open(fn, 'rb', blocksize=100) as f:
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


def test_connect_many():
    from multiprocessing.pool import ThreadPool

    def task(i):
        S3FileSystem(anon=False).ls("")
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


def test_requester_pays():
    fn = test_bucket_name + "/myfile"
    with moto.mock_s3():
        s3 = S3FileSystem(requester_pays=True)
        assert s3.req_kw["RequestPayer"] == "requester"
        s3.mkdir(test_bucket_name)
        s3.touch(fn)
        with s3.open(fn, "rb") as f:
            assert f.req_kw["RequestPayer"] == "requester"
