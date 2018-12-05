# -*- coding: utf-8 -*-
import json
from concurrent.futures import ProcessPoolExecutor
import io
import re
import time
import pytest
from itertools import chain
from s3fs.core import S3FileSystem
from s3fs.utils import seek_delimiter, ignoring, SSEParams
import moto
import boto3

from botocore.exceptions import NoCredentialsError

test_bucket_name = 'test'
secure_bucket_name = 'test-secure'
versioned_bucket_name = 'test-versioned'
files = {'test/accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
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
a = test_bucket_name+'/tmp/test/a'
b = test_bucket_name+'/tmp/test/b'
c = test_bucket_name+'/tmp/test/c'
d = test_bucket_name+'/tmp/test/d'


@pytest.yield_fixture
def s3():
    # writable local S3 system
    try:
        m = moto.mock_s3()
        m.start()
        import boto3
        client = boto3.client('s3')
        client.create_bucket(Bucket=test_bucket_name, ACL='public-read')

        bucket = client.create_bucket(Bucket=versioned_bucket_name, ACL='public-read')
        bucket_versioning = boto3.resource('s3').BucketVersioning(versioned_bucket_name)
        bucket_versioning.enable()

        # initialize secure bucket
        bucket = client.create_bucket(Bucket=secure_bucket_name, ACL='public-read')
        policy = json.dumps({
            "Version": "2012-10-17",
            "Id": "PutObjPolicy",
            "Statement": [
                {
                    "Sid": "DenyUnEncryptedObjectUploads",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::{bucket_name}/*".format(bucket_name=secure_bucket_name),
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
        yield S3FileSystem(anon=False)
        for flist in [files, csv_files, text_files, glob_files]:
            for f, data in flist.items():
                try:
                    client.delete_object(Bucket=test_bucket_name, Key=f, Body=data)
                    client.delete_object(Bucket=secure_bucket_name, Key=f, Body=data)
                except:
                    pass
        for k in [a, b, c, d]:
            try:
                client.delete_object(Bucket=test_bucket_name, Key=k)
                client.delete_object(Bucket=secure_bucket_name, Key=k)
            except:
                pass
    finally:
        m.stop()


def test_simple(s3):
    data = b'a' * (10 * 2**20)

    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
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


def test_tokenize():
    from s3fs.core import tokenize
    a = (1, 2, 3)
    assert isinstance(tokenize(a), (str, bytes))
    assert tokenize(a) != tokenize(a, other=1)


def test_idempotent_connect(s3):
    con1 = s3.connect()
    con2 = s3.connect()
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
    s3.dirs.pop(a)  # remove full path from the cache
    s3.ls(parent)  # fill the cache with parent dir
    assert s3.info(a) == s3.dirs[parent][0]  # correct value
    assert id(s3.info(a)) == id(s3.dirs[parent][0])  # is object from cache


test_xattr_sample_metadata = {'test_xattr': '1'}


def test_xattr(s3):
    bucket, key = (test_bucket_name, 'tmp/test/xattr')
    filename = bucket + '/' + key
    body = b'aaaa'
    public_read_acl = {'Permission': 'READ', 'Grantee': {'URI': 'http://acs.amazonaws.com/groups/global/AllUsers', 'Type': 'Group'}}

    s3.s3.put_object(Bucket=bucket, Key=key,
                     ACL='public-read',
                     Metadata=test_xattr_sample_metadata,
                     Body=body)

    # save etag for later
    etag = s3.info(filename)['ETag']
    assert public_read_acl in s3.s3.get_object_acl(Bucket=bucket, Key=key)['Grants']

    assert s3.getxattr(filename, 'test_xattr') == test_xattr_sample_metadata['test_xattr']
    assert s3.metadata(filename) == test_xattr_sample_metadata

    s3file = s3.open(filename)
    assert s3file.getxattr('test_xattr') == test_xattr_sample_metadata['test_xattr']
    assert s3file.metadata() == test_xattr_sample_metadata

    s3file.setxattr(test_xattr='2')
    assert s3file.getxattr('test_xattr') == '2'
    s3file.setxattr(**{'test_xattr': None})
    assert s3file.metadata() == {}
    assert s3.cat(filename) == body

    # check that ACL and ETag are preserved after updating metadata
    assert public_read_acl in s3.s3.get_object_acl(Bucket=bucket, Key=key)['Grants']
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
    assert set(s3.ls('')) == {test_bucket_name, secure_bucket_name, versioned_bucket_name}
    with pytest.raises((OSError, IOError)):
        s3.ls('nonexistent')
    fn = test_bucket_name+'/test/accounts.1.json'
    assert fn in s3.ls(test_bucket_name+'/test')


def test_pickle(s3):
    import pickle
    s32 = pickle.loads(pickle.dumps(s3))
    assert s3.ls('test') == s32.ls('test')
    s33 = pickle.loads(pickle.dumps(s32))
    assert s3.ls('test') == s33.ls('test')


def test_ls_touch(s3):
    assert not s3.exists(test_bucket_name+'/tmp/test')
    s3.touch(a)
    s3.touch(b)
    L = s3.ls(test_bucket_name+'/tmp/test', True)
    assert {d['Key'] for d in L} == {a, b}
    L = s3.ls(test_bucket_name+'/tmp/test', False)
    assert set(L) == {a, b}


def test_rm(s3):
    assert not s3.exists(a)
    s3.touch(a)
    assert s3.exists(a)
    s3.rm(a)
    assert not s3.exists(a)
    with pytest.raises((OSError, IOError)):
        s3.rm(test_bucket_name+'/nonexistent')
    with pytest.raises((OSError, IOError)):
        s3.rm('nonexistent')
    s3.rm(test_bucket_name+'/nested', recursive=True)
    assert not s3.exists(test_bucket_name+'/nested/nested2/file1')

    #whole bucket
    s3.rm(test_bucket_name, recursive=True)
    assert not s3.exists(test_bucket_name+'/2014-01-01.csv')
    assert not s3.exists(test_bucket_name)


def test_rmdir(s3):
    nested = a + "/nested"
    s3.touch(a)
    s3.touch(nested)
    s3.touch(b)

    with pytest.raises(IOError):  # doesn't delete non empty directory
        s3.rmdir(a)

    s3.rmdir(nested)
    s3.rmdir(a)
    assert a not in s3.ls(test_bucket_name)
    assert nested not in s3.ls(test_bucket_name)

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
    with pytest.raises((OSError, IOError)):
        s3.bulk_delete(['nonexistent/file'])
    with pytest.raises(ValueError):
        s3.bulk_delete(['bucket1/file', 'bucket2/file'])
    filelist = s3.walk(test_bucket_name+'/nested')
    s3.bulk_delete(filelist)
    assert not s3.exists(test_bucket_name+'/nested/nested2/file1')


def test_anonymous_access():
    with ignoring(NoCredentialsError):
        s3 = S3FileSystem(anon=True)
        assert s3.ls('') == []
        ## TODO: public bucket doesn't work through moto
    with pytest.raises((OSError, IOError)):
        s3.mkdir('newbucket')


def test_s3_file_access(s3):
    fn = test_bucket_name+'/nested/file1'
    data = b'hello\n'
    assert s3.cat(fn) == data
    assert s3.head(fn, 3) == data[:3]
    assert s3.tail(fn, 3) == data[-3:]
    assert s3.tail(fn, 10000) == data


def test_s3_file_info(s3):
    fn = test_bucket_name+'/nested/file1'
    data = b'hello\n'
    assert fn in s3.walk(test_bucket_name)
    assert s3.exists(fn)
    assert not s3.exists(fn+'another')
    assert s3.info(fn)['Size'] == len(data)
    with pytest.raises((OSError, IOError)):
        s3.info(fn+'another')


def test_bucket_exists(s3):
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name+'x')
    s3 = S3FileSystem(anon=True)
    assert s3.exists(test_bucket_name)
    assert not s3.exists(test_bucket_name+'x')


def test_du(s3):
    d = s3.du(test_bucket_name, deep=True)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert test_bucket_name+'/nested/file1' in d

    assert s3.du(test_bucket_name + '/test/', total=True) ==\
           sum(map(len, files.values()))
    assert s3.du(test_bucket_name) == s3.du('s3://'+test_bucket_name)


def test_s3_ls(s3):
    fn = test_bucket_name+'/nested/file1'
    assert fn not in s3.ls(test_bucket_name+'/')
    assert fn in s3.ls(test_bucket_name+'/nested/')
    assert fn in s3.ls(test_bucket_name+'/nested')
    assert s3.ls('s3://'+test_bucket_name+'/nested/') == s3.ls(test_bucket_name+'/nested')


def test_s3_big_ls(s3):
    for x in range(1200):
        s3.touch(test_bucket_name+'/thousand/%i.part'%x)
    assert len(s3.walk(test_bucket_name)) > 1200
    s3.rm(test_bucket_name+'/thousand/', recursive=True)
    assert len(s3.walk(test_bucket_name+'/thousand/')) == 0


def test_s3_ls_detail(s3):
    L = s3.ls(test_bucket_name+'/nested', detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_s3_glob(s3):
    fn = test_bucket_name+'/nested/file1'
    assert fn not in s3.glob(test_bucket_name+'/')
    assert fn not in s3.glob(test_bucket_name+'/*')
    assert fn not in s3.glob(test_bucket_name+'/nested')
    assert fn in s3.glob(test_bucket_name+'/nested/*')
    assert fn in s3.glob(test_bucket_name+'/nested/file*')
    assert fn in s3.glob(test_bucket_name+'/*/*')
    assert all(any(p.startswith(f + '/') or p == f
                   for p in s3.walk(test_bucket_name, directories=True))
                       for f in s3.glob(test_bucket_name+'/nested/*'))
    assert [test_bucket_name + '/nested/nested2'] == s3.glob(test_bucket_name + '/nested/nested2')
    assert [ 'test/nested/nested2/file1',
             'test/nested/nested2/file2'] == s3.glob(test_bucket_name + '/nested/nested2/*')

    with pytest.raises(ValueError):
        s3.glob('*')

    # Make sure glob() deals with the dot character (.) correctly.
    assert test_bucket_name+'/file.dat' in s3.glob(test_bucket_name+'/file.*')
    assert test_bucket_name+'/filexdat' not in s3.glob(test_bucket_name+'/file.*')

def test_get_list_of_summary_objects(s3):
    L = s3.ls(test_bucket_name + '/test')

    assert len(L) == 2
    assert [l.lstrip(test_bucket_name).lstrip('/') for l in sorted(L)] == sorted(list(files))

    L2 = s3.ls('s3://' + test_bucket_name + '/test')

    assert L == L2


def test_read_keys_from_bucket(s3):
    for k, data in files.items():
        file_contents = s3.cat('/'.join([test_bucket_name, k]))
        assert file_contents == data

        assert (s3.cat('/'.join([test_bucket_name, k])) ==
                s3.cat('s3://' + '/'.join([test_bucket_name, k])))


def test_url(s3):
    fn = test_bucket_name+'/nested/file1'
    url = s3.url(fn, expires=100)
    assert 'http' in url
    assert '/nested/file1' in url
    exp = int(re.match(".*?Expires=(\d+)", url).groups()[0])
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
    with pytest.raises(IOError):
        s3.open('')


def test_copy(s3):
    fn = test_bucket_name+'/test/accounts.1.json'
    s3.copy(fn, fn+'2')
    assert s3.cat(fn) == s3.cat(fn+'2')


def test_move(s3):
    fn = test_bucket_name+'/test/accounts.1.json'
    data = s3.cat(fn)
    s3.mv(fn, fn+'2')
    assert s3.cat(fn+'2') == data
    assert not s3.exists(fn)


def test_get_put(s3, tmpdir):
    test_file = str(tmpdir.join('test.json'))

    s3.get(test_bucket_name+'/test/accounts.1.json', test_file)
    data = files['test/accounts.1.json']
    assert open(test_file, 'rb').read() == data
    s3.put(test_file, test_bucket_name+'/temp')
    assert s3.du(test_bucket_name+'/temp')[test_bucket_name+'/temp'] == len(data)
    assert s3.cat(test_bucket_name+'/temp') == data


def test_errors(s3):
    with pytest.raises((IOError, OSError)):
        s3.open(test_bucket_name+'/tmp/test/shfoshf', 'rb')

    ## This is fine, no need for interleaving directories on S3
    #with pytest.raises((IOError, OSError)):
    #    s3.touch('tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        s3.rm(test_bucket_name+'/tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        s3.mv(test_bucket_name+'/tmp/test/shfoshf/x', 'tmp/test/shfoshf/y')

    with pytest.raises((IOError, OSError)):
        s3.open('x', 'rb')

    with pytest.raises(IOError):
        s3.rm('unknown')

    with pytest.raises(ValueError):
        with s3.open(test_bucket_name+'/temp', 'wb') as f:
            f.read()

    with pytest.raises(ValueError):
        f = s3.open(test_bucket_name+'/temp', 'rb')
        f.close()
        f.read()

    with pytest.raises((IOError, OSError)):
        s3.mkdir('/')

    with pytest.raises(ValueError):
        s3.walk('')

    with pytest.raises(ValueError):
        s3.walk('s3://')


def test_read_small(s3):
    fn = test_bucket_name+'/2014-01-01.csv'
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


def test_seek_delimiter(s3):
    fn = 'test/accounts.1.json'
    data = files[fn]
    with s3.open('/'.join([test_bucket_name, fn])) as f:
        seek_delimiter(f, b'}', 0)
        assert f.tell() == 0
        f.seek(1)
        seek_delimiter(f, b'}', 5)
        assert f.tell() == data.index(b'}') + 1
        seek_delimiter(f, b'\n', 5)
        assert f.tell() == data.index(b'\n') + 1
        f.seek(1, 1)
        ind = data.index(b'\n') + data[data.index(b'\n')+1:].index(b'\n') + 1
        seek_delimiter(f, b'\n', 5)
        assert f.tell() == ind + 1


def test_read_s3_block(s3):
    data = files['test/accounts.1.json']
    lines = io.BytesIO(data).readlines()
    path = test_bucket_name+'/test/accounts.1.json'
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
    with pytest.raises((IOError, OSError)):
        s3.rmdir('new')
    with pytest.raises((IOError, OSError)):
        s3.rmdir('new/temp')
    s3.rm('new/temp')
    s3.rmdir('new')
    assert 'new' not in s3.ls('')
    assert not s3.exists('new')
    with pytest.raises((IOError, OSError)):
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
    with s3.open(test_bucket_name+'/test', 'wb') as f:
        f.write(b'hello')
    assert s3.cat(test_bucket_name+'/test') == b'hello'
    s3.open(test_bucket_name+'/test', 'wb').close()
    assert s3.info(test_bucket_name+'/test')['Size'] == 0


def test_write_small_secure(s3):
    # Unfortunately moto does not yet support enforcing SSE policies.  It also
    # does not return the correct objects that can be used to test the results
    # effectively.
    # This test is left as a placeholder in case moto eventually supports this.
    sse_params = SSEParams(server_side_encryption='aws:kms')
    with s3.open(secure_bucket_name+'/test', 'wb', writer_kwargs=sse_params) as f:
        f.write(b'hello')
    assert s3.cat(secure_bucket_name+'/test') == b'hello'
    head = s3.s3.head_object(Bucket=secure_bucket_name, Key='test')


def test_write_large_secure(s3):
    mock = moto.mock_s3()
    mock.start()

    # build our own s3fs with the relevant additional kwarg
    s3 = S3FileSystem(s3_additional_kwargs = {'ServerSideEncryption': 'AES256'})
    s3.mkdir('mybucket')

    with s3.open('mybucket/myfile', 'wb') as f:
        f.write(b'hello hello' * 10**6)

    assert s3.cat('mybucket/myfile') == b'hello hello' * 10**6


def test_write_fails(s3):
    with pytest.raises(ValueError):
        s3.touch(test_bucket_name+'/temp')
        s3.open(test_bucket_name+'/temp', 'rb').write(b'hello')
    with pytest.raises(ValueError):
        s3.open(test_bucket_name+'/temp', 'wb', block_size=10)
    f = s3.open(test_bucket_name+'/temp', 'wb')
    f.close()
    with pytest.raises(ValueError):
        f.write(b'hello')
    with pytest.raises((OSError, IOError)):
        s3.open('nonexistentbucket/temp', 'wb').close()


def test_write_blocks(s3):
    with s3.open(test_bucket_name+'/temp', 'wb') as f:
        f.write(b'a' * 2*2**20)
        assert f.buffer.tell() == 2*2**20
        assert not(f.parts)
        f.flush()
        assert f.buffer.tell() == 2*2**20
        assert not(f.parts)
        f.write(b'a' * 2*2**20)
        f.write(b'a' * 2*2**20)
        assert f.mpu
        assert f.parts
    assert s3.info(test_bucket_name+'/temp')['Size'] == 6*2**20
    with s3.open(test_bucket_name+'/temp', 'wb', block_size=10*2**20) as f:
        f.write(b'a' * 15*2**20)
        assert f.buffer.tell() == 0
    assert s3.info(test_bucket_name+'/temp')['Size'] == 15*2**20


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


def test_readline_from_cache(s3):
    data = b'a,b\n11,22\n3,4'
    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
        result = f.readline()
        assert result == b'a,b\n'
        assert f.loc == 4
        assert f.cache == data

        result = f.readline()
        assert result == b'11,22\n'
        assert f.loc == 10
        assert f.cache == data

        result = f.readline()
        assert result == b'3,4'
        assert f.loc == 13
        assert f.cache == data


def test_readline_partial(s3):
    data = b'aaaaa,bbbbb\n12345,6789\n'
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a, 'rb') as f:
        result = f.readline(5)
        assert result == b'aaaaa'
        result = f.readline(5)
        assert result == b',bbbb'
        result = f.readline(5)
        assert result == b'b\n'
        result = f.readline()
        assert result == b'12345,6789\n'


def test_readline_empty(s3):
    data = b''
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a, 'rb') as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(s3):
    data = b'ab\n' + b'a' * (10 * 2**20) + b'\nab'
    with s3.open(a, 'wb') as f:
        f.write(data)
    with s3.open(a, 'rb') as f:
        result = f.readline()
        expected = b'ab\n'
        assert result == expected

        result = f.readline()
        expected = b'a' * (10 * 2**20) + b'\n'
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
        assert f.readline(1) == b'1'
        assert f.readline() == b'23'

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
        f.write(b'a' * 10*2**20)

    with s3.open(b, 'wb') as f:
        f.write(b'a' * 10*2**20)
    s3.merge(test_bucket_name+'/joined', [a, b])
    assert s3.info(test_bucket_name+'/joined')['Size'] == 2*10*2**20


def test_append(s3):
    data = text_files['nested/file1']
    with s3.open(test_bucket_name+'/nested/file1', 'ab') as f:
        assert f.tell() == len(data) # append, no write, small file
    assert s3.cat(test_bucket_name+'/nested/file1') == data
    with s3.open(test_bucket_name+'/nested/file1', 'ab') as f:
        f.write(b'extra')  # append, write, small file
    assert s3.cat(test_bucket_name+'/nested/file1') == data+b'extra'

    with s3.open(a, 'wb') as f:
        f.write(b'a' * 10*2**20)
    with s3.open(a, 'ab') as f:
        pass  # append, no write, big file
    assert s3.cat(a) == b'a' * 10*2**20

    with s3.open(a, 'ab') as f:
        assert f.parts
        assert f.tell() == 10*2**20
        f.write(b'extra')  # append, small write, big file
    assert s3.cat(a) == b'a' * 10*2**20 + b'extra'

    with s3.open(a, 'ab') as f:
        assert f.tell() == 10*2**20 + 5
        f.write(b'b' * 10*2**20)  # append, big write, big file
        assert f.tell() == 20*2**20 + 5
    assert s3.cat(a) == b'a' * 10*2**20 + b'extra' + b'b' *10*2**20


def test_bigger_than_block_read(s3):
    with s3.open(test_bucket_name+'/2014-01-01.csv', 'rb', block_size=3) as f:
        out = []
        while True:
            data = f.read(20)
            out.append(data)
            if len(data) == 0:
                break
    assert b''.join(out) == csv_files['2014-01-01.csv']


def test_current(s3):
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
        return id(s3.connect())


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
        s3.touch(test_bucket_name+'/afile')
        s3.touch(other_bucket_name, acl='public-read')
        s3.touch(other_bucket_name+'/afile', acl='public-read')

        s = S3FileSystem(anon=True)
        with pytest.raises((IOError, OSError)):
            s.ls(test_bucket_name)
        s.ls(other_bucket_name)

        s3.chmod(test_bucket_name, acl='public-read')
        s3.chmod(other_bucket_name, acl='private')
        with pytest.raises((IOError, OSError)):
            s.ls(other_bucket_name, refresh=True)
        assert s.ls(test_bucket_name, refresh=True)

        # public file in private bucket
        with s3.open(other_bucket_name+'/see_me', 'wb', acl='public-read') as f:
            f.write(b'hello')
        assert s.cat(other_bucket_name+'/see_me') == b'hello'
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
    assert len(s3f.parts) == 0


def test_not_fill_cache(s3):
    fname = list(files)[0]
    data = files[fname]
    with s3.open(test_bucket_name+'/'+fname, 'rb', block_size=5,
                 fill_cache=False) as f:
        f.read(6)
        assert len(f.cache) == 11
        assert data.startswith(f.cache)
        f.seek(10)
        f.read(2)
        assert f.start < 10  # keeps some old data

        f.seek(31)
        out = f.read(6)
        assert len(f.cache) == 11  # dropped
        assert out == data[31:37]


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
    assert len(versions) == 2

    with s3.open(versioned_file) as fo:
        assert fo.version_id == '1'
        assert fo.read() == b'2'

    with s3.open(versioned_file, version_id='0') as fo:
        assert fo.version_id == '0'
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


def test_cat__basic_text(s3):
    """cat() should support text mode."""
    s3.mkdir('bucket')
    with s3.open('bucket/file.txt', 'w') as fd:
        fd.write(u'something')

    assert s3.cat('bucket/file.txt', as_text=True) == u'something'


def test_cat__encoding_implies_text(s3):
    """The encoding kwarg implies as_text = True."""
    s3.mkdir('bucket')
    with s3.open('bucket/file.txt', 'w', encoding='ibm500') as fd:
        fd.write(u'something')

    assert s3.cat('bucket/file.txt', encoding='ibm500') == u'something'


def test_cat__binary_ok(s3):
    """Explicitly passing as_text=False for binary mode should still work."""
    s3.mkdir('bucket')
    with s3.open('bucket/file.txt', 'w', encoding='utf-8') as fd:
        fd.write(u'something')

    assert s3.cat('bucket/file.txt', as_text=False) == b'something'

def test_readinto(s3):
    s3.mkdir('bucket')

    with s3.open('bucket/file.txt', 'wb') as fd:
        fd.write(b'Hello, World!')

    contents = bytearray()

    with s3.open('bucket/file.txt', 'rb') as fd:
        assert fd.readinto(contents) == 13

    assert contents == b'Hello, World!'


def test_change_defaults_only_subsequent(monkeypatch):
    """Test for Issue #135

    Ensure that changing the default block size doesn't affect existing file
    systems that were created using that default. It should only affect file
    systems created after the change.
    """
    fs_default = S3FileSystem()
    assert fs_default.default_block_size == 5 * (1024 ** 2)

    fs_overridden = S3FileSystem(default_block_size=64 * (1024 ** 2))
    assert fs_overridden.default_block_size == 64 * (1024 ** 2)

    # Suppose I want all subsequent file systems to have a block size of 1 GiB
    # instead of 5 MiB:
    monkeypatch.setattr(S3FileSystem, 'default_block_size', 1024 ** 3)

    fs_big = S3FileSystem()
    assert fs_big.default_block_size == 1024 ** 3

    # Test the other file systems created to see if their block sizes changed
    assert fs_overridden.default_block_size == 64 * (1024 ** 2)
    assert fs_default.default_block_size == 5 * (1024 ** 2)



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
    s3 = S3FileSystem()
    try:
        s3.__getstate__()
    except NotImplementedError:
        pytest.fail("Unexpected NotImplementedError")


def test_pickle_with_passed_in_session(s3):
    session = boto3.session.Session()
    s3 = S3FileSystem(session=session)
    with pytest.raises(NotImplementedError):
        s3.__getstate__()



