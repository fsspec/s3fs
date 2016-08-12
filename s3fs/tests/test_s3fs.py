# -*- coding: utf-8 -*-
from concurrent.futures import ProcessPoolExecutor
import io
import re
import time
import pytest
from itertools import chain
from s3fs.core import S3FileSystem
from s3fs.utils import seek_delimiter, ignoring, tmpfile
import moto

from botocore.exceptions import NoCredentialsError

test_bucket_name = 'test'
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
a = test_bucket_name+'/tmp/test/a'
b = test_bucket_name+'/tmp/test/b'
c = test_bucket_name+'/tmp/test/c'
d = test_bucket_name+'/tmp/test/d'


@pytest.yield_fixture
def s3():
    # writable local S3 system
    m = moto.mock_s3()
    m.start()
    import boto3
    client = boto3.client('s3')
    client.create_bucket(Bucket=test_bucket_name, ACL='public-read')
    for k in [a, b, c, d]:
        try:
            client.delete_object(Bucket=test_bucket_name, Key=k)
        except:
            pass
    for flist in [files, csv_files, text_files]:
        for f, data in flist.items():
            client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
    yield S3FileSystem(anon=False)
    for flist in [files, csv_files, text_files]:
        for f, data in flist.items():
            try:
                client.delete_object(Bucket=test_bucket_name, Key=f, Body=data)
            except:
                pass
    for k in [a, b, c, d]:
        try:
            client.delete_object(Bucket=test_bucket_name, Key=k)
        except:
            pass
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
    s3.dirs[parent].pop(0)  # disappear our file!
    assert a not in s3.ls(parent)
    assert s3.info(a)  # now uses head_object


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
    assert s3.ls('') == [test_bucket_name]
    with pytest.raises((OSError, IOError)):
        s3.ls('nonexistent')
    fn = test_bucket_name+'/test/accounts.1.json'
    assert fn in s3.ls(test_bucket_name+'/test')


def test_pickle(s3):
    import pickle
    s32 = pickle.loads(pickle.dumps(s3))
    assert s3.ls('test') == s32.ls('test')


def test_ls_touch(s3):
    assert not s3.exists(test_bucket_name+'/tmp/test')
    s3.touch(a)
    s3.touch(b)
    L = s3.ls(test_bucket_name+'/tmp/test', True)
    assert set(d['Key'] for d in L) == set([a, b])
    L = s3.ls(test_bucket_name+'/tmp/test', False)
    assert set(L) == set([a, b])


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
    assert fn in s3.glob(test_bucket_name+'/nested')
    assert fn in s3.glob(test_bucket_name+'/nested/*')
    assert fn in s3.glob(test_bucket_name+'/nested/file*')
    assert fn in s3.glob(test_bucket_name+'/*/*')
    assert all(f in s3.walk(test_bucket_name) for f in s3.glob(test_bucket_name+'/nested/*'))
    with pytest.raises(ValueError):
        s3.glob('*')


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
    assert abs(exp - time.time() - 100) < 5
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


def test_get_put(s3):
    with tmpfile() as fn:
        s3.get(test_bucket_name+'/test/accounts.1.json', fn)
        data = files['test/accounts.1.json']
        assert open(fn, 'rb').read() == data
        s3.put(fn, test_bucket_name+'/temp')
        assert s3.du(test_bucket_name+'/temp')[test_bucket_name+'/temp'] == len(data)
        assert s3.cat(test_bucket_name+'/temp') == data


def test_errors(s3):
    with pytest.raises((IOError, OSError)):
        s3.open(test_bucket_name+'/tmp/test/shfoshf', 'rb')

    ## This is fine, no need for interleving directories on S3
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

def test_write_small(s3):
    with s3.open(test_bucket_name+'/test', 'wb') as f:
        f.write(b'hello')
    assert s3.cat(test_bucket_name+'/test') == b'hello'
    s3.open(test_bucket_name+'/test', 'wb').close()
    assert s3.info(test_bucket_name+'/test')['Size'] == 0

def test_write_fails(s3):
    with pytest.raises(NotImplementedError):
        s3.open(test_bucket_name+'/temp', 'w')
    with pytest.raises(ValueError):
        s3.touch(test_bucket_name+'/temp')
        s3.open(test_bucket_name+'/temp', 'rb').write(b'hello')
    with pytest.raises(ValueError):
        s3.open(test_bucket_name+'/temp', 'wb', block_size=10)
    with pytest.raises(ValueError):
        with s3.open(test_bucket_name+'/temp', 'wb') as f:
            f.write(b'hello')
            f.flush()
            f.write(b'world')
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
