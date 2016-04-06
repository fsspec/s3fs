# -*- coding: utf-8 -*-
import io
import logging
import re
import socket
from hashlib import md5
from contextlib import contextmanager

import boto3
import boto3.compat
import boto3.s3.transfer as trans
from botocore.exceptions import ClientError, ParamValidationError
from botocore.client import Config

from .utils import read_block

logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
S3_RETRYABLE_ERRORS = (
    socket.timeout,
    trans.ReadTimeoutError, trans.IncompleteReadError
)
try:
    FileNotFoundError
except NameError:
    class FileNotFoundError(IOError):
        pass

def tokenize(*args, **kwargs):
    """ Deterministic token

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args = args + (kwargs,)
    return md5(str(tuple(args)).encode()).hexdigest()


def split_path(path):
    """
    Normalise S3 path string into bucket and key.

    Parameters
    ----------
    path : string
        Input path, like `s3://mybucket/path/to/file`

    Examples
    --------
    >>> split_path("s3://mybucket/path/to/file")
    ['mybucket', 'path/to/file']
    """
    if path.startswith('s3://'):
        path = path[5:]
    if '/' not in path:
        return path, ""
    else:
        return path.split('/', 1)


class S3FileSystem(object):
    """
    Access S3 as if it were a file system.

    This exposes a filesystem-like API (ls, cp, open, etc.) on top of S3
    storage.

    Provide credentials either explicitly (``key=``, ``secret=``) or depend
    upon local configuration files (``~/.aws`` or ``~/.boto``).  See boto3
    documentation for more information.

    Parameters
    ----------
    anon : bool or None (default)
        Whether to use anonymous connection (public buckets only). If None,
        tries False and falls back to True.
    key : string (None)
        If not anonymouns, use this key, if specified
    secret : string (None)
        If not anonymous, use this password, if specified
    kwargs : other parameters for boto3 session

    Examples
    --------
    >>> s3 = S3FileSystem(anon=False)  # doctest: +SKIP
    >>> s3.ls('my-bucket/')  # doctest: +SKIP
    ['my-file.txt']

    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     print(f.read())  # doctest: +SKIP
    b'Hello, world!'
    """
    _conn = {}
    connect_timeout=5
    read_timeout=15

    def __init__(self, anon=None, key=None, secret=None, **kwargs):
        self.anon = anon
        self.key = key
        self.secret = secret
        self.kwargs = kwargs
        self.dirs = {}
        if anon is None:
            try:
                self.anon = False
                self.s3 = self.connect()
                self.ls('')
                return
            except ClientError:
                logger.debug('Credentials failed/missing, trying anonymous')
                self.anon = False
        self.s3 = self.connect()
        self.no_refresh = False

    def connect(self, refresh=False):
        """
        Establish S3 connection object.

        Reuses cached object unless refresh is True
        """
        anon, key, secret, kwargs = self.anon, self.key, self.secret, self.kwargs
        tok = tokenize(anon, key, secret, kwargs)
        if refresh:
            self._conn.pop(tok, None)
        if tok not in self._conn:
            logger.debug("Open S3 connection.  Anonymous: %s",
                         self.anon)
            if self.anon:
                ## TODO: test addition of kwargs (e.g., S3 data centre)
                from botocore import UNSIGNED
                conf = Config(connect_timeout=self.connect_timeout,
                              read_timeout=self.read_timeout,
                              signature_version=UNSIGNED, **self.kwargs)
                s3 = boto3.Session().client('s3', config=conf)
            else:
                conf = Config(connect_timeout=self.connect_timeout,
                              read_timeout=self.read_timeout)
                s3 = boto3.Session(self.key, self.secret,
                                   **self.kwargs).client('s3', config=conf)
            self._conn[tok] = s3
        return self._conn[tok]

    def refresh_off(self):
        """ Block auto-refresh when writing.

        Use in conjunction with `refresh_on()` when writing many files to S3.
        """
        self.no_refresh = True

    def refresh_on(self):
        self.no_refresh = False

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['s3']
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.s3 = self.connect()

    def open(self, path, mode='rb', block_size=5*1024**2):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'rb' or 'wb'
        block_size: int
            Size of data-node blocks if reading
        """
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                    " and manage bytes" % (mode[0] + 'b'))
        return S3File(self, path, mode, block_size=block_size)

    def _ls(self, path, refresh=False):
        """ List files in given bucket, or list of buckets.

        Listing is cached unless `refresh=True`.

        Note: only your buckets associated with the login will be listed by
        `ls('')`, not any public buckets (even if already accessed).

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        refresh : bool (=False)
            if False, look in local cache for file details first
        """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        bucket, key = split_path(path)
        if bucket not in self.dirs or (refresh and not self.no_refresh):
            if bucket == '':
                # list of buckets
                if self.anon:
                    # cannot list buckets if not logged in
                    return []
                files = self.s3.list_buckets()['Buckets']
                for f in files:
                    f['Key'] = f['Name']
                    f['Size'] = 0
                    del f['Name']
                self.dirs[''] = files
            else:
                try:
                    pag = self.s3.get_paginator('list_objects')
                    it = pag.paginate(Bucket=bucket)
                    files = []
                    for i in it:
                        files.extend(i.get('Contents', []))
                except ClientError:
                    # bucket not accessible
                    raise FileNotFoundError(bucket)
                for f in files:
                    f['Key'] = "/".join([bucket, f['Key']])
            self.dirs[bucket] = list(sorted(files, key=lambda x: x['Key']))
        files = self.dirs[bucket]
        return files

    def ls(self, path, detail=False):
        """ List single "directory" with or without details """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        files = self._ls(path)
        if path:
            pattern = re.compile(path + '/[^/]*.$')
            files = [f for f in files if pattern.match(f['Key']) is not None]
            if not files:
                try:
                    files = [self.info(path)]
                except (OSError, IOError, ClientError):
                    files = []
        if detail:
            return files
        else:
            return [f['Key'] for f in files]

    def info(self, path):
        """ Detail on the specific file pointed to by path.

        NB: path has trailing '/' stripped to work as `ls` does, so key
        names that genuinely end in '/' will fail.
        """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        bucket, key = split_path(path)
        try:
            info = self.s3.head_object(Bucket=bucket, Key=key)
            info['Size'] = info['ContentLength']
            info['Key'] = '/'.join([bucket, key])
            return info
        except (ClientError, ParamValidationError):
            raise IOError("File not found: %s" % path)

    def walk(self, path):
        """ Return all entries below path """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        return [f['Key'] for f in self._ls(path) if f['Key'].rstrip('/'
                ).startswith(path.rstrip('/') + '/')]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path0 = path
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        bucket, key = split_path(path)
        if "*" in bucket:
            raise ValueError('Bucket cannot contain a "*"')
        if '*' not in path:
            path = path.rstrip('/') + '/*'
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                                        .rstrip('/')
                                        .replace('*', '[^/]*')
                                        .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        if not out:
            out = self.ls(path0)
        return out

    def du(self, path, total=False, deep=False):
        """ Bytes in keys at path """
        if deep:
            files = self.walk(path)
            files = [self.info(f) for f in files]
        else:
            files = self.ls(path, detail=True)
        if total:
            return sum(f.get('Size', 0) for f in files)
        else:
            return {p['Key']: p['Size'] for p in files}

    def exists(self, path):
        """ Does such a file exist? """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        if split_path(path)[1]:
            return bool(self.ls(path))
        else:
            return path in self.ls('')

    def cat(self, path):
        """ Returns contents of file """
        with self.open(path, 'rb') as f:
            return f.read()

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['Size']
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb', block_size=size) as f:
            return f.read(size)

    def get(self, path, filename):
        """ Stream data from file at path to local filename """
        with self.open(path, 'rb') as f:
            with open(filename, 'wb') as f2:
                while True:
                    data = f.read(f.blocksize)
                    if len(data) == 0:
                        break
                    f2.write(data)

    def put(self, filename, path):
        """ Stream data from local filename to file at path """
        with open(filename, 'rb') as f:
            with self.open(path, 'wb') as f2:
                while True:
                    data = f.read(f2.blocksize)
                    if len(data) == 0:
                        break
                    f2.write(data)

    def mkdir(self, path):
        """ Make new bucket or empty key """
        self.touch(path)

    def rmdir(self, path):
        """ Remove empty key or bucket """
        bucket, key = split_path(path)
        if (key and self.info(path)['Size'] == 0) or not key:
            self.rm(path)
        else:
            raise IOError('Path is not directory-like', path)

    def mv(self, path1, path2):
        """ Move file between locations on S3 """
        self.copy(path1, path2)
        self.rm(path1)

    def merge(self, path, filelist):
        """ Create single S3 file from list of S3 files

        Uses multi-part, no data is downloaded. The original files are
        not deleted.

        Parameters
        ----------
        path : str
            The final file to produce
        filelist : list of str
            The paths, in order, to assemble into the final file.
        """
        bucket, key = split_path(path)
        mpu = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        out = [self.s3.upload_part_copy(Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
                            CopySource=f, PartNumber=i+1) for (i, f) in enumerate(filelist)]
        parts = [{'PartNumber': i+1, 'ETag': o['CopyPartResult']['ETag']} for (i, o) in enumerate(out)]
        part_info = {'Parts': parts}
        self.s3.complete_multipart_upload(Bucket=bucket, Key=key,
                    UploadId=mpu['UploadId'], MultipartUpload=part_info)
        self._ls(bucket, refresh=True)
        

    def copy(self, path1, path2):
        """ Copy file between locations on S3 """
        buc1, key1 = split_path(path1)
        buc2, key2 = split_path(path2)
        try:
            self.s3.copy_object(Bucket=buc2, Key=key2, CopySource='/'.join([buc1, key1]))
        except ClientError:
            raise IOError('Copy failed', (path1, path2))
        self._ls(path2, refresh=True)

    def rm(self, path, recursive=False):
        """
        Remove keys and/or bucket.

        Parameters
        ----------
        path : string
            The location to remove.
        recursive : bool (True)
            Whether to remove also all entries below, i.e., which are returned
            by `walk()`.
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        if recursive:
            for f in self.walk(path):
                self.rm(f, recursive=False)
            return
        bucket, key = split_path(path)
        if key:
            try:
                self.s3.delete_object(Bucket=bucket, Key=key)
            except ClientError:
                raise IOError('Delete key failed', (bucket, key))
            self._ls(path, refresh=True)
        else:
            if not self.s3.list_objects(Bucket=bucket).get('Contents'):
                try:
                    self.s3.delete_bucket(Bucket=bucket)
                except ClientError:
                    raise IOError('Delete bucket failed', bucket)
                self.dirs.pop(bucket, None)
                self._ls('', refresh=True)
            else:
                raise IOError('Not empty', path)

    def touch(self, path):
        """
        Create empty key

        If path is a bucket only, attempt to create bucket.
        """
        bucket, key = split_path(path)
        if key:
            self.s3.put_object(Bucket=bucket, Key=key)
            self._ls(bucket, refresh=True)
        else:
            try:
                self.s3.create_bucket(Bucket=bucket)
                self._ls("", refresh=True)
            except ClientError:
                raise IOError('Bucket create failed', path)

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an S3 file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on S3
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> s3.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> s3.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> s3.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['Size']
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


@contextmanager
def no_refresh(s3fs):
    """ Wrap an s3fs with this to temporarily block freshing filecache on writes.

    Use this if writing many small files to a bucket.
    The filelist will only be refreshed by the next writing action, or
    explicit call to `s3fs._ls(bucket, refresh=True)`.

    Usage
    -----
    >>> with no_refresh(s3fs) as fs:    # doctest: +SKIP
            [fs.touch('mybucket/file%i'%i) for i in range(1500)] # doctest: +SKIP
    """
    s3fs.refresh_on()
    try:
        yield s3fs
    finally:
        s3fs.refresh_off()


class S3File(object):
    """
    Open S3 key as a file. Data is only loaded and cached on demand.

    Parameters
    ----------
    s3 : boto3 connection
    bucket : string
        S3 bucket to access
    key : string
        S3 key to access
    blocksize : int
        read-ahead size for finding delimiters

    Examples
    --------
    >>> s3 = S3FileSystem()  # doctest: +SKIP
    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     ...  # doctest: +SKIP

    See Also
    --------
    S3FileSystem.open: used to create ``S3File`` objects
    """
    def __init__(self, s3, path, mode='rb', block_size=5*2**20):
        self.mode = mode
        if mode not in {'rb', 'wb', 'ab'}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'ab'}, not %s" % mode)
        self.path = path
        bucket, key = split_path(path)
        self.s3 = s3
        self.bucket = bucket
        self.key = key
        self.blocksize = block_size
        self.cache = b""
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False
        if mode in {'wb', 'ab'}:
            self.buffer = io.BytesIO()
            self.parts = []
            self.size = 0
            if block_size < 5*2**20:
                raise ValueError('Block size must be >=5MB')
            try:
                self.mpu = s3.s3.create_multipart_upload(Bucket=bucket, Key=key)
            except ClientError:
                raise IOError('Open for write failed', path)
            self.forced = False
            if mode == 'ab' and s3.exists(path):
                self.size = s3.info(path)['Size']
                if self.size < 5*2**20:
                    # existing file too small for multi-upload: download
                    self.write(s3.cat(path))
                else:
                    self.loc = self.size
                    out = self.s3.s3.upload_part_copy(Bucket=self.bucket, Key=self.key,
                                PartNumber=1, UploadId=self.mpu['UploadId'],
                                CopySource=path)
                    self.parts.append({'PartNumber': 1, 'ETag': out['CopyPartResult']['ETag']})
        else:
            try:
                self.size = self.info()['Size']
            except ClientError:
                raise IOError("File not accessible", path)

    def info(self):
        """ File information about this path """
        return self.s3.info(self.path)

    def tell(self):
        """ Current file location """
        return self.loc

    def seek(self, loc, whence=0):
        """ Set current file location """
        if not self.mode == 'rb':
            raise ValueError('Seek only available in read mode')
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError('Seek before start of file')
        self.loc = nloc
        return self.loc

    def readline(self, length=-1):
        '''
        Read and return a line from the stream.
        
        If length is specified, at most size bytes will be read.
        '''  
        self._fetch(self.loc, self.loc+1)
        while True:
            found = self.cache[self.loc-self.start:].find(b'\n') + 1
            if length > 0 and found > length:
                return self.read(length)
            if found:
                return self.read(found)
            if self.end > self.size:
                return self.read(length)
            self._fetch(self.start, self.end + self.blocksize)

    def __next__(self):
        return self.readline()

    next = __next__

    def __iter__(self):
        return self

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def _fetch(self, start, end):
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.cache = _fetch_range(self.s3.s3, self.bucket, self.key,
                                      start, self.end)
        if start < self.start:
            new = _fetch_range(self.s3.s3, self.bucket, self.key,
                               start, self.start)
            self.start = start
            self.cache = new + self.cache
        if end > self.end:
            if end > self.size:
                return
            new = _fetch_range(self.s3.s3, self.bucket, self.key,
                               self.end, end + self.blocksize)
            self.end = end + self.blocksize
            self.cache = self.cache + new

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        """
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        return out

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent to S3 on flush() or if buffer is bigger than blocksize.
        """
        if self.mode not in {'wb', 'ab'}:
            raise ValueError('File not in write mode')
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() > self.blocksize:
            self.flush()
        return out

    def flush(self, force=False, retries=10):
        """
        Write buffered data to S3.

        Due to S3 multi-upload policy, you can only safely force flush to S3
        when you are finished writing.  It is unsafe to call this function
        repeatedly.

        Parameters
        ----------
        force : bool (True)
            Whether to write even if the buffer is less than the blocksize. If
            less than the S3 part minimum (5MB), must be last block.
        """
        if self.mode in {'wb', 'ab'} and not self.closed:
            if self.buffer.tell() < self.blocksize and not force:
                raise ValueError('Parts must be greater than %s', self.blocksize)
            if self.buffer.tell() == 0:
                # no data in the buffer to write
                return
            if force and self.forced and self.buffer.tell() < 5*2**20:
                raise IOError('Under-sized block already written')
            if force and self.buffer.tell() < 5*2**20:
                self.forced = True
            self.buffer.seek(0)
            part = len(self.parts) + 1
            i = 0
            while True:
                try:
                    out = self.s3.s3.upload_part(Bucket=self.bucket, Key=self.key,
                            PartNumber=part, UploadId=self.mpu['UploadId'],
                            Body=self.buffer.read())
                    break
                except S3_RETRYABLE_ERRORS:
                    if i < retries:
                        logger.debug('Exception %e on S3 upload, retrying',
                                     exc_info=True)
                        i += 1
                        continue
                    else:
                        raise IOError('Write failed after %i retries'%retries, self)
                except:
                    raise IOError('Write failed', self)
            self.parts.append({'PartNumber': part, 'ETag': out['ETag']})
            self.buffer = io.BytesIO()

    def close(self):
        """ Close file

        If in write mode, key is only finalized upon close, and key will then
        be available to other processes.
        """
        if self.closed:
            return
        self.flush(True)
        self.cache = None
        self.closed = True
        if self.mode in {'wb', 'ab'}:
            if self.parts:
                part_info = {'Parts': self.parts}
                self.s3.s3.complete_multipart_upload(Bucket=self.bucket, Key=self.key,
                            UploadId=self.mpu['UploadId'], MultipartUpload=part_info)
            else:
                self.s3.s3.put_object(Bucket=self.bucket, Key=self.key)
            self.s3._ls(self.bucket, refresh=True)

    def readable(self):
        '''Return whether the S3File was opened for reading'''
        return self.mode == 'rb'

    def seekable(self):
        '''Return whether the S3File is seekable (only in read mode)'''
        return self.readable()

    def writable(self):
        '''Return whether the S3File was opened for writing'''
        return self.mode in {'wb', 'ab'}

    def __del__(self):
        self.close()

    def __str__(self):
        return "<S3File %s/%s>" % (self.bucket, self.key)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _fetch_range(client, bucket, key, start, end, max_attempts=10):
    logger.debug("Fetch: %s/%s, %s-%s", bucket, key, start, end)
    for i in range(max_attempts):
        try:
            resp = client.get_object(Bucket=bucket, Key=key,
                                     Range='bytes=%i-%i' % (start, end - 1))
            return resp['Body'].read()
        except S3_RETRYABLE_ERRORS as e:
            logger.debug('Exception %e on S3 download, retrying',
                         exc_info=True)
            continue
        except ClientError as e:
            if e.response['Error'].get('Code', 'Unknown') in ['416', 'InvalidRange']:
                return b''
            else:
                raise
    raise RuntimeError("Max number of S3 retries exceeded")
