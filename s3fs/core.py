# -*- coding: utf-8 -*-
import io
import logging
import re
import socket
from hashlib import md5

import boto3
import boto3.compat
import boto3.s3.transfer as trans
from botocore.exceptions import ClientError
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
except:
    class FileNotFoundError(OSError):
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
    Access S3 data as if it were a file system.
    """
    _conn = {}
    connect_timeout=5
    read_timeout=15

    def __init__(self, anon=True, key=None, secret=None, **kwargs):
        """
        Create connection object to S3

        Will use configured key/secret (typically in ~/.aws, see the
        boto3 documentation) unless specified

        Parameters
        ----------
        anon : bool (True)
            whether to use anonymous connection (public buckets only)
        key : string (None)
            if not anonymouns, use this key, if specified
        secret : string (None)
            if not anonymous, use this password, if specified
        kwargs : other parameters for boto3 session
        """
        self.anon = anon
        self.key = key
        self.secret = secret
        self.kwargs = kwargs
        self.dirs = {}
        self.s3 = self.connect()

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
        """ List files below path

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
        path = path.lstrip('s3://').lstrip('/')
        bucket, key = split_path(path)
        if bucket not in self.dirs or refresh:
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
            else:
                try:
                    files = self.s3.list_objects(Bucket=bucket).get('Contents', [])
                except ClientError:
                    files = []
                for f in files:
                    f['Key'] = "/".join([bucket, f['Key']])
            self.dirs[bucket] = list(sorted(files, key=lambda x: x['Key']))
        files = self.dirs[bucket]
        return files

    def ls(self, path, detail=False):
        """ List single "directory" with or without details """
        path = path.lstrip('s3://').rstrip('/')
        try:
            files = self._ls(path)
        except ClientError:
            raise FileNotFoundError(path)
        if path:
            pattern = re.compile(path + '/[^/]*.$')
            files = [f for f in files if pattern.match(f['Key']) is not None]
            if not files:
                try:
                    files = [self.info(path)]
                except (OSError, IOError, ClientError):
                    raise FileNotFoundError(path)
        if detail:
            return files
        else:
            return [f['Key'] for f in files]

    def info(self, path):
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        files = self._ls(path)
        files = [f for f in files if f['Key'].rstrip('/') == path]
        if len(files) == 1:
            return files[0]
        else:
            raise FileNotFoundError(path)

    def walk(self, path):
        """ Return all entries below path """
        return [f['Key'] for f in self._ls(path) if f['Key'].rstrip('/'
                ).startswith(path.rstrip('/') + '/')]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path0 = path
        path = path.lstrip('s3://').lstrip('/')
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
        self.copy(path1, path2)
        self.rm(path1)        

    def copy(self, path1, path2):
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
        bucket, key = split_path(path)
        if key:
            try:
                self.s3.delete_object(Bucket=bucket, Key=key)
            except ClientError:
                raise IOError('Delete key failed', (bucket, key))
            self._ls(path, refresh=True)
        else:
            if recursive or not self.s3.list_objects(Bucket=bucket).get('Contents'):
                try:
                    self.s3.delete_bucket(Bucket=bucket)
                except ClientError:
                    raise IOError('Delete bucket failed', bucket)
                self._ls('', refresh=True)
            else:
                raise IOError('Not empty', path)

    def touch(self, path):
        """
        Create empty key; or if path is a bucket only, attempt to create
        bucket.
        """
        bucket, key = split_path(path)
        if key:
            self.open(path, mode='wb')
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


class S3File(object):
    """
    Cached read-only interface to a key in S3, behaving like a seekable file.

    Optimized for a single continguous block.
    """

    def __init__(self, s3, path, mode='rb', block_size=5*2**20):
        """
        Open S3 as a file. Data is only loaded and cached on demand.

        Parameters
        ----------
        s3 : boto3 connection
        bucket : string
            S3 bucket to access
        key : string
            S3 key to access
        blocksize : int
            read-ahead size for finding delimiters
        """
        self.mode = mode
        if mode not in {'rb', 'wb'}:
            raise NotImplementedError("File mode must be 'rb' or 'wb', not %s" % mode)
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
        if mode == 'wb':
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
        else:
            try:
                self.size = self.info()['Size']
            except ClientError:
                raise IOError("File not accessible", path)

    def info(self):
        return self.s3.info(self.path)

    def tell(self):
        return self.loc

    def seek(self, loc, whence=0):
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
        if self.mode != 'wb':
            raise ValueError('File not in write mode')
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() > self.blocksize:
            self.flush()
        return out

    def flush(self, force=False):
        """
        Write buffered data to S3.

        Parameters
        ----------
        force : bool (True)
            Whether to write even if the buffer is less than the blocksize. If
        less than the S3 part minimum (5MB), must be last block.
        """
        if self.mode == 'wb' and not self.closed:
            if self.buffer.tell() < self.blocksize and not force:
                raise ValueError('Parts must be greater than %s', self.blocksize)
            if self.buffer.tell() == 0:
                # no data in the buffer to write
                return
            if force and self.forced and self.buffer.tell() < 4*2**20:
                raise IOError('Under-sized block already written')
            if force and self.buffer.tell() < 4*2**20:
                self.forced = True
            self.buffer.seek(0)
            try:
                part = len(self.parts) + 1
                out = self.s3.s3.upload_part(Bucket=self.bucket, Key=self.key,
                            PartNumber=part, UploadId=self.mpu['UploadId'],
                            Body=self.buffer.read())
                self.parts.append({'PartNumber': part, 'ETag': out['ETag']})
            except ClientError:
                raise IOError('Write failed', self)
            self.buffer = io.BytesIO()

    def close(self):
        """
        If in write mode, key is only finalized upon close, and key will then
        be available to other processes.
        """
        if self.closed:
            return
        self.flush(True)
        self.cache = None
        self.closed = True
        if self.mode == 'wb':
            if self.parts:
                part_info = {'Parts': self.parts}
                self.s3.s3.complete_multipart_upload(Bucket=self.bucket, Key=self.key,
                            UploadId=self.mpu['UploadId'], MultipartUpload=part_info)
            else:
                self.s3.s3.put_object(Bucket=self.bucket, Key=self.key)
            self.s3._ls(self.bucket, refresh=True)

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
