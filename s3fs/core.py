# -*- coding: utf-8 -*-
from six import raise_from
import errno
import logging
import posixpath
import socket
from hashlib import md5

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
import boto3
import more_itertools as m_iter
from botocore.client import Config
from botocore.exceptions import ClientError, ParamValidationError, BotoCoreError

from s3fs.utils import ParamKwargsHelper

logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

try:
    from boto3.s3.transfer import S3_RETRYABLE_ERRORS
except ImportError:
    S3_RETRYABLE_ERRORS = (
        socket.timeout,
    )

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    class FileNotFoundError(IOError):
        pass

# py2 has no ConnectionError only OSError with different error number for each
# error so we need to catch OSError and compare errno to the following
# error numbers - same as catching ConnectionError on py3
# ref: https://docs.python.org/3/library/exceptions.html#ConnectionError
_CONNECTION_ERRORS = frozenset({
    errno.ECONNRESET,  # ConnectionResetError
    errno.EPIPE, errno.ESHUTDOWN,  # BrokenPipeError
    errno.ECONNABORTED,  # ConnectionAbortedError
    errno.ECONNREFUSED,  # ConnectionRefusedError
})
_VALID_FILE_MODES = {'r', 'w', 'a', 'rb', 'wb', 'ab'}


def tokenize(*args, **kwargs):
    """ Deterministic token

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args += (kwargs,)
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
    path = path.rstrip('/').lstrip('/')
    if '/' not in path:
        return path, ""
    else:
        return path.split('/', 1)


key_acls = {'private', 'public-read', 'public-read-write',
            'authenticated-read', 'aws-exec-read', 'bucket-owner-read',
            'bucket-owner-full-control'}
buck_acls = {'private', 'public-read', 'public-read-write',
             'authenticated-read'}


def is_permission_error(e):
    # type: (ClientError) -> bool
    return e.response['Error'].get('Code', 'Unknown') == 'AccessDenied'


class S3FileSystem(AbstractFileSystem):
    """
    Access S3 as if it were a file system.

    This exposes a filesystem-like API (ls, cp, open, etc.) on top of S3
    storage.

    Provide credentials either explicitly (``key=``, ``secret=``) or depend
    on boto's credential methods. See boto3 documentation for more
    information. If no credentials are available, use ``anon=True``.

    Parameters
    ----------
    anon : bool (False)
        Whether to use anonymous connection (public buckets only). If False,
        uses the key/secret given, or boto's credential resolver (environment
        variables, config files, EC2 IAM server, in that order)
    key : string (None)
        If not anonymous, use this access key ID, if specified
    secret : string (None)
        If not anonymous, use this secret access key, if specified
    token : string (None)
        If not anonymous, use this security token, if specified
    use_ssl : bool (True)
        Whether to use SSL in connections to S3; may be faster without, but
        insecure
    s3_additional_kwargs : dict of parameters that are used when calling s3 api
        methods. Typically used for things like "ServerSideEncryption".
    client_kwargs : dict of parameters for the boto3 client
    requester_pays : bool (False)
        If RequesterPays buckets are supported.
    default_block_size: None, int
        If given, the default block size value used for ``open()``, if no
        specific value is given at all time. The built-in default is 5MB.
    default_fill_cache : Bool (True)
        Whether to use cache filling with open by default. Refer to
        ``S3File.open``.
    version_aware : bool (False)
        Whether to support bucket versioning.  If enable this will require the
        user to have the necessary IAM permissions for dealing with versioned
        objects.
    config_kwargs : dict of parameters passed to ``botocore.client.Config``
    kwargs : other parameters for boto3 session
    session : botocore Session object to be used for all connections.
         This session will be used inplace of creating a new session inside S3FileSystem.


    Examples
    --------
    >>> s3 = S3FileSystem(anon=False)  # doctest: +SKIP
    >>> s3.ls('my-bucket/')  # doctest: +SKIP
    ['my-file.txt']

    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     print(f.read())  # doctest: +SKIP
    b'Hello, world!'
    """
    root_marker = ""
    connect_timeout = 5
    read_timeout = 15
    default_block_size = 5 * 2**20
    protocol = 's3'

    def __init__(self, anon=False, key=None, secret=None, token=None,
                 use_ssl=True, client_kwargs=None, requester_pays=False,
                 default_block_size=None, default_fill_cache=True,
                 version_aware=False, config_kwargs=None,
                 s3_additional_kwargs=None, session=None, **kwargs):
        super().__init__()
        self.anon = anon
        self.session = None
        self.passed_in_session = session
        if self.passed_in_session:
            self.session = self.passed_in_session
        self.key = key
        self.secret = secret
        self.token = token
        self.kwargs = kwargs

        if client_kwargs is None:
            client_kwargs = {}
        if config_kwargs is None:
            config_kwargs = {}
        self.default_block_size = default_block_size or self.default_block_size
        self.default_fill_cache = default_fill_cache
        self.version_aware = version_aware
        self.client_kwargs = client_kwargs
        self.config_kwargs = config_kwargs
        self.req_kw = {'RequestPayer': 'requester'} if requester_pays else {}
        self.s3_additional_kwargs = s3_additional_kwargs or {}
        self.use_ssl = use_ssl
        self.s3 = self.connect()
        self._kwargs_helper = ParamKwargsHelper(self.s3)

    def _filter_kwargs(self, s3_method, kwargs):
        return self._kwargs_helper.filter_dict(s3_method.__name__, kwargs)

    def _call_s3(self, method, *akwarglist, **kwargs):
        additional_kwargs = self._get_s3_method_kwargs(method, *akwarglist,
                                                       **kwargs)
        return method(**additional_kwargs)

    def _get_s3_method_kwargs(self, method, *akwarglist, **kwargs):
        additional_kwargs = self.s3_additional_kwargs.copy()
        for akwargs in akwarglist:
            additional_kwargs.update(akwargs)
        # Add the normal kwargs in
        additional_kwargs.update(kwargs)
        # filter all kwargs
        return self._filter_kwargs(method, additional_kwargs)

    def connect(self, refresh=True):
        """
        Establish S3 connection object.

        Parameters
        ----------
        refresh : bool
            Whether to create new session/client, even if a previous one with
            the same parameters already exists. If False (default), an
            existing one will be used if possible
        """
        if refresh is False:
            # back compat: we store whole FS instance now
            return self.s3
        anon, key, secret, kwargs, ckwargs, token, ssl = (
            self.anon, self.key, self.secret, self.kwargs,
            self.client_kwargs, self.token, self.use_ssl)

        if self.anon:
            from botocore import UNSIGNED
            conf = Config(connect_timeout=self.connect_timeout,
                          read_timeout=self.read_timeout,
                          signature_version=UNSIGNED, **self.config_kwargs)
            if not self.passed_in_session:
                self.session = boto3.Session(**self.kwargs)
        else:
            conf = Config(connect_timeout=self.connect_timeout,
                          read_timeout=self.read_timeout,
                          **self.config_kwargs)
            if not self.passed_in_session:
                self.session = boto3.Session(self.key, self.secret, self.token,
                                             **self.kwargs)

        self.s3 = self.session.client('s3', config=conf, use_ssl=ssl,
                                      **self.client_kwargs)
        return self.s3

    def get_delegated_s3pars(self, exp=3600):
        """Get temporary credentials from STS, appropriate for sending across a
        network. Only relevant where the key/secret were explicitly provided.

        Parameters
        ----------
        exp : int
            Time in seconds that credentials are good for

        Returns
        -------
        dict of parameters
        """
        if self.anon:
            return {'anon': True}
        if self.token:  # already has temporary cred
            return {'key': self.key, 'secret': self.secret, 'token': self.token,
                    'anon': False}
        if self.key is None or self.secret is None:  # automatic credentials
            return {'anon': False}
        sts = self.session.client('sts')
        cred = sts.get_session_token(DurationSeconds=exp)['Credentials']
        return {'key': cred['AccessKeyId'], 'secret': cred['SecretAccessKey'],
                'token': cred['SessionToken'], 'anon': False}

    def _open(self, path, mode='rb', block_size=None, acl='', version_id=None,
              fill_cache=None, cache_type='bytes', **kwargs):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        block_size: int
            Size of data-node blocks if reading
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        acl: str
            Canned ACL to set when writing
        version_id : str
            Explicit version of the object to open.  This requires that the s3
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
        encoding : str
            The encoding to use if opening the file in text mode. The platform's
            default text encoding is used if not given.
        cache_type : str
            "bytes", "mmap" or "none"
        kwargs: dict-like
            Additional parameters used for s3 methods.  Typically used for
            ServerSideEncryption.
        """
        if block_size is None:
            block_size = self.default_block_size
        if fill_cache is None:
            fill_cache = self.default_fill_cache

        acl = acl or self.s3_additional_kwargs.get('ACL', '')
        kw = self.s3_additional_kwargs.copy()
        kw.update(kwargs)
        if not self.version_aware and version_id:
            raise ValueError("version_id cannot be specified if the filesystem "
                             "is not version aware")

        return S3File(self, path, mode, block_size=block_size, acl=acl,
                      version_id=version_id, fill_cache=fill_cache,
                      s3_additional_kwargs=kw, cache_type=cache_type)

    def _lsdir(self, path, refresh=False, max_items=None):
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        bucket, prefix = split_path(path)
        prefix = prefix + '/' if prefix else ""
        if path not in self.dircache or refresh:
            try:
                pag = self.s3.get_paginator('list_objects_v2')
                config = {}
                if max_items is not None:
                    config.update(MaxItems=max_items, PageSize=2 * max_items)
                it = pag.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/',
                                  PaginationConfig=config, **self.req_kw)
                files = []
                dircache = []
                for i in it:
                    dircache.extend(i.get('CommonPrefixes', []))
                    for c in i.get('Contents', []):
                        c['type'] = 'file'
                        c['size'] = c['Size']
                        files.append(c)
                if dircache:
                    files.extend([{'Key': l['Prefix'][:-1], 'Size': 0,
                                  'StorageClass': "DIRECTORY",
                                   'type': 'directory', 'size': 0}
                                  for l in dircache])
                for f in files:
                    f['Key'] = '/'.join([bucket, f['Key']])
                    f['name'] = f['Key']
            except ClientError as e:
                # path not accessible
                if is_permission_error(e):
                    raise
                if not prefix:
                    # list bucket failed - bucket doesn't exist
                    raise FileNotFoundError(bucket)

            self.dircache[path] = files
        return self.dircache[path]

    def walk(self, path, maxdepth=3):
        """The S3 equivalent of os.walk().

        Parameters
        ----------
        path: string
            The absolute path on S3 to begin recursing from.

        maxdepth: int
            The maximum number of levels to recurse into.
        """
        if maxdepth is None:
            new_maxdepth = None
        elif maxdepth <= 0:
            return
        else:
            new_maxdepth = maxdepth - 1

        i_files, i_directories = m_iter.partition(
            (lambda e: e['StorageClass'] == 'DIRECTORY'),
            self._lsdir(path)
        )

        directories = sorted(posixpath.basename(e['Key']) for e in i_directories)
        files = sorted(posixpath.basename(e['Key']) for e in i_files)

        yield path, directories, files

        for name in directories:
            new_top = posixpath.join(path, name)
            yield from self.walk(new_top, new_maxdepth)

    def mkdir(self, path, acl="", **kwargs):
        path = self._strip_protocol(path).rstrip('/')
        if not self._parent(path):
            if acl and acl not in buck_acls:
                raise ValueError('ACL not in %s', buck_acls)
            try:
                params = {"Bucket": path, 'ACL': acl}
                region_name = (kwargs.get("region_name", None) or
                               self.client_kwargs.get("region_name", None))
                if region_name:
                    params['CreateBucketConfiguration'] = {
                        'LocationConstraint': region_name
                    }
                self.s3.create_bucket(**params)
                self.invalidate_cache('')
                self.invalidate_cache(path)
            except (ClientError, ParamValidationError) as e:
                raise_from(IOError('Bucket create failed', path), e)

    def rmdir(self, path):
        path = self._strip_protocol(path).rstrip('/')
        if not self._parent(path):
            try:
                self.s3.delete_bucket(Bucket=path)
            except ClientError as e:
                raise_from(IOError('Delete bucket failed', path), e)
            self.invalidate_cache(path)
            self.invalidate_cache('')

    def _lsbuckets(self, refresh=False):
        if '' not in self.dircache or refresh:
            if self.anon:
                # cannot list buckets if not logged in
                return []
            files = self.s3.list_buckets()['Buckets']
            for f in files:
                f['Key'] = f['Name']
                f['Size'] = 0
                f['StorageClass'] = 'BUCKET'
                f['size'] = 0
                f['type'] = 'directory'
                f['name'] = f['Name']
                del f['Name']
            self.dircache[''] = files
        return self.dircache['']

    def __getstate__(self):
        if self.passed_in_session:
            raise NotImplementedError
        d = self.__dict__.copy()
        del d['s3']
        del d['session']
        del d['_kwargs_helper']
        del d['dircache']
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.s3 = self.connect()
        self.dircache = {}
        self._kwargs_helper = ParamKwargsHelper(self.s3)

    def _ls(self, path, refresh=False):
        """ List files in given bucket, or list of buckets.

        Listing is cached unless `refresh=True`.

        Note: only your buckets associated with the login will be listed by
        `ls('')`, not any public buckets (even if already accessed).

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        refresh : bool (=False)
            if False, look in local cache for file details first
        """
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        if path in ['', '/']:
            return self._lsbuckets(refresh)
        else:
            return self._lsdir(path, refresh)

    def exists(self, path):
        if path in ['', '/']:
            # the root always exists, even if anon
            return True
        bucket, key = split_path(path)
        if key:
            return super().exists(path)
        else:
            try:
                self.ls(path)
                return True
            except FileNotFoundError:
                return False

    def info(self, path, version_id=None):
        if path in ['/', '']:
            return {'name': path, 'size': 0, 'type': 'directory'}
        kwargs = self.kwargs.copy()
        if version_id is not None:
            if not self.version_aware:
                raise ValueError("version_id cannot be specified if the "
                                 "filesystem is not version aware")
            kwargs['VersionId'] = version_id
        if self.version_aware:
            try:
                bucket, key = split_path(path)
                out = self._call_s3(self.s3.head_object, kwargs, Bucket=bucket,
                                    Key=key, **self.req_kw)
                return {
                    'ETag': out['ETag'],
                    'Key': '/'.join([bucket, key]),
                    'LastModified': out['LastModified'],
                    'Size': out['ContentLength'],
                    'size': out['ContentLength'],
                    'path': '/'.join([bucket, key]),
                    'StorageClass': "STANDARD",
                    'VersionId': out.get('VersionId')
                }
            except (ClientError, ParamValidationError) as e:
                logger.debug("Failed to head path %s", path, exc_info=True)
                raise_from(FileNotFoundError(path), e)
        return super().info(path)

    def ls(self, path, detail=False, refresh=False, **kwargs):
        """ List single "directory" with or without details

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        refresh : bool (=False)
            if False, look in local cache for file details first
        kwargs : dict
            additional arguments passed on
        """
        path = self._strip_protocol(path).rstrip('/')
        out = self._ls(self._parent(path), refresh=refresh)
        out = [o for o in out
               if o['name'].rstrip('/') == path and o['type'] != 'directory']
        files = self._ls(path, refresh=refresh) or out
        if detail:
            return files
        else:
            return list(sorted(set([f['name'] for f in files])))

    def object_version_info(self, path, **kwargs):
        if not self.version_aware:
            raise ValueError("version specific functionality is disabled for "
                             "non-version aware filesystems")
        bucket, key = split_path(path)
        kwargs = {}
        out = {'IsTruncated': True}
        versions = []
        while out['IsTruncated']:
            out = self._call_s3(self.s3.list_object_versions, kwargs,
                                Bucket=bucket, Prefix=key, **self.req_kw)
            versions.extend(out['Versions'])
            kwargs['VersionIdMarker'] = out.get('NextVersionIdMarker', '')
        return versions

    _metadata_cache = {}

    def metadata(self, path, refresh=False, **kwargs):
        """ Return metadata of path.

        Metadata is cached unless `refresh=True`.

        Parameters
        ----------
        path : string/bytes
            filename to get metadata for
        refresh : bool (=False)
            if False, look in local cache for file metadata first
        """
        bucket, key = split_path(path)

        if refresh or path not in self._metadata_cache:
            response = self._call_s3(self.s3.head_object,
                                     kwargs,
                                     Bucket=bucket,
                                     Key=key,
                                     **self.req_kw)
            self._metadata_cache[path] = response['Metadata']

        return self._metadata_cache[path]

    def get_tags(self, path):
        """Retrieve tag key/values for the given path

        Returns
        -------
        {str: str}
        """
        bucket, key = split_path(path)
        response = self._call_s3(self.s3.get_object_tagging,
                                 Bucket=bucket, Key=key)
        return {v['Key']: v['Value'] for v in response['TagSet']}

    def put_tags(self, path, tags, mode='o'):
        """Set tags for given existing key

        Tags are a str:str mapping that can be attached to any key, see
        https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html

        This is similar to, but distinct from, key metadata, which is usually
        set at key creation time.

        Parameters
        ----------
        path: str
            Existing key to attach tags to
        tags: dict str, str
            Tags to apply.
        mode:
            One of 'o' or 'm'
            'o': Will over-write any existing tags.
            'm': Will merge in new tags with existing tags.  Incurs two remote
            calls.
        """
        bucket, key = split_path(path)

        if mode == 'm':
            existing_tags = self.get_tags(path=path)
            existing_tags.update(tags)
            new_tags = [{'Key': k, 'Value': v}
                        for k, v in existing_tags.items()]
        elif mode == 'o':
            new_tags = [{'Key': k, 'Value': v} for k, v in tags.items()]
        else:
            raise ValueError("Mode must be {'o', 'm'}, not %s" % mode)

        tag = {'TagSet': new_tags}
        self._call_s3(self.s3.put_object_tagging,
                      Bucket=bucket, Key=key, Tagging=tag)

    def getxattr(self, path, attr_name, **kwargs):
        """ Get an attribute from the metadata.

        Examples
        --------
        >>> mys3fs.getxattr('mykey', 'attribute_1')  # doctest: +SKIP
        'value_1'
        """
        xattr = self.metadata(path, **kwargs)
        if attr_name in xattr:
            return xattr[attr_name]
        return None

    def setxattr(self, path, copy_kwargs=None, **kw_args):
        """ Set metadata.

        Attributes have to be of the form documented in the
        `Metadata Reference`_.

        Parameters
        ---------
        kw_args : key-value pairs like field="value", where the values must be
            strings. Does not alter existing fields, unless
            the field appears here - if the value is None, delete the
            field.
        copy_kwargs : dict, optional
            dictionary of additional params to use for the underlying
            s3.copy_object.

        Examples
        --------
        >>> mys3file.setxattr(attribute_1='value1', attribute_2='value2')  # doctest: +SKIP
        # Example for use with copy_args
        >>> mys3file.setxattr(copy_kwargs={'ContentType': 'application/pdf'},
        ...     attribute_1='value1')  # doctest: +SKIP


        .. Metadata Reference:
        http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-metadata
        """

        bucket, key = split_path(path)
        metadata = self.metadata(path)
        metadata.update(**kw_args)
        copy_kwargs = copy_kwargs or {}

        # remove all keys that are None
        for kw_key in kw_args:
            if kw_args[kw_key] is None:
                metadata.pop(kw_key, None)

        self._call_s3(
            self.s3.copy_object,
            copy_kwargs,
            CopySource="{}/{}".format(bucket, key),
            Bucket=bucket,
            Key=key,
            Metadata=metadata,
            MetadataDirective='REPLACE',
        )

        # refresh metadata
        self._metadata_cache[path] = metadata

    def chmod(self, path, acl, **kwargs):
        """ Set Access Control on a bucket/key

        See http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl

        Parameters
        ----------
        path : string
            the object to set
        acl : string
            the value of ACL to apply
        """
        bucket, key = split_path(path)
        if key:
            if acl not in key_acls:
                raise ValueError('ACL not in %s', key_acls)
            self._call_s3(self.s3.put_object_acl,
                          kwargs, Bucket=bucket, Key=key, ACL=acl)
        else:
            if acl not in buck_acls:
                raise ValueError('ACL not in %s', buck_acls)
            self._call_s3(self.s3.put_bucket_acl,
                          kwargs, Bucket=bucket, ACL=acl)

    def url(self, path, expires=3600, **kwargs):
        """ Generate presigned URL to access path by HTTP

        Parameters
        ----------
        path : string
            the key path we are interested in
        expires : int
            the number of seconds this signature will be good for.
        """
        bucket, key = split_path(path)
        return self.s3.generate_presigned_url(
            ClientMethod='get_object', Params=dict(Bucket=bucket, Key=key,
                                                   **kwargs),
            ExpiresIn=expires)

    def merge(self, path, filelist, **kwargs):
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
        mpu = self._call_s3(
            self.s3.create_multipart_upload,
            kwargs,
            Bucket=bucket,
            Key=key
        )
        out = [self._call_s3(
            self.s3.upload_part_copy,
            kwargs,
            Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
            CopySource=f, PartNumber=i + 1)
            for (i, f) in enumerate(filelist)]
        parts = [{'PartNumber': i + 1, 'ETag': o['CopyPartResult']['ETag']} for
                 (i, o) in enumerate(out)]
        part_info = {'Parts': parts}
        self.s3.complete_multipart_upload(Bucket=bucket, Key=key,
                                          UploadId=mpu['UploadId'],
                                          MultipartUpload=part_info)
        self.invalidate_cache(path)

    def copy_basic(self, path1, path2, **kwargs):
        """ Copy file between locations on S3 """
        buc1, key1 = split_path(path1)
        buc2, key2 = split_path(path2)
        try:
            self._call_s3(
                self.s3.copy_object,
                kwargs,
                Bucket=buc2, Key=key2, CopySource='/'.join([buc1, key1])
            )
        except (ClientError, ParamValidationError) as e:
            raise_from(IOError('Copy failed', (path1, path2)), e)

    def copy_managed(self, path1, path2, **kwargs):
        buc1, key1 = split_path(path1)
        buc2, key2 = split_path(path2)
        copy_source = {
            'Bucket': buc1,
            'Key': key1
        }
        try:
            self.s3.copy(
                CopySource=copy_source,
                Bucket=buc2,
                Key=key2,
                ExtraArgs=self._get_s3_method_kwargs(
                    self.s3.copy_object,
                    kwargs
                )
            )
        except (ClientError, ParamValidationError) as e:
            raise_from(IOError('Copy failed', (path1, path2)), e)

    def copy(self, path1, path2, **kwargs):
        self.copy_managed(path1, path2, **kwargs)
        self.invalidate_cache(path2)

    def bulk_delete(self, pathlist, **kwargs):
        """
        Remove multiple keys with one call

        Parameters
        ----------
        pathlist : listof strings
            The keys to remove, must all be in the same bucket.
        """
        if not pathlist:
            return
        buckets = {split_path(path)[0] for path in pathlist}
        if len(buckets) > 1:
            raise ValueError("Bulk delete files should refer to only one "
                             "bucket")
        bucket = buckets.pop()
        if len(pathlist) > 1000:
            for i in range((len(pathlist) // 1000) + 1):
                self.bulk_delete(pathlist[i * 1000:(i + 1) * 1000])
            return
        delete_keys = {'Objects': [{'Key': split_path(path)[1]} for path
                                   in pathlist]}
        for path in pathlist:
            self.invalidate_cache(self._parent(path))
        try:
            self._call_s3(
                self.s3.delete_objects,
                kwargs,
                Bucket=bucket, Delete=delete_keys)
        except ClientError as e:
            raise_from(IOError('Bulk delete failed'), e)

    def rm(self, path, recursive=False, **kwargs):
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
        bucket, key = split_path(path)
        if recursive:
            files = self.find(path, maxdepth=None)
            if key and not files:
                raise FileNotFoundError(path)
            self.bulk_delete(files, **kwargs)
            if not key:
                self.rmdir(bucket)
            return
        if key:
            if not self.exists(path):
                raise FileNotFoundError(path)
            try:
                self._call_s3(
                    self.s3.delete_object, kwargs, Bucket=bucket, Key=key)
            except ClientError as e:
                raise_from(IOError('Delete key failed', (bucket, key)), e)
            self.invalidate_cache(self._parent(path))
        else:
            if self.exists(bucket):
                try:
                    self.s3.delete_bucket(Bucket=bucket)
                except BotoCoreError as e:
                    raise_from(IOError('Delete bucket failed', bucket), e)
                self.invalidate_cache(bucket)
                self.invalidate_cache('')
            else:
                raise IOError('Not empty', path)

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            self.dircache.pop(path, None)

    def find(self, path, maxdepth=None):
        if path in ['', '*', 's3://']:
            raise ValueError('Cannot crawl all of S3')
        return super().find(path, maxdepth=maxdepth)


class S3File(AbstractBufferedFile):
    """
    Open S3 key as a file. Data is only loaded and cached on demand.

    Parameters
    ----------
    s3 : S3FileSystem
        boto3 connection
    path : string
        S3 bucket/key to access
    mode : str
        One of 'rb', 'wb', 'ab'. These have the same meaning
        as they do for the built-in `open` function.
    block_size : int
        read-ahead size for finding delimiters
    fill_cache : bool
        If seeking to new a part of the file beyond the current buffer,
        with this True, the buffer will be filled between the sections to
        best support random access. When reading only a few specific chunks
        out of a file, performance may be better if False.
    acl: str
        Canned ACL to apply
    version_id : str
        Optional version to read the file at.  If not specified this will
        default to the current version of the object.  This is only used for
        reading.

    Examples
    --------
    >>> s3 = S3FileSystem()  # doctest: +SKIP
    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     ...  # doctest: +SKIP

    See Also
    --------
    S3FileSystem.open: used to create ``S3File`` objects

    """
    retries = 5
    part_min = 5 * 2 ** 20
    part_max = 5 * 2 ** 30

    def __init__(self, s3, path, mode='rb', block_size=5 * 2 ** 20, acl="",
                 version_id=None, fill_cache=True, s3_additional_kwargs=None,
                 autocommit=True, cache_type='bytes'):
        if not split_path(path)[1]:
            raise IOError('Attempt to open non key-like path: %s' % path)
        self.version_id = version_id
        self.acl = acl
        self.mpu = None
        self.parts = None
        self.fill_cache = fill_cache
        self.s3_additional_kwargs = s3_additional_kwargs or {}
        super().__init__(s3, path, mode, block_size, autocommit=autocommit,
                         cache_type=cache_type)
        if self.writable():
            if block_size < 5 * 2 ** 20:
                raise ValueError('Block size must be >=5MB')
        else:
            if version_id and self.fs.version_aware:
                self.version_id = version_id
                self.details = self.fs.info(self.path, version_id=version_id)
                self.size = self.details['size']
            elif self.fs.version_aware:
                self.version_id = self.details.get('VersionId')

        if 'a' in mode and s3.exists(path):
            loc = s3.info(path)['size']
            if loc < 5 * 2 ** 20:
                # existing file too small for multi-upload: download
                self.write(self.fs.cat(self.path))
                self.append_block = False
            else:
                self.append_block = True
            self.loc = loc

    def _call_s3(self, method, *kwarglist, **kwargs):
        return self.fs._call_s3(method, self.s3_additional_kwargs, *kwarglist,
                                **kwargs)

    def _initiate_upload(self):
        bucket, key = split_path(self.path)
        if self.acl and self.acl not in key_acls:
            raise ValueError('ACL not in %s', key_acls)
        self.parts = []
        self.size = 0
        if self.blocksize < 5 * 2 ** 20:
            raise ValueError('Block size must be >=5MB')
        try:
            self.mpu = self._call_s3(
                self.fs.s3.create_multipart_upload,
                Bucket=bucket, Key=key, ACL=self.acl)
        except (ClientError, ParamValidationError) as e:
            raise_from(IOError('Initiating write failed: %s' % self.path), e)
        if 'a' in self.mode and self.fs.exists(self.path):
            if self.append_block:
                # use existing data in key when appending,
                # and block is big enough
                out = self.fs._call_s3(
                    self.fs.s3.upload_part_copy,
                    self.s3_additional_kwargs,
                    Bucket=bucket,
                    Key=key, PartNumber=1,
                    UploadId=self.mpu['UploadId'],
                    CopySource=self.path)
                self.parts.append({'PartNumber': 1,
                                   'ETag': out['CopyPartResult']['ETag']})

    def metadata(self, refresh=False, **kwargs):
        """ Return metadata of file.
        See :func:`~s3fs.S3Filesystem.metadata`.

        Metadata is cached unless `refresh=True`.
        """
        return self.fs.metadata(self.path, refresh, **kwargs)

    def getxattr(self, xattr_name, **kwargs):
        """ Get an attribute from the metadata.
        See :func:`~s3fs.S3Filesystem.getxattr`.

        Examples
        --------
        >>> mys3file.getxattr('attribute_1')  # doctest: +SKIP
        'value_1'
        """
        return self.fs.getxattr(self.path, xattr_name, **kwargs)

    def setxattr(self, copy_kwargs=None, **kwargs):
        """ Set metadata.
        See :func:`~s3fs.S3Filesystem.setxattr`.

        Examples
        --------
        >>> mys3file.setxattr(attribute_1='value1', attribute_2='value2')  # doctest: +SKIP
        """
        if self.writable():
            raise NotImplementedError('cannot update metadata while file '
                                      'is open for writing')
        return self.fs.setxattr(self.path, copy_kwargs=copy_kwargs, **kwargs)

    def url(self, **kwargs):
        """ HTTP URL to read this file (if it already exists)
        """
        return self.fs.url(self.path, **kwargs)

    def _fetch_range(self, start, end):
        bucket, key = self.path.split('/', 1)
        return _fetch_range(self.fs.s3, bucket, key, self.version_id, start, end)

    def _upload_chunk(self, final=False):
        bucket, key = split_path(self.path)
        self.buffer.seek(0)
        (data0, data1) = (None, self.buffer.read(self.blocksize))
        while data1:
            (data0, data1) = (data1, self.buffer.read(self.blocksize))
            data1_size = len(data1)

            if 0 < data1_size < self.blocksize:
                remainder = data0 + data1
                remainder_size = self.blocksize + data1_size

                if remainder_size <= self.part_max:
                    (data0, data1) = (remainder, None)
                else:
                    partition = remainder_size // 2
                    (data0, data1) = (remainder[:partition], remainder[partition:])

            part = len(self.parts) + 1

            for attempt in range(self.retries + 1):
                try:
                    out = self._call_s3(
                        self.fs.s3.upload_part,
                        Bucket=bucket,
                        PartNumber=part, UploadId=self.mpu['UploadId'],
                        Body=data0, Key=key)
                except S3_RETRYABLE_ERRORS:
                    if attempt < self.retries:
                        logger.debug('Exception %e on S3 write, retrying',
                                     exc_info=True)
                except Exception as exc:
                    raise IOError('Write failed', self, exc)
                else:
                    break
            else:
                raise IOError(
                    'Write failed after %i retries' % self.retries, self)

            self.parts.append({'PartNumber': part, 'ETag': out['ETag']})

        if self.autocommit and final:
            self.commit()

    def commit(self):
        logger.debug("COMMIT")
        bucket, key = self.path.split('/', 1)
        part_info = {'Parts': self.parts}
        write_result = self._call_s3(
            self.fs.s3.complete_multipart_upload,
            Bucket=bucket,
            Key=key,
            UploadId=self.mpu['UploadId'],
            MultipartUpload=part_info)
        if self.fs.version_aware:
            self.version_id = write_result.get('VersionId')

        # complex cache invalidation, since file's appearance can cause several
        # directories
        parts = self.path.split('/')
        path = parts[0]
        for p in parts[1:]:
            if path in self.fs.dircache and not [
                    True for f in self.fs.dircache[path]
                    if f['name'] == path + '/' + p]:
                self.fs.invalidate_cache(path)
            path = path + '/' + p


def _fetch_range(client, bucket, key, version_id, start, end, max_attempts=10,
                 req_kw=None):
    if req_kw is None:
        req_kw = {}
    logger.debug("Fetch: %s/%s, %s-%s", bucket, key, start, end)
    for i in range(max_attempts):
        try:
            if version_id is not None:
                kwargs = dict({'VersionId': version_id}, **req_kw)
            else:
                kwargs = req_kw
            resp = client.get_object(Bucket=bucket, Key=key,
                                     Range='bytes=%i-%i' % (start, end - 1),
                                     **kwargs)
            return resp['Body'].read()
        except S3_RETRYABLE_ERRORS as e:
            logger.debug('Exception %e on S3 download, retrying', e,
                         exc_info=True)
            continue
        except OSError as e:
            # only retry connection_errors - similar to catching
            # ConnectionError on py3
            if e.errno not in _CONNECTION_ERRORS:
                raise
            logger.debug('ConnectionError %e on S3 download, retrying', e,
                         exc_info=True)
            continue
        except ClientError as e:
            if e.response['Error'].get('Code', 'Unknown') in ['416',
                                                              'InvalidRange']:
                return b''
            else:
                raise
        except Exception as e:
            if 'time' in str(e).lower():  # Actual exception type changes often
                continue
            else:
                raise
    raise RuntimeError("Max number of S3 retries exceeded")
