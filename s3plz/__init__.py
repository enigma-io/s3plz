# -*- coding: utf-8 -*-

import boto
import boto.s3
import os
import utils
import warnings

from boto.s3.key import Key
from dateutil import parser


def connect(uri, **kw):
    """
    A wrapper for the core class
    for a more elegant API:

        import s3plz

        s3 = s3plz.connect('s3://enigma-euclid')
        s3.get('test/file.txt')

    """
    return S3(uri, **kw)


class S3AuthError(Exception):
    """
    If Auth values are not set, this error will be thrown.
    """


class S3:

    """
    A class for connecting to a s3 bucket and
    uploading/downloading files.

    Includes support for automatically formatting
    filepaths from time / contextual variables / uids
    as well as serializing and deserializing objects
    to and from s3.
    """

    serializers = {
        'json': utils.to_json,
        'gz': utils.to_gz,
        'zip': utils.to_zip,
        'pickle': utils.to_pickle,
    }

    deserializers = {
        'json': utils.from_json,
        'gz': utils.from_gz,
        'zip': utils.from_zip,
        'pickle': utils.from_pickle,
    }

    def __init__(self, uri, **kw):

        # get bucket name / abs root.
        self.bucket_name = utils.parse_s3_bucket(uri)
        self.s3root = "s3://{}".format(self.bucket_name)

        # connect to bucket
        self.bucket = self._connect_to_bucket(**kw)

        # set public/private
        self.acl_str = self._set_acl_str(kw.get('public', False))

        # set a default serializer for this connection.
        self._serializer = kw.get('serializer', None)

    def put(self, data, filepath, **kw):
        """
        Upload a file to s3 with serialization.
        """
        return self._put(data, filepath, **kw)

    def create(self, data, filepath, **kw):
        """
        Upload a file if it doesnt already exist,
        otherwise return False
        """
        k = self._gen_key_from_fp(filepath, **kw)
        if not k.exists():
            return self._put(data, filepath, **kw)
        else:
            return False

    def upsert(self, data, filepath, **kw):
        """
        Synonym for create, kept for backwards
        compatibility, can/will eventually be
        deprecated in a major version release
        """
        warnings.warn("""upsert is a deprecated method kept for backwards compatibility.
                         please move to `S3.create` for the same functionality""")
        k = self._gen_key_from_fp(filepath, **kw)
        if not k.exists():
            return self._put(data, filepath, **kw)
        else:
            return False

    def get(self, filepath, **kw):
        """
        Download a file from s3. If it doesn't exist return None.
        """
        return self._get(filepath, **kw)

    def get_meta(self, filepath, **kw):
        """
        Get a dictionary of metadata fields for a filepath.
        """
        k = self._gen_key_from_fp(filepath, **kw)
        k.name = k.key
        k = self.bucket.get_key(k.name)
        if k:
            return {
                "content_type": k.content_type,
                "last_modified": parser.parse(k.last_modified),
                "content_language": k.content_language,
                "content_encoding": k.content_encoding,
                "content_length": k.content_length
            }
        else:
            return None

    def get_age(self, filepath, **kw):
        """
        Get the age of a filepath. Returns a datetime.timedelta object.
        """
        meta = self.get_meta(filepath, **kw)
        if meta:
            if not meta['last_modified']:
                return None
            return utils.now(ts=False) - meta['last_modified']
        else:
            return None

    def exists(self, filepath, **kw):
        """
        Check if a file exists on s3.
        """
        k = self._gen_key_from_fp(filepath, **kw)
        if k.exists():
            return self._make_abs(str(k.key))
        else:
            return False

    def ls(self, directory='', **kw):
        """
        Return a generator of filepaths under a directory.
        """
        directory = self._format_filepath(directory, **kw)

        # s3 requires directories end with '/'
        if not directory.endswith('/'):
            directory += "/"

        for k in self.bucket.list(directory):
            yield self._make_abs(str(k.key))

    def stream(self, directory='', **kw):
        """
        Return a generator which contains a
        tuple of (filepath, filecontents) from s3.
        """
        directory = self._format_filepath(directory, **kw)

        # s3 requires directories end with '/'
        if not directory.endswith('/'):
            directory += "/"

        for k in self.bucket.list(directory):
            fp = self._make_abs(str(k.key))
            obj = self._get(fp, **kw)
            yield fp, obj

    def delete(self, filepath, **kw):
        """
        Delete a file from s3.
        """
        return self._delete(filepath, **kw)

    def serialize(self, obj, **kw):
        """
        Function for serializing object => string.
        This can be overwritten for custom
        uses.

        The default is to do nothing ('serializer'=None)
        If the connection is intialized with 'serializer' set to
        'json.gz', 'json', 'gz', or 'zip', we'll do the
        transformations.

        Any number of serializers can be specified in dot delimited
        format, and will be applied left to right.
        """
        serializer = kw.get('serializer',  self._serializer)

        # Default is do nothing
        if serializer is None:
            return obj

        result = obj
        for name in serializer.split('.'):
            # Apply dot seperated serializers left to right
            try:
                result = self.serializers[name](result)
            except KeyError:
                raise NotImplementedError(
                    '{} is not a supported serializer. Try one of: {}'.format(
                        name,
                        ','.join(self.serializers.keys())
                    )
                )

        return result

    def deserialize(self, string, **kw):
        """
        Function for serializing object => string.
        This can be overwritten for custom
        uses.

        The default is to do nothing ('serializer'=None)
        If the connection is intialized with 'serializer' set to
        'json.gz', 'json', 'gz', or 'zip', we'll do the
        transformations.

        Any number of serializers can be specified in dot delimited
        format, and will be applied right to left.
        """

        serializer = kw.get('serializer',  self._serializer)

        # Default is do nothing
        if serializer is None:
            return string

        result = string
        for name in reversed(serializer.split('.')):
            # Apply dot seperated serializers left to right
            try:
                result = self.deserializers[name](result)
            except KeyError:
                raise NotImplementedError(
                    '{} is not a supported deserializer. Try one of: {}'.format(
                        name,
                        ','.join(self.deserializers.keys())
                    )
                )

        return result

    def _set_acl_str(self, public):
        """
        Simplified lookup for acl string settings.
        """

        return {True: 'public-read', False: 'private'}.get(public)

    def _connect_to_bucket(self, **kw):
        """
        Connect to a pre-existing s3 code. via
        kwargs or OS
        """

        # get keys from kwargs / environment
        key = kw.get('key', \
            os.getenv('AWS_ACCESS_KEY_ID'))
        secret = kw.get('secret', \
            os.getenv('AWS_SECRET_ACCESS_KEY'))

        # check for valid key / secret
        if not key or not secret:
            raise S3AuthError, \
            'You must pass in a "key" and "secret" to s3plz.connect() or set ' \
            '"AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" as environment variables.'

        try:
            conn = boto.connect_s3(key, secret)
        except Exception as e:
            raise S3AuthError, \
            "Your supplied credentials were invalid!"

        # lookup bucket
        return conn.get_bucket(self.bucket_name)

        # bucket doesn't exist.
        raise ValueError(
            'Bucket {} Does Not Exist!'\
            .format(self.bucket_name))

    def _gen_key_from_fp(self, filepath, **kw):
        """
        Take in a filepath and create a `boto.Key` for
        interacting with the file. Optionally reset serializer too!

        """
        k = Key(self.bucket)
        fp = self._format_filepath(filepath, **kw)
        k.key = fp
        k.name = fp
        return k

    def _format_filepath(self, filepath, **kw):
        """
        Allow for inclusion of absolute filepaths / format strings.
        """
        if filepath.startswith('s3://'):
            # boto doesn't accept absolute s3paths
            filepath = filepath.replace(self.s3root, '')

        if filepath.startswith('/'):
            # these can be left straggling
            # by the above conditional
            filepath = filepath[1:]

        return utils.format_filepath(filepath, **kw)

    def _make_abs(self, filepath):
        """
        Output only absolute filepaths. Fight me.
        """
        if not filepath.startswith('s3://'):
            filepath = '{}/{}'.format(self.s3root, filepath)
        return filepath

    def _put(self, data, filepath, **kw):
        """
        Wrapper for serialization => s3
        """
        headers = kw.pop('headers', {})
        k = self._gen_key_from_fp(filepath, **kw)
        string = self.serialize(data, **kw)
        k.set_contents_from_string(string, headers=headers)
        k.set_acl(self.acl_str)
        return self._make_abs(str(k.key))

    def _get(self, filepath, **kw):
        """
        Wrapper for s3 => deserialization
        """
        headers = kw.pop('headers', {})
        k = self._gen_key_from_fp(filepath, **kw)
        if k.exists():
            string = k.get_contents_as_string(headers=headers)
            return self.deserialize(string, **kw)
        else:
            return None

    def _delete(self, filepath, **kw):
        """
        Wrapper for delete. Unnecessary but
        Is nice to have for expanding on
        the core class without writing `boto` code.
        """
        k = self._gen_key_from_fp(filepath, **kw)
        self.bucket.delete_key(k)
        return self._make_abs(str(k.key))
