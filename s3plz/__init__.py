# -*- coding: utf-8 -*-

import os
import boto
import boto.s3
from boto.s3.key import Key
from dateutil import parser
import utils


def connect(uri, **kw):
    """
    A wrapper for the core class 
    for a more elegant API:

        import s3plz

        s3 = s3plz.connect('s3://enigma-euclid')
        s3.get('test/file.txt')

    """
    return S3(uri, **kw)

class S3:
    
    """ 
    A class for connecting to a s3 bucket and 
    uploading/downloading files.

    Includes support for automatically formatting 
    filepaths from time / contextual variables / uids 
    as well as serializing and deserializing objects 
    to and from s3. 
    """

    def __init__(self, uri, **kw):
      
        # get bucket name / abs root.
        self.bucket_name = utils.parse_s3_bucket(uri)
        self.s3root = "s3://{}".format(self.bucket_name)
        
        # connect to bucket
        self.bucket = self._connect_to_bucket(**kw)
        
        # set public/private
        self.acl_str = self._set_acl_str(kw.get('public', False))

        # get the serializer, we don't set a default 
        # for reasons explained in the `self.serializer`
        # docs.
        self.serializer = kw.get('serializer', None)

 
    def put(self, data, filepath, **kw):
        """
        Upload a file to s3 with serialization.
        """
        return self._put(data, filepath, **kw)


    def upsert(self, data, filepath, **kw): 
        """
        Upload a file if it doesnt already exist,
        otherwise return False
        """
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
        return {
            "content_type": k.content_type,
            "last_modified": parser.parse(k.last_modified),
            "content_language": k.content_language,
            "content_encoding": k.content_encoding,
            "content_length": k.content_length
        }

    def get_age(self, filepath, **kw):
        meta = self.get_meta(filepath, **kw)
        if not meta['last_modified']:
            return None
        return utils.now(ts=False) - meta['last_modified']

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
            obj = self._get(fp)
            yield fp, obj

    def delete(self, filepath, **kw):
        """
        Delete a file from s3.
        """
        return self._delete(filepath, **kw)

    def serialize(self, obj):
        """
        Function for serializing object => string.
        This can be overwritten for custom 
        uses.

        The default is to do nothing ('serializer'=None)
        If the connection is intialized with 'serializer' set to 
        'json.gz', 'json', 'gz', or 'zip', we'll do the 
        transformations.
        """

        if self.serializer == "json.gz":
            return utils.to_gz(utils.to_json(obj))
        
        elif self.serializer == "json":
            return utils.to_json(obj)

        elif self.serializer == "gz":
            assert(isinstance(obj, basestring))
            return utils.to_gz(obj)

        elif self.serializer == "zip":
            assert(isinstance(obj, basestring))
            return utils.to_zip(obj)

        elif self.serializer == "pickle":
            return utils.to_pickle(obj)

        elif self.serializer is not None:

            raise NotImplementedError(
                'Only json, gz, json.gz, zip, and pickle'
                'are supported as serializers.')

        return obj

    def deserialize(self, string):
        """
        Function for serializing object => string.
        This can be overwritten for custom 
        uses.

        The default is to do nothing ('serializer'=None)
        If the connection is intialized with 'serializer' set to 
        'json.gz', 'json', 'gz', or 'zip', we'll do the 
        transformations.
        """

        if self.serializer == "json.gz":
            return utils.from_json(utils.from_gz(string))
        
        elif self.serializer == "json":
            return utils.from_json(string)

        elif self.serializer == "gz":
            return utils.from_gz(string)

        elif self.serializer == "zip":
            return utils.from_zip(string)

        elif self.serializer == "pickle":
            return utils.from_pickle(obj)

        elif self.serializer is not None:

            raise NotImplementedError(
                'Only json, gz, json.gz, zip, and pickle'
                'are supported as serializers.')
        
        return string


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
        
        # connect
        conn = boto.connect_s3(key, secret)

        # lookup bucket
        for b in conn.get_all_buckets():
            if self.bucket_name == b.name:
                return b

        # bucket doesn't exist.
        raise ValueError(
            'Bucket {} Does Not Exist!'\
            .format(self.bucket_name))

    def _gen_key_from_fp(self, filepath, **kw):
        """
        Take in a filepath and create a `boto.Key` for
        interacting with the file. Optionally reset serializer too! 

        """
        self.serializer = kw.get('serializer', self.serializer)
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
        string = self.serialize(data)
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
            return self.deserialize(string)
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


