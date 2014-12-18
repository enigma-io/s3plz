# -*- coding: utf-8 -*-

import os
import boto
import boto.s3
from boto.s3.key import Key

from utils import *

def please(uri, **kw):
    """
    A wrapper for the core class 
    for a more elegant API:

        import s3pyo

        plz = s3pyo.please('s3://enigma-euclid')
        plz.get('test/file.txt')

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
        self.bucket_name = parse_s3_uri(uri)
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
        only upload a file if it doesnt already exist,
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
        for k in self.bucket.list(directory):
            yield self._make_abs(str(k.key))

    def stream(self, directory='', **kw):
        """
        Return a generator which contains a 
        tuple of (filepath, filecontents from s3.).
        """
        # stream contents and optionally match filepaths
        directory = self._format_filepath(directory, **kw)
        for k in self.bucket.list(directory):
            fp = self._make_abs(str(k.key))
            obj = self.deserialize(k)
            yield fp, obj

    def delete(self, filepath, **kw):
        """
        Delete a File from s3.
        """
        k = self._gen_key_from_fp(filepath, **kw)
        self.bucket.delete_key(k)
        return self._make_abs(str(k.key))

    def serialize(self, obj):
        """
        Function for serializing object => string.
        This can be overwritten for custom 
        uses.

        The default is `None` which means 
        We wont do anything to the object.
        """

        if self.serializer == "json.gz":
            return to_gz(to_json(obj))
        
        elif self.serializer == "json":
            return to_json(obj)

        elif self.serializer == "gz":
            return to_gz(obj)

        elif self.serializer is not None:

            raise NotImplementedError(
                'Only json, gz, and json.gz'
                'are supported as serializers.')

        return obj

    def deserialize(self, string):
        """
        Function for deserializing string => object.
        This can be overwritten for custom 
        uses.

        The default is `None` which means 
        We wont do anything to the string.
        """

        if self.serializer == "json.gz":
            return from_json(from_gz(string))
        
        elif self.serializer == "json":
            return from_json(string)

        elif self.serializer == "gz":
            return from_gz(string)

        elif self.serializer is not None:

            raise NotImplementedError(
                'Only json, gz, and json.gz'
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
        access_key = kw.get('key', \
            os.getenv('AWS_ACCESS_KEY_ID'))
        secret_key = kw.get('secret', \
            os.getenv('AWS_ACCESS_KEY_SECRET'))
        
        # connect
        conn = boto.connect_s3(access_key, secret_key)
        
        # lookup bucket
        for i in conn.get_all_buckets():
            if self.bucket_name == i.name:
                return i

        # bucket doesn't exist.
        raise ValueError(
            'Bucket {} Does Not Exist!'\
            .format(self.bucket_name))

    def _gen_key_from_fp(self, filepath, **kw):
        """
        Take in a filepath and create a `boto.Key` for
        interacting with it.
        """
        k = Key(self.bucket)
        k.key = self._format_filepath(filepath, **kw)
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

        return format_filepath(filepath, **kw)

    def _make_abs(self, filepath):
        """
        Output absolute filepaths from ls and stream!
        """
        if not filepath.startswith('s3://'):
            filepath = '{}/{}'.format(self.s3root, filepath)
        return filepath

    def _put(self, data, filepath, **kw):
        """
        Wrapper for serialization => s3
        """
        
        k = self._gen_key_from_fp(filepath, **kw)
        string = self.serialize(data)
        k.set_contents_from_string(string)
        k.set_acl(self.acl_str)
        return self._make_abs(str(k.key))

    def _get(self, filepath, **kw):
        """
        Wrapper for s3 => deserialization
        """
        k = self._gen_key_from_fp(filepath, **kw)
        if k.exists():
            string = k.get_contents_as_string()
            return self.deserialize(string)
        else:
            return None


