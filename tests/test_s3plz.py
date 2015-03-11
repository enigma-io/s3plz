# -*- coding: utf-8 -*-
import unittest
import s3plz
import os
from datetime import timedelta
import logging
logging.getLogger('boto').setLevel(logging.CRITICAL)

MY_TEST_BUCKET = os.getenv('S3PLZ_TEST_BUCKET', None)
if not MY_TEST_BUCKET:
    raise ValueError("""
        To run tests, you must set these 
        environmental variabels:

        export AWS_ACCESS_KEY_ID='fdsaf'
        export AWS_ACCESS_KEY_SECRET='fdsaf'
        export S3PLZ_TEST_BUCKET='s3://my-cool-bucket'
        """
        )

class TestS3plz(unittest.TestCase):
    
    def test_s3plz(self):
        """
        Simple workflow which addresses 
        all methods and use cases.
        """
        # try:
        # connect to s3
        plz = s3plz.connect(MY_TEST_BUCKET, 
            public = False
        )   

        # create an object and formatstring
        obj1 = {"key": "value"}
        formatstring = 's3plztest-0.1.2/{@date_path}/{key}/{@uid}.json.gz'

        # put the object
        fp1 = plz.put(obj1, formatstring, serializer = "json.gz", **obj1)

        # check exists / put method (did the object make it there?)
        assert(plz.exists(fp1) is not False)

        # check get method
        obj2 =  plz.get(fp1, serializer = "json.gz",)
        
        # check whether serialization / deserialization works
        assert(obj1 == obj2)

        # check ls / filepath formatting.
        for fp in plz.ls('s3plztest-0.1.2/'):
            print fp
            assert('value' in fp)

        # check streaming method / deserialization
        for fp, obj in plz.stream('s3plztest-0.1.2/', serializer="json.gz"):
            print obj
            assert("key" in obj)
            assert(isinstance(obj, dict))
            assert("value" in fp)

        # check upsert method / whether 
        # updates to contextual time variables
        # are reflected in formatted filepaths.
        fp2 = plz.upsert(obj1, formatstring,  serializer="json.gz", **obj1)
        assert(fp2 is not False)
        assert(fp1 != fp2)

        # check on-the-fly serialization
        obj1 = {"foo":"value"}
        fp = plz.put(obj1, "s3plztest-0.1.2/{foo}.json.gz", serializer="json.gz", **obj1)
        obj2 = plz.get(fp, serializer="json.gz")
        assert(obj1 == obj2)

        string1 = "hello world"
        fp = plz.put(string1, "s3plztest-0.1.2/string.zip", serializer="zip")
        string2 = plz.get(fp, serializer="zip")
        assert(string1 == string2)

        # check metadata
        meta = plz.get_meta(fp)
        assert(meta['last_modified'] is not None)

        # check age 
        age = plz.get_age(fp)
        assert(isinstance(age, timedelta))

        # check whether seri

        # check whether delete method works 
        for fp in plz.ls('s3plztest-0.1.2/'):
            plz.delete(fp)
        assert(len(list(plz.ls('s3plztest-0.1.2/'))) == 0)


    def test_json(self):
        obj1 = {"key": "value"}
        jsonstring = s3plz.utils.to_json(obj1)
        obj2 = s3plz.utils.from_json(jsonstring)
        assert(obj1 == obj2)

    def test_gz(self):
        string1 = "uqbar"
        gzstring = s3plz.utils.to_gz(string1)
        string2 = s3plz.utils.from_gz(gzstring)
        assert(string1 == string2)

    def test_zip(self):
        string1 = "uqbar"
        gzstring = s3plz.utils.to_zip(string1)
        string2 = s3plz.utils.from_zip(gzstring)
        assert(string1 == string2)

    def test_pickle(self):
        string1 = "uqbar"
        gzstring = s3plz.utils.to_pickle(string1)
        string2 = s3plz.utils.from_pickle(gzstring)
        assert(string1 == string2)

    def test_auth_error(self):
        print "bad s3plz connections should throw an S3AuthError"
        try:
            s3plz.connect(MY_TEST_BUCKET, key=None, secret=None)
        except s3plz.S3AuthError:
            assert True 
        else:
            assert False







