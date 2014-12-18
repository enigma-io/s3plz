# -*- coding: utf-8 -*-
import unittest
import s3pyo

MY_TEST_BUCKET = 's3://enigma-euclid'

class TestS3PO(unittest.TestCase):
	
	def test_s3po(self):
		"""
		simple sanity test in lieu of full suite
		"""
		plz = s3pyo.please(MY_TEST_BUCKET, 
			serializer="json.gz",
			public = False
		)	
		obj = {"key": "value"}
		filepath = 'test/{@date_path}/{key}/{@timestamp}.json.gz'
		fp = plz.put(obj, filepath, **obj)
		print fp
		obj =  plz.get(fp)
		print obj
		assert(obj == obj)


