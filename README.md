# s3pyo 

A polite interface for sending python objects to and from Amazon S3.

## Installation

```
pip install s3pyo
```

## Tests

**NOTE** very basic for now. They also assume you have set `AWS_ACCESS_KEY_ID` and 
`AWS_ACCESS_KEY_SECRET` set as environmental variables and have access to an already-created 
bucket.

```
nosetests
```

## Bacic Usage

```python

import s3pyo

# return an `s3pyo.S3` object with 
# methods for sending objects
# to and from Amazon S3.

plz = s3pyo.please('s3://asteroid', 
	key='navigate',
	secret='shield',
	serializer="json.gz",
	public = False
)

# You can also set `AWS_ACCESS_KEY_ID` and 
# `AWS_ACCESS_KEY_SECRET` as environmental variables


# Serialize an object, format its
# filepath, put it on s3, and return
# the formatted filepath (with an absolute s3path) 
# for your records

obj = {"key": "value"}
filepath = 'test/{key}.json.gz'

fp = plz.put(obj, filepath, **obj)
print fp

# >>> 's3://my-bucket/test/value.json.gz'
# you can now fetch this object with its filepath

obj =  plz.get(fp)
assert(obj == obj)

```

## Customization.

## Filepaths

`s3pyo` will attempt to format your filepath
for you given arbitary `**kwargs` passed to 
any method. You also have access to utility values 
accessed by the "@" operator.

These include:

- '@second': "56"
- '@minute': "54"
- '@hour': : "23"
- '@day': "29"
- '@month': "01"
- '@year': : "2014,
- '@timestamp' : "1234567"
- '@date_path' : "2014/01/14"
- '@date_slug' : "2014-01-14,
- '@datetime_slug' : "2013-12-12-06-08-52"
- '@uid': 'dasfas-23r32-sad-3sadf-sdf"

**NOTE** ALL TIME VARIABLES ARE CURRENT UTC.

For instance,

``` python 
import s3pyo

obj = {"key": "value"}
filepath = 'test/{key}/{@date_path}/{@uid}.json.gz'

plz = s3pyo.please('s3://my-bucket')
fp = plz.put(obj, filepath, **obj)
print fp 
# >>> 's3://my-bucket/value/2014/08/25/3225-sdsa-35235-asdfas-235.json.gz'

```



### Serialization

By default, you can serialize / deserialize objects to / from `json.gz`, 
`json`, or `gz` (set with `serialize` via `s3pyo.please`. However, you can also inherit from the core `s3pyo.S3` class and overwrite the `serialize` and `deserialize` methods.

```python

from s3pyo import S3

class SqlAlchemyToS3(S3):

	def serialize(self, obj):
		return "Do something hipster here."

	def deserialize(self, string):
		return "Undo it."

plz = SqlAlchemyToS3('s3://bucket')
print plz.get('s3://bucket/file.mycoolformat')
# >>> `A SqLAlchemy Model`
```

_I can assure you they will never get me onto one of those dreadful buckets._
