# s3plz 

A polite, minimal interface for sending python objects 
to and from Amazon S3.

## Installation

```
pip install s3plz
```

## Tests

To run tests, you must first set these 
environmental variabels:

```
export AWS_ACCESS_KEY_ID='fdsaf'
export AWS_ACCESS_KEY_SECRET='fdsaf'
export S3PLZ_TEST_BUCKET='s3://my-cool-bucket'

```
and then run:
```
nosetests
```

## Bacic Usage

```python

import s3plz

# Return an `s3plz.S3` object with 
# methods for sending objects
# to and from Amazon S3.

plz = s3plz.connect('s3://asteroid', 
	key='navigate',
	secret='shield',
	serializer="json.gz",
	public = False
)

# You can also set `AWS_ACCESS_KEY_ID` and 
# `AWS_ACCESS_KEY_SECRET` as environmental variables
# instead of passing `key` and `secret` to `s3plz.connect`



# Serialize an object, format its
# filepath, put it on s3, and return
# the formatted filepath (with an absolute s3path) 
# for your records

obj1 = {"key": "value"}
filepath = 'test/{key}.json.gz'

fp = plz.put(obj1, filepath, **obj1)
print fp

# >>> 's3://asteroid/test/value.json.gz'
# you can now fetch this object with its filepath

obj2 =  plz.get(fp)
assert(obj1 == obj2)

```

## Customization.

## Filepaths

`s3plz` will attempt to format your filepath
for you given arbitary `**kwargs` passed to 
any method. You also have access to UTC 
time via the "@" operator.

These include:

- `@second`: `56`
- `@minute`: `54`
- `@hour`: `23`
- `@day`: `29`
- `@month`: `01`
- `@year`: `2014`
- `@timestamp`: `1234567`
- `@date_path`: `2014/01/14`
- `@date_slug`: `2014-01-14`
- `@datetime_slug`: `2013-12-12-06-08-52`
- `@uid`: `dasfas-23r32-sad-3sadf-sdf`

For instance,

``` python 
import s3plz

obj = {"key": "value"}
filepath = 'test/{key}/{@date_path}/{@uid}.json.gz'

plz = s3plz.connect('s3://my-bucket')
fp = plz.put(obj, filepath, **obj)
print fp 
# >>> 's3://my-bucket/test/value/2014/08/25/3225-sdsa-35235-asdfas-235.json.gz'

```

### Serialization

By default, `s3plz` will send strings to/from S3. You can also serialize / deserialize objects to / from `json.gz`, `json`, `gz`, or `zip` (set with `serialize` via `s3plz.connect`). However, you can also inherit from the core `s3plz.S3` class and overwrite the `serialize` and `deserialize` methods.

```python

from s3plz import S3

class SqlAlchemyToS3(S3):

	def serialize(self, obj):
		return "Do something here."

	def deserialize(self, string):
		return "Undo it."

s3 = SqlAlchemyToS3('s3://bucket')
print s3.get('s3://bucket/file.mycoolformat')
# >>> `A SqLAlchemy Model`
```
