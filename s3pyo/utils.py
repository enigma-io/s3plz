# -*- coding: utf-8 -*-

from urlparse import urlparse
from datetime import datetime 
import ujson
import gzip
import uuid
import cStringIO

def is_s3_uri(uri):
    """
    Return True if *uri* can be parsed into an S3 URI, False otherwise.
    """
    try:

        parse_s3_uri(uri)
        return True
    
    except ValueError:

        return False

def parse_s3_uri(uri):
    """Parse an S3 URI into (bucket, key)

    >>> parse_s3_uri('s3://walrus/tmp/')
    ('walrus', 'tmp/')

    If ``uri`` is not an S3 URI, raise a ValueError
    """
    if not uri.endswith('/'):
        uri += '/'
      
    components = urlparse(uri)

    if (components.scheme not in ('s3', 's3n')
            or '/' not in components.path):
    
        raise ValueError('Invalid S3 URI: {}'.format(uri))

    return components.netloc

def filepath_opts():
    """
    Get a dictionary of timestrings
    to pass as default options 
    for `format_filepath`

    These can be accessed with the '@' key.
    
    """
    dt = datetime.utcnow()  
    return {
        '@second': "%02d" % int(dt.second),
        '@minute': "%02d" % int(dt.minute),
        '@hour': "%02d" % int(dt.hour),
        '@day': "%02d" % int(dt.day),
        '@month': "%02d" % int(dt.month),
        '@year': dt.year,
        '@timestamp': dt.strftime('%s'),
        '@date_path': dt.strftime('%Y/%m/%d'),
        '@date_slug' : dt.date().isoformat(),
        '@datetime_slug': dt.strftime('%Y-%m-%d-%H-%M-%S'),
        '@uid': uuid.uuid1()
    }

def format_filepath(fp, **kw):
    """
    Given a format string,
    fill in fields with defaults / data.

    Since this .format is idempotent, it wont 
    affect non-format strings. Thanks @jak
    """
    kw.update(filepath_opts())
    return fp.format(**kw)


def to_gz(s):
    """
    string > gzip
    """
    out = cStringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(s)
    return out.getvalue()

def from_gz(s):
    """
    gzip > string
    """
    fileobj = cStringIO.StringIO(s)
    with gzip.GzipFile(fileobj=fileobj, mode="r") as f:
        return f.read()

def from_json(s):
    """
    jsonstring > obj
    """
    return ujson.loads(s)

def to_json(obj):
    """
    obj > jsonstring
    """
    return ujson.dumps(obj)


