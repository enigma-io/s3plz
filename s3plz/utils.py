# -*- coding: utf-8 -*-

from urlparse import urlparse
from datetime import datetime
import json
import gzip
import zipfile
import uuid
import pickle
import cStringIO
import pytz


def now(ts=True):
    dt = datetime.now(pytz.utc)
    if ts:
        return int(dt.strftime('%s'))
    return dt


def is_s3_uri(uri):
    """
    Return True if *uri* can be parsed into an S3 URI, False otherwise.
    """
    try:

        parse_s3_bucket(uri)
        return True

    except ValueError:

        return False


def parse_s3_bucket(uri, _return_path=False):
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

    if _return_path:
        return components.netloc, components.path

    else:
        return components.netloc


def filepath_opts():
    """
    Get a dictionary of timestrings
    to pass as default options
    for ``s3pyo.utils.format_filepath``

    These can be accessed with the '@' key.

    """
    dt = now(ts=False)
    return {
        '@second': "%02d" % int(dt.second),
        '@minute': "%02d" % int(dt.minute),
        '@hour': "%02d" % int(dt.hour),
        '@day': "%02d" % int(dt.day),
        '@month': "%02d" % int(dt.month),
        '@year': dt.year,
        '@timestamp': dt.strftime('%s'),
        '@date_path': dt.strftime('%Y/%m/%d'),
        '@date_slug': dt.date().isoformat(),
        '@datetime_slug': dt.strftime('%Y-%m-%d-%H-%M-%S'),
        '@uid': uuid.uuid1()
    }


def s3_to_url(s3uri):
    # get the bucket & path, this is a hack for
    # internal purposes, soorry.
    bucket, path = parse_s3_bucket(s3uri, _return_path=True)
    return "http://{}.s3.amazonaws.com/{}".format(bucket, path)


def url_to_s3(url):
    nohttp = url.split('http://')[1]
    bucket, path = nohttp.split('.s3.amazonaws.com/')
    return "s3://{}/{}".forat(bucket, path)


def format_filepath(fp, **kw):
    """
    Given a format string,
    fill in fields with defaults / data.

    Since .format() is idempotent, it wont
    affect non-format strings. Thanks @jak
    """
    kw.update(filepath_opts())
    return fp.format(**kw)


def to_gz(s):
    """
    string > gzip
    """
    assert(isinstance(s, basestring))
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
    return json.loads(s)


def to_json(obj):
    """
    obj > jsonstring
    """
    return json.dumps(obj)


def to_zip(s):
    """
    string > zip
    """
    fileobj = cStringIO.StringIO()
    with zipfile.ZipFile(fileobj, 'w') as f:
        f.writestr('s3plz.txt', s)
    return fileobj.getvalue()


def from_zip(s):
    """
    zip > string
    """
    zpd = cStringIO.StringIO(s)
    zpf = zipfile.ZipFile(zpd, "r")
    return zpf.read(zpf.namelist()[0])


def to_pickle(obj):
    """
    obj > picklestring
    """
    return pickle.dumps(obj)


def from_pickle(s):
    """
    picklestring > object
    """
    return pickle.loads(s)
