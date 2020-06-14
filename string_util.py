"""Collection of string wranglers, very few depenencies."""

import re  # camel_to_separated()
import unicodedata  # clean_string()
import urllib
import urlparse


def clean_string(s):
    """Returns lowercase, ascii-only version of the string. See
    var f for only allowable chars to return."""

    # *Replace* unicode special chars with closest related char, decode to
    # string from unicode.
    if isinstance(s, unicode):
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore')
    s = s.lower()
    f = 'abcdefghijklmnopqrstuvwxyz'
    return filter(lambda x: x in f, s)


def encode_uri_non_ascii(s):
    """Replace non-ascii characters with percent-coded equivalents for URIs."""
    return ''.join(map(
        lambda c: c if ord(c) < 128 else urllib.quote(c.encode("utf-8")),
        s
    ))


def str_is_numeric(s, typ=float):
    try:
        typ(s)
        return True
    except ValueError:
        return False


# Quick way of detecting if a kwarg was specified or not.
sentinel = object()


def set_query_parameters(url, new_fragment=sentinel, **new_params):
    """Given a URL, set a query parameter or fragment and return the URL.

    Setting to a [falsy value][1] removes the parameter or hash/fragment.
    Inspired by [SO][2].

    > set_query_parameter('http://me.com?foo=bar&biz=baz', foo='stuff', biz='')
    'http://me.com?foo=stuff'

    [1]: https://docs.python.org/2.4/lib/truth.html
    [2]: http://stackoverflow.com/questions/4293460/how-to-add-custom-parameters-to-an-url-query-string-with-python
    """
    scheme, netloc, path, query_string, fragment = urlparse.urlsplit(url)
    query_params = urlparse.parse_qs(query_string)

    query_params.update(new_params)
    query_params = {k: v for k, v in query_params.items()
                    if v not in ['', None]}
    new_query_string = urllib.urlencode(query_params, doseq=True)

    if new_fragment is not sentinel:
        fragment = new_fragment

    return urlparse.urlunsplit(
        (scheme, netloc, path, new_query_string, fragment))


def camel_to_separated(s, sep='_'):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1{}\2'.format(sep), s)
    return re.sub('([a-z0-9])([A-Z])', r'\1{}\2'.format(sep), s1).lower()


def separated_to_camel(s, sep='_', standing=False):
    parts = s.split(sep)
    first_part = parts[0].title() if standing else parts[0]
    return first_part + ''.join(p.title() for p in parts[1:])
