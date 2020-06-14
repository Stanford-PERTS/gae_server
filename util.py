"""Collection of utility functions."""

from collections import OrderedDict
from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata  # delete_everything()
import datetime
import json
import logging
import os  # is_development()
import time  # delete_everything()

from simple_profiler import Profiler
# re-exported
from string_util import (
    camel_to_separated,
    clean_string,
    encode_uri_non_ascii,
    separated_to_camel,
    set_query_parameters,
    str_is_numeric,
)
import config


# A 'global' profiler object that's used in BaseHandler.get. So, to profile
# any request handler, add events like this:
# util.profiler.add_event("did the thing")
# and when you're ready, print the results, perhaps like this:
# logging.info(util.profiler)
profiler = Profiler()


# Some poorly-behaved libraries screw with the default logging level,
# killing our 'info' and 'warning' logs. Make sure it's set correctly
# for our code.
logging.getLogger().setLevel(logging.DEBUG)


class PermissionDenied(Exception):
    """You can't do that."""
    pass


def delete_everything():
    kinds = metadata.get_kinds()
    for kind in kinds:
        if kind.startswith('_'):
            pass  # Ignore kinds that begin with _, they are internal to GAE
        else:
            q = ndb.Query(kind=kind)
            keys = q.fetch(keys_only=True)

            # Delete 1000 entities at a time.
            for i in range(len(keys) / 1000 + 1):
                portion = keys[i*1000: i*1000+1000]
                ndb.delete_multi(portion)


# |      Environment         | localhost | codeship | development | testing |
# |--------------------------|-----------|----------|-------------|---------|
# | Laptop, SDK              | X         |          | X           |         |
# | Laptop, unit tests       | X         |          | X           | X       |
# | Codeship, unit tests     |           | X        | X           | X       |
# | Deployed, dev project    |           |          | X           |         |
# | Deployed, other projects |           |          |             |         |


def is_localhost():
    """Is running on the development SDK, i.e. NOT deployed to app engine."""
    return (os.environ.get('SERVER_SOFTWARE', '').startswith('Development') and
            not is_codeship())


def is_codeship():
    """Is running on a codeship virtual machine."""
    # Codeship sets this by default.
    # https://documentation.codeship.com/continuous-integration/set-environment-variables/#default-environment-variables
    return os.environ.get('CI', '') == 'true'


def is_development():
    """Localhost OR the neptune-dev app are development.

    The neptuneplatform app is production.
    """
    # see http://stackoverflow.com/questions/5523281/how-do-i-get-the-application-id-at-runtime
    is_dev_project = app_identity.get_application_id() == \
        os.environ['DEVELOPMENT_PROJECT_ID']
    return is_localhost() or is_codeship() or is_dev_project


def is_testing():
    # This is set in unit_test_helper.PertsTestCase.setUp().
    return os.environ.get('CURRENTLY_TESTING', '') == 'true'


def list_by(l, p):
    """Turn a list of objects into a dictionary of lists, keyed by p.

    Example: Given list of pd entities and 'user', returns
    {
        'User_ABC': [pd1, pd2],
        'User_DEF': [pd3, pd4],
    }
    Objects lacking property p will be indexed under None.
    """
    d = {}
    for x in l:
        if isinstance(x, dict):
            key = x.get(p, None)
        else:
            key = getattr(x, p, None)
        if key not in d:
            d[key] = []
        d[key].append(x)
    return d


def datelike_to_iso_string(obj):
    # Need to be careful with this checking, b/c datetime is a subclass
    # of date, so isinstance(myDatetime, datetime.date) is True
    if isinstance(obj, datetime.datetime):
        return obj.strftime(config.iso_datetime_format)
    elif isinstance(obj, datetime.date):
        return obj.strftime(config.iso_date_format)
    else:
        raise Exception("Not a date-like object: {} {}".format(obj, type(obj)))


def iso_datetime_to_sql(iso):
    dt = datetime.datetime.strptime(iso, config.iso_datetime_format)
    return dt.strftime(config.sql_datetime_format)


def json_dumps_default(obj):
    """Specify this when serializing to JSON to handle more data types.

    Currently can handle dates, times, and datetimes.

    Example:

    json_string = json.dumps(my_object, default=util.json_dumps_default)
    """
    if isinstance(obj, datetime.date):
        # This isinstance test covers both dates and datetimes.
        return datelike_to_iso_string(obj)
    else:
        raise TypeError("{} is not JSON serializable. Consider extending "
                        "util.json_dumps_default().".format(obj))


def get_upload_bucket():
    # The bucket must be created in the app, and it must be set so that all
    # files uploaded to it are public. All of this is easy with the developer's
    # console; look for the three-vertical-dots icon after creating the bucket.
    return app_identity.get_application_id() + '-upload'


def cached_properties_key(uid):
    if not isinstance(uid, basestring):
        raise Exception("Bad type: {}".format(type(uid)))
    return 'cached_properties:{}'.format(uid)


def cached_query_key(query_name, **kwargs):
    if not isinstance(query_name, basestring) or query_name == '':
        raise Exception(u"Bad query name: {}".format(query_name))

    kwarg_hash = json.dumps(OrderedDict(
        (k, kwargs[k]) for k in sorted(kwargs.keys())
    ))
    return u'{}:{}'.format(query_name, kwarg_hash)


def get_domain():
    if is_localhost():
        # http is required for full linking, consistent with other case
        domain = app_identity.get_default_version_hostname()
    else:
        domain = os.environ['HOSTING_DOMAIN']

    # http is required for full linking, consistent with other case
    return "http://{}".format(domain)


def truncate_recursive(x, depth=0, max_depth=2, str_len=20, dict_len=4,
                       list_len=2):
    """Truncate dicts, lists, and strings in any nested structure.

    Special treatment for properties relevant to our data model: uid and label.
    """
    if depth > max_depth:
        return None

    preserved = ('uid', 'label')
    out = x

    if isinstance(x, dict):
        out = {k: truncate_recursive(v, depth=(depth+1))
               for k, v in x.items()[:dict_len]}
        # Always include certain critical string values.
        for k in preserved:
            if k in x:
                out[k] = x[k]
        # Indicate if keys have been dropped.
        if len(out) < len(x):
            out['...'] = "..."
    elif isinstance(x, list):
        out = [truncate_recursive(v, depth=(depth+1))
               for v in x[:list_len]]
        if len(out) < len(x):
            out.append("...")
    elif isinstance(x, basestring):
        if len(x) > str_len + 3:
            out = x[:str_len] + "..."
    return out


def truncate_json(json_str, **kwargs):
    x = json.loads(json_str)
    return json.dumps(truncate_recursive(x, **kwargs))


def get_endpoint_str(method=None, platform=None, path=None):
    """Describe the current request with a formalized string.

    NOT domain-specific, rather it's platform-specific, i.e. all neptune
    environments have the same endpoint description.
    """
    return '{method} //{platform}{path}'.format(
        method=method.upper(),
        platform=platform or config.platform_name,
        path=path,
    )
