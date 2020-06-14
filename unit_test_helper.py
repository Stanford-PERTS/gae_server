"""Making setting up unit tests easier."""

from google.appengine.api import memcache
from google.appengine.api import namespace_manager
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from webapp2_extras.securecookie import SecureCookieSerializer
import logging
import os
import unittest

import config
import jwt_helper
import util


class PertsTestCase(unittest.TestCase):
    """Contains important global settings for running unit tests.

    Errors related to logging, and appstats not being able to access memcache,
    would appear without these settings.

    Use Example:

    class MyTestCase(unit_test_help.PertsTestCase):
        def set_up(self):
            # Let PertsTestCase do its important work
            super(MyTestCase, self).setUp()

            # Add your own stubs here
            self.testbed.init_user_stub()

        # Add your tests here
        def test_my_stuff(self):
            pass
    """

    def setUp(self):
        """Sets self.testbed and activates it, among other global settings.

        This function (noticeably not named with PERTS-standard underscore
        case) is automatically called when starting a test by the unittest
        module. We use it for basic configuration and delegate further set
        up to the more canonically named set_up() of inheriting classes.
        """
        if not util.is_localhost():
            # Logging test activity in production causes errors. This
            # suppresses all logs of level critical and lower, which means all
            # of them. See
            # https://docs.python.org/2/library/logging.html#logging.disable
            logging.disable(logging.CRITICAL)

        # Use the same namespace our test apps will use
        # i.e. webapp2.WSGIApplication
        if os.environ['NAMESPACE']:
            namespace_manager.set_namespace(os.environ['NAMESPACE'])

        # Start a clean testing environment for one test.
        self.testbed = testbed.Testbed()
        self.testbed.activate()

        # Stubs for services we use throughout.
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()

        # NDB has lots of fancy caching features, whic are normally great, but
        # get in the way of testing consistency.
        # https://cloud.google.com/appengine/docs/python/ndb/cache
        ndb_context = ndb.get_context()
        ndb_context.set_cache_policy(lambda x: False)
        ndb_context.set_memcache_policy(lambda x: False)

        # for simulating google users
        self.testbed.init_user_stub()

        # Allow code to detect whether or not it's running in a unit test.
        self.testbed.setup_env(currently_testing='true', overwrite=True)

        # Let inheriting classes to their own set up.
        if hasattr(self, 'set_up'):
            self.set_up()

    def tearDown(self):
        """Automatically called at end of test by the unittest module."""
        # Re-enable logging.
        logging.disable(logging.NOTSET)
        # Tear down the testing environment used by a single test so the next
        # test gets a fresh start.
        self.testbed.deactivate()

    def assertEqual(self, a, b):
        """If a and b are strings, limit to the region of difference to make it
        easier to inspect long strings."""
        too_long =  20
        if (
            isinstance(a, basestring) and
            isinstance(b, basestring) and
            a != b and
            (len(a) > too_long or len(b) > too_long)
        ):
            for i, char in enumerate(a):
                if i >= len(b) or char != b[i]:
                    break
            left = i - too_long
            right = i + too_long
            a =  a[left:right]
            b =  b[left:right]

            try:
                super(PertsTestCase, self).assertEqual(a, b)
            except AssertionError as e:
                raise AssertionError(
                    "(unit_test_helper.assertEqual trimming string comparison "
                    "for readability) {}".format(e)
                )

        super(PertsTestCase, self).assertEqual(a, b)


class ConsistencyTestCase(PertsTestCase):
    """A standard datastore environment for testing entities.

    Attributes:
        consistency_probability: int, default 0, the probability, as a decimal,
            that an eventually-consistent query will return accurate results
            from recent writes. See: https://cloud.google.com/appengine/docs/python/tools/localunittesting?hl=en#Python_Writing_High_Replication_Datastore_tests
    """

    consistency_probability = 0

    def set_up(self):
        # Create a consistency policy that will simulate the High Replication
        # consistency model. A zero probability means it will be on its 'worst'
        # behavior: eventually consistent queries WILL return stale results.
        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
            probability=self.consistency_probability)

        # Initialize the datastore stub with this policy.
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)

        # Swap the above two lines with this to see the effects of a more
        # forgiving datastore, where eventually consistent queries MIGHT
        # return stale results.
        # self.testbed.init_datastore_v3_stub()


def login_headers(user_id):
    """Simulate a logged-in session cookie to be included in requests.

    http://www.recursion.org/2011/10/12/testing-webapp2-sessions-with-webtest
    """
    session = {'user': user_id}
    scs = SecureCookieSerializer(config.default_session_cookie_secret_key)
    cookie_value = scs.serialize(config.session_cookie_name, session)
    headers = {
        'Cookie': '{}={}'.format(config.session_cookie_name, cookie_value)
    }
    return headers

def jwt_headers(user):
    payload = {'user_id': user.uid, 'email': user.email}
    return {'Authorization': 'Bearer ' + jwt_helper.encode(payload)}
