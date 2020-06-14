# Watch out for this one! We're not using typical Python 2 floor division in
# this file, but rather floating point division.
from __future__ import division

from math import ceil
from google.appengine.api import namespace_manager
from google.appengine.api import users as app_engine_users
from google.appengine.ext import ndb
from webapp2_extras import sessions
from webapp2_extras.routes import RedirectRoute
import datetime
import json
import logging
import os
import re
import webapp2

from model import get_sql_models, User
import jwt_helper
import mysql_connection
import config
import util


class BaseHandler(webapp2.RequestHandler):
    """Ancestor of all other views/handlers."""

    @classmethod
    def using_sessions(klass):
        return bool(getattr(config, 'session_cookie_name', False))

    def dispatch(self):
        """Wraps the other request handlers.

        * Manages sessions
        * Manages request profiling
        """
        util.profiler.add_event("BaseHandler.dispatch()")

        # ** Code to run before all handlers goes here. ** #

        # The App Engine runtime does weird caching of classes and class
        # properties such that you can't expect them to be cleanly segregated
        # or reset between requests. But we want to use this property to avoid
        # multiple lookups of the same user within a request. So make sure it
        # has a clean start.
        # https://cloud.google.com/appengine/docs/standard/python/how-requests-are-handled#app-caching
        self._user = None

        if util.is_localhost():
            # ports are arbitrary, but convenient
            os.environ['YELLOWSTONE_DOMAIN'] = 'localhost:9080'
            os.environ['YELLOWSTONE_PROTOCOL'] = 'http'
            os.environ['NEPTUNE_DOMAIN'] = 'localhost:8080'
            os.environ['NEPTUNE_PROTOCOL'] = 'http'
            os.environ['TRITON_DOMAIN'] = 'localhost:10080'
            os.environ['TRITON_PROTOCOL'] = 'http'
        else:
            # Various DOMAINs remain set as in app.yaml
            os.environ['YELLOWSTONE_PROTOCOL'] = 'https'
            os.environ['NEPTUNE_PROTOCOL'] = 'https'
            os.environ['TRITON_PROTOCOL'] = 'https'

        # Set the namespace, which varies by branch.
        namespace = os.environ['NAMESPACE']
        if namespace:
            logging.info("Setting namespace: {}".format(namespace))
            namespace_manager.set_namespace(namespace)

        # Newly deployed dev branches might not have a database in their
        # namespace yet.
        self.init_database()

        if self.using_sessions():
            # Get a session store for this request.
            self.session_store = sessions.get_store(request=self.request)

        # Allow load testing services to log in quickly.
        if util.is_development() and self.request.get('demo_login', None) == 'wamxdkrwnkgey':
            user = User.get_by_id('User_demo')
            self.log_in(user)
            self.redirect(self.request.path)

        # Handler classes may set a class property `requires_auth` which triggers a check
        # for an authenticated user. If there isn't one, the request is immediately
        # rejeted with a 401. This does not apply to preflight OPTIONS calls which never
        # include Authorization headers (they're about figuring out the server's CORS
        # rules, not taking any actions).
        authed = getattr(self, 'requires_auth', False)
        options = self.request.method == 'OPTIONS'

        # This may be used by downstream handlers to override permissions if
        # necessary.
        self.allowed_by_jwt = self.jwt_allows_endpoint(self.get_endpoint_str())
        if self.allowed_by_jwt:
            logging.info("BaseHandler: this request is ALLOWED by the jwt.")

        if authed and not options:
            user = self.get_current_user()
            if user.user_type == 'public' and not self.allowed_by_jwt:
                return self.http_unauthorized()

        try:
            # Call the overridden dispatch(), which has the effect of running
            # the get() or post() etc. of the inheriting class.
            webapp2.RequestHandler.dispatch(self)

        finally:
            # ** Code to run after all handlers goes here. ** #

            if self.using_sessions():
                # Save all sessions.
                self.session_store.save_sessions(self.response)

            util.profiler.add_event("END")

            # Turn on for debugging/profiling.
            # logging.info(util.profiler)

            util.profiler.clear()

    def head(self, *args, **kwargs):
        """Perform everything a GET would do, but drop the response body.

        This ensures all headers, like the content length, are set, but per the
        HTTP spec, no body should be present.
        https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/head
        """
        if hasattr(self, 'get'):
            self.get(*args, **kwargs)
            # Webapp is clever and calculates content length for us, which is
            # always going to be zero if we blank the body. But HEAD responses
            # are supposed to have the content length the response _would_ have
            # if it was a GET. So override.
            body = self.response.body
            self.response.clear()
            self.response.headers['Content-Length'] = str(len(body))
        else:
            # It's against the spec to 405 a GET or HEAD. Cheat and just
            # pretend it doesn't exist.
            self.error(404)

    def options(self, *args, **kwargs):
        # OPTION Response based on ->
        # http://zacstewart.com/2012/04/14/http-options-method.html
        self.response.set_status(200)
        self.response.headers['Allow'] = 'GET,HEAD,OPTIONS'

    def get_current_user(self):
        """Get the logged in user."""
        cached_user = getattr(self, '_user', None)
        if cached_user:
            logging.info("BaseHandler.get_current_user() returning {} from "
                         "cache.".format(cached_user))
            return cached_user

        # Jwt overrides session. I.e. if your session said "User_A", but your
        # jwt says "User_B", we go with User B and change the session cookie.
        jwt_user, error = self.get_jwt_user()
        if jwt_user:
            logging.info("BaseHandler.get_current_user() returning {} from "
                         "jwt.".format(jwt_user))
            self.log_in(jwt_user)
            return jwt_user

        if self.using_sessions():
            session_user = User.get_by_id(self.session.get('user', None))
            if session_user:
                logging.info(
                    "BaseHandler.get_current_user() returning {} from "
                    "session cookie.".format(session_user)
                )
                self.log_in(session_user)
                return session_user
            if 'user' not in self.session:
                # Make sure the session keys always exist, even if they are
                # empty.
                self.session['user'] = None

        logging.info("BaseHandler.get_current_user() returning public user.")
        return User.create_public()

    def get_jwt(self):
        """Attempt to read JWT from Authorization header."""
        pattern = re.compile(r'^Bearer (\S+)$')
        match = pattern.match(self.request.headers.get('Authorization', ''))

        if match:
            return match.groups()[0]
        else:
            # There was no recognizable JWT header.
            return None

    def get_jwt_user(self, jwt_kwargs={}, token=None):
        """Is there a JWT that authenticates the user?

        Returns a tuple as (User or None, error str or None) where the error
        may be 'not found', 'used', or 'expired', just like
        AuthToken.checkTokenString. Error will only be not None if there is
        a JWT present, i.e. if the client isn't even trying to use JWT, the
        return value is (None, None).
        """
        token = token or self.get_jwt()
        payload, error = jwt_helper.decode(token, **jwt_kwargs)

        if not payload:
            # No valid token, so no user.
            return (None, error)

        if 'user_id' not in payload or 'email' not in payload:
            # No user in the token; this may only specify allowed_endpoints.
            return (None, jwt_helper.NO_USER)

        # Retrieve or create the users information.
        user = self.sync_user_with_token(payload)

        return (user, None)

    def jwt_allows_endpoint(self, endpoint_str=None):
        """Certain handlers are designed to be called from other platforms but
        require explicit permission from that platform to use.

        Returns boolean.
        """
        payload, error = jwt_helper.decode(self.get_jwt())
        if not payload or error:
            return False

        if endpoint_str is None:
            endpoint_str = self.get_endpoint_str()

        return endpoint_str in payload.get('allowed_endpoints', [])

    def get_endpoint_str(self, method=None, platform=None, path=None):
        """Describe the current request with a formalized string.

        NOT domain-specific, rather it's platform-specific, i.e. all neptune
        environments have the same endpoint description.
        """
        return util.get_endpoint_str(
            method=method or self.request.method,
            platform=platform,
            path=path or self.request.path,
        )

    def sync_user_with_token(self, payload):
        # The token is correctly signed and has valid structure.
        def create_user(payload):
            short_uid = User.convert_uid(payload['user_id'])
            kwargs = {k: v for k, v in payload.items()
                      if k in ('email', 'user_type')}
            # Setting the user type is a potential security hole, so this
            # should only be used after the jwt has been verified.
            user = User.create(id=short_uid, **kwargs)
            user.put()
            return user

        is_auth_server = getattr(config, 'is_auth_server', False)
        if is_auth_server:
            # We are the auth server, the arbiter of what id goes with what
            # email, so we never _change_ ids. But do create the user if
            # necessary to help solve bad sync states with other systems.
            user = User.get_by_id(payload['user_id'])
            if not user:
                user = create_user(payload)
        else:
            # Not the auth server, defer to the id in the payload.
            if User.email_exists(payload['email']):
                user = User.get_by_auth('email', payload['email'])
                if user.uid != payload['user_id']:
                    logging.error("User id mismatch found, more info in logs.")
                    logging.info("Original user: {}".format(user.to_dict()))
                    logging.info("Received token payload: {}".format(payload))
                    user = User.resolve_id_mismatch(user, payload['user_id'])
            else:
                user = create_user(payload)

        return user

    def get_third_party_auth(self, auth_type):
        """Wrangle and return authentication data from third parties.

        Args:
            auth_type: str, currently only 'google'
        Returns:
            dictionary of user information, which will always contain
                the key 'auth_id', or None if no third-party info is found.
        """
        if auth_type == 'google':
            gae_user = app_engine_users.get_current_user()
            if not gae_user:
                logging.error("No google login found.")
                return None
            # Get user first and last names from nickname
            first_name = None
            last_name = None
            if gae_user.nickname():
                nickname = gae_user.nickname()
                if ' ' in nickname:
                    first_name = nickname.split(' ')[0]
                    last_name = nickname.split(' ')[1]
                else:
                    if '@' in nickname:
                        first_name = nickname.split('@')[0]
                    else:
                        first_name = nickname
            # Combine fields in user keyword arguments
            user_kwargs = {
                'email': gae_user.email(),
                'google_id': gae_user.user_id(),
                'first_name': first_name,
                'last_name': last_name,
            }

        return user_kwargs

    def authenticate(self, auth_type, email=None, password=None):
        """Takes various kinds of credentials (email/password, google
        account) and logs you in.

        Returns:
          User entity             the user has been successfully authenticated
          'credentials_invalid'   either because a password is wrong or no
                                  account exists for those credentials
          'credentials_missing'   looked for credentials but didn't find any of
                                  the appropriate kind.
          'email_exists:[auth_type]'  the supplied credentials are invalid AND
                                      a user with the same email exists with
                                      another auth type.
        """
        # fetch matching users
        if auth_type == 'email':
            if email is None or password is None:
                return 'credentials_missing'
            auth_id = email.lower()

        elif auth_type in ['google']:
            user_kwargs = self.get_third_party_auth(auth_type)
            if not user_kwargs:
                return 'credentials_missing'
            auth_id = user_kwargs['google_id']

        user = User.get_by_auth(auth_type, auth_id)
        # interpret the results of the query
        if user is None:
            # Make it easy for devs to become admins.
            is_admin = app_engine_users.is_current_user_admin
            if (auth_type == 'google' and is_admin()):
                return self.register('google')
            return 'credentials_invalid'

        # For direct authentication, PERTS is in charge of checking their
        # credentials, so validate the password.
        if auth_type == 'email':
            # A user-specific salt AND how many "log rounds" (go read about key
            # stretching) should be used is stored IN the user's hashed
            # password; that's why it's an argument here.
            # http://pythonhosted.org/passlib/
            if not User.verify_password(password, user.hashed_password):
                # invalid password for this email
                return 'credentials_invalid'

        # If we got this far, all's well, log them in and return the matching
        # user
        self.log_in(user)
        return user

    def log_in(self, user):
        if self.using_sessions():
            self.session['user'] = user.uid

        self._user = user
        if not user.super_admin and hasattr(user, 'last_login'):
            # Updates last login for non-supers because we're interested in
            # how active they are on the system. Don't update it for supers
            # because it hampers batch processes like making lots of api calls.
            user.last_login = datetime.datetime.now()
            user.put()
        # Update the token so it gets fresh expiration time.
        self.set_jwt(jwt_helper.encode_user(user))

    def set_jwt(self, token):
        self.response.headers.update({
            'Authorization': 'Bearer ' + token,
        })

    def log_out(self):
        redirect = self.request.get('redirect') or '/'

        self.response.delete_cookie('ACSID')
        self.response.delete_cookie('SACSID')

        # The name of the login cookie is defined in
        # url_handlers.BaseHandler.session()
        if self.using_sessions():
            self.response.delete_cookie(config.session_cookie_name)

        # This cookie is created when logging in with a google account with
        # the app engine sdk (on localhost).
        self.response.delete_cookie('dev_appserver_login')

        # If there's a jwt set, remove it.
        self.response.headers.pop('Authorization', None)

        # Clear the cached user.
        self._user = None

    def init_database(self):
        """If necessary, create the database and populate it with tables."""
        # Only do this in development, because it takes some small amount of time
        # out of each query. We're willing to pay that cost in development for
        # ease of quick deployment, but we want to optimize for production.
        if not util.is_development() or util.is_codeship():
            return

        db_params = mysql_connection.get_params()

        with mysql_connection.connect(specify_db=False) as sql:
            params = (db_params['db_name'],)
            rows = sql.query('SHOW DATABASES LIKE %s', param_tuple=params)
            if len(rows) == 0:
                sql.query('CREATE DATABASE `{}`'.format(db_params['db_name']))
                sql.query('USE `{}`'.format(db_params['db_name']))
                sql.reset({m.table: m.get_table_definition()
                           for m in get_sql_models()})

    def http_no_content(self):
        self.response.headers.pop('Content-Type', None)
        self.response.body = ''
        self.response.status = 204

    def http_bad_request(self, message=''):
        self.error(400)
        self.response.write(json.dumps(message))

    def http_unauthorized(self, message=''):
        self.error(401)
        self.response.write(json.dumps(message))
        # This is appropriate for JWT, which we're in the process of
        # changing to.
        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.2
        self.response.headers['WWW-Authenticate'] = 'Bearer'

    def http_not_found(self):
        self.error(404)

    def http_forbidden(self, message=''):
        self.error(403)
        self.response.write(json.dumps(message))

    def http_method_not_allowed(self, allow):
        self.error(405)
        self.response.headers['Allow'] = allow

    def http_conflict(self, message=''):
        self.error(409)
        self.response.write(json.dumps(message))

    def http_payload_too_large(self, message=''):
        self.error(413)
        self.response.write(json.dumps(message))

    @webapp2.cached_property
    def session(self):
        """Allows set/get of session data within handler methods.
        To set a value: self.session['foo'] = 'bar'
        To get a value: foo = self.session.get('foo')"""
        # Returns a session based on a cookie. Other options are 'datastore'
        # and 'memcache', which may be useful if we continue to have bugs
        # related to dropped sessions. Setting the name is critical, because it
        # allows use to delete the cookie during logout.
        # http://webapp-improved.appspot.com/api/webapp2_extras/sessions.html
        if self.using_sessions():
            return self.session_store.get_session(
                name=config.session_cookie_name,
                backend='securecookie',
            )
            
        return None


class Route(RedirectRoute):
    """Webapp route subclass that handles trailing slashes gracefully.

    https://webapp-improved.appspot.com/api/webapp2_extras/routes.html
    """
    def __init__(self, template, handler, strict_slash=True, name=None,
                 **kwargs):

        # Routes with 'strict_slash=True' must have a name
        if strict_slash and name is None:
            # Set a name from the template
            # ** Be sure this isn't creating duplicate errors
            # ** but 'template' should be unique so I think it's good.
            name = template

        return super(Route, self).__init__(
            template, handler=handler, strict_slash=strict_slash, name=name,
            **kwargs
        )
