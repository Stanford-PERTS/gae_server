"""Abstract class for request handlers dealing in a JSON API."""

from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import ndb
import datetime
import json
import logging
import re
import traceback

from gae_models import DatastoreModel, SqlModel
from permission import owns
import config
import util

from .base import BaseHandler


debug = util.is_development()


class InvalidParamType(Exception):
    """A query param was supposed to be some type, but couldn't be coerced."""
    pass


class ApiHandler(BaseHandler):
    """Superclass for all api-related urls."""

    # Set in the query string, e.g. ?envelope=true, triggers putting an
    # "envelope" around the response, which allows the server to provide
    # meta data.
    # Example of normal repsonse:
    # {"uid": "User_xyz", ...}
    # Example of response with envelope:
    # {"status": 200, "data": {"uid": "User_xyz", ...}}
    envelope = None

    def dispatch(self, *args, **kwargs):
        """Wrap all api calls in a try/catch so the server never breaks when
        the client hits an api URL."""

        # All api handlers respond with JSON.
        self.response.headers.update({
            'Content-Type': 'application/json; charset=utf-8',
        })

        self.allow_cors()

        try:
            # Call the descendant handler's method matching the appropriate
            # verb, GET, POST, etc.
            self.log_traffic('request')

            # Check if we should apply an evelope.
            self.envelope = self.request.GET.pop('envelope', None)

            # Call the method-specific handler.
            BaseHandler.dispatch(self)

            self.log_traffic('response')

        except Exception as error:
            if isinstance(error, InvalidParamType):
                self.error(400)  # Bad Request
            else:
                self.error(500)
            trace = traceback.format_exc()
            # We don't want to tell the public about our exception messages.
            # Just provide the exception type to the client, but log the full
            # details on the server.
            logging.error("{}\n{}".format(error, trace))
            response = error.__class__.__name__
            if debug:
                response = "{}: {}\n\n{}".format(
                    error.__class__.__name__, error, trace)
            self.response.write(json.dumps(response))

    def log_traffic(self, direction):
        """Log either requests or responses.

        Args:
            direction: str, either 'request' or 'response'
        """
        # For requests: assume that we log. Sensitive handlers have
        # `should_log_request` explicitly set to False. This is very important
        # for POSTs where plain text data is provided, like password-related or
        # survey response endpoints.

        # For responses: Assume that we log. This should be fine for all
        # handlers, but you can explicitly set the class property
        # `should_log_response` to override.

        should_attribute = 'should_log_' + direction
        # e.g. self.should_log_request
        should_log = getattr(self, should_attribute, True)

        # e.g. self.request
        obj = getattr(self, direction)

        content_type = obj.headers.get('Content-Type', '')
        is_json = 'application/json' in content_type

        if should_log and obj.body:
            if is_json:
                try:
                    log_body = util.truncate_json(obj.body)
                except Exception as e:
                    logging.warning(e)
                    log_body = obj.body
            else:
                log_body = 'Content-Type: {}, not logged'.format(content_type)
            logging.info(u"{} body:\n{}".format(direction, log_body))

    def allow_cors(self):
        """If this request comes from an allowed origin, apply CORS headers."""
        origin = self.request.headers.get('Origin', '')

        allow_origins = getattr(config, 'allow_origins', [])
        is_allowed = any(re.match(p, origin) for p in allow_origins)

        # If we're on localhost, allow requests which are also local.
        local = re.match(r'^http://localhost', origin) and util.is_localhost()

        if is_allowed or local:
            self.response.headers.update({
                # Tell the client they're allowed to read our response...
                'Access-Control-Allow-Origin': origin,
                # ...send an Authorization header...
                'Access-Control-Allow-Headers': 'Authorization, Content-Type',
                # ...read our Authorization header...
                'Access-Control-Expose-Headers': 'Authorization, Link',
                # ...and use a variety of methods.
                'Access-Control-Allow-Methods': 'HEAD, GET, POST, PUT, PATCH, DELETE',
            })

    def get_envelope(self, data, **kwargs):
        return dict(
            {
                'status': self.response.status_int,
                'data': data,
            },
            **kwargs
        )

    def convert_for_client(self, d):
        def convert(o):
            return o.to_client_dict() if hasattr(o, 'to_client_dict') else o

        is_single_entity = (isinstance(d, dict) or isinstance(d, SqlModel) or
                            isinstance(d, ndb.Model))

        if is_single_entity:
            d = convert(d)
        elif hasattr(d, '__iter__'):
            # The __iter__ test here will pick up dictionaries, so make sure to
            # take care of them first. It's written this way so it passes for
            # both lists and generators, the latter we expect from some ndb
            # queries.
            # In the case of generators, values can only be read out once, so
            # it's important to extract them into memory so we can do multiple
            # checks on the contents.
            d = [convert(o) for o in d]
        # else we rely on json.dumps to understand d, whatever it is

        return d

    def write(self, d):
        client_d = self.convert_for_client(d)

        to_write = self.get_envelope(client_d) if self.envelope else client_d

        # Although DatastoreModel.to_client_dict() should take care of all
        # values that json.dumps() normally can't handle (like datetimes),
        # sometimes we want to write() dictionaries or other values that might
        # contain datetimes. This includes "instances" of SqlModel. So make
        # sure to include a default function that can handle them here also.
        self.response.write(json.dumps(
            to_write,
            default=util.json_dumps_default,
        ))

    def process_json_body(self):
        """Enable webob request to accept JSON data in a request body."""

        # This method clears the request body to avoid webapp2 silliness. But
        # that sacrifices idempotence. So save and restore it from a custom
        # attribute.
        if hasattr(self.request, 'original_body'):
            body = self.request.original_body
        else:
            body = self.request.body
            self.request.original_body = body
            # The request object doesn't know how to interpret the JSON in the
            # request body so if we didn't do this, self.request.POST would be
            # full of junk.
            self.request.body = ''

        try:  # Client may not send valid JSON.
            # "Manually" interpret the request payload; webob doesn't know how.
            json_payload = json.loads(body)
        except ValueError:
            # This might be a more traditional foo=bar&baz=quz type payload,
            # so leave it alone; webob can interpret it correctly without help.
            json_payload = None

        return json_payload

    def override_json_body(self, params):
        """Sometimes convenient to change incoming parameters to enforce
        permissions, e.g. in PUT requests which only accept certain
        propeties."""
        self.request.original_body = json.dumps(
            params, default=util.json_dumps_default)

    def coerce_param(self, k, v, typ):
        if typ == 'json':
            out = (json.loads(v) if isinstance(v, basestring) else None)
        elif typ == 'datetime':
            # Parse a UTC dateime string in strict ISO 8601 format.
            out = (datetime.datetime.strptime(v, config.iso_datetime_format)
                   if v else None)
        elif typ == 'date':
            # Parse an ISO 8601 date, YYYY-MM-DD
            out = (datetime.datetime.strptime(v, config.iso_date_format).date()
                   if v else None)
        elif typ == 'cursor':
            if util.str_is_numeric(v, typ=int):
                # These are legitimate cursors for SqlModels.
                out = int(v)
            else:
                # Assume an encoded Datastore cursor.
                out = Cursor(urlsafe=v)
        elif typ == list:
            if self.request.method in ['POST', 'PUT']:
                # Load list in data directly
                out = v
            else:
                # Turn parameters that show up many times into a list.
                out = self.request.get_all(k)
        elif typ is bool and v == 'false':
            # Break the normal python coercion rules to allow the
            # string 'false' to be False.
            out = False
        elif typ is str:
            # If there are any non-ascii characters in the value
            # they'll create an error. Silently drop them. If you want
            # non-ascii characters, then specify unicode type in your
            # handler.
            out = (
                None if v is None
                # unicode -> ascii-only bytes -> unicode -> str
                else str(v.encode('ascii', 'ignore').decode('ascii'))
            )
        else:
            # For all other types (unicode, int, bool), just do
            # standard type coercion.
            out = None if v is None else typ(v)
        return out

    def get_params(self, param_types={}, required=False, source=None):
        """Paper over webapp2 weirdness, do type coercion and filtering.

        Args:
            param_types: dict, default None, if specified, will attempt to
                extract the specified query string paramters from the request,
                coerced to the specified types. Types are actual type objects,
                or the strings 'json', 'date', or 'datetime'.

                Example structure:
                {'user_id': str, 'last_login': 'date'}

            required: bool, if True this function will fill in default values
                based on type, so you're sure to have the requested keys in the
                returned dictionary.

            source: dict, default None, in which case it uses the params of
                the current request. Otherwise it uses/coerces the data
                provided.

        Raises: InvalidParamType when a parameter string can't be coerced to
            the requested type.

        Returns: dictionary of query string parameters.
        """

        # JSON data in POSTs and PUTs require extra work.
        content_types = self.request.headers.get('Content-Type', [])
        is_json = 'application/json' in content_types
        if isinstance(source, dict):
            params = source
        elif self.request.method in ['POST', 'PUT'] and is_json:
            params = self.process_json_body()
            if type(params) is not dict:
                raise Exception(
                    'get_params() only works with JSON objects, got {}'
                    .format(params)
                )
        else:
            params = {k: self.request.get(k) for k in self.request.arguments()}

        # If types specified, coerce each named parameter to its type.
        for k, v in params.items():
            k_original = k  # allow for params to be postpended with '!'
            if k_original.endswith('!'):
                k = k_original[:-1]
            if k in param_types:
                typ = param_types[k]
                if typ == 'json' and is_json:
                    params[k_original] = v  # Already parsed.
                elif typ is None:
                    # Allow "None" as the type which means no coercion.
                    params[k_original] = v
                else:
                    try:
                        params[k_original] = self.coerce_param(k_original, v,
                                                               typ)
                    except Exception as error:
                        logging.error(error)
                        raise InvalidParamType(error)
            # We should remove any 'bad' data not allowed by the api
            else:
                params.pop(k_original)

        defaults = {typ: typ() for typ in [str, unicode, int, bool, list]}
        defaults['json'] = u''
        defaults['datetime'] = None
        defaults['date'] = None

        # If a key was specified in param_types, but was missing from the
        # request, we need to know if we're in "required" mode or not.
        # In required mode, we want to be able to reference the key without a
        # KeyError, so fill in a default.
        if required:
            missing_keys = set(param_types.keys())
            missing_keys.difference_update(params.keys())
            params.update({k: defaults[param_types[k]] for k in missing_keys})
        # else:
        #   Don't force the key into existence. It's optional.

        return params

    def get_param(self, key, typ, *args):
        params = self.get_params({key: typ})
        if len(args) == 0:
            return params.get(key)
        elif len(args) == 1:
            return params.get(key, args[0])
        else:
            raise Exception(
                "ApiHandler.get_param() got unexpected arguments: {}"
                .format(args[1:])
            )

    def get_long_uid(self, url_kind, short_uid):
        """Unlike DatastoreModel.get_long_uid(): urls use lowercase plurals."""
        if url_kind == 'programs':
            # Programs are "identified" by label, not uid's; there's no
            # difference between long and short.
            return short_uid
        else:
            klass = DatastoreModel.url_kind_to_class(url_kind)
            return klass.get_long_uid(short_uid)
