"""Abstract class for request handlers serving HTML views."""

from google.appengine.api import users as app_engine_users
import datetime
import jinja2
import json
import logging
import os
import traceback

import util

from .base import BaseHandler

debug = util.is_development()


class ViewHandler(BaseHandler):
    """Superclass for page-generating handlers."""

    def get_jinja_environment(self, template_path='templates'):
        return jinja2.Environment(
            autoescape=True,
            extensions=['jinja2.ext.autoescape'],
            loader=jinja2.FileSystemLoader(template_path),
        )

    def write(self, template_filename, template_path='templates', **kwargs):
        jinja_environment = self.get_jinja_environment(template_path)

        # Jinja environment filters:

        @jinja2.evalcontextfilter
        def jinja_json_filter(eval_context, value):
            """Seralize value as JSON and mark as safe for jinja."""
            return jinja2.Markup(json.dumps(value))

        jinja_environment.filters['to_json'] = jinja_json_filter

        def format_datetime(value):
            # Formats datetime as Ex: "January 9, 2015"
            return '{dt:%B} {dt.day}, {dt.year}'.format(dt=value)

        jinja_environment.filters['datetime'] = format_datetime

        user = self.get_current_user()

        # default parameters that all views get
        kwargs['user'] = user.to_client_dict()
        # Python keeps time to the microsecond, but we don't need it, and
        # it's easier to render as ISO 8601 without it.
        kwargs['server_time'] = datetime.datetime.today().replace(microsecond=0)
        kwargs['is_localhost'] = util.is_localhost()
        kwargs['hosting_domain'] = os.environ['HOSTING_DOMAIN']
        kwargs['yellowstone_domain'] = os.environ['YELLOWSTONE_DOMAIN']
        kwargs['browser_api_key'] = (
            os.environ['LOCALHOST_BROWSER_API_KEY'] if util.is_localhost() else
            os.environ['DEPLOYED_BROWSER_API_KEY'])

        # Try to load the requested template. If it doesn't exist, replace
        # it with a 404.
        try:
            template = jinja_environment.get_template(template_filename)
        except jinja2.exceptions.TemplateNotFound:
            logging.error("TemplateNotFound: {}".format(template_filename))
            return self.http_not_found()

        # Render the template with data and write it to the HTTP response.
        self.response.write(template.render(kwargs))

    def dispatch(self):
        try:
            logging.info("ViewHandler.dispatch()")
            # Call the overridden dispatch(), which has the effect of running
            # the get() or post() etc. of the inheriting class.
            BaseHandler.dispatch(self)

            if self.response.has_error():
                self.apply_error_template()

        except Exception as error:
            trace = traceback.format_exc()
            # We don't want to tell the public about our exception messages.
            # Just provide the exception type to the client, but log the full
            # details on the server.
            logging.error("{}\n{}".format(error, trace))
            response = {
                'success': False,
                'message': error.__class__.__name__,
            }
            if debug:
                self.response.write('<pre>{}</pre>'.format(
                    traceback.format_exc()))
            else:
                # @todo: make a pretty 500 page
                self.response.write("We are having technical difficulties.")
            return

    def apply_error_template(self):
        status = self.response.status_int
        original_body = self.response.body

        if status == 401:
            self.response.body = "401 Unauthorized: {}".format(original_body)
        elif status == 403:
            self.response.body = "403 Forbidden: {}".format(original_body)
        elif status == 404:
            self.response.body = "404 Not Found: {}".format(original_body)
        elif status == 500:
            self.response.body = "500 Internal Server Error: {}".format(
                original_body)
