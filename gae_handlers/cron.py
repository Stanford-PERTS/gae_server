from google.appengine.api import app_identity
from google.appengine.api import urlfetch
import datetime
import httplib
import json
import logging
import traceback

import cloudstorage as gcs

from gae_models import DatastoreModel
from .base import BaseHandler
import config
import jwt_helper


def rserve_jwt():
    payload = {
        'user_id': 'User_rserve',
        'email': 'rserve@perts.net',
        'user_type': 'super_admin',
    }
    # Devs often use this credential to work with the reporting API, and it
    # would be more practical to not have to refresh it multiple times a day.
    return jwt_helper.encode_rsa(payload, expiration_minutes = 8 * 60)


class CronHandler(BaseHandler):
    """Superclass for all cron-related urls.

    Very similar to ApiHandler, but with fewer security concerns (app.yaml
    enforces that only app admins can reach these endpoints).
    """

    def dispatch(self, *args, **kwargs):
        """Wrap all cron calls in a try/catch so we can log a trace."""

        # app.yaml specifies that only project admins can hit these URLs, so
        # don't worry further about permissions.

        self.response.headers['Content-Type'] = (
            'application/json; charset=utf-8')

        try:
            # Call the descendant handler's method matching the appropriate
            # verb, GET, POST, etc.
            BaseHandler.dispatch(self)

        except Exception as error:
            self.error(500)
            trace = traceback.format_exc()
            logging.error("{}\n{}".format(error, trace))
            response = "{}: {}\n\n{}".format(
                error.__class__.__name__, error, trace)
            self.response.write(json.dumps(response))

        else:
            # If everything about the request worked out, but no data was
            # returned, put out a standard empty response.
            if not self.response.body:
                self.write(None)

    def write(self, obj):
        # In the extremely common cases where we want to return an entity or
        # a list of entities, translate them to JSON-serializable dictionaries.
        if isinstance(obj, DatastoreModel):
            obj = obj.to_client_dict()
        elif type(obj) is list and all([isinstance(x, DatastoreModel) for x in obj]):
            obj = [x.to_client_dict() for x in obj]
        self.response.write(json.dumps(obj))


class BackupToGcsHandler(CronHandler):
    def get(self):
        kinds = self.request.get_all('kind'),
        if not kinds:
            raise Exception("Backup handler requires kinds.")

        bucket = self.request.get('bucket', None)
        if not bucket:
            raise Exception("Backup handler requires bucket.")

        access_token, _ = app_identity.get_access_token(
            'https://www.googleapis.com/auth/datastore')
        app_id = app_identity.get_application_id()

        entity_filter = {
            'kinds': kinds,
            'namespace_ids': self.request.get_all('namespace_id')
        }
        request = {
            'project_id': app_id,
            'output_url_prefix': 'gs://{}'.format(bucket),
            'entity_filter': entity_filter
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token
        }
        url = 'https://datastore.googleapis.com/v1/projects/{}:export'.format(
            app_id)

        try:
            result = urlfetch.fetch(
                url=url,
                payload=json.dumps(request),
                method=urlfetch.POST,
                deadline=60,
                headers=headers)
            if result.status_code == httplib.OK:
                logging.info(result.content)
            elif result.status_code >= 500:
                logging.error(result.content)
            else:
                logging.warning(result.content)
            self.response.status_int = result.status_code
        except urlfetch.Error:
            raise Exception("Failed to initiate export.")

        self.response.write("Export initiated.")


class BackupSqlToGcsHandler(CronHandler):
    def get(self, instance, db, bucket):
        date = datetime.date.today().strftime(config.iso_date_format)

        access_token, _ = app_identity.get_access_token([
            'https://www.googleapis.com/auth/sqlservice.admin',
            'https://www.googleapis.com/auth/cloud-platform',
        ])

        payload = {
            'exportContext': {
                'fileType': 'SQL',
                'uri': 'gs://{bucket}/{db}_{date}.sql'.format(
                    bucket=bucket, db=db, date=date),
                'databases': [db],
            },
        }
        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Content-Type': 'application/json',
        }
        url = (
            'https://www.googleapis.com/sql/v1beta4/projects/{app_id}/'
            'instances/{instance}/export'
        ).format(
            app_id=app_identity.get_application_id(),
            instance=instance,
        )

        try:
            result = urlfetch.fetch(
                url=url,
                payload=json.dumps(payload),
                method=urlfetch.POST,
                deadline=60,
                headers=headers,
            )
            if result.status_code == httplib.OK:
                logging.info(result.content)
            elif result.status_code >= 500:
                logging.error(result.content)
            else:
                logging.warning(result.content)
            self.response.status_int = result.status_code
        except urlfetch.Error:
            raise Exception("Failed to initiate SQL export.")

        self.response.write("SQL export initiated.")


class CleanGcsBucket(CronHandler):
    def get(self, bucket):
        num_deleted = 0
        for f in gcs.listbucket('/' + bucket):
            gcs.delete(f.filename)
            num_deleted += 1
        self.write({'files deleted': num_deleted})
