import datetime
import json
import logging
import util

from google.appengine.api import app_identity
from google.appengine.api import urlfetch


def timedelta_to_sql_time(delta):
    # https://stackoverflow.com/questions/8906926/formatting-python-timedelta-objects
    days = delta.days
    hours, h_remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(h_remainder, 60)

    if days > 0 or hours >= 839 or hours <= -839:
        # https://dev.mysql.com/doc/refman/5.7/en/time.html
        raise Exception(
            "Timedelta too long for SQL format. Days: {}, Hours: {}"
            .format(days, hours)
        )

    return "{}:{}:{}".format(hours, minutes, seconds)


def dumps_default(obj):
    # Tells json.dumps() how to handle special values.
    if isinstance(obj, datetime.timedelta):
        return timedelta_to_sql_time(obj)
    return util.json_dumps_default(obj)


class BigQueryApi(object):
    """Use the BigQuery REST API."""
    scopes = [
        'https://www.googleapis.com/auth/bigquery',
    ]

    def __init__(self):
        """Set project-based constants."""
        self.projectId = app_identity.get_application_id()
        self.auth_token = None
        self.url_prefix = (
            'https://www.googleapis.com/bigquery/v2/projects/{}'
            .format(self.projectId)
        )
        self.headers = {}

    def __enter__(self):
        """Get access token, which will only live until __exit__()."""
        self.auth_token, _ = app_identity.get_access_token(self.scopes)
        self.headers = {
            'Authorization': 'Bearer {}'.format(self.auth_token),
        }
        return self

    def __exit__(self, type, value, traceback):
        self.auth_token = None

    def fetch(self, method, path, headers=None, body=None):
        """Convenience wrapper for urlfetch.fetch()."""
        if headers is None:
            headers = {}

        if body is not None:
            headers.update({'Content-Type': 'application/json'})

        response = urlfetch.fetch(
            self.url_prefix + path,
            method=getattr(urlfetch, method),
            headers=dict(self.headers, **headers),
            payload=json.dumps(body, default=dumps_default),
        )
        logging.info(
            'Call complete. {} {} {}\n\n Body {}'
            .format(method, path, response.status_code, response.content)
        )
        return (response.status_code, json.loads(response.content))

    def list_datasets(self):
        status, response_body = self.fetch('GET', '/datasets')

        if status != 200:
            raise Exception(
                "BigQueryApi.list_datasets() failed: {} {}"
                .format(status, response_body)
            )

        return response_body

    def ensure_dataset(self, datasetId):
        """Create a dataset if not already present."""
        status, response_body = self.fetch(
            'POST',
            '/datasets',
            body={
                "datasetReference": {
                  "datasetId": datasetId,
                  "projectId": self.projectId,
                },
            },
        )

        err_status = response_body.get('error', {}).get('status', None)
        if (status == 409 and err_status == 'ALREADY_EXISTS') or status == 200:
            # Duplicate errors are fine.
            return

        raise Exception(
            "BigQueryApi.ensure_dataset() failed: {} {}"
            .format(status, response_body)
        )

    def ensure_table(self, datasetId, tableId, schema=None):
        """Create a table if not already present."""
        status, response_body = self.fetch(
            'POST',
            '/datasets/{}/tables'.format(datasetId),
            body={
                "tableReference": {
                  "datasetId": datasetId,
                  "projectId": self.projectId,
                  "tableId": tableId,
                },
                "schema": schema or {},
            },
        )
        return status, response_body

        err_status = response_body.get('error', {}).get('status', None)
        if (status == 409 and err_status == 'ALREADY_EXISTS') or status == 200:
            return

        raise Exception(
            "BigQueryApi.ensure_table() failed: {} {}"
            .format(status, response_body)
        )

    def insert_data(self, datasetId, tableId, row_dicts, insert_id_field=None):
        """Stream rows to table. Doesn't start a job.

        Args:
            datasetId - str
            tableId - str
            row_dicts - list of dicts
            insert_id_field - str, optional. If provided, will be passed to
                BigQuery, which will attempt to avoid duplication based on its
                value, kind of like a unique index, but they say "best effort".

        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
        """
        rows = []
        for r in row_dicts:
            insert_row = {"json": r}
            if insert_id_field and insert_id_field in r:
                insert_row["insertId"] = r[insert_id_field]
            rows.append(insert_row)

        status, response_body = self.fetch(
            'POST',
            '/datasets/{}/tables/{}/insertAll'.format(datasetId, tableId),
            body={
                "skipInvalidRows": True,
                "ignoreUnknownValues": True,
                # "templateSuffix": string,
                "rows": rows,
            },
        )

        if status != 200:
            raise Exception(
                "BigQueryApi.insert_data() failed: {} {}"
                .format(status, response_body)
            )

        return status, response_body
