"""Wrangling functions for dealing with the output of a 2nd gen MySQL instance
with the db flag `log_output` set to 'FILE' and `slow_query_log` set to 'On'.

Used by cron jobs to parse and transform the log files for storage in BigQuery.
"""

import datetime
import json
import logging
import re

from big_query_api import BigQueryApi
import config
import datetime
import cloudstorage as gcs


# This translates the convention that MySQL uses when logging slow queries to
# a table into a BigQuery table schema.
#
# Allowed values for types are:
# STRING, BYTES, INTEGER, FLOAT, BOOLEAN, TIMESTAMP, DATE, TIME, DATETIME
# There's also a "mode" with NULLABLE, REQUIRED and REPEATED
#
# See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
schema = {
    "fields": [
        {
            "name": "start_time",
            "type": "TIMESTAMP",
        },
        {
            "name": "user_host",
            "type": "STRING",
        },
        # This part of the standard slow_log schema, but very hard to work
        # with, when we just want a duration as a number.
        {
            "name": "query_time",
            "type": "TIME",
        },
        {
            "name": "lock_time",
            "type": "TIME",
        },
        {
            "name": "rows_sent",
            "type": "INTEGER",
        },
        {
            "name": "rows_examined",
            "type": "INTEGER",
        },
        {
            "name": "db",
            "type": "STRING",
        },
        {
            "name": "last_insert_id",
            "type": "INTEGER",
        },
        {
            "name": "insert_id",
            "type": "INTEGER",
        },
        {
            "name": "server_id",
            "type": "INTEGER",
        },
        {
            "name": "sql_text",
            "type": "STRING",
        },
        {
            "name": "thread_id",
            "type": "INTEGER",
        },
        # Not part of the standard slow_log schema, but easier to work with.
        {
            "name": "query_duration_ms",
            "type": "FLOAT",
        },
    ],
}


# Folder within gcs bucket, defined by Google when db flag `log_output` is set
# to `FILE`.
slow_query_path = 'cloudsql.googleapis.com/mysql-slow.log'


# For stripping query text of potentially sensistive info. Also helps collapse
# queries that only differ by uninteresting parameters, like object ids.
sql_string_literal = re.compile(r"'.*?'")
sql_IN = re.compile(r'IN\s*\(.*?\)')


def json_batch_gen(bucket):
    """A generator, to avoid reading all the files in at once."""
    file_names = list_slow_log_fragments(bucket)
    for fn in file_names:
        with gcs.open(fn, 'r') as fh:
            lines = fh.read().split('\n')
        if len(lines) > 0 and lines[0]:
            yield (lines, fn)


def json_lines_to_entries(lines):
    """Google unhelpfully creates an "entry" (JSON object) for each line in
    the log file, except each true entry in the log file is spread out over
    multiple lines, so we have to stitch it back together."""
    raw_entries = [json.loads(l) for l in lines if l]

    grouped_entries = {}
    for e in raw_entries:
        timestamp = e['timestamp']

        if timestamp not in grouped_entries:
            grouped_entries[timestamp] = {'query': ''}
        d = grouped_entries[timestamp]

        d['timestamp'] = timestamp
        d['database'] = e['resource']['labels']['database_id']
        text = e['textPayload']

        if text.startswith('# Query_time:'):
            text_parts = text.split(' ')
            d['lock_time'] = float(text_parts[5])
            d['query_time'] = float(text_parts[2])
            d['rows_examined'] = int(text_parts[10])
            d['rows_sent'] = int(text_parts[7])

        ignore = [
            '#',
            'SET timestamp',
            'use neptune;',
            'commit;',
        ]

        # Gather lines of query text into a single string for the  entry.
        if not any(text.startswith(s) for s in ignore):
            if d['query'] != '' and not d['query'].endswith('\n'):
                d['query'] += '\n'
            d['query'] += strip_query(text)

    return grouped_entries


def list_slow_log_fragments(bucket):
    path = '/{}/{}'.format(bucket, slow_query_path)
    return [f.filename for f in gcs.listbucket(path)]


def seconds_to_sql_time(float_seconds):
    """Returns HH:MM:SS to microseconds.

    See https://dev.mysql.com/doc/refman/5.7/en/time.html
    """
    if float_seconds < 0:
        raise Exception("Negative time intervals not supported as query times.")

    if float_seconds < 10:
        # Most times we encounter can use this cheap shortcut.
        return '00:00:0{}'.format(
            ('%f' % round(float_seconds, 6)).ljust(8, '0')
        )

    if float_seconds >= 24 * 60 * 60:
        # Because we use datetime.time.
        raise Exception("seconds_to_sql_time() only supports up to 24 hours")

    hours = int(float_seconds // 3600)
    wo_hours = float_seconds - hours * 3600

    minutes = int(wo_hours // 60)
    wo_minutes = wo_hours - minutes * 60

    seconds = int(wo_minutes // 1)
    wo_seconds = wo_minutes - seconds

    microseconds = int(round(wo_seconds, 6) * 1000000)

    time = datetime.time(hours, minutes, seconds, microseconds)
    return time.strftime('%H:%M:%S.%f')


def strip_query(query):
    wo_literals = sql_string_literal.subn("'...'", query)[0]
    wo_IN = sql_IN.subn("IN(...)", wo_literals)[0]

    return wo_IN


def to_slow_schema(entry):
    """When `log_ouput` is set to 'TABLE', MySQL creates a table with a fixed
    schema. Convert our entries to that schema."""

    timestamp = datetime.datetime.strptime(
        entry['timestamp'],
        '%Y-%m-%dT%H:%M:%S.%fZ',
    )
    since_epoch = timestamp - datetime.datetime(1970, 1, 1)
    micro_time = int(since_epoch.total_seconds() * 1000000)
    return {
        'db': entry['database'],
        'insert_id': micro_time,
        'lock_time': seconds_to_sql_time(entry.get('lock_time', 0)),
        'rows_sent': entry.get('rows_sent', 0),
        'rows_examined': entry.get('rows_examined', 0),
        'sql_text': entry['query'],
        'start_time': timestamp.strftime(config.sql_datetime_format),
        # This part of the standard slow_log schema, but very hard to work
        # with, when we just want a duration as a number.
        'query_time': seconds_to_sql_time(entry.get('query_time', 0)),
        # Not part of the standard slow_log schema, but easier to work with.
        'query_duration_ms': round(entry.get('query_time', 0), 3) * 1000,
    }
