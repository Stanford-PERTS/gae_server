"""Convenience wrapper for MySQLdb."""

from __future__ import division
import collections
import google.appengine.api.app_identity as app_identity
import logging
import MySQLdb
import re
import time

import util


class MySQLApi(object):
    """Given credentials, connects to a Cloud SQL instance and simplifies
    various kinds of queries. Use `with` statement to ensure connections are
    correctly closed.

    Example:

    with Api(**credentials) as mysql_api:
        result = mysql_api.select_star_where('checkpoint', status='incomplete')

    N.B. Does _not_ pool/cache connections, b/c:

    > We recommend that a new connection is created to service each HTTP
    > request, and re-used for the duration of that request (since the time to
    > create a new connection is similar to that required to test the liveness
    > of an existing connection).

    https://groups.google.com/forum/#!topic/google-cloud-sql-discuss/sS38Nh7MriY
    """

    connection = None
    cursor = None
    num_tries = 4
    retry_interval_ms = 500  # base for exponential backoff: 500, 1000, 2000...
    # Which exception for which to attempt recovery
    exceptions = (MySQLdb.ProgrammingError, MySQLdb.OperationalError,
                  MySQLdb.InterfaceError)

    # Configurable on instantiation.
    cloud_sql_instance = None  # Cloud SQL instance name in project.
    cloud_sql_user = 'root'
    local_user = None
    local_password = None
    local_ip = '127.0.0.1'
    local_port = 3306
    db_name = None
    retry_on_error = True

    def __init__(self, **kwargs):
        keys = ['cloud_sql_instance', 'cloud_sql_user', 'local_user',
                'local_password', 'local_ip', 'local_port', 'db_name',
                'retry_on_error']
        for k in keys:
            if k in kwargs:
                setattr(self, k, kwargs[k])

    def __enter__(self):
        self.connect_to_db()
        return self

    def __exit__(self, exception_type, exception, traceback):
        if exception:
            # Exceptions at this level may be normal, e.g. when someone POSTs
            # data that conflicts with a unique key, so don't log errors. Note
            # that we don't catch the exception here, so we can trust that
            # it won't go unnoticed.
            # Example error message:
            # 'IntegrityError(1062, "Duplicate entry SECRET INFO")'
            # We want to extract the code for debugging, but not expose any
            # data in the logs.
            # For deciphering codes, see https://dev.mysql.com/doc/refman/5.7/en/server-error-reference.html
            c = exception.args[0] if len(exception.args) > 0 else None
            # Make sure the code is safe to log, should be numeric
            error_code = c if re.match(r'^\d+$', str(c)) else None
            logging.warning('{}, MySQL error code {}'.format(
                exception.__class__.__name__,
                error_code,
            ))
            logging.warning(
                "Exiting mysql connection with exception; rolling back."
            )
            self.connection.rollback()
        else:
            self.connection.commit()

        self.connection.close()

    def _cursor_retry_wrapper(self, method_name, query_string, param_tuple):
        """Wrap the normal cursor.execute from MySQLdb with a retry.

        Args:
            method_name     str, either 'execute' or 'executemany'
        """
        # logging.info(query_string)
        # logging.info(param_tuple)

        if not self.retry_on_error:
            getattr(self.cursor, method_name)(query_string, param_tuple)
            return

        final_exception = None
        tries = 0
        while True:
            try:
                # Either execute or execute_many
                getattr(self.cursor, method_name)(query_string, param_tuple)
                break  # call succeeded, don't try again
            except self.exceptions as e:
                # Log the error and try again. Close the problematic connection
                # and make a new one.
                logging.warning("MySQLApi exception on query; will retry.")
                logging.warning(e)
                final_exception = e
                self.connection.close()
                self.connect_to_db()
            tries += 1
            if tries >= self.num_tries:
                # That's enough tries, just throw an error.
                logging.info(query_string)
                logging.info(param_tuple)
                logging.error("Recurrent exception, gave up querying.")
                raise final_exception
            self.sleep_for_backoff_interval(tries)

    def _cursor_execute(self, query_string, param_tuple):
        self._cursor_retry_wrapper('execute', query_string, param_tuple)

    def _cursor_executemany(self, query_string, param_tuple):
        self._cursor_retry_wrapper('executemany', *args)

    def get_credentials(self):
        """Establish connection to MySQL db instance.

        Either Google Cloud SQL or local MySQL server. Detects environment with
        functions from util module.
        """
        if util.is_localhost() or util.is_codeship():
            credentials = {
                'host': self.local_ip,
                'port': self.local_port,
                'user': self.local_user,
                'passwd': self.local_password
            }
        else:
            # Note: for second generation cloud sql instances, the instance
            # name must include the region, e.g. 'us-central1:production-01'.
            credentials = {
                'unix_socket': '/cloudsql/{app_id}:{instance_name}'.format(
                    app_id=app_identity.get_application_id(),
                    instance_name=self.cloud_sql_instance),
                'user': self.cloud_sql_user,
            }
        if self.db_name:
            credentials['db'] = self.db_name

        return credentials

    def connect_to_db(self, **kwargs):
        # Although the docs say you can specify a `cursorclass` keyword
        # here as an easy way to get dictionaries out instead of lists, that
        # only works in version 1.2.5, and App Engine only has 1.2.4b4
        # installed as of 2015-03-30. Don't use it unless you know the
        # production library has been updated.
        # tl;dr: the following not allowed!
        # self.connection = MySQLdb.connect(
        #     charset='utf8', cursorclass=MySQLdb.cursors.DictCursor, **creds)
        kwargs = self.get_credentials()
        kwargs.update(charset='utf8')

        tries = 0
        final_exception = None
        while True:
            try:
                self.connection = MySQLdb.connect(**kwargs)
                break  # call succeeded, don't try again
            except self.exceptions as e:
                # Log the error and try again.
                logging.warning("MySQLApi exception on connect; will retry.")
                logging.warning(e)
                final_exception = e
            tries += 1
            if tries >= self.num_tries:
                # That's enough tries, just throw an error.
                logging.error("Recurrent exception, gave up connecting.")
                raise final_exception
            self.sleep_for_backoff_interval(tries)

        self.cursor = self.connection.cursor()

        # We use 5.6 locally, but production uses 5.7, which is more strict by
        # default. Make sure our local instances are equally strict.
        self.cursor.execute(
            "SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,"
            "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'",
            tuple(),
        )

    def sleep_for_backoff_interval(self, tries):
        # Sleep interval between tries backs off exponentially.
        # N.B. retry interval is in ms while time.sleep() takes seconds.
        time.sleep(2 ** (tries - 1) * self.retry_interval_ms / 1000)

    def table_columns(self, table):
        result = self.query("SHOW columns FROM `{}`".format(table))
        return [column[0] for column in result]

    def reset(self, table_definitions):
        """Drop all given tables and re-created them.

        Takes a dictionary of table name to with CREATE TABLE query string.
        """
        if not util.is_development():
            raise Exception("You REALLY don't want to do that.")

        for table, definition in table_definitions.items():
            self.query('DROP TABLE IF EXISTS `{}`;'.format(table))
            self.query(definition)

    def query(self, query_string, param_tuple=tuple(), n=None):
        """Run a general-purpose query. Returns a tuple of tuples."""
        self._cursor_execute(query_string, param_tuple)
        if n is None:
            return self.cursor.fetchall()
        else:
            return self.cursor.fetchmany(n)

    def select_query(self, query_string, param_tuple=tuple(), n=None):
        """Simple extension of .query() by making results more convenient.

        Interpolate with %s syntax and the param_tuple argument. Example:
        sql_api.query(
            "SELECT * FROM heroes WHERE name = %s AND age = %s",
            ('Hector', 20)
        )
        """
        result = self.query(query_string, param_tuple, n)

        # Results come back as a tuple of tuples. Discover the names of the
        # SELECTed columns and turn it into a list of dictionaries.
        fields = [f[0] for f in self.cursor.description]
        return [{fields[i]: v for i, v in enumerate(row)} for row in result]

    def where_clause_from_params(self, **where_params):
        if where_params:
            # Note: different right hand side for comparison to None (NULL)
            lhs = ['`{}`'.format(k) for k in where_params.keys()]
            rhs = ['IS NULL' if v is None else '= %s'
                   for v in where_params.values()]
            where_parts = ['{} {}'.format(*parts) for parts in zip(lhs, rhs)]
            # Note: omit None values since there's no %s
            values = tuple(v for v in where_params.values() if v is not None)
        else:
            where_parts = ['1']
            values = tuple()

        return (' AND '.join(where_parts), values)

    def count_where(self, table, **where_params):
        """Return number of rows matching params."""
        where_clause, values = self.where_clause_from_params(**where_params)

        query = """
            SELECT COUNT(`uid`)
            FROM `{table}`
            WHERE {where_clause}
        """.format(table=table, where_clause=where_clause)

        return int(self.select_single_value(query, values))

    def select_single_value(self, query_string, param_tuple=tuple()):
        """Returns the first value of the first row of results, or None."""
        self._cursor_execute(query_string, param_tuple)
        result = self.cursor.fetchone()

        # result is None if no rows returned, else a tuple.
        return result if result is None else result[0]

    def select_star_where(self, table, order_by=None, descending=False,
                          limit=100, offset=None, **where_params):
        """Get whole rows matching filters. Restricted but convenient."""

        where_clause, values = self.where_clause_from_params(**where_params)

        if limit == float('inf'):
            limit_clause = ''
        else:
            limit_clause = 'LIMIT {offset}{limit}'.format(
                offset='{},'.format(offset) if offset else '',
                limit=limit,
            )

        query = """
            SELECT *
            FROM `{table}`
            WHERE {where_clause}
            {order_by}{descending}
            {limit_clause}
        """.format(
            table=table,
            where_clause=where_clause,
            order_by='ORDER BY `{}`'.format(order_by) if order_by else '',
            descending=' DESC' if order_by and descending else '',
            limit_clause=limit_clause,
        )

        return self.select_query(query, values)

    def select_single_value(self, query_string, param_tuple=tuple()):
        """Returns the first value of the first row of results, or None."""
        self._cursor_execute(query_string, param_tuple)
        result = self.cursor.fetchone()

        # result is None if no rows returned, else a tuple.
        return result if result is None else result[0]

    def select_row_for_update(self, table, id_col, id):
        """Selects and locks one row. Lock released on commit or rollback."""
        query = """
            SELECT *
            FROM `{table}`
            WHERE `{id_col}` = %s
            FOR UPDATE  # locks row
        """.format(
            table=table,
            id_col=id_col,
        )

        return self.select_query(query, (id,))

    def insert_or_update(self, table, row_dict,
                         on_duplicate_key_update=tuple()):
        """Either INSERT a new row or UPDATE a matching row.

        Args:
            table - str
            row_dict - dict, must include the key `uid`
            on_duplicate_key_update - tuple, optional, iff the incoming uid
                isn't found in table, adds ON DUPLICATE KEY UPDATE to the INSERT
                query, which may result in an update (even though the provided
                uid doesn't exist) if the row collides with some unique index
                other than the PRIMARY.

        Returns: affected_rows, int, 0-2, meaning:
            * 0 - no rows changed, the row matched an existing one perfectly.
            * 1 - row inserted or existing row with matching uid updated
            * 2 - update to existing row via INSERT ON DUPLICATE KEY UPDATE,
              so the uid of the row changed will _not_ match the given one.
        """

        # Note that we could accomplish identical functionality by just running
        # the INSERT blind, catching possible duplication errors and then
        # cleaning up with an UPDATE. That would save time in the INSERT case
        # because only one query would be necessary. But CAM did a test
        # of a table with 750k rows and found (avg over 1000 points each):
        #
        # Count-first strategy when inserting:     576 microseconds
        # Count-first strategy when updating:      582 microseconds
        # Catch-duplicate strategy when inserting: 414 microseconds
        # Catch-duplicate strategy when updating:  616 microseconds
        #
        # So while catching duplicates might be faster overall, it's by a tiny
        # margin, and the necessary code is more complex.

        # Check if this uid exists.
        query = 'SELECT COUNT(*) FROM `{}` WHERE `uid` = %s'.format(table)
        params = (row_dict['uid'],)
        count = self.select_single_value(query, params)

        if count == 0:
            # If the uid is new, do INSERT, possibly with ON DUPLICATE KEY
            # UPDATE.
            affected_rows = self.insert_row_dicts(
                table, row_dict, on_duplicate_key_update)
        else:
            # If the uid exists, update that row.
            uid = row_dict.pop('uid')
            # Note that this may fail with MySQLdb.IntegrityError if there's
            # some unique index other than uid/PRIMARY that collides.
            affected_rows = self.update_row(table, 'uid', uid, **row_dict)

        return affected_rows

    def insert_row_dicts(self, table, row_dicts, on_duplicate_key_update=None):
        """INSERT one record or many records.

        Args:
            table: str name of the table
            row_dicts: a single dictionary or a list of them
            on_duplicate_key_update: tuple of the fields to update in the
                existing row if there's a duplicate key error.

        Returns: affected_rows, int, 0-2, see [MySQL docs][1] (v 5.6 an 5.7):

        > For INSERT ... ON DUPLICATE KEY UPDATE statements, the affected-rows
          value per row is 1 if the row is inserted as a new row, 2 if an
          existing row is updated, and 0 if an existing row is set to its
          current values.

        [1]: https://dev.mysql.com/doc/refman/5.6/en/mysql-affected-rows.html
        """
        # Standardize to list.
        if type(row_dicts) is not list:
            row_dicts = [row_dicts]

        # Turn each row dictionary into an ordered dictionary
        ordered_rows = [collections.OrderedDict(
            sorted(d.items(), key=lambda t: t[0])) for d in row_dicts]

        # Make sure each dictionary has the same set of keys.
        correct_keys = ordered_rows[0].keys()
        if not all([row.keys() == correct_keys for row in ordered_rows]):
            raise Exception("Inconsistent fields: {}.".format(ordered_rows))

        # Backticks critical for avoiding collisions with MySQL reserved words,
        # e.g. 'condition'!
        query_string = 'INSERT INTO `{}` (`{}`) VALUES ({})'.format(
            table,
            '`, `'.join(correct_keys),
            ', '.join(['%s'] * len(correct_keys)),
        )

        # MySQLdb expects a tuple or a list of tuples for the values.
        value_tuples = [tuple(row.values()) for row in ordered_rows]
        if len(row_dicts) is 1:
            insert_method = 'execute'
            params = value_tuples[0]
        else:
            insert_method = 'executemany'
            params = value_tuples

        if on_duplicate_key_update:
            # Add the extra query syntax. This tells MySQL: when you encounter
            # an inserted row that would result in a duplicate key, instead do
            # an UPDATE on the existing row. The values set are: for each field
            # named in on_duplicate_key_update, pull the corresponding value
            # from VALUES.
            # N.B. This is better than INSERT IGNORE because it records the new
            # data and doesn't ignore other unrelated errors, and it's better
            # than REPLACE INTO because that deletes the existing row and
            # inserts a new one, which is a bigger disturbance to the indexes
            # and can mess up the last inserted id.
            # http://stackoverflow.com/a/21419029/385132
            # http://stackoverflow.com/questions/2366813/on-duplicate-key-ignore

            # However, we should never update a uid.
            if 'uid' in on_duplicate_key_update:
                raise Exception("Can't update a uid.")

            query_string += ' ON DUPLICATE KEY UPDATE {}'.format(
                ', '.join(
                    ['`{field}` = VALUES(`{field}`)'.format(field=f)
                     for f in on_duplicate_key_update]
                )
            )

        self._cursor_retry_wrapper(insert_method, query_string, params)

        affected_rows = self.connection.affected_rows()

        return affected_rows

    def update_row(self, table, id_col, id, **params):
        """UPDATE a row by id, assumed to be unique key."""
        query_string = 'UPDATE `{}` SET {} WHERE `{}` = %s'.format(
            table,
            ', '.join(['`{}` = %s'.format(k) for k in params.keys()]),
            id_col,
        )

        p = params.values()
        p.append(id)

        self.query(query_string, param_tuple=tuple(p))

        affected_rows = self.connection.affected_rows()

        return int(affected_rows)

    def delete(self, table, id_col, id_or_ids):
        """DELETE a row by id, assumed to be the unique key."""
        if type(id_or_ids) is not list:
            id_or_ids = [id_or_ids]

        if len(id_or_ids) == 0:
            return

        query_string = 'DELETE FROM `{}` WHERE `{}` IN ({})'.format(
            table,
            id_col,
            ','.join(['%s'] * len(id_or_ids)),
        )

        self.query(query_string, param_tuple=tuple(id_or_ids))

        affected_rows = self.connection.affected_rows()

        return int(affected_rows)
