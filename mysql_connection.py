"""Defines the Neptune MySQL database connection credentials."""

from MySQLdb import InterfaceError, OperationalError, ProgrammingError
import os

import mysql_api
import util


# For personal reference, how to grant privileges when setting up your local
# db:
# GRANT ALL PRIVILEGES ON neptune.* TO 'neptune'@'localhost' IDENTIFIED BY 'neptune';


def connect(retry_on_error=True, specify_db=True):
    params = get_params()
    if not specify_db:
        # Allow connections that don't USE DATABASE, in case there is no
        # database in the instance, or we don't know what its name should be.
        del params['db_name']

    return mysql_api.MySQLApi(retry_on_error=retry_on_error, **params)


def get_params():
    """Get MySQL db connection for any environment."""

    if util.is_localhost():
        if util.is_testing():
            # testing on localhost
            params = {
                'db_name': os.environ['LOCAL_SQL_TEST_DB_NAME'],
                'local_user': os.environ['LOCAL_SQL_USER'],
                'local_password': os.environ['LOCAL_SQL_PASSWORD'],
            }
        else:
            # normal app running on localhost
            params = {
                'db_name': os.environ['LOCAL_SQL_DB_NAME'],
                'local_user': os.environ['LOCAL_SQL_USER'],
                'local_password': os.environ['LOCAL_SQL_PASSWORD'],
            }
    elif util.is_codeship():
        # testing on codeship
        params = {
            # These are set up for us by codeship.
            # https://documentation.codeship.com/databases/mysql/
            'db_name': 'test',
            'local_user': os.environ['MYSQL_USER'],
            'local_password': os.environ['MYSQL_PASSWORD'],
        }
    else:
        # Deployed, either on the dev app or the production app
        params = {
            'db_name': os.environ['CLOUD_SQL_DB_NAME'],
            'cloud_sql_instance': os.environ['CLOUD_SQL_INSTANCE_ID'],
            'cloud_sql_user': 'root',
        }

    return params
