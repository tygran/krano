#/usr/bin/python
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)
from datetime import datetime
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class ConnectionSettings(object):
    """Encapsulates the specific connection settings for a PostgreSQL database connection.

    Args:
        name (str): Name for the connection settings.
        host (str): Host of the database server.
        database_name (str): Name of the database.
        username (str): Name of the database user.
        password (str): Password of the database user.
        application_name (str): Application name for the database connection, defaults to 'krano'.
        client_encoding (str): Encoding to be used fpr the database connection, defaults to 'utf-8'.

    Raises:
        ValueError: One of the non-optional arguments is not available.
    """

    def __init__(self, name, host, database_name, username, password, application_name='krano', client_encoding='utf-8'):
        self.name = name
        self.host = host
        self.database_name = database_name
        self.username = username
        self.password = password
        self.application_name = application_name
        self.client_encoding = client_encoding

        if not self.name:
            raise ValueError('You must provide a name for the connection setting.')

        if not self.host:
            raise ValueError('You must provide a host for the connection setting.')

        if not self.database_name:
            raise ValueError('You must provide a database_name for the connection setting.')

        if not self.username:
            raise ValueError('You must provide a username for the connection setting.')

        if not self.password:
            raise ValueError('You must provide a password for the connection setting.')

    def __repr__(self):
        repr = "<ConnectionSettings name={0}>".format(self.name)
        return repr


class QueryResult(object):
    """Stores the result of a executed query.

    Args:
        sql_statement (str): The SQL query that was used to fetch the records.
        records (list): A list of rows (tuples) fetched with the SQL query.
        column_names (list): A list containing the column names for the records.
        query_duration (datetime.timedelta): Execution time of the SQL query.
    """
    def __init__(self, sql_statement, records, column_names, query_duration):
        self.sql_statement = sql_statement
        self.records = records
        self.column_names = column_names
        self.query_duration = query_duration
        self.record_count = len(self.records)

    def isempty(self):
        """Indicates if the list of rows is empty or not."""
        return self.record_count == 0


class Database(object):
    """Connects to a PostgreSQL Database and executes given SQL queries.

    Args:
        connection_settings (ConnectionSettings): An instance of a ConnectionSettings object.
    """
    def __init__(self, connection_settings):
        self.connection_settings = connection_settings
        self.connection = None

        if not self.connection_settings:
            raise ValueError('You must provide connection settings.')

    def _get_connection(self):
        if self.connection:
            return self.connection

        logger.info("Opening database connection to {0}...".format(self.connection_settings.name))
        self.connection = psycopg2.connect(host=self.connection_settings.host, dbname=self.connection_settings.database_name,
                                           user=self.connection_settings.username, password=self.connection_settings.password,
                                           application_name=self.connection_settings.application_name)
        self.connection.set_client_encoding('utf-8')

        return self.connection

    def query(self, sql_statement):
        """Executes the SQL query against the database and returns the result.

        Returns:
            An instance of a QueryResult object containing the results of the executed query.
        """
        conn = self._get_connection()
        logger.info("Executing SQL query against database {0}...".format(self.connection_settings.name))

        cursor = conn.cursor()
        query_start_time = datetime.now().replace(microsecond=0)

        cursor.execute(sql_statement)
        conn.commit()
        records = cursor.fetchall()

        query_end_time = datetime.now().replace(microsecond=0)
        query_duration = query_end_time - query_start_time

        column_names = [column[0] for column in cursor.description]
        query_result = QueryResult(sql_statement, records, column_names, query_duration)
        logger.info("Fetched {0} records from the database {1}".format(query_result.record_count, self.connection_settings.name))

        return query_result

    def close(self):
        if self.connection:
            logger.info("Closing database connection to {0}...".format(self.connection_settings.name))
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()