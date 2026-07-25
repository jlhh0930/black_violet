import configparser
import boto3
import os
import psycopg2
from dateutil.relativedelta import relativedelta
from datetime import date

from utils import AWSUtils

class RedshiftUtils(object):
    _instance = None
    _logger = None

    def __new__(cls,
                host,
                database_name,
                secret_arn,
                region_name,
                log,
                new_conn=False,
                statement_timeout=None):
        if cls._instance is None or cls._instance.dbname != database_name or new_conn:
            cls._instance = object.__new__(cls)

            log.debug('New Redshift Connection')

            RedshiftUtils._instance.rs_secret_string = AWSUtils.get_secret_string(secret_arn, region_name)

            db_config = {
                'dbname': cls._instance.rs_secret_string['dbname'],
                'host': cls._instance.rs_secret_string['host'],
                'password': cls._instance.rs_secret_string['password'],
                'port': cls._instance.rs_secret_string['port'],
                'user': cls._instance.rs_secret_string['user'],
                'sslmode': 'require'
            }

            if statement_timeout:
                db_config['options'] = f'-c statement_timeout={statement_timeout}'
            try:
                log = RedshiftUtils._logger = log
                log.info('Connecting to Redshift database ... {}'.format(database_name))
                connection = RedshiftUtils._instance.connection = psycopg2.connect(**db_config)
                cursor = RedshiftUtils._instance.cursor = connection.cursor()
                RedshiftUtils._instance.dbname = database_name
                RedshiftUtils._instance.connection = connection
                cursor.execute('SELECT VERSION()')
                db_version = cursor.fetchone()
            except Exception as e:
                log.info('Error connecting to Redshift database ... {}'.format(e))
                RedshiftUtils._instance = None

        return cls._instance

    def __init__(self):
        self.rs_secret_string = self._instance.rs_secret_string
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.dbname = self._instance.dbname
        self.log = self._log

    def query(self, sql_query) -> object:
        try:
            self.log.debug(sql_query)
            self.cursor.execute(sql_query)
            result = self.cursor
        except Exception as e:
            self.log.error('Error executing query "{}", error: {}'.format(sql_query, e))
            return None
        else:
            return result

    def get_db_message(self):
        if self.connection.notices:
            return self.connection.notices[-1]
        else:
            return None

    def set_isolation_level_autocommit(self):
        self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def __del__(self):
        if hasattr(self, 'connection'):
            self.connection.close()
        if hasattr(self, 'cursor'):
            self.cursor.close()