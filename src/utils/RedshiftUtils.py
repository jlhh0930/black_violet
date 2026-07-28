import configparser
import boto3
import os
import psycopg2
from dateutil.relativedelta import relativedelta
from datetime import date

from utils import AWSUtils

class RedshiftUtils(object):
    _instance = None
    _log = None

    def __new__(cls, config, logger, new_conn=False, statement_timeout=None):
        if cls._instance is None or cls._instance.dbname != config.dbname or new_conn:
            cls._instance = object.__new__(RedshiftUtils)

            RedshiftUtils._instance.rs_secret_string = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)

            db_config = {
                'dbname': config.dbname,
                'host': cls._instance.rs_secret_string['host'],
                'port': config.rs_secret_string['port'],
                'user': config.rs_secret_string['user'],
                'sslmode': 'require'
            }

            if statement_timeout:
                db_config['statement_timeout'] = f'-c statement_timeout {statement_timeout}'

            try:
                log = RedshiftUtils._log = logger
                log.info(f'Connecting to Redshift DB: {db_config}')
                connection = RedshiftUtils._instance.connection = psycopg2.connect(**db_config)
                cursor = RedshiftUtils._instance.cursor = connection.cursor()
                RedshiftUtils._instance.dbname = db_config['dbname']
                cursor.execute('SELECT VERSION(')
                db_version = cursor.fetchone()[0]

            except Exception as e:
                log.info(f'Error: connection not established. \n{e}')
                RedshiftUtils._instance = None

            else:
                logger.info('Connection established: {}'.format(db_version[0]))

        return cls._instance

    def __init__(self):
        self.rs_secret_string = self._instance.rs_secret_string
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.dbname = self._instance.dbname
        self._log = self._log

    def execute_query(self, query) -> object:
        try:
            self.debug(query)
            result = self.cursor
        except Exception as e:
            self.log.error('Error executing query: {}, \nError: {}'.format(query, e))
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
            self.connection.cursor()
