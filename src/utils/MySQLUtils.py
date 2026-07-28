# mysql_utils.py
from __future__ import annotations

from typing import Any, Optional

import mysql.connector

from utils import AWSUtils


class MySQLUtils:
    """
    Functionally similar to RedshiftUtils, but for MySQL (mysql-connector-python).
    Singleton per (database, host).
    """

    _instances: dict[tuple[str, str], "MySQLUtils"] = {}

    def __new__(
        cls,
        config: Any,
        logger: Any,
        *,
        new_conn: bool = False,
        connect_timeout: Optional[int] = None,
    ):
        secret = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)
        key = (config.dbname, secret["host"])

        if new_conn or key not in cls._instances:
            inst = super().__new__(cls)

            db_config = {
                "database": config.dbname,
                "host": secret["host"],
                "port": secret.get("port", 3306),
                "user": secret["user"],
                "password": secret.get("password"),
            }

            # allow custom SSL if provided in config/secret
            ssl = getattr(config, "ssl", None)
            if ssl:
                db_config["ssl_ca"] = getattr(ssl, "ssl_ca", None)
                db_config["ssl_disabled"] = False

            if connect_timeout is not None:
                db_config["connection_timeout"] = int(connect_timeout)

            # remove None keys to avoid connector errors
            db_config = {k: v for k, v in db_config.items() if v is not None}

            try:
                logger.info("Connecting to MySQL DB=%s at %s:%s", config.dbname, db_config["host"], db_config["port"])
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()

                cursor.execute("SELECT VERSION()")
                row = cursor.fetchone()
                logger.info("Connection established: %s", row[0] if row else "unknown")

                inst._init_state(connection=connection, cursor=cursor, dbname=config.dbname, logger=logger, secret=secret)

            except Exception as e:
                logger.exception("Error: connection not established.")
                try:
                    if "connection" in locals() and connection:
                        connection.close()
                except Exception:
                    pass
                raise

            cls._instances[key] = inst

        return cls._instances[key]

    def _init_state(self, *, connection, cursor, dbname: str, logger, secret):
        self.connection = connection
        self.cursor = cursor
        self.dbname = dbname
        self._log = logger
        self.rs_secret_string = secret

        # parity helper (MySQL autocommit property exists)
        try:
            self.connection.autocommit = True
        except Exception:
            pass

    def __init__(self, *args, **kwargs):
        pass

    def debug(self, msg: str) -> None:
        if hasattr(self._log, "debug"):
            self._log.debug(msg)

    def execute_query(
        self,
        query: str,
        params: Optional[tuple | dict] = None,
        *,
        fetch: str = "none",  # none|one|all
        commit: bool = False,
    ) -> object:
        try:
            if hasattr(self._log, "debug"):
                self._log.debug(query)

            with self.connection.cursor() as cur:
                cur.execute(query, params)
                if commit:
                    self.connection.commit()

                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                return None
        except Exception as e:
            self._log.exception("Error executing query. query=%r error=%s", query, e)
            try:
                self.connection.rollback()
            except Exception:
                pass
            return None

    def get_db_message(self) -> Optional[str]:
        # MySQL connector doesn't have "notices"; return last query status if possible
        try:
            return getattr(self.cursor, "statement", None)
        except Exception:
            return None

    def set_isolation_level_autocommit(self) -> None:
        try:
            self.connection.autocommit = True
        except Exception:
            pass

    def close(self) -> None:
        try:
            if getattr(self, "cursor", None):
                self.cursor.close()
        finally:
            if getattr(self, "connection", None):
                self.connection.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
