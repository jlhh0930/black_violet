# postgres_utils.py
from __future__ import annotations

from typing import Any, Optional

import psycopg2

from utils import AWSUtils


class PostgresUtils:
    """
    Functionally similar to RedshiftUtils, but for PostgreSQL (psycopg2).
    Singleton per (dbname, host) (keyed by config.dbname + secret host).
    """

    _instances: dict[tuple[str, str], "PostgresUtils"] = {}

    def __new__(
        cls,
        config: Any,
        logger: Any,
        *,
        new_conn: bool = False,
        statement_timeout: Optional[int] = None,
    ):
        secret = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)
        key = (config.dbname, secret["host"])

        if new_conn or key not in cls._instances:
            inst = super().__new__(cls)

            db_config = {
                "dbname": config.dbname,
                "host": secret["host"],
                "port": secret["port"],
                "user": secret["user"],
                "sslmode": getattr(config, "sslmode", "require"),
            }
            if statement_timeout is not None:
                db_config["options"] = f"-c statement_timeout={int(statement_timeout)}"

            try:
                logger.info(
                    "Connecting to Postgres DB=%s at %s:%s",
                    db_config["dbname"],
                    db_config["host"],
                    db_config["port"],
                )
                connection = psycopg2.connect(**db_config)
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
        self.rs_secret_string = secret  # parity naming

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
        fetch: str = "none",
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
        notices = getattr(self.connection, "notices", None)
        if notices:
            return notices[-1]
        return None

    def set_isolation_level_autocommit(self) -> None:
        self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

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
