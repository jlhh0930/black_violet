import configparser
import boto3
import os
import psycopg2
from dateutil.relativedelta import relativedelta
from datetime import date

from utils import AWSUtils

class RedshiftUtils(object):
    from __future__ import annotations

    import psycopg2
    from typing import Any, Optional

    from utils import AWSUtils

    class RedshiftUtils:
        """
        Small helper around a single psycopg2 connection + cursor.

        Notes:
        - Uses a keyed singleton by (dbname) so different databases don't collide.
        - Uses context handling for cursors.
        """

        _instances: dict[tuple[str], "RedshiftUtils"] = {}

        def __new__(
                cls,
                config: Any,
                logger: Any,
                *,
                new_conn: bool = False,
                statement_timeout: Optional[int] = None,
        ):
            key = (config.dbname,)

            if new_conn or key not in cls._instances:
                inst = super().__new__(cls)

                secret = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)

                db_config = {
                    "dbname": config.dbname,
                    "host": secret["host"],
                    "port": secret["port"],
                    "user": secret["user"],
                    "sslmode": "require",
                }

                # psycopg2 supports setting statement_timeout via options.
                if statement_timeout is not None:
                    db_config["options"] = f"-c statement_timeout={int(statement_timeout)}"

                try:
                    logger.info(
                        "Connecting to Redshift DB %s at %s:%s",
                        db_config["dbname"],
                        db_config["host"],
                        db_config["port"],
                    )
                    connection = psycopg2.connect(**db_config)
                    cursor = connection.cursor()

                    cursor.execute("SELECT VERSION()")
                    db_version = cursor.fetchone()
                    logger.info("Connection established: %s", db_version[0] if db_version else "unknown")

                    inst._init_state(connection=connection, cursor=cursor, dbname=config.dbname, logger=logger,
                                     secret=secret)

                except Exception:
                    logger.exception("Failed to connect to Redshift; creating instance aborted.")
                    # Ensure no cached broken instance is stored
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

        def __init__(self, *args, **kwargs):
            # Intentionally minimal: state is created in __new__
            pass

        def debug(self, msg: str) -> None:
            # Optional hook; keeps behavior centralized
            if hasattr(self._log, "debug"):
                self._log.debug(msg)

        def execute_query(
                self,
                query: str,
                params: Optional[tuple | dict] = None,
                *,
                fetch: str = "none",  # "none" | "one" | "all"
                commit: bool = False,
        ) -> Any:
            """
            Execute a query safely with optional parameters.

            fetch:
              - "none": returns None
              - "one": returns cursor.fetchone()
              - "all": returns cursor.fetchall()
            """
            try:
                self.debug("Executing query: %s", )
                self._log.debug(query) if hasattr(self._log, "debug") else None

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
                # Don't swallow silently: return None if caller expects it
                self.connection.rollback()
                return None

        def get_db_message(self) -> Optional[str]:
            notices = getattr(self.connection, "notices", None)
            if notices:
                return notices[-1]
            return None

        def set_isolation_level_autocommit(self) -> None:
            self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        def close(self) -> None:
            # Explicit close (recommended over __del__)
            try:
                if getattr(self, "cursor", None):
                    self.cursor.close()
            finally:
                if getattr(self, "connection", None):
                    self.connection.close()

        def __del__(self):
            # Best-effort cleanup; avoid raising from __del__
            try:
                self.close()
            except Exception:
                pass
