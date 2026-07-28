from __future__ import annotations

import snowflake.connector
from typing import Any, Optional

from utils import AWSUtils


class SnowflakeUtils:
    """
    Functionally similar to RedshiftUtils, but backed by Snowflake.

    - Singleton per (dbname, account) (keyed by config.snowflake_db + account).
    - Creates a single connection and reuses it.
    - Provides execute_query, get_db_message, set autocommit helper, close.
    """

    _instances: dict[tuple[str, str], "SnowflakeUtils"] = {}

    def __new__(
        cls,
        config: Any,
        logger: Any,
        *,
        new_conn: bool = False,
        statement_timeout: Optional[int] = None,  # kept for template parity; Snowflake differs
    ):
        key = (getattr(config, "snowflake_db"), getattr(config, "account"))

        if new_conn or key not in cls._instances:
            inst = super().__new__(cls)

            # Load secrets (expected to contain username/password, plus optional host/warehouse/schema params)
            secret = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)

            # Build Snowflake connection parameters
            sf_config = {
                "user": secret["user"],
                "password": secret["password"],
                "account": getattr(config, "account"),
                "role": getattr(config, "role", None),
                "warehouse": getattr(config, "warehouse", None),
                "database": getattr(config, "snowflake_db", None),
                "schema": getattr(config, "schema", None),
            }

            # Remove None entries
            sf_config = {k: v for k, v in sf_config.items() if v is not None}

            try:
                logger.info(
                    "Connecting to Snowflake DB=%s account=%s warehouse=%s role=%s",
                    sf_config.get("database"),
                    sf_config.get("account"),
                    sf_config.get("warehouse"),
                    sf_config.get("role"),
                )

                connection = snowflake.connector.connect(**sf_config)
                cursor = connection.cursor()

                # Template parity: "version" check
                cursor.execute("SELECT CURRENT_VERSION()")
                row = cursor.fetchone()
                db_version = row[0] if row else "unknown"
                logger.info("Connection established: %s", db_version)

                inst._init_state(
                    connection=connection,
                    cursor=cursor,
                    dbname=sf_config.get("database"),
                    logger=logger,
                    secret=secret,
                )

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
        self.rs_secret_string = secret  # kept name for template parity

        # Snowflake generally supports autocommit at connection level
        self.connection.autocommit = True

    def __init__(self, *args, **kwargs):
        # state created in __new__
        pass

    def debug(self, msg: str, *args, **kwargs) -> None:
        if hasattr(self._log, "debug"):
            self._log.debug(msg, *args, **kwargs)

    def execute_query(self, query: str, params: Optional[tuple | dict] = None) -> object:
        """
        Template-parity execute_query. Returns cursor for callers that want to fetch.

        If you want "fetchone"/"fetchall", you can easily add those.
        """
        try:
            self.debug("Executing query: %s", query)

            # Using a dedicated cursor per call is safer, but to stay close to your template
            # we reuse self.cursor.
            if params is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, params)

            return self.cursor

        except Exception as e:
            self._log.exception("Error executing query. query=%r error=%s", query, e)
            try:
                self.connection.rollback()
            except Exception:
                pass
            return None

    def get_db_message(self) -> Optional[str]:
        """
        Snowflake connectors don't expose "notices" like psycopg2.
        Provide a lightweight parity method: query ID / last status if available.
        """
        try:
            # cursor.sfqid is often available after execution
            sfqid = getattr(self.cursor, "sfqid", None)
            return sfqid
        except Exception:
            return None

    def set_isolation_level_autocommit(self) -> None:
        # Closest equivalent to Redshift autocommit behavior.
        self.connection.autocommit = True

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
