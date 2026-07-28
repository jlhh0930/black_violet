# mongo_utils.py
from __future__ import annotations

from typing import Any, Optional

from pymongo import MongoClient

from utils import AWSUtils


class MongoUtils:
    """
    Functionally similar to RedshiftUtils, but for MongoDB.

    Note: there is no server-side "statement_timeout" like Postgres.
    Singleton per (database, host).
    """

    _instances: dict[tuple[str, str], "MongoUtils"] = {}

    def __new__(
        cls,
        config: Any,
        logger: Any,
        *,
        new_conn: bool = False,
    ):
        secret = AWSUtils.get_secret(config.secret_arn, config.region_name, logger)
        host = secret.get("host")
        key = (config.dbname, host)

        if new_conn or key not in cls._instances:
            inst = super().__new__(cls)

            # Support either "uri" in secret, or build from host/port/user/password
            mongo_uri = secret.get("uri")
            if not mongo_uri:
                user = secret.get("user")
                password = secret.get("password")
                port = secret.get("port", 27017)

                # Build minimal URI; for complex auth options, prefer secret["uri"]
                if user and password:
                    mongo_uri = f"mongodb://{user}:{password}@{host}:{port}/"
                else:
                    mongo_uri = f"mongodb://{host}:{port}/"

            try:
                logger.info("Connecting to MongoDB DB=%s at %s", config.dbname, host)
                connection = MongoClient(mongo_uri)
                # "cursor" parity: use db reference as "cursor"
                db = connection[config.dbname]

                # Version check parity
                server_info = connection.admin.command("buildInfo")
                logger.info("Connection established: %s", server_info.get("version", "unknown"))

                inst._init_state(connection=connection, cursor=db, dbname=config.dbname, logger=logger, secret=secret)

            except Exception:
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
        self.cursor = cursor  # db handle
        self.dbname = dbname
        self._log = logger
        self.rs_secret_string = secret

    def __init__(self, *args, **kwargs):
        pass

    def debug(self, msg: str) -> None:
        if hasattr(self._log, "debug"):
            self._log.debug(msg)

    def execute_query(self, query: Any, params: Optional[dict] = None) -> object:
        """
        Mongo doesn't have a universal "execute SQL string" API. To stay
        functionally similar, accept a callable/lambda that receives the db.

        Example:
            utils.execute_query(lambda db: db.col.find_one({"x": 1}))
        """
        try:
            if callable(query):
                return query(self.cursor)
            raise ValueError("MongoUtils.execute_query expects a callable like lambda db: ...")
        except Exception as e:
            self._log.exception("Error executing query. error=%s", e)
            return None

    def get_db_message(self) -> Optional[str]:
        try:
            return self.connection.admin.command("ping")  # returns {"ok": 1.0}
        except Exception:
            return None

    def set_isolation_level_autocommit(self) -> None:
        # Mongo uses different transaction semantics; no-op.
        return

    def close(self) -> None:
        try:
            if getattr(self, "connection", None):
                self.connection.close()
        except Exception:
            pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
