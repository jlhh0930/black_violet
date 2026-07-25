import snowflake.connector

from utils import AWSUtils


class SnowflakeUtils(object):
    _instance = None
    _logger = None

    def __new__(cls,
                secret_arn,
                region_name,
                log,
                # Optional overrides (if you prefer to keep some values out of the secret)
                account=None,
                warehouse=None,
                database_name=None,
                schema_name=None,
                role=None,
                new_conn=False,
                statement_timeout_seconds=None,
                autocommit=False):
        if cls._instance is None or getattr(cls._instance, "dbname", None) != database_name or new_conn:
            cls._instance = object.__new__(cls)
            log.debug("New Snowflake Connection")

            secret = AWSUtils.get_secret_string(secret_arn, region_name) or {}

            # Recommended secret keys:
            # - account (e.g., "xy12345" or full account locator as supported by your org)
            # - user
            # - password
            # - role (optional)
            # - warehouse/database/schema (optional)
            #
            # Allow parameters to override secret values.
            sf_account = account or secret.get("account")
            sf_user = secret.get("user")
            sf_password = secret.get("password")

            sf_role = role or secret.get("role")
            sf_warehouse = warehouse or secret.get("warehouse")
            sf_database = database_name or secret.get("database")
            sf_schema = schema_name or secret.get("schema")

            missing = [k for k, v in {
                "account": sf_account,
                "user": sf_user,
                "password": sf_password,
            }.items() if not v]
            if missing:
                raise ValueError(f"Missing required Snowflake secret fields: {', '.join(missing)}")

            connector_kwargs = {
                "account": sf_account,
                "user": sf_user,
                "password": sf_password,

                # These are optional but good to set if you know defaults.
                "warehouse": sf_warehouse,
                "database": sf_database,
                "schema": sf_schema,

                # Set connector-level default behavior.
                "autocommit": autocommit,
            }

            # Only include optional keys if they’re not None.
            if sf_role:
                connector_kwargs["role"] = sf_role
            if connector_kwargs.get("warehouse") is None:
                connector_kwargs.pop("warehouse", None)
            if connector_kwargs.get("database") is None:
                connector_kwargs.pop("database", None)
            if connector_kwargs.get("schema") is None:
                connector_kwargs.pop("schema", None)

            try:
                SnowflakeUtils._logger = log
                log.info(f"Connecting to Snowflake database ... {sf_database}")

                connection = snowflake.connector.connect(**connector_kwargs)
                cursor = connection.cursor()

                # Optional per-session statement timeout.
                if statement_timeout_seconds is not None:
                    cursor.execute(
                        f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(statement_timeout_seconds)}"
                    )

                cursor.execute("SELECT CURRENT_VERSION()")
                _ = cursor.fetchone()

                cls._instance.rs_secret_string = secret
                cls._instance.connection = connection
                cls._instance.cursor = cursor

                # Store the “dbname” notion to keep your Redshift singleton behavior similar.
                cls._instance.dbname = sf_database
                cls._instance.warehouse = sf_warehouse
                cls._instance.schema_name = sf_schema
                cls._instance.role = sf_role

            except Exception as e:
                log.info(f"Error connecting to Snowflake database ... {e}")
                SnowflakeUtils._instance = None

        return cls._instance

    def __init__(self):
        self.rs_secret_string = self._instance.rs_secret_string
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.dbname = self._instance.dbname
        self.log = self._logger

    def query(self, sql_query):
        """
        Executes SQL. For SELECT-like statements, returns fetched rows.
        For non-result statements (INSERT/UPDATE/DDL), returns None.
        """
        try:
            self.log.debug(sql_query)
            self.cursor.execute(sql_query)

            # If the statement produced a resultset, cursor.description is not None.
            if getattr(self.cursor, "description", None):
                return self.cursor.fetchall()
            return None
        except Exception as e:
            self.log.error(f'Error executing query "{sql_query}", error: {e}')
            return None

    def get_db_message(self):
        # Snowflake connector doesn't have a direct equivalent to psycopg2 notices.
        return None

    def set_isolation_level_autocommit(self, autocommit=True):
        # Snowflake transactions: autocommit is a connector/session setting.
        try:
            self.connection.autocommit(autocommit)
        except Exception as e:
            self.log.error(f"Error setting autocommit={autocommit}: {e}")

    def __del__(self):
        try:
            if hasattr(self, "cursor") and self.cursor is not None:
                self.cursor.close()
        except Exception:
            pass
        try:
            if hasattr(self, "connection") and self.connection is not None:
                self.connection.close()
        except Exception:
            pass
