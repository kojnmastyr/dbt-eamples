import base64
import datetime
import os
import sys

if sys.version_info < (3, 9):
    from functools import lru_cache

    cache = lru_cache(maxsize=None)
else:
    from functools import cache

import pytz
import re
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from time import sleep

from typing import Optional, Tuple, Union, Any, List, Iterable, TYPE_CHECKING

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
import requests
import snowflake.connector
import snowflake.connector.constants
import snowflake.connector.errors
from snowflake.connector.errors import (
    Error,
    DatabaseError,
    InternalError,
    InternalServerError,
    ServiceUnavailableError,
    GatewayTimeoutError,
    RequestTimeoutError,
    BadGatewayError,
    OtherHTTPRetryableError,
    BindUploadError,
)

from dbt_common.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
    DbtConfigError,
)
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.record import get_record_mode_from_env, RecorderMode
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.events.functions import warn_or_error
from dbt.adapters.events.types import AdapterEventWarning, AdapterEventError
from dbt_common.ui import line_wrap_message, warning_tag
from dbt.adapters.snowflake.record import SnowflakeRecordReplayHandle

from dbt.adapters.snowflake.auth import private_key_from_file, private_key_from_string

if TYPE_CHECKING:
    import agate


logger = AdapterLogger("Snowflake")

if os.getenv("DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING"):
    for logger_name in ["snowflake.connector", "botocore", "boto3"]:
        logger.debug(f"Setting {logger_name} to DEBUG")
        logger.set_adapter_dependency_log_level(logger_name, "DEBUG")

_TOKEN_REQUEST_URL = "https://{}.snowflakecomputing.com/oauth/token-request"

ERROR_REDACTION_PATTERNS = {
    re.compile(r"Row Values: \[(.|\n)*\]"): "Row Values: [redacted]",
    re.compile(r"Duplicate field key '(.|\n)*'"): "Duplicate field key '[redacted]'",
}


@cache
def snowflake_private_key(private_key: RSAPrivateKey) -> bytes:
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_token(username: str, password: str) -> str:
    """
    Fetches OAuth Token needed during Snowflake Connection
    """
    account_identifier = "<account_identifier>"  # Replace appropriately
    oauth_url = f"https://{account_identifier}.snowflakecomputing.com/oauth/token-request"
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'scope': 'SESSION:ROLE-ANY',
        'client_id': account_identifier
    }
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    try:
        response = requests.post(oauth_url, data=data, headers=headers, verify=False)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.RequestException as e:
        raise FailedToConnectError(f"Failed to get OAuth token: {e}")


@dataclass
class SnowflakeCredentials(Credentials):
    account: str
    user: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None
    authenticator: Optional[str] = "oauth"
    token: Optional[str] = None
    # other existing fields...

    def _get_access_token(self) -> str:
        # Your custom token generation logic based on username and password
        if not self.user or not self.password:
            raise FailedToConnectError("Username and password are required to fetch an OAuth token.")
        return get_token(self.user, self.password)

    def auth_args(self):
        result = {}
        if self.user:
            result["user"] = self.user

        if self.authenticator == "oauth":
            if not self.token:
                # Fetch token using your custom method
                self.token = self._get_access_token()
            result["token"] = self.token
            result["authenticator"] = self.authenticator

        # Additional auth arguments, if needed...
        return result


class SnowflakeConnectionManager(SQLConnectionManager):
    TYPE = "snowflake"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)

            # A class of Snowflake errors -- such as a failure from attempting to merge
            # duplicate rows -- includes row values in the error message, i.e.
            # [12345, "col_a_value", "col_b_value", etc...]. We don't want to log potentially
            # sensitive user data.
            for regex_pattern, replacement_message in ERROR_REDACTION_PATTERNS.items():
                msg = re.sub(regex_pattern, replacement_message, msg)

            logger.debug("Snowflake query id: {}".format(e.sfqid))
            logger.debug("Snowflake error: {}".format(msg))

            if "Empty SQL statement" in msg:
                logger.debug("got empty sql statement, moving on")
            elif "This session does not have a current database" in msg:
                raise FailedToConnectError(
                    (
                        "{}\n\nThis error sometimes occurs when invalid "
                        "credentials are provided, or when your default role "
                        "does not have access to use the specified database. "
                        "Please double check your profile and try again."
                    ).format(msg)
                )
            else:
                raise DbtDatabaseError(msg)
        except Exception as e:
            if isinstance(e, snowflake.connector.errors.Error):
                logger.debug("Snowflake query id: {}".format(e.sfqid))

            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, DbtRuntimeError):
                raise
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        timeout = creds.connect_timeout

        def connect():
            session_parameters = {}

            if creds.query_tag:
                session_parameters.update({"QUERY_TAG": creds.query_tag})
            handle = None

            try:
                # Attempt to fetch a token if authenticator is set to OAuth
                if creds.authenticator == "oauth":
                    creds.token = creds._get_access_token()

                handle = snowflake.connector.connect(
                    account=creds.account,
                    database=creds.database,
                    schema=creds.schema,
                    warehouse=creds.warehouse,
                    role=creds.role,
                    autocommit=True,
                    client_session_keep_alive=creds.client_session_keep_alive,
                    application="dbt",
                    insecure_mode=creds.insecure_mode,
                    session_parameters=session_parameters,
                    **creds.auth_args(),  # This includes token-based auth
                )
            except FailedToConnectError as e:
                if "authentication has expired" in str(e).lower():
                    logger.debug("Refreshing OAuth token due to expiration.")
                    creds.token = creds._get_access_token()
                    handle = snowflake.connector.connect(
                        account=creds.account,
                        database=creds.database,
                        schema=creds.schema,
                        warehouse=creds.warehouse,
                        role=creds.role,
                        autocommit=True,
                        client_session_keep_alive=creds.client_session_keep_alive,
                        application="dbt",
                        insecure_mode=creds.insecure_mode,
                        session_parameters=session_parameters,
                        **creds.auth_args(),
                    )
                else:
                    raise e

            return handle

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [
            InternalError,
            InternalServerError,
            ServiceUnavailableError,
            GatewayTimeoutError,
            RequestTimeoutError,
            BadGatewayError,
            OtherHTTPRetryableError,
            BindUploadError,
        ]
        # these two options are for backwards compatibility
        if creds.retry_all:
            retryable_exceptions = [Error]
        elif creds.retry_on_database_errors:
            retryable_exceptions.insert(0, DatabaseError)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=creds.connect_retries,
            retry_timeout=timeout if timeout is not None else exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        handle = connection.handle
        sid = handle.session_id

        connection_name = connection.name

        sql = "select system$cancel_all_queries({})".format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        code = cursor.sqlstate

        if code is None:
            code = "SUCCESS"
        query_id = str(cursor.sfqid) if cursor.sfqid is not None else None
        return AdapterResponse(
            _message="{} {}".format(code, cursor.rowcount),
            rows_affected=cursor.rowcount,
            code=code,
            query_id=query_id,
        )

    # disable transactional logic by default on Snowflake
    # except for DML statements where explicitly defined
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @classmethod
    def _split_queries(cls, sql):
        "Splits sql statements at semicolons into discrete queries"

        sql_s = str(sql)
        sql_buf = StringIO(sql_s)
        split_query = snowflake.connector.util_text.split_statements(sql_buf)
        return [part[0] for part in split_query]

    @staticmethod
    def _fix_rows(rows: Iterable[Iterable]) -> Iterable[Iterable]:
        # See note in process_results().
        for row in rows:
            fixed_row = []
            for col in row:
                if isinstance(col, datetime.datetime) and col.tzinfo:
                    offset = col.utcoffset()
                    assert offset is not None
                    offset_seconds = offset.total_seconds()
                    new_timezone = pytz.FixedOffset(int(offset_seconds // 60))
                    col = col.astimezone(tz=new_timezone)
                fixed_row.append(col)

            yield fixed_row

    @classmethod
    def process_results(cls, column_names, rows):
        # Override for Snowflake. The datetime objects returned by
        # snowflake-connector-python are not pickleable, so we need
        # to replace them with sane timezones.
        return super().process_results(column_names, cls._fix_rows(rows))

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        # don't apply the query comment here
        # it will be applied after ';' queries are split
        from dbt_common.clients.agate_helper import empty_table

        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return response, table

    def add_standard_query(self, sql: str, **kwargs) -> Tuple[Connection, Any]:
        # This is the happy path for a single query. Snowflake has a few odd behaviors that
        # require preprocessing within the 'add_query' method below.
        return super().add_query(self._add_query_comment(sql), **kwargs)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        if bindings:
            # The snowflake connector is stricter than, e.g., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        stripped_queries = self._stripped_queries(sql)

        if set(query.lower() for query in stripped_queries).issubset({"begin;", "commit;"}):
            connection, cursor = self._add_begin_commit_only_queries(
                stripped_queries,
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
            )
        else:
            connection, cursor = self._add_standard_queries(
                stripped_queries,
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
            )

        if cursor is None:
            self._raise_cursor_not_found_error(sql)

        return connection, cursor

    def _stripped_queries(self, sql: str) -> List[str]:
        def strip_query(query):
            """
            hack -- after the last ';', remove comments and don't run
            empty queries. this avoids using exceptions as flow control,
            and also allows us to return the status of the last cursor
            """
            without_comments_re = re.compile(
                r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE
            )
            return re.sub(without_comments_re, "", query).strip()

        return [query for query in self._split_queries(sql) if strip_query(query) != ""]

    def _add_begin_commit_only_queries(
        self, queries: List[str], **kwargs
    ) -> Tuple[Connection, Any]:
        # if all we get is `begin;` and/or `commit;`
        # raise a warning, then run as standard queries to avoid an error downstream
        message = (
            "Explicit transactional logic should be used only to wrap "
            "DML logic (MERGE, DELETE, UPDATE, etc). The keywords BEGIN; and COMMIT; should "
            "be placed directly before and after your DML statement, rather than in separate "
            "statement calls or run_query() macros."
        )
        logger.warning(line_wrap_message(warning_tag(message)))

        for query in queries:
            connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _add_standard_queries(self, queries: List[str], **kwargs) -> Tuple[Connection, Any]:
        for query in queries:
            if query.lower() == "begin;":
                super().add_begin_query()
            elif query.lower() == "commit;":
                super().add_commit_query()
            else:
                connection, cursor = self.add_standard_query(query, **kwargs)
        return connection, cursor

    def _raise_cursor_not_found_error(self, sql: str):
        conn = self.get_thread_connection()
        try:
            conn_name = conn.name
        except AttributeError:
            conn_name = None

        raise DbtRuntimeError(
            f"""Tried to run an empty query on model '{conn_name or "<None>"}'. If you are """
            f"""conditionally running\nsql, e.g. in a model hook, make """
            f"""sure your `else` clause contains valid sql!\n\n"""
            f"""Provided SQL:\n{sql}"""
        )

    def release(self):
        if self.profile.credentials.reuse_connections:
            return
        super().release()

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        assert isinstance(type_code, int)
        return snowflake.connector.constants.FIELD_ID_TO_NAME[type_code]
