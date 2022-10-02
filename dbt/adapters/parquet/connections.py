from dataclasses import dataclass
from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.base import BaseConnectionManager
from dbt.clients import agate_helper
import dbt.exceptions
from dbt.contracts.connection import Connection, ConnectionState, AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger
import duckdb
import typing as tp
import agate
import pathlib
import os


@dataclass
class ParquetCredentials(Credentials):
    database: str = os.getcwd()
    schema: str = ""

    @property
    def type(self):
        return "parquet"

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        return ("database", "schema")

    def __post_init__(self):
        if not pathlib.Path(self.database).is_absolute():
            self.database = str(pathlib.Path.cwd() / self.database)


class ParquetConnectionManager(BaseConnectionManager):
    TYPE = "parquet"

    @classmethod
    def open(cls, connection: Connection):

        if connection.state == ConnectionState.OPEN:
            return connection

        connection.handle = duckdb.connect()
        connection.state = ConnectionState.OPEN

        return connection

    def cancel(self, connection: Connection):
        pass

    def cancel_open(self) -> None:
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.RuntimeException:
            raise
        except RuntimeError as e:
            logger.debug("duckdb error: {}".format(str(e)))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.RuntimeException(str(exc)) from exc

    @staticmethod
    def get_table_from_response(resp: duckdb.DuckDBPyConnection):
        column_names = [field[0] for field in resp.description]  # type: ignore
        return agate_helper.table_from_rows(resp.fetchall(), column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> tp.Tuple[AdapterResponse, agate.Table]:
        conn: duckdb.DuckDBPyConnection = self.get_thread_connection().handle

        r = conn.execute(sql)
        if fetch:
            table = self.get_table_from_response(r)
        else:
            table = agate_helper.empty_table()

        message = "OK"
        response = AdapterResponse(_message=message)
        return response, table
