import os
import pathlib
import threading
import typing as tp
from contextlib import contextmanager
from dataclasses import dataclass

import agate
import duckdb
import fs.base
import fs.osfs

import dbt.exceptions
from . import util
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.base import Credentials
from dbt.clients import agate_helper
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class ParquetCredentials(Credentials):
    database: str
    schema: str = ""

    @property
    def type(self):
        return "parquet"

    def create_fs_interface(self) -> fs.base.FS:
        if os.path.exists(self.database):
            db_folder = fs.osfs.OSFS(self.database)
            return db_folder
        raise FileNotFoundError(f"folder {self.database} not found")

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        return ("database", "schema")

    def __post_init__(self):
        if not pathlib.Path(self.database).is_absolute():
            db_path = pathlib.Path.cwd() / self.database
        else:
            db_path = pathlib.Path(self.database)
        self.database = str(db_path.resolve())


@dataclass
class ParquetHandle:
    db: duckdb.DuckDBPyConnection
    fs: fs.base.FS

    def close(self):
        self.db.close()


class ParquetConnectionManager(BaseConnectionManager):
    TYPE = "parquet"
    FS: tp.Optional[fs.base.FS] = None
    CONN: tp.Optional[duckdb.DuckDBPyConnection] = None
    LOCK = threading.RLock()
    CONNECTION_COUNT = 0

    @classmethod
    def open(cls, connection: Connection):

        if connection.state == ConnectionState.OPEN:
            return connection
        if cls.FS is None:
            cls.FS = connection.credentials.create_fs_interface()

        with cls.LOCK:
            if cls.CONN is None:
                cls.CONN = duckdb.connect()

                # first connection -- initialize all the existing tables
                for schema in util.list_schemas_from_fs(cls.FS):
                    if schema:
                        cls.CONN.execute(f"create schema if not exists {schema}")

                    relations = util.list_relations_from_fs(
                        cls.FS, connection.credentials.database, schema
                    )
                    for relation in relations:
                        cls.CONN.execute(relation.register_as_view_cmd())

        connection.handle = ParquetHandle(db=cls.CONN.cursor(), fs=cls.FS)
        cls.CONNECTION_COUNT += 1
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        connection = super().close(connection)
        if connection.state == ConnectionState.CLOSED:
            with cls.LOCK:
                cls.CONNECTION_COUNT -= 1
                if cls.CONNECTION_COUNT == 0 and cls.CONN:
                    # close the filesystem to ensure writes
                    # we keep the in-memory duckdb connection alive so that we don't need to reload metadata
                    if cls.FS is not None:
                        cls.FS.close()
                        cls.FS = None

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
    def exception_handler(self, sql: str, connection_name="main"):
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
        cur: duckdb.DuckDBPyConnection = self.get_thread_connection().handle.db
        try:
            r = cur.execute(sql)
            if fetch:
                table = self.get_table_from_response(r)
            else:
                table = agate_helper.empty_table()

        except Exception as e:
            raise dbt.exceptions.RuntimeException(str(e))

        message = "OK"
        response = AdapterResponse(_message=message)
        return response, table
