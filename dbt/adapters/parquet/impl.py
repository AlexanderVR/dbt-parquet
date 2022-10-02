from dbt.adapters.base import BaseAdapter, RelationType, available
from dbt.adapters.parquet import ParquetConnectionManager
from dbt.events import AdapterLogger
import dbt.exceptions
from pathlib import Path
from .relation import ParquetRelation
from .column import ParquetColumn
import os
import typing as tp
import agate
import subprocess

logger = AdapterLogger("Parquet")


class ParquetAdapter(BaseAdapter):
    ConnectionManager = ParquetConnectionManager

    RELATION_TYPES = {
        "TABLE": RelationType.Table,
        "VIEW": RelationType.View,
    }

    connections: ParquetConnectionManager

    Relation = ParquetRelation
    Column = ParquetColumn

    @classmethod
    def date_function(cls):
        return "now()"

    @classmethod
    def is_cancelable(cls):
        return False

    def drop_relation(self, relation: ParquetRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema)  # type: ignore[arg-type]
        if is_cached:
            self.cache_dropped(relation)

        path = relation.render_path()
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def truncate_relation(self, relation: ParquetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            "`truncate` is not implemented for this adapter!"
        )

    def rename_relation(
        self, from_relation: ParquetRelation, to_relation: ParquetRelation
    ) -> None:

        self.cache_renamed(from_relation, to_relation)
        os.rename(from_relation.render_path(), to_relation.render_path())

    @available
    def list_schemas(self, database: str) -> tp.List[str]:
        return sorted(os.listdir(database))

    @available.parse_none
    def check_schema_exists(self, database: str, schema: str) -> bool:
        return schema in self.list_schemas(database)

    def get_columns_in_relation(self, relation: ParquetRelation) -> tp.List[ParquetColumn]:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        q = f"select * from {relation.render_parquet_scan()} LIMIT 0"
        as_arrow = client.execute(q).arrow()
        return [ParquetColumn(column=f.name, dtype=f.type) for f in as_arrow.schema]

    def expand_column_types(self, goal: ParquetRelation, current: ParquetRelation) -> None:  # type: ignore[override]
        # This is a no-op
        pass

    def expand_target_column_types(
        self, from_relation: ParquetRelation, to_relation: ParquetRelation
    ) -> None:
        # This is a no-op
        pass

    @available.parse_list
    def list_relations_without_caching(
        self, schema_relation: ParquetRelation
    ) -> tp.List[ParquetRelation]:

        relations = []
        for schema in os.listdir(
            schema_relation.include(database=True, schema=False, identifier=False).render_path()
        ):
            identifier = schema.split(".parquet")[0]

            relation = ParquetRelation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=identifier,
                type=RelationType.Table,
            )
            relations.append(relation)
        return relations

    def create_schema(self, relation: ParquetRelation) -> None:
        schema_path = relation.include(identifier=False).render_path()
        logger.debug("Creating schema {} in cwd: {}.", schema_path, os.getcwd())
        try:
            os.mkdir(schema_path)
        except FileExistsError:
            pass

    def drop_schema(self, relation: ParquetRelation) -> None:
        database = relation.database
        schema = relation.schema
        schema_path = relation.include(identifier=False).render_path()
        logger.debug("Dropping schema {} in cwd: {}.", schema_path, os.getcwd())
        try:
            for p in Path(schema_path).glob("*.parquet"):
                os.remove(p)
            os.rmdir(schema_path)
        except FileNotFoundError:
            pass
        self.cache.drop_schema(database, schema)

    @available
    def load_dataframe(self, database, schema, table_name, agate_table):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        csv_file = agate_table.original_abspath
        rel = ParquetRelation.create(database, schema, table_name)
        client.execute(
            f"""
            copy
                (select * from read_csv_auto('{csv_file}'))
            to '{rel.render_path()}' (format 'parquet');
            """
        )

    def run_sql_for_tests(self, sql, fetch, conn=None):
        """
        For testing only
        Run an SQL query on the Parquet adapter.
        """

        do_fetch = fetch != "None"
        _, res = self.execute(sql, fetch=do_fetch)

        # convert dataframe to matrix-ish repr
        if fetch == "one":
            return res[0]
        else:
            return list(res)

    @classmethod
    def quote(cls, identifier: str) -> str:
        return '"{}"'.format(identifier)

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "float64" if decimals else "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @available
    def duckdb(self):
        cmds = ["install parquet", "load parquet"]
        db = self.config.credentials.database
        for schema in self.list_schemas(db):
            cmds.append(f"create schema {schema}")
            for rel in self.list_relations(db, schema):
                view_name = rel.identifier
                cmds.append(
                    f"""
                    create view {schema}.{view_name} as
                        select * from {rel.render()}
                    """
                )
        cmds.append(
            """
            select
                table_schema as schema,
                table_name as view
            from information_schema.tables
            order by all
            """
        )
        subprocess.run(["duckdb", "-cmd", "; ".join(cmds)])
