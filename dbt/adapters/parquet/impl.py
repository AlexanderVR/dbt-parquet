import os
import subprocess
import typing as tp

import agate
import duckdb
import fs.base
import fs.errors

import dbt.exceptions
from . import util
from .column import ParquetColumn
from .relation import ParquetRelation
from dbt.adapters.base import available
from dbt.adapters.base import BaseAdapter
from dbt.adapters.base import BaseRelation
from dbt.adapters.base import RelationType
from dbt.adapters.parquet import ParquetConnectionManager
from dbt.events import AdapterLogger

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

    def get_conn_handle(self) -> duckdb.DuckDBPyConnection:
        """
        Get a handle to the in-memory duckdb connection
        """
        handle = self.connections.get_thread_connection().handle
        return handle.db

    def get_fs_handle(self) -> fs.base.FS:
        """
        Get a handle to the root PyFilesystem directory that acts as our "database"
        """
        handle = self.connections.get_thread_connection().handle
        return handle.fs

    def drop_relation(self, relation: ParquetRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema)  # type: ignore[arg-type]
        if is_cached:
            self.cache_dropped(relation)

        path = relation.render_path()
        try:
            self.get_fs_handle().remove(path)
        except fs.errors.ResourceNotFound:
            pass

    def truncate_relation(self, relation: ParquetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            "`truncate` is not implemented for this adapter!"
        )

    def rename_relation(
        self, from_relation: ParquetRelation, to_relation: ParquetRelation
    ) -> None:
        if from_relation.render() == to_relation.render():
            return
        self.cache_renamed(from_relation, to_relation)
        self.get_fs_handle().move(
            from_relation.render_resource_path(), to_relation.render_resource_path()
        )
        self.execute(f"drop view if exists {from_relation.render()}")
        self.execute(to_relation.register_as_view_cmd())

    @available
    def list_schemas(self, database: str) -> tp.List[str]:
        # assume there is only ever one database
        root_dir = self.get_fs_handle()
        return util.list_schemas_from_fs(root_dir)

    @available.parse_none
    def check_schema_exists(self, database: str, schema: str) -> bool:
        return schema in self.list_schemas(database)

    def get_columns_in_relation(self, relation: ParquetRelation) -> tp.List[ParquetColumn]:
        conn = self.get_conn_handle()
        if not os.path.exists(relation.render_path()):
            return []
        q = f"select * from {relation.render_parquet_scan()} LIMIT 0"
        as_arrow = conn.execute(q).arrow()
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
        root_dir = self.get_fs_handle()
        pq = schema_relation.include(database=True, schema=True, identifier=False).parquet_table
        schema = pq.schema
        relations = util.list_relations_from_fs(root_dir, schema_relation.database, schema)
        # if schema:
        #     self.execute(f"create schema if not exists {schema}")

        for relation in relations:
            self.execute(relation.register_as_view_cmd())
        return relations

    def create_schema(self, relation: ParquetRelation) -> None:
        schema_path = relation.include(identifier=False).render_resource_path()
        try:
            root_dir = self.get_fs_handle()
            root_dir.makedir(schema_path)
        except fs.errors.DirectoryExists:
            pass
        self.execute(f"create schema if not exists {relation.schema}")
        self.cache.add_schema(relation.database, relation.schema)

    def drop_schema(self, relation: ParquetRelation) -> None:
        database = relation.database
        schema = relation.schema or "/"
        root_dir = self.get_fs_handle()
        try:
            root_dir.removetree(schema)
        except fs.errors.ResourceNotFound:
            pass
        self.execute(f"drop schema if exists {relation.schema} cascade")
        self.cache.drop_schema(database, schema)

    @available
    def load_dataframe(self, database, schema, table_name, agate_table):
        conn = self.get_conn_handle()
        csv_file = agate_table.original_abspath
        rel = ParquetRelation.create(database, schema, table_name)
        conn.execute(
            f"""
            copy
                (select * from read_csv_auto('{csv_file}'))
            to '{rel.render_path()}' (format 'parquet');
            """
        )
        conn.execute(rel.register_as_view_cmd())

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

    def _register_view_cmds(self) -> tp.List[str]:
        """
        Return the set of sql commands needed to register all the parquet files as
        views in duckdb.
        """
        cmds = []
        db = self.config.credentials.database
        for schema in self.list_schemas(db):
            if schema:
                cmds.append(f"create schema if not exists {schema}")
            relations = tp.cast(tp.List[ParquetRelation], self.list_relations(db, schema))
            for rel in relations:
                cmds.append(rel.register_as_view_cmd())
        return cmds

    @available
    def duckdb(self):
        """
        Run the duckdb CLI with each parquet files having a corresponding view

        Usage: `dbt run-operation duckdb`
        """
        cmds = ["install parquet", "load parquet"]
        cmds.extend(self._register_view_cmds())
        cmds.append(
            """
            select
                table_schema as schema,
                table_name as view
            from information_schema.tables
            order by all
            """
        )
        try:
            subprocess.run(["duckdb", "--version"]).check_returncode()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "duckdb cli not installed. See instructions at https://duckdb.org/ or use a package manager like homebrew"
            ) from e
        subprocess.run(["duckdb", "-cmd", "; ".join(cmds)])

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: tp.Optional[tp.List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: tp.List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(tp.cast(ParquetRelation, relation_a))
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(tp.cast(ParquetRelation, relation_a)),
            relation_b=str(tp.cast(ParquetRelation, relation_b)),
            except_op=except_operator,
        )

        return sql


# Change `table_a/b` to `table_aaaaa/bbbbb` to avoid duckdb binding issues when relation_a/b
# is called "table_a" or "table_b" in some of the dbt tests
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_aaaaa as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_bbbbb as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_aaaaa.num_rows - table_bbbbb.num_rows as difference
    from table_aaaaa, table_bbbbb
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
""".strip()
