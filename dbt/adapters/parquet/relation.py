from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, ComponentName
import typing as tp
import pathlib


@dataclass
class ParquetTable:
    """
    Dataclass for describing how parquet data is organized by data_dir, schema, and table (file) name
    """

    data_dir: str
    schema: tp.Optional[str]
    table: tp.Optional[str]

    @property
    def root_path(self) -> pathlib.Path:
        return pathlib.Path(self.data_dir or "")

    @property
    def schema_path(self) -> pathlib.Path:
        return pathlib.Path(self.schema or "")

    @property
    def table_path(self) -> pathlib.Path:
        return pathlib.Path(self.table + ".parquet") if self.table else pathlib.Path()

    @property
    def full_path(self) -> pathlib.Path:
        return self.root_path / self.schema_path / self.table_path

    def __str__(self) -> str:
        return str(self.full_path)

    def tmp_view_name(self):
        if self.table:
            return f'"{str(self.schema_path / self.table_path)}"'
        return str(self)


@dataclass(frozen=True, eq=False, repr=False)
class ParquetRelation(BaseRelation):
    """
    The slightly tricky part is to make the `{{ ref() }}` and `{{ source() }}` templates:
    1) render correctly
    2) refer to proper duckdb identifiers

    This is an issue because `parquet_scan('...')` is a table-valued function, and hence not an identifier.

    To solve this, we provide three ways to address each relation:

    - the actual path -- defined by `.render_path()`. This is where the corresponding parquet file is located.
    - the rendered relation name -- this is defined by `.render()`
    - the table function `parquet_scan('{file_path}')` that can be interpreted by `duckdb`.
        This is defined by `.render_parquet_scan()`

    Each `dbt` materialization can resolve the rendered relation name by creating a temporary view
    from the `parquet_scan` table function  with the rendered relation name as the view name.
    See the `parquet__create_table_as` macro for details.

    """

    quote_character: str = ""

    @property
    def parquet_table(self) -> ParquetTable:
        d = {}
        for k, v in self._render_iterator():
            if k == ComponentName.Database:
                d["data_dir"] = v
            elif k == ComponentName.Schema:
                d["schema"] = v
            elif k == ComponentName.Identifier:
                d["table"] = v
        return ParquetTable(data_dir=d["data_dir"], schema=d.get("schema"), table=d.get("table"))

    def render_path(self) -> str:
        """
        Render the full path to database/schema/identifier.parquet file
        """
        return str(self.parquet_table)

    def render_parquet_scan(self) -> str:
        return f"parquet_scan('{self.render_path()}')"

    def render(self):
        """
        The rendered identifier name.

        This is currently the quoted relative path of the identifier, relative to the database location.
        """
        return self.parquet_table.tmp_view_name()
