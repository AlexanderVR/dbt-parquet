from dataclasses import dataclass
import typing as tp

import pyarrow
from dbt.adapters.base.column import Column

Self = tp.TypeVar("Self", bound="ParquetColumn")


@dataclass
class ParquetColumn(Column):
    TYPE_LABELS: tp.ClassVar[tp.Dict[str, str]] = {
        "STRING": "TEXT",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT",
        "INTEGER": "INT",
    }
    column: str
    dtype: pyarrow.DataType

    @property
    def quoted(self):
        return '"{}"'.format(self.column)

    def literal(self, value):
        return "cast({} as {})".format(value, self.data_type)

    def is_string(self) -> bool:
        return pyarrow.types.is_string(self.dtype)

    def is_integer(self) -> bool:
        return pyarrow.types.is_integer(self.dtype)

    def is_numeric(self) -> bool:
        return pyarrow.types.is_decimal(self.dtype)

    def is_float(self):
        return pyarrow.types.is_floating(self.dtype)

    def __repr__(self) -> str:
        return "<ParquetColumn {} ({})>".format(self.name, self.data_type)

    @property
    def data_type(self) -> str:
        return str(self.dtype)
