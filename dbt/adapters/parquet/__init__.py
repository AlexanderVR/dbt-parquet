from dbt.adapters.base import AdapterPlugin
from dbt.adapters.parquet.connections import ParquetConnectionManager  # noqa
from dbt.adapters.parquet.connections import ParquetCredentials
from dbt.adapters.parquet.impl import ParquetAdapter
from dbt.include import parquet


Plugin = AdapterPlugin(
    adapter=ParquetAdapter,  # type: ignore
    credentials=ParquetCredentials,
    include_path=parquet.PACKAGE_PATH,
)
