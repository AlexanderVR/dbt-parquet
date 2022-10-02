# `dbt-parquet`

Run [dbt](https://www.getdbt.com/) on a directory of Parquet files, with [duckdb](https://duckdb.org/) as the computation engine.

## Installation

This repo is still in development. Until it's on Pypi, you can install with
`pip3 install git+https://github.com/AlexanderVR/dbt-parquet.git#egg=dbt-parquet`

## Usage

Use `dbt init` to create a new project with this adapter.

Or manually add to `~/.dbt/profiles.yml` something like

```yaml
jaffle_shop:
  target: dev
  outputs:
    default:
      type: parquet
      threads: 4
      database: ./data
```

Note that the `database` option indicates the path that we will store the Parquet files.

Data is assumed to be laid out as follows:

- `{database}/{table_name}.parquet` if no schema is provided
- `{database}/{schema}/{table_name}.parquet` otherwise

See [dbt/adapters/parquet/relation.py](dbt/adapters/parquet/relation.py) for details.

## Why

- `dbt` provides solid DAG-based abstractions for managing collections of related data transformations.
- I don't always need a costly data warehouse for my data problems. Have very successfully used [dbt-duckdb](https://github.com/jwills/dbt-duckdb) and [dbt-sqlite](https://github.com/codeforkjeff/dbt-sqlite)
- When data resides elsewhere, loading it into `duckdb` or `sqlite` just to run `dbt`, then exporting the desired output tables, is not ideal. E.g. when refreshing only parts of the `dbt` graph.

More generally, thinking in terms of each `dbt` "model" as generating a data "asset", which can have a wide variety of metadata and be an input to other computations (`dbt` or otherwise), can lead in very fruitful directions as illustrated by the [dagster + dbt integration](https://dagster.io/blog/dagster-0-15-0-dbt-python).

The hope with `dbt-parquet` is that by breaking out assets from a monolithic data "warehouse" or database file, the semantics become as clean and portable as, well, the humble file.

## Current deficiencies

- Note that only table materializations are supported, as views do not make sense with parquet files.
- With the `httpfs` extension, `duckdb` can run queries over files stored in S3. The necessary changes to `dbt-parquet` would involve abstracting out any calls in involving the file path (e.g. listing, removal, rename, creation, and the `get_catalog` macro) to work against S3.
- For "huge" data, might be nice to support partitioned files.

## Acknowledgements

Inspired by [dbt-duckdb](https://github.com/jwills/dbt-duckdb), [dagster](https://dagster.io) and of course [duckdb](https://duckdb.org/)
