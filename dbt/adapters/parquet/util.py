import typing as tp

import fs.base

from .relation import ParquetRelation
from dbt.adapters.base import RelationType


def list_schemas_from_fs(root_dir: fs.base.FS):
    """
    List all the schemas in the filesystem.

    This is just all subfolders plus the default (empty) schema
    """
    folders = root_dir.filterdir("/", exclude_files=["*"])

    # include the empty, i.e. default schema in the response
    return [""] + sorted([d.name for d in folders])


def list_relations_from_fs(
    root_dir: fs.base.FS, database: tp.Optional[str], schema: str
) -> tp.List[ParquetRelation]:
    """
    List all relations within a "schema" subfolder.

    This is all parquet files under the subfolder.
    File 'blah.parquet' is mapped to identifier "blah".
    """

    subdir = "/" + schema if schema else "/"
    if not root_dir.exists(subdir):
        return []

    relations = []
    for item in root_dir.filterdir(subdir, files=["*.parquet"], exclude_dirs=["*"]):
        relation = ParquetRelation.create(
            database=database,
            schema=schema,
            identifier=item.name[: -len(".parquet")],
            type=RelationType.Table,
        )
        relations.append(relation)
    return relations
