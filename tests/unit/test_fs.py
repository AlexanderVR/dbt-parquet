from dbt.tests.util import get_connection
from .util import config_from_parts_or_dicts
from dbt.adapters.parquet.impl import ParquetAdapter
from dbt.adapters.parquet.relation import ParquetRelation
import pytest
import tempfile

@pytest.fixture()
def adapter():
    with tempfile.TemporaryDirectory() as tmpdir:
        profile_cfg = {
            'outputs': {
                'test': {
                    "type": "parquet",
                    "threads": 1,
                    "database": tmpdir,
                },
            },
            'target': 'test',
        }

        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            },
            'query-comment': 'dbt',
            'config-version': 2,
        }
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        a = ParquetAdapter(config)
        with get_connection(a):
            yield a 

def test_schema_add_drop(adapter):
    schema = "my_schema"
    as_relation = ParquetRelation.create(schema=schema)
    db = adapter.config.credentials.database
    adapter.create_schema(as_relation)
    
    assert adapter.list_schemas(db) == ['', schema]
