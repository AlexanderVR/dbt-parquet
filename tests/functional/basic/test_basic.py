import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relations_equal,
)


class TestSimpleMaterializationsParquet(BaseSimpleMaterializations):
    def test_base(self, project):
        """
        Override existing test_base to:
          - remove calls to check_relation_types, as everything is a table
          - remove incremental materialization test, since these are not supported
        """

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        if project.test_config.get("require_full_refresh", False):  # required for BigQuery
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

    pass


class TestSingularTestsParquet(BaseSingularTests):
    pass

@pytest.mark.skip("parquet files are not ephemeral")
class TestSingularTestsEphemeralParquet(BaseSingularTestsEphemeral):
    pass


class TestEmptyParquet(BaseEmpty):
    pass


@pytest.mark.skip("parquet files are not ephemeral")
class TestEphemeralParquet(BaseEphemeral):
    pass

@pytest.mark.skip("parquet files do not support incremental materializations")
class TestIncrementalParquet(BaseIncremental):
    pass


class TestGenericTestsParquet(BaseGenericTests):
    pass

@pytest.mark.skip("parquet files do not support snapshots")
class TestSnapshotCheckColsParquet(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip("parquet files do not support snapshots")
class TestSnapshotTimestampParquet(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodParquet(BaseAdapterMethod):
    pass
