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


class TestSimpleMaterializationsParquet(BaseSimpleMaterializations):
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
