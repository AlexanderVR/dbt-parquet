from tests.functional.adapter.base import DBTIntegration, run_dbt, DbtProjectInfo


class TestSingleTransform(DBTIntegration):

    @property
    def schema(self) -> str:
        return "source_schema"


    def test_run_dbt(self, dbt_project: DbtProjectInfo):
        success = run_dbt(dbt_project.root_dir, args=["run"])
        assert success, "dbt exit state did not match expected:\n%s" % dbt_project