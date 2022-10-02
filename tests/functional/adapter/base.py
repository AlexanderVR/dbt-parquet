import os
import dbt.main
import pytest
import logging
import tempfile
import pathlib
import typing as tp
import yaml
import shutil
import subprocess
from dataclasses import dataclass, asdict
from dbt.adapters.parquet.relation import ParquetTable
import duckdb
import sys

logger = logging.getLogger(__name__)


def run_dbt(root_dir: tp.Union[str, pathlib.Path], args: tp.List[str]):
    if not args:
        args = ["run"]

    final_args = ["--profiles-dir", "."]

    if os.getenv("DBT_TEST_SINGLE_THREADED") in ("y", "Y", "1"):
        final_args.append("--single-threaded")

    final_args.extend(args)

    final_args.append("--log-cache-events")
    # dbt.main.handle_and_check
    logger.info("Invoking dbt with {}".format(final_args))
    try:
        subprocess.run(
            ["dbt"] + final_args, cwd=root_dir, capture_output=False, check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


@dataclass
class DbtProjectInfo:
    root_dir: pathlib.Path
    dbt_profile_config: dict
    dbt_project_config: dict

    def __str__(self):
        d = asdict(self)
        d["root_dir"] = str(d["root_dir"])
        return yaml.safe_dump(d)


class DBTIntegration:
    """
    Base class for running DBT integration tests against the Parquet adapter

    Test cases can override any of the @property functions to customize

    """

    @property
    def threads(self) -> int:
        return 1

    @property
    def models(self) -> str:
        return "models"

    @property
    def seeds(self) -> str:
        return "seeds"

    @property
    def test_paths(self) -> tp.List[str]:
        return []

    @property
    def schema(self) -> str:
        return "test_schema"

    @property
    def dbt_profile(self) -> str:
        return "test_parquet"

    @property
    def dbt_project_config_overrides(self):
        return {}

    @property
    def original_root_dir(self) -> pathlib.Path:
        class_file = sys.modules[self.__class__.__module__].__file__
        return pathlib.Path(class_file).parent

    @property
    def original_data_dir(self) -> pathlib.Path:
        return self.original_root_dir / "data"

    @pytest.fixture()
    def root_dir(self) -> tp.Iterator[pathlib.Path]:
        with tempfile.TemporaryDirectory(prefix="dbt-test-") as dir_name:
            d = pathlib.Path(dir_name)
            # d = pathlib.Path(__file__).parent / "test-runs"
            model_dir = self.original_root_dir / self.models
            if model_dir.exists():
                shutil.copytree(model_dir, d / self.models)
            seed_dir = self.original_root_dir / self.seeds
            if seed_dir.exists():
                shutil.copytree(seed_dir, d / self.seeds)
            data_dir = self.original_root_dir / "data"
            if data_dir.exists():
                shutil.copytree(data_dir, d / "data")
            yield d
        

    @pytest.fixture()
    def data_dir(self, root_dir: pathlib.Path) -> pathlib.Path:
        return root_dir / "data"

    @pytest.fixture()
    def dbt_profile_config(self, root_dir: pathlib.Path, data_dir: pathlib.Path):
        profile_config = {
            "type": "parquet",
            "threads": self.threads,
            "database": str(data_dir),
        }
        config = {
            "config": {"send_anonymous_usage_stats": False},
            self.dbt_profile: {"outputs": {"default": profile_config}},
        }
        profiles_path = root_dir / "profiles.yml"
        with open(profiles_path, "w") as fp:
            yaml.safe_dump(config, fp, indent=2)

        return dict(profile_config)

    @pytest.fixture()
    def dbt_project_config(self, root_dir: pathlib.Path):
        config = {
            "name": "test",
            "version": "1.0",
            "config-version": 2,
            "test-paths": self.test_paths,
            "model-paths": [self.models],
            "seed-paths": [self.seeds],
            "profile": self.dbt_profile,
            "models": {"+materialized": "table"},
        } | self.dbt_project_config_overrides

        with open(root_dir / "dbt_project.yml", "w") as fp:
            yaml.safe_dump(config, fp, indent=2)
        return dict(config)

    @pytest.fixture()
    def dbt_project(
        self, root_dir: pathlib.Path, dbt_project_config: dict, dbt_profile_config: dict
    ):
        logger.info(f"populated dbt test project dir for {self.__class__.__name__}")
        yield DbtProjectInfo(
            root_dir=root_dir,
            dbt_profile_config=dbt_profile_config,
            dbt_project_config=dbt_project_config,
        )
        logger.info(f"removed dbt test project dir for {self.__class__.__name__}")

    def test_run_dbt(self, dbt_project: DbtProjectInfo):
        success = run_dbt(dbt_project.root_dir, args=["run"])
        assert success, "dbt exit state did not match expected:\n%s" % dbt_project

