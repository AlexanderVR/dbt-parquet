import pytest
import pathlib

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]

# all testing data goes here
_TESTING_DATA_LOC = pathlib.Path(__file__).parent / "data"


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    _TESTING_DATA_LOC.mkdir(exist_ok=True)

    return {
        "type": "parquet",
        "threads": 4,
        "database": str(_TESTING_DATA_LOC),
    }
