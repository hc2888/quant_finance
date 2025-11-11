"""List of all the tests to pass into `run_tests_airflow`."""

from typing import Callable, List

from test_air_env.test_files.postgres_maintenance import postgres_maintenance_tests

# ----------------------------------------------------------------------------------------------------------------------
total_tests_air_env: List[Callable[[], None]] = []
total_tests_air_env += postgres_maintenance_tests
