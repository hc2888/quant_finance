"""Test functions for the Airflow DAGs Python files to ensure all import statements are successful."""

import logging
import warnings
from typing import Callable, List

# Suppress Airflow POSIX RuntimeWarnings
warnings.filterwarnings(
    action="ignore",
    category=RuntimeWarning,
    message="Airflow currently can be run on POSIX-compliant Operating Systems.*",
)
# Mute noisy Airflow logging
logging.getLogger(name="airflow").setLevel(level=logging.ERROR)
logging.getLogger(name="airflow.models.dagbag").setLevel(level=logging.ERROR)


# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
    """Launch each function in a separate process."""
    from test_air_env.total_tests import total_tests_air_env
    from tests_airflow_utils import multi_execute_funcs, test_dag_parsing

    test_dag_parsing()  # Execute DagBag first
    test_functions: List[Callable[[], None]] = []
    test_functions += test_functions + total_tests_air_env

    multi_execute_funcs(process_functions=test_functions)


if __name__ == "__main__":
    main()
