"""Test Airflow Task functions for target DAG."""

from typing import Callable, List

from tests_airflow_utils import run_import_statements

SOURCE_DAG: str = "dags_air_env.dag_air_env_postgres_maintenance"


# ----------------------------------------------------------------------------------------------------------------------
def test_cleanup_queries() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import cleanup_queries

    run_import_statements(
        py_func=cleanup_queries,
        target_source_str=rf"{SOURCE_DAG} import cleanup_queries",
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_user_tables_distro() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import user_tables_distro

    run_import_statements(
        py_func=user_tables_distro,
        target_source_str=rf"{SOURCE_DAG} import user_tables_distro",
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_vacuum_full_tables() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import vacuum_full_tables

    run_import_statements(
        py_func=vacuum_full_tables,
        target_source_str=rf"{SOURCE_DAG} import vacuum_full_tables",
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_reindex_tables() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import reindex_tables

    run_import_statements(
        py_func=reindex_tables,
        target_source_str=rf"{SOURCE_DAG} import reindex_tables",
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_analyze_tables() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import analyze_tables

    run_import_statements(
        py_func=analyze_tables,
        target_source_str=rf"{SOURCE_DAG} import analyze_tables",
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_system_tables() -> None:
    """Compile the task function to ensure function-level import statements are correct."""
    from dags_air_env.dag_air_env_postgres_maintenance import system_tables

    run_import_statements(
        py_func=system_tables,
        target_source_str=rf"{SOURCE_DAG} import system_tables",
    )


# ----------------------------------------------------------------------------------------------------------------------
postgres_maintenance_tests: List[Callable[[], None]] = [
    test_cleanup_queries,
    test_user_tables_distro,
    test_vacuum_full_tables,
    test_reindex_tables,
    test_analyze_tables,
    test_system_tables,
]
