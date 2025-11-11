"""Executes PostGres Maintenance."""

from typing import List, Sequence, Tuple, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "air_env_postgres_maintenance"


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="lib_version_sql", trigger_rule=DEFAULT_TRIGGER)
def lib_version_sql() -> None:
    """Generates a SQL table with the inherent packages from the Airflow Docker image."""
    import sys
    from typing import Dict

    import altair
    import filelock
    import fsspec
    import jedi
    import jinja2
    import mlflow
    import networkx
    import numpy
    import opentelemetry.exporter.otlp.proto.common as otlp_proto_common
    import pandas
    import platformdirs
    import psycopg2
    import pyarrow
    import requests_toolbelt
    import setuptools
    import sqlalchemy
    import sympy
    import tomlkit
    import tqdm
    import urllib3
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE
    from PIL import Image

    # noinspection PyUnresolvedReferences
    libraries_dict: Dict[str, str] = {
        "python_version": sys.version,
        "numpy": numpy.__version__,
        "pandas": pandas.__version__,
        "psycopg2": psycopg2.__version__,
        "pyarrow": pyarrow.__version__,
        "sqlalchemy": sqlalchemy.__version__,
        "mlflow": mlflow.__version__,
        "urllib3": urllib3.__version__,
        "platformdirs": platformdirs.__version__,
        "requests_toolbelt": requests_toolbelt.__version__,
        "tomlkit": tomlkit.__version__,
        "tqdm": tqdm.__version__,
        "jedi": jedi.__version__,
        "altair": altair.__version__,
        "filelock": filelock.__version__,
        "sympy": sympy.__version__,
        "networkx": networkx.__version__,
        "jinja2": jinja2.__version__,
        "fsspec": fsspec.__version__,
        "pillow": Image.__version__,  # Access pillow version via PIL
        "setuptools": setuptools.__version__,
        "opentelemetry_exporter_otlp_proto_common": otlp_proto_common.__version__,
    }

    libraries_df: pandas.DataFrame = pandas.DataFrame(
        data=list(libraries_dict.items()),
        columns=["packages", "version"],
    )

    libraries_df.to_sql(
        schema="global",
        name="lib_versions",
        con=SQL_ENGINE,
        if_exists="replace",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(libraries_df.columns)),
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="user_tables_distro", trigger_rule=DEFAULT_TRIGGER)
def user_tables_distro(cpu_cores_per_task: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Create temp distro tables.

    :param cpu_cores_per_task: int: max distributed CPU cores allowed for the DAG.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE
    from sqlalchemy import text
    from sqlalchemy.engine.row import Row

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        # NOTE: This query also gets SQL tables that are NOT partitioned / indexed.
        sql_query: str = r"""
        SELECT DISTINCT
        schemaname, tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND tablename NOT IN (SELECT table_name FROM global.tables_roster)
        ORDER BY schemaname ASC, tablename ASC
        """
        sql_result: Sequence[Row] = sql_connection.execute(text(sql_query)).fetchall()
        tables_list: List[Tuple[str, str]] = [(row[0], row[1]) for row in sql_result]
        print(rf"TOTAL TABLES: {len(tables_list)}")
        split_num: int = min(len(tables_list), cpu_cores_per_task)
        context["ti"].xcom_push(key="split_num", value=split_num)
        print(rf"TOTAL SPLIT NUM COUNT: {split_num}")

        # Splits out the list of all table names to evenly distributed lists.
        # Turns them into Pandas Dataframes and creates temp SQL tables for each Dataframe.
        group_num: int
        for group_num in range(split_num):
            temp_list: List[Tuple[str, str]] = tables_list[group_num::cpu_cores_per_task]
            temp_df: pd.DataFrame = pd.DataFrame(data=temp_list, columns=["schema_name", "table_name"])
            temp_df.to_sql(
                schema="temp_tables",
                name=f"sm_infra_table_{group_num}",
                con=sql_connection,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
            )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="vacuum_full_tables", trigger_rule=DEFAULT_TRIGGER)
def vacuum_full_tables(group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Execute `VACUUM FULL` Postgres SQL queries.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    # Use this value to determine if this task will be activated or not
    split_num: int = context["ti"].xcom_pull(
        task_ids="user_tables_distro",
        key="split_num",
        map_indexes=[None],
    )[0]
    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""SELECT * FROM temp_tables.sm_infra_table_{group_num}"""
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            tables_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_connection)
            # noinspection PyTypeChecker
            tables_list: List[Tuple[str, str]] = list(tables_df.itertuples(index=False, name=None))

            schema_name: str
            table_name: str
            for schema_name, table_name in tables_list:
                print(rf"EXECUTING {schema_name}.{table_name}")
                sql_connection.execute(text(rf"VACUUM FULL {schema_name}.{table_name}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="reindex_tables", trigger_rule=DEFAULT_TRIGGER)
def reindex_tables(group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Execute `REINDEX TABLE` Postgres SQL queries.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    # Use this value to determine if this task will be activated or not
    split_num: int = context["ti"].xcom_pull(
        task_ids="user_tables_distro",
        key="split_num",
        map_indexes=[None],
    )[0]
    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""SELECT * FROM temp_tables.sm_infra_table_{group_num}"""
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            tables_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_connection)
            # noinspection PyTypeChecker
            tables_list: List[Tuple[str, str]] = list(tables_df.itertuples(index=False, name=None))

            schema_name: str
            table_name: str
            for schema_name, table_name in tables_list:
                print(rf"EXECUTING {schema_name}.{table_name}")
                sql_connection.execute(text(rf"REINDEX TABLE {schema_name}.{table_name}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="analyze_tables", trigger_rule=DEFAULT_TRIGGER)
def analyze_tables(group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Execute `ANALYZE` Postgres SQL queries.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    # Use this value to determine if this task will be activated or not
    split_num: int = context["ti"].xcom_pull(
        task_ids="user_tables_distro",
        key="split_num",
        map_indexes=[None],
    )[0]
    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""SELECT * FROM temp_tables.sm_infra_table_{group_num}"""
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            tables_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_connection)
            # noinspection PyTypeChecker
            tables_list: List[Tuple[str, str]] = list(tables_df.itertuples(index=False, name=None))

            schema_name: str
            table_name: str
            for schema_name, table_name in tables_list:
                print(rf"EXECUTING {schema_name}.{table_name}")
                sql_connection.execute(text(rf"ANALYZE {schema_name}.{table_name}"))

            sql_connection.execute(text(rf"DROP TABLE temp_tables.sm_infra_table_{group_num}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="system_tables", trigger_rule=DEFAULT_TRIGGER)
def system_tables() -> None:
    """Executes maintenance-related SQL queries on all Postgres system-related tables."""
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text
    from sqlalchemy.engine.row import Row

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = r"""
        SELECT DISTINCT
        schemaname, tablename
        FROM pg_tables
        WHERE schemaname IN ('pg_catalog', 'information_schema')
        """
        sql_result: Sequence[Row] = sql_connection.execute(text(sql_query)).fetchall()
        system_list: List[Tuple[str, str]] = [(row[0], row[1]) for row in sql_result]

        schema_name: str
        table_name: str
        for schema_name, table_name in system_list:
            print(rf"VACUUM FULL {schema_name}.{table_name}")
            sql_connection.execute(text(rf"VACUUM FULL {schema_name}.{table_name}"))
        for schema_name, table_name in system_list:
            print(rf"REINDEX TABLE {schema_name}.{table_name}")
            sql_connection.execute(text(rf"REINDEX TABLE {schema_name}.{table_name}"))
        for schema_name, table_name in system_list:
            print(rf"ANALYZE {schema_name}.{table_name}")
            sql_connection.execute(text(rf"ANALYZE {schema_name}.{table_name}"))


# ----------------------------------------------------------------------------------------------------------------------
def cleanup_queries() -> None:
    """Drop and re-create the `temp_tables` schema."""
    from global_utils.sql_utils import SQL_ENGINE, SQL_PARTITION_CORES
    from sqlalchemy import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(r"DROP SCHEMA IF EXISTS temp_tables CASCADE"))
        sql_connection.execute(text(r"CREATE SCHEMA IF NOT EXISTS temp_tables"))
        sql_connection.execute(text(rf"VACUUM (PARALLEL {SQL_PARTITION_CORES})"))


@task(task_id="cleanup_queries_1", trigger_rule=DEFAULT_TRIGGER)
def cleanup_queries_1() -> None:
    """Drop and re-create the `temp_tables` schema."""
    cleanup_queries()


@task(task_id="cleanup_queries_2", trigger_rule=DEFAULT_TRIGGER)
def cleanup_queries_2() -> None:
    """Drop and re-create the `temp_tables` schema."""
    cleanup_queries()


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["air_env"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from global_utils.general_utils import calc_cpu_cores_per_task

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
    num_max_parallel_dynamic_tasks: int = 1
    cpu_cores_per_task: int = calc_cpu_cores_per_task(num_max_parallel_dynamic_tasks=num_max_parallel_dynamic_tasks)

    distro_range_list: List[int] = [num for num in range(cpu_cores_per_task)]
    vacuum_full_tables_tasks: PythonOperator = vacuum_full_tables.expand(group_num=distro_range_list)
    reindex_tables_tasks: PythonOperator = reindex_tables.expand(group_num=distro_range_list)
    analyze_tables_tasks: PythonOperator = analyze_tables.expand(group_num=distro_range_list)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> lib_version_sql()
        >> cleanup_queries_1()
        >> user_tables_distro(cpu_cores_per_task=cpu_cores_per_task)
        >> vacuum_full_tables_tasks
        >> reindex_tables_tasks
        >> analyze_tables_tasks
        >> cleanup_queries_2()
        >> system_tables()
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
