"""Variables & Functions related to Airflow processes."""

from datetime import datetime, timedelta
from typing import Any, Dict, Union

import pytz
from airflow.models import TaskInstance
from airflow.sdk import Context, task
from sqlalchemy.sql import text

# ----------------------------------------------------------------------------------------------------------------------
DEFAULT_ARGS: Dict[str, Any] = {
    "start_date": datetime(
        year=1900,
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=pytz.timezone(zone="America/New_York"),
    ),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "provide_context": True,
}
DEFAULT_TRIGGER: str = "none_failed_min_one_success"


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="store_dag_info", trigger_rule=DEFAULT_TRIGGER)
def store_dag_info(**context: Union[Context, TaskInstance]) -> None:
    """Stores relevant info about the DAG & the particular DAG run.

    :param context: Union[Context, Dict[str, str], TaskInstance]: set of variables that are
        inherent in every Airflow Task function that can be pulled and used.
    """
    import datetime as dt
    import re

    import pytz
    from global_utils.general_utils import create_unique_nomen

    # Define the timezone
    timezone_var: dt.tzinfo = pytz.timezone(zone="America/New_York")
    # Get the current time in the specified timezone
    now_in_timezone: dt.datetime = dt.datetime.now(tz=timezone_var)
    exec_timestamp: str = now_in_timezone.strftime("%Y-%m-%d %H:%M:%S")  # type: ignore[call-arg]
    # noinspection PyArgumentList
    # create Xcom message of the `exec_timestamp` to pass on to other tasks
    context["ti"].xcom_push(key="exec_timestamp", value=exec_timestamp)

    # noinspection PyArgumentList
    # create Xcom message of the `unique_nomen` to pass on to other tasks
    context["ti"].xcom_push(key="unique_nomen", value=create_unique_nomen())

    # get the run_id for this DAG run
    job_run_id: str = context["dag_run"].run_id
    # define the regex expression; replace all non-alphanumeric characters
    pattern: str = r"[^0-9a-zA-Z\s]+"
    # perform a regex substitution to clean the job_run_id string
    cleaned_job_run_id: str = re.sub(string=job_run_id, repl="_", pattern=pattern)
    # noinspection PyArgumentList
    # create Xcom message of the job_run_id to pass on to other tasks
    context["ti"].xcom_push(key="job_run_id", value=cleaned_job_run_id)


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="drop_temp_tables", trigger_rule=DEFAULT_TRIGGER)
def drop_temp_tables(temp_table_prefix: str, drop_tables: bool = True, **context: Union[Context, TaskInstance]) -> None:
    """Drops any SQL tables that were created on a temporary basis.

    :param temp_table_prefix: str: prefix specific to DAG.
    :param drop_tables: bool: whether to drop the SQL table or not.
    :param context: Union[Context, Dict[str, str], TaskInstance]: set of variables that are
        inherent in every Airflow Task function that can be pulled and used.
    """
    from typing import List

    import pandas as pd
    from global_utils.develop_vars import DROP_ALL_TEMP
    from global_utils.sql_utils import SQL_ENGINE, TEMP_SCHEMA

    dag_run_id: str = context["dag_run"].run_id
    print(dag_run_id)

    if DROP_ALL_TEMP or drop_tables:
        drop_tables_sql_query: str = (
            rf"""
            SELECT 'DROP TABLE IF EXISTS {TEMP_SCHEMA}.' || tablename || ' CASCADE' AS sql_tables
            FROM pg_tables
            WHERE schemaname = '{TEMP_SCHEMA}'
            AND tablename LIKE '{temp_table_prefix}_%'
            """.replace(
                "%", "%%"
            )
        )

        drop_tables_df: pd.DataFrame = pd.read_sql(sql=drop_tables_sql_query, con=SQL_ENGINE)
        drop_tables_list: List[str] = list(drop_tables_df["sql_tables"])

        if drop_tables_list:
            with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
                sql_command: str
                for sql_command in drop_tables_list:
                    sql_connection.execute(text(rf"""{sql_command}"""))
