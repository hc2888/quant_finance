"""Functions used for various things."""

import inspect
import urllib.error
from http.client import IncompleteRead, RemoteDisconnected
from inspect import Signature
from types import FrameType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

import numpy as np
from global_utils.develop_vars import TOTAL_CPU_CORES
from requests.exceptions import RequestException

# ----------------------------------------------------------------------------------------------------------------------
URL_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    RequestException,
    IncompleteRead,
    urllib.error.URLError,
    RemoteDisconnected,
)


# ----------------------------------------------------------------------------------------------------------------------
# Used for `print()` and `logging()` color-coded messaging
# Do NOT use `r` formatter on these string variables
GREEN_START: str = "\033[92m"
BLUE_START: str = "\033[94m"
RED_START: str = "\033[91m"
COLOR_END: str = "\033[0m"


# ----------------------------------------------------------------------------------------------------------------------
def json_read(file_name_path: str) -> Dict[str, Any]:
    """Convert a JSON file to a Python dictionary.

    :param file_name_path: str: Path to file including the file name.

    :return: Dict[str, Any]: Python dictionary object.
    """
    import json

    with open(file=file_name_path, mode="r", encoding="utf-8") as json_file:
        python_dict: Dict[str, Any] = json.load(fp=json_file)

    return python_dict


# ----------------------------------------------------------------------------------------------------------------------
def json_write(python_dict: Dict[str, Any], file_path: str, file_name: str) -> None:
    """Convert a Python dictionary to a JSON file.

    :param python_dict: Dict[str, Any]: Python dictionary object.
    :param file_path: str: Path to file.
    :param file_name: str: Name of file.
    """
    import json
    import os

    # Ensure the directory exists
    os.makedirs(name=file_path, exist_ok=True)

    with open(file=rf"{file_path}/{file_name}.json", mode="w", encoding="utf-8") as json_file:
        # noinspection PyTypeChecker
        json.dump(obj=python_dict, fp=json_file, ensure_ascii=False, indent=4)


# ----------------------------------------------------------------------------------------------------------------------
def time_pause(num_seconds: int = 10) -> None:
    """Execute time pauses in code execution.

    :param num_seconds: int: Number of seconds to pause the code execution.
    """
    import time

    print(rf"EXECUTING TIME PAUSE FOR {num_seconds} SECONDS")
    num: int
    for num in reversed(range(num_seconds)):
        print(rf"{num + 1}")
        time.sleep(1)


# ----------------------------------------------------------------------------------------------------------------------
def kill_all_sql_connections() -> None:
    """Kill all active SQL connections."""

    from global_utils.develop_secrets import SQL_DATABASE_NAME
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    sql_query: str = rf"""
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = '{SQL_DATABASE_NAME}' AND pid <> pg_backend_pid()
    """
    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(sql_query))

    print(rf"ALL SQL CONNECTIONS TERMINATED")


# ----------------------------------------------------------------------------------------------------------------------
def create_exec_timestamp_str(future_days: int = 0) -> str:
    """Create a timestamp in `%Y-%m-%d %H:%M:%S` format.

    :param future_days: int: How many days in the future you want the timestamp to be. `0` == CURRENT timestamp.
    :return: str: timestamp as a string in `%Y-%m-%d %H:%M:%S` format.
    """
    import datetime as dt

    import pytz

    # Define the timezone
    timezone_var: dt.tzinfo = pytz.timezone(zone="America/New_York")
    # Get the current time in the specified timezone
    produced_datetime: dt.datetime = dt.datetime.now(tz=timezone_var) + dt.timedelta(days=future_days)
    # Format the date to `%Y-%m-%d %H:%M:%S` format
    exec_timestamp: str = produced_datetime.strftime("%Y-%m-%d %H:%M:%S")  # type: ignore[call-arg]

    return exec_timestamp


# ----------------------------------------------------------------------------------------------------------------------
def create_unique_nomen() -> str:
    """Generate a unique 9-character UUID string."""
    import uuid

    unique_nomen: str = str(uuid.uuid4()).replace("-", "")[:9]

    return unique_nomen


# ----------------------------------------------------------------------------------------------------------------------
def calc_cpu_cores_per_task(
    num_max_parallel_dynamic_tasks: int,
    num_cpu_cores: int = int(TOTAL_CPU_CORES * (15 / 16)),
) -> int:
    """Returns CPU count for set of dynamic Airflow Tasks.

    :param num_max_parallel_dynamic_tasks: int: Number of max parallel dynamic Airflow
        tasks happening at any point in the DAG.
    :param num_cpu_cores: int: Number of CPU cores to utilize for multiprocessing.

    :return: int: The CPU count for set of dynamic Airflow Tasks.
    """
    import math

    # Invoke error if `TOTAL_CPU_CORES` is LESS THAN `num_max_parallel_dynamic_tasks`
    total_cores: str = rf"Total number of physical CPU cores available for use ({num_cpu_cores})"
    max_tasks: str = rf"max parallel dynamic tasks in DAG ({num_max_parallel_dynamic_tasks})."
    if num_cpu_cores < num_max_parallel_dynamic_tasks:
        raise ValueError(rf"{total_cores} must be GREATER THAN {max_tasks}")

    cpu_cores_per_task: int = math.floor(num_cpu_cores / num_max_parallel_dynamic_tasks)

    return cpu_cores_per_task


# ----------------------------------------------------------------------------------------------------------------------
def distribute_files_evenly(data_list: List[str], num_lists: int) -> List[List[str]]:
    """Distribute items in `data_list` evenly into `num_lists` number of lists.

    :param data_list: List[str]: List of all the file names.
    :param num_lists: int: Number of lists that need to be created.

    :return: List[List[str]]: A list of lists with file names evenly distributed.
    """
    result_list: List[List[str]] = [[] for _ in range(num_lists)]

    index_num: int
    item_str: str
    for index_num, item_str in enumerate(data_list):
        result_list[index_num % num_lists].append(item_str)

    # Filter out any empty lists
    result_list: List[List[str]] = [list_item for list_item in result_list if list_item]

    return result_list


# ----------------------------------------------------------------------------------------------------------------------
def list_type_files(directory_path: str, file_type: str = "csv") -> List[str]:
    """Lists all CSV files in the specified directory.

    :param directory_path: str: The path of the directory to search in.
    :param file_type: str: The file type to target.

    :return: List[str]: List of strings where each string is the name of a `.csv` file found in the directory.
    """
    import os

    # List all files in the specified directory
    files_in_directory: List[str] = os.listdir(path=directory_path)

    # Filter out all files that do not end with the `file_type`
    file_list: List[str] = [file for file in files_in_directory if file.endswith(rf".{file_type}")]

    return file_list


# ----------------------------------------------------------------------------------------------------------------------
def verify_no_null_values(target_func: Callable[..., Any]) -> bool:
    """Make sure there are no `None` and / or `np.nan` values among the target function's input parameters.

    :param target_func: Callable[..., Any]: Target function to check.
    :returns: bool: Whether any of the function's input parameter values are `None` and / or `np.nan`.
    """
    target_func_signature: Signature = inspect.signature(obj=target_func)
    target_func_frame: Optional[FrameType] = inspect.currentframe()

    no_null_values_exist: bool = False
    if all(
        [
            target_func_frame is not None,
            target_func_frame.f_back is not None,
        ]
    ):
        # Capture the calling frame's local variables (where the function was called)
        caller_locals: Dict[str, Any] = target_func_frame.f_back.f_locals
        param_names: List[str] = list(target_func_signature.parameters)
        param_values: Dict[str, Any] = {name: caller_locals.get(name, None) for name in param_names}
        # `np.isnan()` does NOT take keyword arguments
        boolean_list: List[bool] = [
            value is not None and (not (isinstance(value, (float, int, np.floating, np.integer)) and np.isnan(value)))
            for value in param_values.values()
        ]
        no_null_values_exist: bool = all(boolean_list)

    return no_null_values_exist
