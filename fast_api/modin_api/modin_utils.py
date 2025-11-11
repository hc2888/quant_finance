"""Modin-specific utility functions."""

import os
import shutil
from typing import Tuple, Type

import modin.pandas as mpd
import psycopg2
import ray
import sqlalchemy
from global_utils.develop_secrets import QUANT_PROJ_PATH
from global_utils.develop_vars import TOTAL_CPU_CORES, TOTAL_MEMORY
from global_utils.general_utils import BLUE_START, COLOR_END, GREEN_START

# ----------------------------------------------------------------------------------------------------------------------
# Number of `bytes` in a `GB`
BYTE_NUM: int = 1073741824
# Max amount of CPU cores Modin[Ray] is allowed to use
MAX_CORES: int = int(TOTAL_CPU_CORES * (7 / 8))
# Max amount of GB Memory Modin[Ray] is allowed to use
# `6 / 8` MOST STABLE & RECOMMENDED; because you're setting aside `2 / 8` of your computer RAM for NON-Ray processes
MAX_MEMORY: int = int(TOTAL_MEMORY * (6 / 8))
# Directory where all the Modin Ray logs are stored
TEMP_DIR_LOCAL: str = rf"{QUANT_PROJ_PATH}/temp_data/ray_metadata"
# Directory where Ray Plasma Objects are stored
RAY_PLASMA_DIR: str = rf"{QUANT_PROJ_PATH}/temp_data/ray_plasma"
# noinspection PyUnresolvedReferences
DB_CONN_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    psycopg2.OperationalError,
    sqlalchemy.exc.OperationalError,
    ray.exceptions.RayTaskError,
)
# Fast API Relative Path
FAST_API_RELATIVE_PATH: str = rf"fast_api/modin_api"

LOG_LINE_DIVIDE: str = r"---------------------------------------------------------------------------------------------"
LOG_LINE_DIVIDE: str = BLUE_START + LOG_LINE_DIVIDE + COLOR_END

# ----------------------------------------------------------------------------------------------------------------------
"""MODIN NOTES."""
# NOTE: AVOID using following Modin Pandas methods; too many issues.
# NOTE: Most issues arise due to distributed shuffling, scheduler overhead, or unsupported edge cases.
# `.drop_duplicates()`
# `.groupby()`
# `.apply()`
# `.rolling()`
# `.agg()`


# ----------------------------------------------------------------------------------------------------------------------
def activate_ray(num_cpu_cores: int = MAX_CORES, memory_gb: int = MAX_MEMORY) -> None:
    """Create Modin[Ray] instance for multiprocessing Pandas operations.

    :param num_cpu_cores: int: Number of CPU cores to utilize for multiprocessing.
    :param memory_gb: int: Amount of memory in `GB` to use for Ray instance.
    """
    import math

    print(LOG_LINE_DIVIDE)
    print(r"DELETING RAY METADATA LOGS")
    # TypeError: nt._path_exists() takes no keyword arguments
    if os.path.exists(TEMP_DIR_LOCAL):
        shutil.rmtree(path=TEMP_DIR_LOCAL)
    print(r"CREATING RAY PLASMA DIRECTORY")
    os.makedirs(name=RAY_PLASMA_DIR, exist_ok=True)

    if num_cpu_cores > MAX_CORES:
        num_cpu_cores: int = MAX_CORES
    if memory_gb > MAX_MEMORY:
        memory_gb: int = MAX_MEMORY

    print(GREEN_START + rf"CREATING RAY INSTANCE: TOTAL MEMORY GB: {memory_gb}" + COLOR_END)
    heap_memory: int = int(memory_gb * (7 / 8))
    print(GREEN_START + rf"RAY HEAP MEMORY GB: {heap_memory}" + COLOR_END)
    store_memory: int = int(memory_gb * (1 / 8))
    print(GREEN_START + rf"RAY OBJECT STORE MEMORY GB: {store_memory}" + COLOR_END)

    os.environ["RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"]: str = "1"

    # NOTE: Create a Ray Instance; https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html
    # NOTE: Use below CMD in Git Bash when Ray does not start; wait 60 seconds for the CMD to finish executing.
    # NOTE: ray stop --force --verbose --grace-period 0
    ray.init(
        _memory=int(BYTE_NUM * heap_memory),
        object_store_memory=int(BYTE_NUM * store_memory),
        system_reserved_memory=int((BYTE_NUM * memory_gb) / 64),
        num_cpus=num_cpu_cores,
        num_gpus=0,
        system_reserved_cpu=math.ceil(TOTAL_CPU_CORES / 64),
        _temp_dir=TEMP_DIR_LOCAL,
        _plasma_directory=RAY_PLASMA_DIR,
        object_spilling_directory=RAY_PLASMA_DIR,
        enable_resource_isolation=True,
        _cgroup_path=RAY_PLASMA_DIR,
        include_dashboard=False,
    )
    print(GREEN_START + rf"RAY INSTANCE INITIALIZED: NUM_CPU_CORES: {num_cpu_cores}" + COLOR_END)


# ----------------------------------------------------------------------------------------------------------------------
def shutdown_ray() -> None:
    """Properly shutdown Modin[Ray]."""

    # noinspection PyProtectedMember,PyUnresolvedReferences
    print(GREEN_START + rf"RAY AVAILABLE RESOURCES: {ray.available_resources()}" + COLOR_END)
    # Properly shutdown Ray instance
    print(r"SHUTTING DOWN RAY")
    ray.shutdown()
    print(r"RAY SUCCESSFULLY SHUT DOWN")
    print(r"DELETING RAY PLASMA DIRECTORY")
    shutil.rmtree(path=RAY_PLASMA_DIR)
    print(r"RAY PLASMA DIRECTORY DELETED")
    print(LOG_LINE_DIVIDE)


# ----------------------------------------------------------------------------------------------------------------------
def clean_ray_memory() -> None:
    """Completely clean out Modin[Ray] memory usage."""
    import gc

    # noinspection PyProtectedMember
    from ray._private.internal_api import global_gc, memory_summary

    gc.collect()
    global_gc()
    print(memory_summary(stats_only=True))


# ----------------------------------------------------------------------------------------------------------------------
def execute_subprocess(sub_folder_path: str, target_file_name: str) -> None:
    """Executes subprocess on target file.

    :param sub_folder_path: str: Sub-folder path to target file.
    :param target_file_name: str: Name of the target Python file to execute.
    """
    import subprocess

    subprocess.run(args=["python", rf"{QUANT_PROJ_PATH}/{sub_folder_path}/{target_file_name}.py"], check=True)


# ----------------------------------------------------------------------------------------------------------------------
def verify_no_null_rows(target_df: mpd.DataFrame) -> None:
    """Verify that the DataFrame has NO NULL values in it.

    :param target_df: mpd.DataFrame: Target DataFrame.
    """
    # Check for any rows that have at least one null value
    # noinspection PyTypeChecker
    has_null_rows: bool = target_df.isnull().any(axis=1).any()
    if has_null_rows:
        raise ValueError("ERROR: ONE OR MORE ROWS IN `Modin DataFrame` CONTAINS `NULL` VALUES. ABORTING SQL INSERT.")
    else:
        data_rows: str = "{:,}".format(len(target_df))
        print(BLUE_START + rf"NO NULL VALUES DETECTED: INSERTING SQL DATA: NUMBER OF ROWS: {data_rows}" + COLOR_END)
