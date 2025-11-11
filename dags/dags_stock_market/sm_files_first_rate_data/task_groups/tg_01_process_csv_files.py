"""Processes CSV files from First Rate Data."""

from typing import Dict, List

from airflow.sdk import task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="gather_symbols", trigger_rule=DEFAULT_TRIGGER)
def gather_symbols() -> None:
    """Gather the symbols that need to be processed."""
    import pandas as pd
    from dags_stock_market.const_vars import MODEL_PREP_API_SYMBOLS_LIST
    from global_utils.develop_vars import TEMP_DATA_PATH
    from global_utils.general_utils import json_write
    from global_utils.sql_utils import SQL_ENGINE

    etf_query: str = "SELECT DISTINCT symbol FROM model_prep_api.etf_non_adj_daily"
    etf_df: pd.DataFrame = pd.read_sql(sql=etf_query, con=SQL_ENGINE)
    etf_list: List[str] = list(etf_df["symbol"])

    index_query: str = "SELECT DISTINCT symbol FROM model_prep_api.index_daily"
    index_df: pd.DataFrame = pd.read_sql(sql=index_query, con=SQL_ENGINE)
    index_list: List[str] = list(index_df["symbol"])

    symbol_list: List[str] = etf_list + index_list
    missing_dict: Dict[str, List[str]] = {
        "symbols": [symbol for symbol in MODEL_PREP_API_SYMBOLS_LIST if symbol not in symbol_list]
    }
    json_write(python_dict=missing_dict, file_path=TEMP_DATA_PATH, file_name="gather_symbols")


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="process_csv_files", trigger_rule=DEFAULT_TRIGGER)
def process_csv_files() -> None:
    """Invoke Modin API to process CSV files."""
    import requests
    from global_utils.develop_vars import DOCKER_IP, MODIN_FASTAPI_PORT
    from requests.models import Response

    sub_api_name: str = "sm_files_first_rate_data"
    sub_api_target_func: str = "process_csv_files"

    target_api_route: str = rf"{sub_api_name}/{sub_api_target_func}/"
    http_response: Response = requests.post(url=rf"http://{DOCKER_IP}:{MODIN_FASTAPI_PORT}/{target_api_route}")
    http_response.raise_for_status()


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="process_csv_files")
def tg_01_process_csv_files() -> None:
    """Task Group for the entire FRED data-related process."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    """MAIN FLOW"""
    # noinspection PyUnresolvedReferences,PyStatementEffect,PyTypeChecker
    (gather_symbols() >> process_csv_files() >> tg_finish)
