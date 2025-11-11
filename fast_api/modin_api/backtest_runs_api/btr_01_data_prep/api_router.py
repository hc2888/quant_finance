"""DAG-specific API Functions."""

from fastapi import APIRouter
from modin_api.backtest_runs_api.const_vars import API_NAME
from modin_api.modin_utils import FAST_API_RELATIVE_PATH, execute_subprocess

# ----------------------------------------------------------------------------------------------------------------------
router_btr_01_data_prep: APIRouter = APIRouter()

SUB_API_NAME: str = "btr_01_data_prep"
# Fast API Subfolder Path
SUB_FOLDER_PATH: str = rf"{FAST_API_RELATIVE_PATH}/{API_NAME}/{SUB_API_NAME}"


# ----------------------------------------------------------------------------------------------------------------------
@router_btr_01_data_prep.post(rf"/{SUB_API_NAME}/fill_missing_price_data/")
def fill_missing_price_data() -> None:
    """Fills in missing price data for every `symbol`s `market_timestamp`s."""
    execute_subprocess(sub_folder_path=SUB_FOLDER_PATH, target_file_name="fill_missing_price_data")


# ----------------------------------------------------------------------------------------------------------------------
@router_btr_01_data_prep.post(rf"/{SUB_API_NAME}/create_predictions/")
def create_predictions() -> None:
    """Create price predictions for various future minute intervals."""
    execute_subprocess(sub_folder_path=SUB_FOLDER_PATH, target_file_name="create_predictions")
