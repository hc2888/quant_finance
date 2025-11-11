"""DAG-specific API Functions."""

from fastapi import APIRouter
from modin_api.modin_utils import FAST_API_RELATIVE_PATH, execute_subprocess
from modin_api.stock_market_api.const_vars import API_NAME

# ----------------------------------------------------------------------------------------------------------------------
router_sm_etl_01_fred: APIRouter = APIRouter()

SUB_API_NAME: str = "sm_etl_01_fred"
# Fast API Subfolder Path
SUB_FOLDER_PATH: str = rf"{FAST_API_RELATIVE_PATH}/{API_NAME}/{SUB_API_NAME}"


# ----------------------------------------------------------------------------------------------------------------------
@router_sm_etl_01_fred.post(rf"/{SUB_API_NAME}/process_combined_data/")
def process_combined_data() -> None:
    """Takes combined temp FRED SQL table, date-sorts the values, and forward fills missing data."""
    execute_subprocess(sub_folder_path=SUB_FOLDER_PATH, target_file_name="process_combined_data")
