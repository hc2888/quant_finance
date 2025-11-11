"""DAG-specific API Functions."""

from fastapi import APIRouter
from modin_api.modin_utils import FAST_API_RELATIVE_PATH, execute_subprocess
from modin_api.stock_market_api.const_vars import API_NAME

# ----------------------------------------------------------------------------------------------------------------------
router_sm_files_first_rate_data: APIRouter = APIRouter()

SUB_API_NAME: str = "sm_files_first_rate_data"
# Fast API Subfolder Path
SUB_FOLDER_PATH: str = rf"{FAST_API_RELATIVE_PATH}/{API_NAME}/{SUB_API_NAME}"


# ----------------------------------------------------------------------------------------------------------------------
@router_sm_files_first_rate_data.post(rf"/{SUB_API_NAME}/process_csv_files/")
def process_csv_files() -> None:
    """Processes data from CSV File."""
    execute_subprocess(sub_folder_path=SUB_FOLDER_PATH, target_file_name="process_csv_files")
