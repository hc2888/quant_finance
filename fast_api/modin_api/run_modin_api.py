"""Main API File."""

from typing import Dict

from fastapi import FastAPI
from global_utils.develop_vars import MODIN_FASTAPI_PORT
from modin_api.backtest_runs_api.btr_01_data_prep.api_router import router_btr_01_data_prep
from modin_api.stock_market_api.sm_etl_01_fred.api_router import router_sm_etl_01_fred
from modin_api.stock_market_api.sm_files_first_rate_data.api_router import router_sm_files_first_rate_data

# Initialize the FastAPI app
app: FastAPI = FastAPI()


# ----------------------------------------------------------------------------------------------------------------------
# Define a root endpoint
@app.get(r"/")
def default_root() -> Dict[str, str]:
    """Root endpoint returning a message.

    :return: Dict[str, str]: Default message.
    """
    return {"message": "API LAUNCHED"}


# ----------------------------------------------------------------------------------------------------------------------
# ALL THE API FUNCTIONS FROM THE ROUTERS
app.include_router(router=router_sm_etl_01_fred)
app.include_router(router=router_sm_files_first_rate_data)
app.include_router(router=router_btr_01_data_prep)
# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="localhost", port=MODIN_FASTAPI_PORT)
