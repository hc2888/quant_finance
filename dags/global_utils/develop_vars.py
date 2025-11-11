"""Set of development-wide variables."""

from multiprocessing import cpu_count

import psutil

# NOTE: How many MAX attempts for an API hit
API_MAX_ATTEMPTS: int = 60

"""IP ADDRESSES."""
LOCALHOST_IP: str = r"127.0.0.1"
DOCKER_IP: str = r"host.docker.internal"

"""TOTAL NUMBER OF PHYSICAL (NOT LOGICAL) CPU CORES."""
TOTAL_CPU_CORES: int = cpu_count()
"""TOTAL AVAILABLE `MEMORY` IN `GBs`."""
TOTAL_MEMORY: int = round(psutil.virtual_memory().total / (1024**3))

"""PATH TO THE .csv FILES W/ ALL THE FINANCIAL DATA."""
DATA_FILES_PATH: str = r"/opt/airflow/data_files"

"""PATH TO TEMP DATA FILES THAT ARE TO BE DELETED."""
TEMP_DATA_PATH: str = r"/opt/airflow/temp_data"

"""PREVENTS DAG FROM DROPPING TEMP TABLES IF SET TO False."""
# DROP_ALL_TEMP: bool = True
DROP_ALL_TEMP: bool = False

"""IF `PROCESS_ALL_DATA = True`, THEN `FIRST_MARKET_DATE` AND `LAST_MARKET_DATE` DOES NOT MATTER."""
"""FOR MATH & METADATA DAGs."""
PROCESS_ALL_DATA: bool = True
# PROCESS_ALL_DATA: bool = False

"""FOR VARIOUS ETL PROCESSES INCLUDING MATH & METADATA ANALYSIS."""
# https://www.alphavantage.co/documentation/#listing-status; ANY `YYYY-MM-DD >= 2010-01-01` IS SUPPORTED
FIRST_MARKET_DATE: str = "2010-01-01"
LAST_MARKET_DATE: str = "2050-12-31"
BACKTEST_BEGIN_DATE: str = "2010-01-04"  # This was the first market trading day in 2010

"""STARTING & ENDING MARKET YEARS FOR SQL PARTITIONED TABLES."""
START_MARKET_YEAR: int = 1980
END_MARKET_YEAR: int = 2050

"""DESIGNATED PORTS."""
ML_FLOW_PORT: int = 5000
POSTGRES_DB_PORT: int = 5432
MODIN_FASTAPI_PORT: int = 10000
PYTORCH_FASTAPI_PORT: int = 10001

"""ML FLOW SERVER URI."""
# mlflow.set_tracking_uri(uri=rf"http://{DOCKER_IP}:{ML_FLOW_PORT}")
# Since the ML Flow server is set up WITHOUT SSL, use `http` instead of `https` in the tracking URI
ML_FLOW_URI: str = rf"http://{DOCKER_IP}:{ML_FLOW_PORT}"
