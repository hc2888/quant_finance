"""Takes combined temp FRED SQL table, date-sorts the values, and forward fills missing data."""

import warnings
from typing import Dict

from modin_api.modin_utils import activate_ray, shutdown_ray, verify_no_null_rows

warnings.filterwarnings(action="ignore")


# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
    """Main execution."""
    import modin.pandas as mpd
    from global_utils.develop_secrets import QUANT_PROJ_PATH
    from global_utils.general_utils import create_exec_timestamp_str, json_write
    from global_utils.sql_utils import SQL_CHUNKSIZE_MODIN, SQL_CONN_STRING, cx_read_sql

    exec_timestamp: str = create_exec_timestamp_str()

    activate_ray()
    print(r"READING SQL DATA")
    sql_query: str = "SELECT * FROM temp_tables.sm_etl_01_fred_combined_data"
    combined_df: mpd.DataFrame = cx_read_sql(sql_query=sql_query, create_partition=False, df_type="modin")

    print(r"PROCESSING SQL DATA")
    combined_df: mpd.DataFrame = combined_df.sort_values(by=["market_date"], ascending=True, ignore_index=True)
    combined_df: mpd.DataFrame = combined_df.ffill(axis=0)
    # IMPORTANT: EVERY metric must return values; if only SOME of them have data, then it's no good
    combined_df: mpd.DataFrame = combined_df.dropna(ignore_index=True)

    combined_df["market_year"]: mpd.Series = mpd.to_datetime(arg=combined_df["market_date"]).dt.strftime("%Y")
    combined_df["market_month"]: mpd.Series = mpd.to_datetime(arg=combined_df["market_date"]).dt.month
    combined_df["exec_timestamp"]: mpd.Series = exec_timestamp

    if not combined_df.empty:
        verify_no_null_rows(target_df=combined_df)
        combined_df.to_sql(
            schema="fred",
            name="raw_data",
            con=SQL_CONN_STRING,
            if_exists="append",
            index=False,
            chunksize=int(SQL_CHUNKSIZE_MODIN / len(combined_df.columns)),
        )
        print(r"SQL DATA INSERT COMPLETE")
        task_var: str = "process_raw_data.dedupe_sql_table"
    else:
        print(r"NO SQL DATA TO PROCESS")
        task_var: str = "process_raw_data.no_data_to_process"

    output_dict: Dict[str, str] = {"task_var": task_var}
    file_path: str = rf"{QUANT_PROJ_PATH}/temp_data/sm_etl_01_fred"
    json_write(python_dict=output_dict, file_path=file_path, file_name="process_combined_data")

    shutdown_ray()


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
