"""SQL queries used throughout the DAG."""

from global_utils.sql_utils import SQL_PARTITION_CORES, TEMP_SCHEMA

TEMP_TABLE_PREFIX: str = "sm_etl_01_fred"
DATE_SCHEMA_TABLE: str = rf"{TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_combined_fred_dates"
COMBINED_DATA_TABLE: str = rf"{TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_combined_data"
DATE_COL_NAME: str = "market_date"
# ----------------------------------------------------------------------------------------------------------------------
data_count_query: str = rf"""
WITH data_rows AS
(
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_wlemuindxd
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_usepuindxd
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_dgs1
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10y2y
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10yff
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_daaa
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_aaa10y
)
SELECT COUNT(*) FROM data_rows;
"""
# ----------------------------------------------------------------------------------------------------------------------
combine_market_dates_query: str = rf"""
CREATE TABLE {DATE_SCHEMA_TABLE} AS
(
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_wlemuindxd
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_usepuindxd
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_dgs1
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10y2y
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10yff
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_daaa
    UNION
    SELECT market_date FROM {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_aaa10y
)
ORDER BY market_date ASC;
"""
# ----------------------------------------------------------------------------------------------------------------------
combine_all_data_query: str = f"""
CREATE TABLE {COMBINED_DATA_TABLE} AS
(
SELECT
FD.market_date

, WL.wlemuindxd
, US.usepuindxd
, DG.dgs1
, T10Y2Y.t10y2y
, T10YFF.t10yff
, DA.daaa
, AA.aaa10y
, (ROW_NUMBER() OVER () % {SQL_PARTITION_CORES}) AS cx_partition

FROM {DATE_SCHEMA_TABLE} AS FD

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_wlemuindxd AS WL
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_usepuindxd AS US
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_dgs1 AS DG
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10y2y AS T10Y2Y
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_t10yff AS T10YFF
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_daaa AS DA
USING (market_date)

LEFT JOIN {TEMP_SCHEMA}.{TEMP_TABLE_PREFIX}_aaa10y AS AA
USING (market_date)
)
"""
