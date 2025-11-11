"""Variables & Functions related to SQL-related processes."""

from typing import List, Literal, Optional, Tuple

import pandas as pd
import sqlalchemy
from global_utils.develop_secrets import RDS_PASSWORD, RDS_USERNAME, SQL_DATABASE_NAME
from global_utils.develop_vars import (
    DOCKER_IP,
    END_MARKET_YEAR,
    POSTGRES_DB_PORT,
    START_MARKET_YEAR,
    TOTAL_CPU_CORES,
    TOTAL_MEMORY,
)
from global_utils.general_utils import create_exec_timestamp_str
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

# ----------------------------------------------------------------------------------------------------------------------
SQL_LOGIN: str = rf"""{RDS_USERNAME}:{RDS_PASSWORD}"""
SQL_DATABASE: str = rf"""{DOCKER_IP}:{POSTGRES_DB_PORT}/{SQL_DATABASE_NAME}"""
SQL_CONN_STRING: str = rf"postgresql+psycopg2://{SQL_LOGIN}@{SQL_DATABASE}"
SQL_ENGINE: Engine = sqlalchemy.create_engine(url=SQL_CONN_STRING)

# NOTE: .to_sql(chunksize = `SQL_CHUNKSIZE_` / `num of columns`)
SQL_CHUNKSIZE_PANDAS: int = TOTAL_CPU_CORES * 1_000
SQL_CHUNKSIZE_MODIN: int = SQL_CHUNKSIZE_PANDAS * 10
# NOTE: How many CPU cores to use for general SQL read / write actions and overall data transformation operations
SQL_PARTITION_CORES: int = int(TOTAL_CPU_CORES * (15 / 16))
# ----------------------------------------------------------------------------------------------------------------------
TEMP_SCHEMA: str = "temp_tables"
CORRELATION_METHOD_LIST: List[str] = ["pearson", "spearman", "kendall"]
# NOTE: Use whenever executing SQL queries involving LARGE tables
HALF_SQL_PARTITION_CORES: int = int(SQL_PARTITION_CORES / 2)
HALF_MEMORY: int = int(TOTAL_MEMORY / 2)
# ----------------------------------------------------------------------------------------------------------------------
BIG_QUERY_TUNERS: List[str] = [
    "SET random_page_cost = 1.1",  # Prefer sequential scans slightly
    "SET seq_page_cost = 1",  # Encourage parallel sequential scans
    "SET jit = off",  # Disable JIT overhead on large joins
    "SET temp_buffers = '1.99GB'",  # More in-memory space for temp results
    "SET enable_nestloop = off",  # Avoid slow nested loops on big joins
    f"SET max_parallel_workers = {SQL_PARTITION_CORES}",  # Allow full hardware utilization
    f"SET max_parallel_workers_per_gather = {HALF_SQL_PARTITION_CORES}",  # Half of `max_parallel_workers` cores
    "SET parallel_setup_cost = 100",  # Lower cost barrier for parallel plans
    "SET parallel_tuple_cost = 0.01",  # Encourage planner to use parallelism
    "SET min_parallel_table_scan_size = '4MB'",  # Allow even smaller tables to parallelize
    "SET min_parallel_index_scan_size = '4MB'",  # Same for index scans
    "SET work_mem = '1.99GB'",  # Each worker’s local memory (join/hash)
    "SET maintenance_work_mem = '1.99GB'",  # Index build + vacuum memory
    f"SET effective_cache_size = '{HALF_MEMORY}GB'",  # Tell planner that half of RAM is cacheable
]


def sql_big_query_prep(sql_connection: Engine) -> None:
    """Reconfigure SQL Engine settings for a big SQL Session.

    :param sql_connection: Engine: The SQL Engine connection for the SQL session.
    """
    tune_query: str
    for tune_query in BIG_QUERY_TUNERS:
        sql_connection.execute(text(tune_query))


# ----------------------------------------------------------------------------------------------------------------------
numerical_query: str = """
, column_name TEXT
, mean NUMERIC(10,2)
, std_dev NUMERIC(10,2)
, std_dev_mean_perc NUMERIC(10,2)
, min NUMERIC(10,2)
, percentile_25 NUMERIC(10,2)
, median NUMERIC(10,2)
, percentile_75 NUMERIC(10,2)
, max NUMERIC(10,2)
, skewness NUMERIC(10,2)
, kurtosis NUMERIC(10,2)
, std_1_68_perc NUMERIC(10,2)
, std_2_95_perc NUMERIC(10,2)
, std_3_99_perc NUMERIC(10,2)
"""

correlation_query: str = """
, feature_1 TEXT
, feature_2 TEXT
, correlation_coefficient NUMERIC(3,2)
"""

categorical_query: str = """
, column_name TEXT
, avail_count INTEGER
, null_count INTEGER
, null_count_perc NUMERIC(5,2)
, distinct_count INTEGER
"""

segments_query: str = """
, feature_name TEXT
, feature_value_name TEXT
, feature_value_count INTEGER
, feature_value_perc NUMERIC(5,2)
"""

metadata_labels: List[Tuple[str, str]] = [
    ("numerical", numerical_query),
    ("correlation", correlation_query),
    ("categorical", categorical_query),
    ("segments", segments_query),
]


# ----------------------------------------------------------------------------------------------------------------------
def cx_read_sql(
    sql_query: str,
    create_partition: bool = True,
    partition_column: str = "cx_partition",
    num_cpu_cores: int = SQL_PARTITION_CORES,
    df_type: Literal["modin", "pandas"] = "pandas",
) -> DataFrame:
    """Use `Connector X` Python library for optimal SQL querying to turn into a DataFrame.

    :param sql_query: str: SQL Query to execute.
    :param create_partition: bool: Whether to create a `cx_partition` column to properly execute SQL query pull.
    :param partition_column: str: The column to split the SQL query into evenly distributed parallel processing.
    :param num_cpu_cores: int: Number of parallel SQL query splitting which equates to number of CPU cores to utilize.
    :param df_type: Literal["modin", "pandas"]: The type of DataFrame to output the results.

    :return: DataFrame: Returns either a Pandas or Modin DataFrame.
    """
    import connectorx as cx

    if create_partition:
        sql_query: str = f"""
        WITH main_query AS ({sql_query})
        SELECT *, (ROW_NUMBER() OVER () % {num_cpu_cores}) AS {partition_column} FROM main_query
        """

    print(r"EXECUTING `CONNECTOR X` SQL QUERY")
    # noinspection PyTypeChecker
    main_df: DataFrame = cx.read_sql(
        # `cx.read_sql()` does not recognize `postgresql+psycopg2` syntax
        conn=rf"postgresql://{SQL_LOGIN}@{SQL_DATABASE}",
        query=sql_query,
        partition_on=partition_column,
        partition_num=num_cpu_cores,
        return_type=df_type,
    ).drop(columns=[partition_column])
    print(r"`CONNECTOR X` SQL QUERY COMPLETED")

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def verify_sql_table_boolean(sql_schema: str, sql_table: str) -> bool:
    """Checks to see if the SQL table exists.

    :param sql_schema: str: name of SQL schema.
    :param sql_table: str: name of SQL table.

    :return: bool: whether the table exists or not.
    """
    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = f"""
            SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE
            schemaname = '{sql_schema}' AND
            tablename  = '{sql_table}'
            )
            """
        table_exists_status: bool = sql_connection.execute(text(sql_query)).scalar()

    return table_exists_status


# ----------------------------------------------------------------------------------------------------------------------
def _create_sql_main_table(
    sql_schema: str,
    sql_table: str,
    sql_columns_string: str,
    sql_primary_key_str: str = "unique_id",
    partition_column_name: str = "market_date",
    index_columns: Optional[List[str]] = None,
    reset_exec_timestamp: bool = False,
) -> None:
    """Creates the main version of the SQL table for a given dataset.

    :param sql_schema: str: name of SQL schema.
    :param sql_table: str: name of SQL table.
    :param sql_columns_string: str: SQL query that auto-generates all the column statements.
    :param sql_primary_key_str: str: name(s) of the primary key column(s) for the given SQL table.
    :param partition_column_name: str: name of the partition range column for the given SQL table.
    :param index_columns: Optional[List[str]]: The columns to execute SQL table indexing against.
    :param reset_exec_timestamp: bool: Whether to reset the `exec_timestamp` column in the SQL table(s).
    """
    if index_columns is None:
        index_columns: List[str] = ["market_date"]
    sql_main_table: str = f"""CREATE TABLE IF NOT EXISTS {sql_schema}.{sql_table} (""" + sql_columns_string

    if sql_primary_key_str.strip():
        # The partitioning column HAS to be part of the Primary Key(s)
        sql_main_table += f""", PRIMARY KEY ({sql_primary_key_str}, {partition_column_name})"""

    sql_main_table += f""") PARTITION BY RANGE ({partition_column_name})"""

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(sql_main_table))
        index_name: str = rf"{sql_schema}_{sql_table}"
        schema_table: str = rf"{sql_schema}.{sql_table}"

        unique_id_index: str = rf"CREATE INDEX IF NOT EXISTS {index_name}_unique_id ON {schema_table} (unique_id)"
        sql_connection.execute(text(unique_id_index))

        # Used to verify if an entry needs to be added to `global.table_roster` SQL table
        table_exists: bool = verify_sql_table_boolean(sql_schema=sql_schema, sql_table=sql_table)
        sql_query: str = f"""
            SELECT EXISTS (
            SELECT FROM global.tables_roster
            WHERE
            schema_name = '{sql_schema}' AND
            table_name  = '{sql_table}'
            )
        """
        table_roster_exists: bool = sql_connection.execute(text(sql_query)).scalar()

        if not table_exists or not table_roster_exists:
            # Add `sql_schema.sql_table` data entry to `global.table_roster` SQL table
            data_df: pd.DataFrame = pd.DataFrame(data={"schema_name": [sql_schema], "table_name": [sql_table]})
            data_df.to_sql(
                schema="global",
                name="tables_roster",
                con=SQL_ENGINE,
                if_exists="append",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(data_df.columns)),
            )

        index_columns_str: str = str(index_columns)[1:-1].replace("'", "")
        index_columns_str: str = rf"{index_columns_str}, exec_timestamp DESC"
        idx_name: str = rf"{index_name}_partition"
        create_index_query: str = rf"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema_table} ({index_columns_str})"

        if reset_exec_timestamp:
            # NOTE: Reset the `exec_timestamp` values so that there is only ONE `exec_timestamp` value.
            # NOTE: This ensures the De-duplication runs remain @ optimal speed etc.
            sql_connection.execute(text(rf"DROP INDEX IF EXISTS {sql_schema}.{idx_name}"))

            alter_query: str = rf"ALTER TABLE {sql_schema}.{sql_table} DROP COLUMN IF EXISTS exec_timestamp"
            sql_connection.execute(text(alter_query))

            exec_timestamp: str = create_exec_timestamp_str()
            add_column_1: str = rf"ALTER TABLE {sql_schema}.{sql_table}"
            add_column_2: str = rf"ADD COLUMN exec_timestamp TEXT DEFAULT '{exec_timestamp}' NOT NULL"
            update_query: str = f"{add_column_1} {add_column_2}"
            sql_connection.execute(text(update_query))
            sql_connection.execute(text(create_index_query))
        else:
            sql_connection.execute(text(create_index_query))


# ----------------------------------------------------------------------------------------------------------------------
def _create_sql_metadata_table(
    sql_schema: str,
    sql_table: str,
    metadata_type: str,
    metadata_query: str,
    reset_exec_timestamp: bool = False,
) -> None:
    """Creates the metadata version of the SQL table for a given dataset.

    :param sql_schema: str: name of SQL schema.
    :param sql_table: str: name of SQL table.
    :param metadata_type: str: the type of metadata for the table.
    :param metadata_query: str: the SQL query specific for the metadata type.
        Taken from `global_utils.sql_utils.py` file.
    :param reset_exec_timestamp: bool: Whether to reset the `exec_timestamp` column in the SQL table(s).
    """
    meta_schema: str = rf"{sql_schema}_metadata"
    meta_table: str = rf"{sql_table}_{metadata_type}"

    sql_metadata_table: str = f"""CREATE TABLE IF NOT EXISTS {meta_schema}.{meta_table} ("""
    sql_metadata_table += """
    unique_id UUID DEFAULT gen_random_uuid()
    , exec_timestamp TEXT NOT NULL
    , begin_market_date TEXT NOT NULL
    , end_market_date TEXT NOT NULL
    """
    sql_metadata_table += metadata_query
    sql_metadata_table += """, PRIMARY KEY (unique_id, begin_market_date)) PARTITION BY RANGE (begin_market_date)"""

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(sql_metadata_table))
        index_name: str = rf"{meta_schema}_{meta_table}"
        schema_table: str = rf"{meta_schema}.{meta_table}"

        unique_id_index: str = rf"CREATE INDEX IF NOT EXISTS {index_name}_unique_id ON {schema_table} (unique_id)"
        sql_connection.execute(text(unique_id_index))

        # Used to verify if an entry needs to be added to `global.table_roster` SQL table
        table_exists: bool = verify_sql_table_boolean(sql_schema=meta_schema, sql_table=meta_table)
        sql_query: str = f"""
            SELECT EXISTS (
            SELECT FROM global.tables_roster
            WHERE
            schema_name = '{meta_schema}' AND
            table_name  = '{meta_table}'
            )
        """
        table_roster_exists: bool = sql_connection.execute(text(sql_query)).scalar()

        if not table_exists or not table_roster_exists:
            # Add `sql_schema.sql_table` data entry to `global.table_roster` SQL table
            data_df: pd.DataFrame = pd.DataFrame(data={"schema_name": [meta_schema], "table_name": [meta_table]})
            data_df.to_sql(
                schema="global",
                name="tables_roster",
                con=SQL_ENGINE,
                if_exists="append",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(data_df.columns)),
            )

        index_columns_str: str = r"begin_market_date, exec_timestamp DESC"
        idx_name: str = rf"{index_name}_partition"
        create_index_query: str = rf"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema_table} ({index_columns_str})"

        if reset_exec_timestamp:
            # NOTE: Reset the `exec_timestamp` values so that there is only ONE `exec_timestamp` value.
            # NOTE: This ensures the De-duplication runs remain @ optimal speed etc.
            sql_connection.execute(text(rf"DROP INDEX IF EXISTS {meta_schema}.{idx_name}"))

            alter_query: str = rf"ALTER TABLE {meta_schema}.{meta_table} DROP COLUMN IF EXISTS exec_timestamp"
            sql_connection.execute(text(alter_query))

            exec_timestamp: str = create_exec_timestamp_str()
            add_column_1: str = rf"ALTER TABLE {meta_schema}.{meta_table}"
            add_column_2: str = rf"ADD COLUMN exec_timestamp TEXT DEFAULT '{exec_timestamp}' NOT NULL"
            update_query: str = f"{add_column_1} {add_column_2}"
            sql_connection.execute(text(update_query))
            sql_connection.execute(text(create_index_query))
        else:
            sql_connection.execute(text(create_index_query))


# ----------------------------------------------------------------------------------------------------------------------
def create_sql_partitioned_table(
    sql_schema: str,
    sql_table: str,
    columns_data_type_list: List[str],
    start_year: int = START_MARKET_YEAR,
    end_year: int = END_MARKET_YEAR,
    sql_primary_key_str: str = "unique_id",
    partition_column_name: str = "market_date",
    index_columns: Optional[List[str]] = None,
    monthly_partition: bool = False,
    create_metadata_table: bool = True,
    reset_exec_timestamp: bool = False,
) -> None:
    """Creates the partitioned SQL main & metadata table for a given dataset.

    :param sql_schema: str: name of SQL schema.
    :param sql_table: str: name of SQL table.
    :param columns_data_type_list: List[str]: list of all the columns relevant for the given dataset.
    :param start_year: int: the starting year you want for the partitions in the SQL table.
    :param end_year: int: the ending year you want for the partitions in the SQL table.
    :param sql_primary_key_str: str: name(s) of the primary key column(s) for the given SQL table.
    :param partition_column_name: str: name of the partition range column for the given SQL table.
    :param index_columns: Optional[List[str]]: The columns to execute SQL table indexing against.
    :param monthly_partition: bool: Whether the partitioning table should be set for every month.
    :param create_metadata_table: bool: Whether to create a metadata table for the main partitioned table.
    :param reset_exec_timestamp: bool: Whether to reset the `exec_timestamp` column in the SQL table(s).
    """
    if index_columns is None:
        index_columns: List[str] = ["market_date"]
    sql_prepared_columns_list: List[str] = []

    column_type: str
    for column_type in columns_data_type_list:
        print(f"""ACTIVE COLUMN: {column_type}""")
        # Add a `,` before every column name and data type to prepare a SQL `CREATE TABLE` QUERY
        sql_prepared_columns_list.append(rf",{column_type}")

    sql_columns_string: str = "".join(sql_prepared_columns_list)[1:]

    _create_sql_main_table(
        sql_schema=sql_schema,
        sql_table=sql_table,
        sql_columns_string=sql_columns_string,
        sql_primary_key_str=sql_primary_key_str,
        partition_column_name=partition_column_name,
        index_columns=index_columns,
        reset_exec_timestamp=reset_exec_timestamp,
    )

    if create_metadata_table:
        metadata_type: str
        metadata_query: str
        for metadata_type, metadata_query in metadata_labels:
            if metadata_type == "correlation":
                method: str
                for method in CORRELATION_METHOD_LIST:
                    _create_sql_metadata_table(
                        sql_schema=sql_schema,
                        sql_table=sql_table,
                        metadata_type=rf"correlation_{method}",
                        metadata_query=metadata_query,
                        reset_exec_timestamp=reset_exec_timestamp,
                    )
            else:
                _create_sql_metadata_table(
                    sql_schema=sql_schema,
                    sql_table=sql_table,
                    metadata_type=metadata_type,
                    metadata_query=metadata_query,
                    reset_exec_timestamp=reset_exec_timestamp,
                )

    years_list: List[int] = []
    temp_year: int = start_year
    while temp_year <= end_year:
        years_list.append(temp_year)
        temp_year += 1

    partition_list: List[Tuple[str, str, str, str]] = []
    year: int
    for year in years_list:
        if monthly_partition:
            temp_list: List[Tuple[str, str, str, str]] = [
                (f"{year}-01-01", f"{year}-02-01", f"{year}_01_01", f"{year}_01_31"),
                (f"{year}-02-01", f"{year}-03-01", f"{year}_02_01", f"{year}_02_28"),
                (f"{year}-03-01", f"{year}-04-01", f"{year}_03_01", f"{year}_03_31"),
                (f"{year}-04-01", f"{year}-05-01", f"{year}_04_01", f"{year}_04_30"),
                (f"{year}-05-01", f"{year}-06-01", f"{year}_05_01", f"{year}_05_31"),
                (f"{year}-06-01", f"{year}-07-01", f"{year}_06_01", f"{year}_06_30"),
                (f"{year}-07-01", f"{year}-8-01", f"{year}_07_01", f"{year}_07_31"),
                (f"{year}-08-01", f"{year}-09-01", f"{year}_08_01", f"{year}_08_31"),
                (f"{year}-09-01", f"{year}-10-01", f"{year}_09_01", f"{year}_09_30"),
                (f"{year}-10-01", f"{year}-11-01", f"{year}_10_01", f"{year}_10_31"),
                (f"{year}-11-01", f"{year}-12-01", f"{year}_11_01", f"{year}_11_30"),
                (f"{year}-12-01", f"{year + 1}-01-01", f"{year}_12_01", f"{year}_12_31"),
            ]
        else:
            temp_list: List[Tuple[str, str, str, str]] = [
                (f"{year}-01-01", f"{year}-07-01", f"{year}_01_01", f"{year}_06_30"),
                (f"{year}-07-01", f"{year + 1}-01-01", f"{year}_07_01", f"{year}_12_31"),
            ]

        partition_list += temp_list

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        month_1: str
        month_2: str
        name_1: str
        name_2: str
        for month_1, month_2, name_1, name_2 in partition_list:
            partition_table_name: str = f"{sql_schema}.{sql_table}_{name_1}_{name_2}"
            sql_query_partition: str = f"""
            CREATE TABLE IF NOT EXISTS {partition_table_name}
            PARTITION OF {sql_schema}.{sql_table}
            FOR VALUES FROM ('{month_1}') TO ('{month_2}');
            """
            sql_query_partition: str = sql_query_partition
            sql_connection.execute(text(sql_query_partition))
        if create_metadata_table:
            metadata_type: str
            metadata_query: str
            for metadata_type, metadata_query in metadata_labels:
                if metadata_type == "correlation":
                    method: str
                    for method in CORRELATION_METHOD_LIST:
                        month_1: str
                        month_2: str
                        name_1: str
                        name_2: str
                        for month_1, month_2, name_1, name_2 in partition_list:
                            partition_schema: str = f"{sql_schema}_metadata"
                            partition_table: str = f"{sql_table}_correlation_{method}_{name_1}_{name_2}"
                            partition_table_name: str = f"{partition_schema}.{partition_table}"
                            partition_correlation: str = f"""
                                CREATE TABLE IF NOT EXISTS {partition_table_name}
                                PARTITION OF {partition_schema}.{sql_table}_correlation_{method}
                                FOR VALUES FROM ('{month_1}') TO ('{month_2}');
                                """
                            sql_query_partition_correlation: str = partition_correlation
                            sql_connection.execute(text(sql_query_partition_correlation))
                else:
                    month_1: str
                    month_2: str
                    name_1: str
                    name_2: str
                    for month_1, month_2, name_1, name_2 in partition_list:
                        partition_schema: str = f"{sql_schema}_metadata"
                        partition_table: str = f"{sql_table}_{metadata_type}_{name_1}_{name_2}"
                        partition_table_name: str = f"{partition_schema}.{partition_table}"
                        partition_metadata: str = f"""
                            CREATE TABLE IF NOT EXISTS {partition_table_name}
                            PARTITION OF {partition_schema}.{sql_table}_{metadata_type}
                            FOR VALUES FROM ('{month_1}') TO ('{month_2}');
                            """
                        sql_query_partition_metadata: str = partition_metadata
                        sql_connection.execute(text(sql_query_partition_metadata))


# ----------------------------------------------------------------------------------------------------------------------
def dedupe_sql_table(
    sql_schema: str,
    sql_table: str,
    partition_by_cols: List[str],
    main_index_column: str = "market_date",
    begin_market_date: str = "1900-01-01",
    end_market_date: str = "2050-12-31",
) -> None:
    """Deduplicate a SQL table.

    :param sql_schema: str: Name of SQL schema.
    :param sql_table: str: Name of SQL table.
    :param partition_by_cols: List[str]: List of all the columns to partition against.
    :param main_index_column: str: The main indexed column in the target SQL table.
    :param begin_market_date: str: Beginning market date for SQL date range.
    :param end_market_date: str: Ending market date for SQL date range.
    """
    partition_by_cols_str: str = ",".join(partition_by_cols)

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        delete_query: str = f"""
        DELETE FROM {sql_schema}.{sql_table}
        WHERE unique_id IN (
            SELECT unique_id
            FROM (
                SELECT
                unique_id
                , ROW_NUMBER() OVER (PARTITION BY {partition_by_cols_str} ORDER BY exec_timestamp DESC) AS row_num
                FROM {sql_schema}.{sql_table}
                WHERE {main_index_column} BETWEEN '{begin_market_date}' AND '{end_market_date}'
            ) AS ranked
            WHERE row_num > 1
        )
        """
        sql_connection.execute(text(delete_query))
