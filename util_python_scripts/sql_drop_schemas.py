"""Executes dropping all tables from RDS."""

from global_utils.sql_utils import SQL_ENGINE
from sql_infra.universal_vars import SCHEMA_TOTAL_LIST
from sqlalchemy.sql import text

# ----------------------------------------------------------------------------------------------------------------------
with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
    schema_var: str
    for schema_var in SCHEMA_TOTAL_LIST:
        print("---------------------------------------------------------------------")
        print(rf"DROPPING SCHEMA: {schema_var}")
        sql_connection.execute(text(f"""DROP SCHEMA IF EXISTS {schema_var} CASCADE"""))
        print(rf"SCHEMA DROPPED")
