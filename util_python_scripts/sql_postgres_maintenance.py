"""Cleans up and optimizes the PostGres RDS Environment."""

from datetime import datetime, timedelta

from global_utils.develop_secrets import SQL_DATABASE_NAME
from global_utils.develop_vars import TOTAL_CPU_CORES
from global_utils.sql_utils import SQL_ENGINE
from sqlalchemy.sql import text

# ----------------------------------------------------------------------------------------------------------------------
start_time: datetime = datetime.now()

with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
    print("---------------------------------------------------------------------")
    print(rf"EXECUTING VACUUM (PARALLEL {TOTAL_CPU_CORES})")
    sql_connection.execute(text(rf"VACUUM (PARALLEL {TOTAL_CPU_CORES})"))
    print(rf"VACUUM (PARALLEL {TOTAL_CPU_CORES}) COMPLETE")
    print("---------------------------------------------------------------------")
    print("EXECUTING VACUUM FULL")
    sql_connection.execute(text("VACUUM FULL"))
    print("VACUUM FULL COMPLETE")
    print("---------------------------------------------------------------------")
    print("EXECUTING REINDEX")
    sql_connection.execute(text(rf"REINDEX DATABASE {SQL_DATABASE_NAME}"))
    print("REINDEX COMPLETE")
    print("---------------------------------------------------------------------")
    print("EXECUTING ANALYZE")
    sql_connection.execute(text("ANALYZE"))
    print("ANALYZE COMPLETE")
    print("---------------------------------------------------------------------")
# ----------------------------------------------------------------------------------------------------------------------
end_time: datetime = datetime.now()
runtime: timedelta = end_time - start_time

hours: int
remainder: int
minutes: int
seconds: int
hours, remainder = divmod(runtime.seconds, 3600)
minutes, seconds = divmod(remainder, 60)
print(f"RUNTIME --> HOURS: {hours}, MINUTES: {minutes}, SECONDS: {seconds}")
