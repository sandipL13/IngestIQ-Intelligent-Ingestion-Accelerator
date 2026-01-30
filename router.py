from jobs.df_snowflake import SnowflakeJob
from jobs.df_redshift import RedshiftJob

JOB_MAP = {
    "SNOWFLAKE": SnowflakeJob,
    "REDSHIFT": RedshiftJob
}
