"""
Router - Job Name to Class Mapping
"""
from jobs.job_implementations import JOB_SCR1_SNOWFLAKE_JOB, JOB_SCR1_REDSHIFT_JOB, JOB_SCR1_S3_JOB

# Job Mapping Dictionary
JOB_MAPPING = {
    "JOB_SCR1_SNOWFLAKE_JOB": JOB_SCR1_SNOWFLAKE_JOB,
    "JOB_SCR1_REDSHIFT_JOB": JOB_SCR1_REDSHIFT_JOB,
    "JOB_SCR1_S3_JOB": JOB_SCR1_S3_JOB,
    
    # Aliases
    "JOB1": JOB_SCR1_SNOWFLAKE_JOB,
    "JOB2": JOB_SCR1_REDSHIFT_JOB,
    "JOB3": JOB_SCR1_S3_JOB,
    "SNOWFLAKE": JOB_SCR1_SNOWFLAKE_JOB,
    "REDSHIFT": JOB_SCR1_REDSHIFT_JOB,
    "S3": JOB_SCR1_S3_JOB
}

def get_available_jobs():
    """Get list of available job names"""
    return list(JOB_MAPPING.keys())

def get_job_class(job_name):
    """Get job class for given job name"""
    if job_name not in JOB_MAPPING:
        raise ValueError(f"Job '{job_name}' not found. Available: {get_available_jobs()}")
    return JOB_MAPPING[job_name]