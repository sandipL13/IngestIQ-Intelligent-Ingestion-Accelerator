from router import JOB_MAP
from core.utils.logger import get_logger

def run(job_name, spark, config):
    logger = get_logger(job_name)
    job_cls = JOB_MAP[job_name]
    job = job_cls(**config)
    logger.info(f"Running job: {job_name}")
    return job
