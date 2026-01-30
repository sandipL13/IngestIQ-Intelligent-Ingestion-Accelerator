from datetime import datetime
from .constants import DATE_FMT

def now():
    return datetime.utcnow().strftime(DATE_FMT)
