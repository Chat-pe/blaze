"""
Configuration settings for the Blaze Web API
"""

import os
from pathlib import Path

# API Configuration
API_TITLE = "Blaze System API"
API_DESCRIPTION = "API for managing Blaze system jobs and sequences"
API_VERSION = "1.0.0"
API_HOST = "0.0.0.0"
API_PORT = 8000

# File paths
LOCK_FILE_PATH = os.getenv("BLAZE_LOCK_PATH", "/tmp/scheduler_lock.json")
JOBS_FILE_PATH = os.getenv("BLAZE_JOBS_PATH", "/tmp/scheduler_jobs.json")
LOG_DIR = os.getenv("BLAZE_LOG_DIR", "./log")

# Default job settings
DEFAULT_CRON_INTERVAL = "*/5 * * * *"
DEFAULT_JOB_TIMEOUT = 3600

# API response settings
MAX_LOG_ENTRIES = 100
MAX_SEQUENCES_PER_REQUEST = 50

# Validation rules
VALID_CRON_FIELDS = 5  # Standard cron has 5 fields
MIN_SEQUENCE_ID_LENGTH = 1
MAX_SEQUENCE_ID_LENGTH = 100
MAX_PARAMETER_DEPTH = 10

def validate_paths():
    """Validate that required directories exist"""
    log_path = Path(LOG_DIR)
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)
    
    tmp_path = Path("/tmp")
    if not tmp_path.exists():
        tmp_path.mkdir(parents=True, exist_ok=True)

def get_system_info():
    """Get system information for debugging"""
    return {
        "lock_file_path": LOCK_FILE_PATH,
        "jobs_file_path": JOBS_FILE_PATH,
        "log_directory": LOG_DIR,
        "lock_file_exists": os.path.exists(LOCK_FILE_PATH),
        "jobs_file_exists": os.path.exists(JOBS_FILE_PATH),
        "log_dir_exists": os.path.exists(LOG_DIR)
    } 