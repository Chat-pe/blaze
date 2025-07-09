from src.core import Blaze
from src.core import BlazeLogger
from src.core import BlazeBlock
from src.core import BlazeSequence
from src.core import SubmitSequenceData
from src.core import BlazeJobs
from src.core import BlazeLock

from typing import List
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


_scheduler = None
_logger = BlazeLogger()
_LOCK_PATH = "/tmp/blaze_lock.json"
_JOBS_PATH = "/tmp/blaze_jobs.json"
_MONGODB_URI = os.getenv("MONGODB_URI")


def get_scheduler():
    global _scheduler
    if _scheduler is None:
        raise ValueError("Scheduler not found")
    return _scheduler

def get_or_create_scheduler(blaze_blocks: BlazeBlock, sequences: BlazeSequence):
    global _scheduler
    if _scheduler is None:
        _scheduler = Blaze(
            blaze_blocks=blaze_blocks,
            sequences=sequences,    
            logger=_logger,
            loop_interval=5,
            job_lock_path=_LOCK_PATH,
            job_file_path=_JOBS_PATH,
            mongo_uri=_MONGODB_URI,
        )
    return _scheduler
def start_scheduler():
    _scheduler.start()

def stop_scheduler(shutdown: bool = False):
    if not os.path.exists(_LOCK_PATH):
        raise FileNotFoundError(f"Lock file not found at {_LOCK_PATH} | Scheduler not Running")
    with open(_LOCK_PATH, "r") as f:
        lock_data = BlazeLock(**json.loads(f.read()))
    lock_data.is_running = False
    lock_data.is_paused = not shutdown
    lock_data.is_stopped = shutdown
    lock_data.last_updated = datetime.now()
    with open(_LOCK_PATH, "w") as f:
        json.dump(lock_data.model_dump(mode='json'), f)
    
    # Also save to MongoDB if available
    if _scheduler and _scheduler.mongo_client:
        try:
            _scheduler.mongo_client.create_lock(lock_data)
        except Exception:
            pass

def update_jobs(submitted_jobs: List[SubmitSequenceData]):
    
    jobs = BlazeJobs(lock_path=_LOCK_PATH, logger=_logger, mongo_client=_scheduler.mongo_client if _scheduler else None)
    check = jobs.update_jobs(submitted_jobs)
    if check:
        return True
    else:
        return False
