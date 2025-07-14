from ..core import Blaze
from ..core import BlazeLogger
from ..core import BlazeBlock
from ..core import BlazeSequence
from ..core import SubmitSequenceData
from ..core import BlazeJobs
from ..core import BlazeLock

from typing import List, Dict, Any
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import hashlib

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
    hex_digest = []
    for job in submitted_jobs:
        hex_digest.append(hashlib.sha256(str(job).encode()).hexdigest())
    if check:
        return hex_digest
    else:
        return None

def get_all_jobs_status() -> Dict[str, Any]:
    """
    Get status of all jobs from MongoDB only.
    
    Returns:
        Dict[str, Any]: JSON object with job status information and metadata
    """
    if not _MONGODB_URI:
        return {
            "success": False,
            "error": "MongoDB URI not configured. Cannot retrieve job status.",
            "jobs": [],
            "total_jobs": 0
        }
    
    try:
        mongo_client = BlazeMongoClient(_MONGODB_URI)
        
        if not mongo_client.test_connection():
            return {
                "success": False,
                "error": "Failed to connect to MongoDB.",
                "jobs": [],
                "total_jobs": 0
            }
        
        # Get all job states from MongoDB
        job_states = mongo_client.fetch_all_job_states()
        
        if not job_states:
            mongo_client.close()
            return {
                "success": True,
                "message": "No jobs found in MongoDB.",
                "jobs": [],
                "total_jobs": 0
            }
        
        # Prepare job data
        jobs_data = []
        
        for job_state in job_states:
            # Format datetime values
            last_run = job_state.last_run.isoformat() if job_state.last_run else None
            next_run = job_state.next_run.isoformat() if job_state.next_run else None
            
            # Calculate average execution time
            avg_execution_time = (job_state.total_execution_time / job_state.total_runs) if job_state.total_runs > 0 else 0.0
            
            jobs_data.append({
                "seq_id": job_state.seq_id,
                "last_run": last_run,
                "next_run": next_run,
                "status": job_state.run_state.value if job_state.run_state else "UNKNOWN",
                "total_execution_time": round(job_state.total_execution_time, 4),
                "total_runs": job_state.total_runs,
                "avg_execution_time": round(avg_execution_time, 4)
            })
        
        mongo_client.close()
        
        return {
            "success": True,
            "jobs": jobs_data,
            "total_jobs": len(job_states),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error retrieving job status from MongoDB: {str(e)}",
            "jobs": [],
            "total_jobs": 0
        }
