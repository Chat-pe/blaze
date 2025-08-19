from ..core import Blaze
from ..core import BlazeLogger
from ..core import BlazeBlock
from ..core import BlazeSequence
from ..core import SubmitSequenceData
from ..core import BlazeJobs
from ..core import BlazeLock
from ..db.mongo import BlazeMongoClient

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
        # SSL configuration for MongoDB Atlas
        ssl_options = {
            'ssl': True,
            'ssl_cert_reqs': 'CERT_NONE',  # For development - use CERT_REQUIRED in production
            'tlsAllowInvalidCertificates': True,  # For development - remove in production
            'tlsAllowInvalidHostnames': True,  # For development - remove in production
        }
        
        mongo_client = BlazeMongoClient(_MONGODB_URI, ssl_options=ssl_options)
        
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
                "job_id": job_state.job_id, 
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


def get_blaze_scheduler_jobs_status() -> Dict[str, Any]:
    try:
        # First try to get jobs from the running scheduler instance
        if _scheduler and hasattr(_scheduler, 'state_manager'):
            # Get job states directly from the scheduler's state manager
            job_states = _scheduler.state_manager.job_states
            
            if job_states:
                # Prepare job data
                jobs_data = []
                
                for job_id, job_state in job_states.items():
                    # Format datetime values
                    last_run = job_state.last_run.isoformat() if job_state.last_run else None
                    next_run = job_state.next_run.isoformat() if job_state.next_run else None
                    
                    # Calculate average execution time
                    avg_execution_time = (job_state.total_execution_time / job_state.total_runs) if job_state.total_runs > 0 else 0.0
                    
                    jobs_data.append({
                        "job_id": job_id, 
                        "seq_id": job_state.seq_id,
                        "last_run": last_run,
                        "next_run": next_run,
                        "status": job_state.run_state.value if job_state.run_state else "UNKNOWN",
                        "total_execution_time": round(job_state.total_execution_time, 4),
                        "total_runs": job_state.total_runs,
                        "avg_execution_time": round(avg_execution_time, 4)
                    })
                
                return {
                    "success": True,
                    "jobs": jobs_data,
                    "total_jobs": len(job_states),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                pass
        
        # Fallback: Try to get jobs from MongoDB if scheduler is not accessible
        if _MONGODB_URI:
            try:
                # SSL configuration for MongoDB Atlas
                ssl_options = {
                    'ssl': True,
                    'ssl_cert_reqs': 'CERT_NONE',
                    'tlsAllowInvalidCertificates': True,
                    'tlsAllowInvalidHostnames': True,
                }
                
                mongo_client = BlazeMongoClient(_MONGODB_URI, ssl_options=ssl_options)
                
                if mongo_client.test_connection():
                    # Get all job states from MongoDB
                    job_states = mongo_client.fetch_all_job_states()
                    
                    if job_states:
                        # Prepare job data
                        jobs_data = []
                        
                        for job_state in job_states:
                            # Format datetime values
                            last_run = job_state.last_run.isoformat() if job_state.last_run else None
                            next_run = job_state.next_run.isoformat() if job_state.next_run else None
                            
                            # Calculate average execution time
                            avg_execution_time = (job_state.total_execution_time / job_state.total_runs) if job_state.total_runs > 0 else 0.0
                            
                            jobs_data.append({
                                "job_id": job_state.job_id,  # Use the actual job_id from MongoDB
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
                            "timestamp": datetime.now().isoformat(),
                            "source": "mongodb_fallback"
                        }
                    
                    mongo_client.close()
                else:
                    mongo_client.close()
            except Exception as mongo_e:
                pass
        
        # Second fallback: Try to read from local state files
        try:
            
            # Try to read from the log directory where job states are actually stored
            log_dir = "log"  # Job states are stored in the log/ directory
            if os.path.exists(log_dir):
                # Look for job state directories (they're named with full job IDs)
                job_dirs = [d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d)) and len(d) > 20]
                
                if job_dirs:
                    # Prepare job data
                    jobs_data = []
                    
                    for job_dir in job_dirs:
                        state_file = os.path.join(log_dir, job_dir, "state.json")
                        if os.path.exists(state_file):
                            try:
                                with open(state_file, "r") as f:
                                    job_state_data = json.load(f)
                                
                                # Extract job information
                                job_id = job_dir  # The directory name is the job_id
                                seq_id = job_state_data.get("seq_id", "unknown")
                                last_run = job_state_data.get("last_run")
                                next_run = job_state_data.get("next_run")
                                run_state = job_state_data.get("run_state", "UNKNOWN")
                                total_execution_time = job_state_data.get("total_execution_time", 0.0)
                                total_runs = int(job_state_data.get("total_runs", 0))
                                
                                # Calculate average execution time
                                avg_execution_time = (total_execution_time / total_runs) if total_runs > 0 else 0.0
                                
                                jobs_data.append({
                                    "job_id": job_id,  # Use full job_id, not truncated display format
                                    "seq_id": seq_id,
                                    "last_run": last_run,
                                    "next_run": next_run,
                                    "status": run_state,
                                    "total_execution_time": round(total_execution_time, 4),
                                    "total_runs": total_runs,
                                    "avg_execution_time": round(avg_execution_time, 4)
                                })
                                
                            except Exception as e:
                                pass
                    
                    if jobs_data:
                        return {
                            "success": True,
                            "jobs": jobs_data,
                            "total_jobs": len(jobs_data),
                            "timestamp": datetime.now().isoformat(),
                            "source": "local_state_files"
                        }
                
        except Exception as local_e:
            pass
        
        # If no jobs found from any source
        return {
            "success": True,
            "message": "No jobs found in Blaze scheduler or MongoDB.",
            "jobs": [],
            "total_jobs": 0,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error retrieving job status from Blaze scheduler: {str(e)}",
            "jobs": [],
            "total_jobs": 0
        }
