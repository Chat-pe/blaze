from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import json
import os
from datetime import datetime
from pathlib import Path
import glob

# Import types from the core module
from ..core._types import (
    BlockData,
    SeqBlockData,
    SeqData,
    SubmitSequenceData,
    SequenceExecutionData,
    SequenceStatus,
    SequenceResult,
    JobFile,
    JobExecutuionData,
    JobState,
    SequenceData,
    BlazeLock
)
from ..core.jobs import BlazeJobs
from ..core.logger import BlazeLogger

# Import configuration
from config import (
    API_TITLE, API_DESCRIPTION, API_VERSION,
    LOCK_FILE_PATH, JOBS_FILE_PATH, LOG_DIR,
    validate_paths, get_system_info
)

# Initialize paths
validate_paths()

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION
)

class SystemStatus(BaseModel):
    name: str
    is_running: bool
    is_paused: bool
    is_stopped: bool
    blocks: List[str]
    sequences: List[str]
    loop_interval: int
    last_updated: str

class JobSubmissionRequest(BaseModel):
    seq_id: str
    parameters: Dict[str, Any] = {}
    seq_run_interval: str = "*/5 * * * *"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class JobSubmissionResponse(BaseModel):
    success: bool
    message: str

class SequenceStatusResponse(BaseModel):
    seq_id: str
    status: str
    last_run: Optional[str]
    next_run: Optional[str]
    latest_result: Optional[Dict[str, Any]]
    total_execution_time: Optional[float]
    error_logs: Optional[List[str]]

class ExecutionLog(BaseModel):
    status: str
    start_time: str
    execution_time: float
    error: Optional[str]

def read_lock_file() -> Optional[BlazeLock]:
    """Read and parse the scheduler lock file"""
    if not os.path.exists(LOCK_FILE_PATH):
        return None
    
    try:
        with open(LOCK_FILE_PATH, "r") as f:
            data = json.load(f)
            return BlazeLock(**data)
    except Exception as e:
        print(f"Error reading lock file: {e}")
        return None

def read_jobs_file() -> Optional[JobFile]:
    """Read and parse the scheduler jobs file"""
    if not os.path.exists(JOBS_FILE_PATH):
        return None
    
    try:
        with open(JOBS_FILE_PATH, "r") as f:
            data = json.load(f)
            return JobFile(**data)
    except Exception as e:
        print(f"Error reading jobs file: {e}")
        return None

def read_sequence_state(seq_id: str) -> Optional[Dict[str, Any]]:
    """Read sequence state from log directory"""
    state_file = os.path.join(LOG_DIR, seq_id, "state.json")
    if not os.path.exists(state_file):
        return None
    
    try:
        with open(state_file, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading sequence state: {e}")
        return None

def read_sequence_runs(seq_id: str) -> List[Dict[str, Any]]:
    """Read sequence execution logs from log directory"""
    runs_file = os.path.join(LOG_DIR, seq_id, "runs.json")
    if not os.path.exists(runs_file):
        return []
    
    try:
        with open(runs_file, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading sequence runs: {e}")
        return []

def get_all_sequences() -> List[str]:
    """Get all sequence IDs from log directory"""
    if not os.path.exists(LOG_DIR):
        return []
    
    sequences = []
    for item in os.listdir(LOG_DIR):
        item_path = os.path.join(LOG_DIR, item)
        if os.path.isdir(item_path):
            sequences.append(item)
    
    return sequences

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Blaze System API",
        "version": "1.0.0",
        "endpoints": {
            "system_status": "/status",
            "active_blocks": "/blocks",
            "sequence_status": "/sequences/{seq_id}/status",
            "submit_job": "/jobs/submit",
            "sequence_logs": "/sequences/{seq_id}/logs"
        }
    }

@app.get("/status", response_model=SystemStatus)
async def get_system_status():
    """Get the current status of the Blaze system"""
    lock_data = read_lock_file()
    
    if not lock_data:
        raise HTTPException(status_code=503, detail="Blaze system is not running or lock file not found")
    
    return SystemStatus(
        name=lock_data.name,
        is_running=lock_data.is_running,
        is_paused=lock_data.is_paused,
        is_stopped=lock_data.is_stopped,
        blocks=lock_data.blocks,
        sequences=lock_data.sequences,
        loop_interval=lock_data.loop_interval,
        last_updated=lock_data.last_updated
    )

@app.get("/blocks")
async def get_active_blocks():
    """Get all active blocks in the system"""
    lock_data = read_lock_file()
    
    if not lock_data:
        raise HTTPException(status_code=503, detail="Blaze system is not running or lock file not found")
    
    return {
        "blocks": lock_data.blocks,
        "count": len(lock_data.blocks),
        "system_name": lock_data.name
    }

@app.get("/sequences")
async def get_all_sequences_status():
    """Get status of all sequences"""
    sequences = get_all_sequences()
    
    if not sequences:
        return {"sequences": [], "count": 0}
    
    sequence_statuses = []
    for seq_id in sequences:
        state = read_sequence_state(seq_id)
        if state:
            sequence_statuses.append({
                "seq_id": seq_id,
                "status": state.get("run_state", "UNKNOWN"),
                "last_run": state.get("last_run"),
                "next_run": state.get("next_run"),
                "total_execution_time": state.get("total_execution_time", 0.0)
            })
    
    return {
        "sequences": sequence_statuses,
        "count": len(sequence_statuses)
    }

@app.get("/sequences/{seq_id}/status", response_model=SequenceStatusResponse)
async def get_sequence_status(seq_id: str):
    """Get the status and last result of a specific sequence"""
    state = read_sequence_state(seq_id)
    
    if not state:
        raise HTTPException(status_code=404, detail=f"Sequence '{seq_id}' not found")
    
    return SequenceStatusResponse(
        seq_id=seq_id,
        status=state.get("run_state", "UNKNOWN"),
        last_run=state.get("last_run"),
        next_run=state.get("next_run"),
        latest_result=state.get("latest_result"),
        total_execution_time=state.get("total_execution_time", 0.0),
        error_logs=state.get("error_logs", [])
    )

@app.get("/sequences/{seq_id}/logs")
async def get_sequence_logs(seq_id: str):
    """Get all execution logs for a specific sequence"""
    runs = read_sequence_runs(seq_id)
    
    if not runs:
        # Check if sequence exists
        state = read_sequence_state(seq_id)
        if not state:
            raise HTTPException(status_code=404, detail=f"Sequence '{seq_id}' not found")
        
        return {
            "seq_id": seq_id,
            "logs": [],
            "count": 0,
            "message": "No execution logs found for this sequence"
        }
    
    logs = []
    for run in runs:
        logs.append(ExecutionLog(
            status=run.get("status", "UNKNOWN"),
            start_time=run.get("start_time", ""),
            execution_time=run.get("execution_time", 0.0),
            error=run.get("error")
        ))
    
    return {
        "seq_id": seq_id,
        "logs": logs,
        "count": len(logs),
        "latest_log": logs[-1] if logs else None
    }

@app.post("/jobs/submit", response_model=JobSubmissionResponse)
async def submit_job(job_request: JobSubmissionRequest):
    """Submit a new job for a sequence"""
    lock_data = read_lock_file()
    
    if not lock_data:
        raise HTTPException(status_code=503, detail="Blaze system is not running")
    
    if not lock_data.is_running:
        raise HTTPException(status_code=503, detail="Blaze system is not running")
    
    # Validate sequence exists
    if job_request.seq_id not in lock_data.sequences:
        raise HTTPException(
            status_code=400, 
            detail=f"Sequence '{job_request.seq_id}' not found. Available sequences: {lock_data.sequences}"
        )
    
    try:
        # Parse dates if provided
        start_date = None
        end_date = None
        
        if job_request.start_date:
            start_date = datetime.fromisoformat(job_request.start_date.replace('Z', '+00:00'))
        
        if job_request.end_date:
            end_date = datetime.fromisoformat(job_request.end_date.replace('Z', '+00:00'))
        
        # Create job submission data
        job_data = SubmitSequenceData(
            seq_id=job_request.seq_id,
            parameters=job_request.parameters,
            seq_run_interval=job_request.seq_run_interval,
            start_date=start_date,
            end_date=end_date
        )
        
        # Initialize logger
        logger = BlazeLogger(silent=True)
        
        # Submit job using BlazeJobs
        blaze_jobs = BlazeJobs(lock_path=LOCK_FILE_PATH, logger=logger)
        success = blaze_jobs.update_jobs([job_data])
        
        if success:
            return JobSubmissionResponse(
                success=True,
                message=f"Job for sequence '{job_request.seq_id}' submitted successfully"
            )
        else:
            return JobSubmissionResponse(
                success=False,
                message=f"Failed to submit job for sequence '{job_request.seq_id}'"
            )
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error submitting job: {str(e)}")

@app.get("/jobs")
async def get_current_jobs():
    """Get all currently submitted jobs"""
    jobs_data = read_jobs_file()
    
    if not jobs_data:
        return {
            "jobs": [],
            "count": 0,
            "scheduler_name": None,
            "message": "No jobs file found"
        }
    
    return {
        "jobs": [job.model_dump() for job in jobs_data.submitted_jobs],
        "count": len(jobs_data.submitted_jobs),
        "scheduler_name": jobs_data.scheduler_name,
        "last_updated": jobs_data.last_updated
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    lock_data = read_lock_file()
    
    if not lock_data:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "message": "Blaze system not running"}
        )
    
    return {
        "status": "healthy" if lock_data.is_running else "unhealthy",
        "system_name": lock_data.name,
        "is_running": lock_data.is_running,
        "is_paused": lock_data.is_paused,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/debug/system-info")
async def debug_system_info():
    """Debug endpoint to show system file information"""
    return get_system_info()

@app.get("/debug/files")
async def debug_files():
    """Debug endpoint to check file contents"""
    debug_info = {
        "lock_file": None,
        "jobs_file": None,
        "log_directories": [],
        "error_messages": []
    }
    
    # Try to read lock file
    try:
        if os.path.exists(LOCK_FILE_PATH):
            with open(LOCK_FILE_PATH, "r") as f:
                debug_info["lock_file"] = json.load(f)
        else:
            debug_info["error_messages"].append(f"Lock file not found: {LOCK_FILE_PATH}")
    except Exception as e:
        debug_info["error_messages"].append(f"Error reading lock file: {str(e)}")
    
    # Try to read jobs file
    try:
        if os.path.exists(JOBS_FILE_PATH):
            with open(JOBS_FILE_PATH, "r") as f:
                debug_info["jobs_file"] = json.load(f)
        else:
            debug_info["error_messages"].append(f"Jobs file not found: {JOBS_FILE_PATH}")
    except Exception as e:
        debug_info["error_messages"].append(f"Error reading jobs file: {str(e)}")
    
    # List log directories
    try:
        if os.path.exists(LOG_DIR):
            for item in os.listdir(LOG_DIR):
                item_path = os.path.join(LOG_DIR, item)
                if os.path.isdir(item_path):
                    debug_info["log_directories"].append({
                        "name": item,
                        "has_state": os.path.exists(os.path.join(item_path, "state.json")),
                        "has_runs": os.path.exists(os.path.join(item_path, "runs.json"))
                    })
        else:
            debug_info["error_messages"].append(f"Log directory not found: {LOG_DIR}")
    except Exception as e:
        debug_info["error_messages"].append(f"Error reading log directory: {str(e)}")
    
    return debug_info

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 