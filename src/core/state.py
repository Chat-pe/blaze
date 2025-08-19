import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from pydantic import BaseModel, Field
import croniter
from pathlib import Path
from ._types import SubmitSequenceData, JobExecutuionData, SequenceResult, SequenceStatus, JobState
from tabulate import tabulate
import pandas as pd
import numpy as np

if TYPE_CHECKING:
    from src.db.mongo import BlazeMongoClient

class BlazeState:
    """
    Manages the state of Blaze jobs and their execution history.
    Keeps track of next run times, past executions, and results.
    """
    
    def __init__(self, state_dir: str = "./log", mongo_client: Optional['BlazeMongoClient'] = None):
        """
        Initialize the state manager with a directory for storing state data.
        
        Args:
            state_dir (str): Directory to store state files
            mongo_client (Optional[BlazeMongoClient]): MongoDB client for backup
        """
        self.state_dir = state_dir
        self.job_states: Dict[str, JobState] = {}
        self.mongo_client = mongo_client
        self._ensure_state_directory()
    
    def _ensure_state_directory(self):
        """Ensure the state directory exists."""
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
    
    
    def _ensure_sequence_directory(self, seq_id: str):
        """Ensure the sequence-specific directory exists."""
        seq_dir = os.path.join(self.state_dir, seq_id)
        Path(seq_dir).mkdir(parents=True, exist_ok=True)
        return seq_dir
    
    def _clean_nat_values(self, obj):

        if obj is None:
            return None
        elif isinstance(obj, (np.ndarray, pd.Series)):
            if len(obj) == 0:
                return []
            elif len(obj) == 1:
                return self._clean_nat_values(obj[0])
            else:
                # Convert to list and clean each element
                return [self._clean_nat_values(item) for item in obj.tolist()]
        elif isinstance(obj, dict):
            return {k: self._clean_nat_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_nat_values(item) for item in obj]
        elif pd.isna(obj) or (hasattr(pd, 'NaT') and obj is pd.NaT):
            return None
        elif hasattr(obj, '__class__') and 'NaTType' in str(obj.__class__):
            return None
        else:
            return obj
    
    def _clean_for_json(self, obj):
        
        if obj is None:
            return None
        elif isinstance(obj, (np.ndarray, pd.Series)):
            # Handle numpy arrays and pandas Series
            if len(obj) == 0:
                return []
            elif len(obj) == 1:
                return self._clean_for_json(obj[0])
            else:
                # Convert to list and clean each element
                return [self._clean_for_json(item) for item in obj.tolist()]
        elif isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_for_json(item) for item in obj]
        elif pd.isna(obj) or (hasattr(pd, 'NaT') and obj is pd.NaT):
            return None
        elif isinstance(obj, (pd.NaTType, type(pd.NaT))):
            return None
        elif hasattr(obj, '__class__') and 'NaTType' in str(obj.__class__):
            return None
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, int)):
            return int(obj)
        elif isinstance(obj, (np.floating, float)):
            return float(obj)
        elif hasattr(obj, 'isoformat'):  # datetime-like objects
            return obj.isoformat()
        else:
            return obj
    
    def get_sequence_state(self, job_id: str) -> Optional[JobState]:
        """
        Get the current state for a sequence.
        
        Args:
            job_id (str): Job ID
            
        Returns:
            Optional[JobState]: The job state if it exists, None otherwise
        """
        return self.job_states.get(job_id)
    
    def print_state(self, loop_interval: int, blaze_name: str, pid: int):

        def seconds_to_human_readable(total_seconds):
            """
            Convert seconds to human-readable time format
            """
            if total_seconds < 60:
                return f"{int(total_seconds)}s" if total_seconds > 3 else "NOW"
            elif total_seconds < 3600:  # Less than 1 hour
                minutes = total_seconds // 60
                return f"~{int(minutes)} mins" if minutes > 1 else "~1 min"
            elif total_seconds < 86400:  # Less than 1 day
                hours = total_seconds // 3600
                return f"~{int(hours)} hrs" if hours > 1 else "~1 hr"
            elif total_seconds < 604800:  # Less than 1 week
                days = total_seconds // 86400
                return f"~{int(days)} days" if days > 1 else "~1 day"
            elif total_seconds < 2629746:  # Less than 1 month (30.44 days average)
                weeks = total_seconds // 604800
                return f"~{int(weeks)} weeks" if weeks > 1 else "~1 week"
            elif total_seconds < 31556952:  # Less than 1 year (365.24 days)
                months = total_seconds // 2629746
                return f"~{int(months)} months" if months > 1 else "~1 month"
            else:
                years = total_seconds // 31556952
                return f"~{int(years)} years" if years > 1 else "~1 year"

        statement1 = f"Blaze {blaze_name} - PID {pid} \nThere are {len(self.job_states)} jobs in the state"
        statement2 = f"The state will be next updated in {(datetime.now() + timedelta(seconds=loop_interval)).strftime('%D %H:%M:%S')}"
        headers = ["Job ID", "Seq ID", "Last Run", "Next Run", "Status", "Total Execution Time", "Total Runs", "Avg Execution Time"]
        data = []
        for job in self.job_states.values():
            next_run = ((job.next_run - datetime.now()).total_seconds() if job.next_run else 0)
            next_run_human = seconds_to_human_readable(next_run)
            data.append([job.job_id[:3]+"..."+job.job_id[-5:], job.seq_id, job.last_run.strftime('%D %H:%M:%S') if job.last_run else "N/A", f"{job.next_run.strftime('%D %H:%M:%S')} <({next_run_human})", job.run_state, f"{job.total_execution_time:.4f}s", job.total_runs, f"{job.total_execution_time/job.total_runs:.4f}s" if job.total_runs > 0 else "N/A"])
        statement3 = tabulate(data, headers=headers, tablefmt="grid")


    
    def add_job(self, job: JobState) -> JobState:
        """
        Add a new job to the state or update an existing one.
        
        Args:
            job (JobState): The job data
            
        Returns:
            JobState: The updated job state
        """
        try:
            if not job.job_id in self.job_states.keys():
                start_date = job.start_date if job.start_date and job.start_date > datetime.now() else datetime.now()
                cron_iter = croniter.croniter(job.seq_run_interval, start_date)
                next_run = cron_iter.get_next(datetime)
                
                job = JobState(
                    job_id=job.job_id,
                    seq_id=job.seq_id,
                    parameters=job.parameters,
                    seq_run_interval=job.seq_run_interval,
                    start_date=job.start_date,
                    end_date=job.end_date,
                    execution_func=job.execution_func,
                    run_state=SequenceStatus.PENDING,
                    last_run=None,
                    next_run=next_run,
                    latest_result=None,
                    error_logs=[],
                    total_execution_time=0.0,
                    total_runs=int(0),
                    retries=0,
                    retry_delay=0,
                    seq_run_timeout=300
                )

            self.job_states[job.job_id] = job
            self._save_job_state(job.job_id)
            return job
        except Exception as e:
            raise e
    
    def update_next_run(self, job_id: str, update_time: bool = True) -> Optional[datetime]:
        """
        Update the next run time for a sequence based on its cron schedule.
        
        Args:
            seq_id (str): Sequence ID
            update_time (bool): Whether to update the time or just return the next run
            
        Returns:
            Optional[datetime]: The next run time if the sequence exists, None otherwise
        """
        if job_id not in self.job_states:
            raise ValueError(f"Sequence {job_id} not found")
            
        seq_data = self.job_states[job_id]
        base_time = datetime.now()
        
        if seq_data.end_date and base_time > seq_data.end_date:
            return None
            
        cron_iter = croniter.croniter(seq_data.seq_run_interval, base_time)
        next_run = cron_iter.get_next(datetime)
        
        if update_time:
            seq_data.next_run = next_run
            # Ensure data types are clean before saving
            seq_data.total_runs = int(seq_data.total_runs)
            seq_data.total_execution_time = float(seq_data.total_execution_time)
            seq_data.retries = int(seq_data.retries)
            seq_data.retry_delay = int(seq_data.retry_delay)
            seq_data.seq_run_timeout = int(seq_data.seq_run_timeout)
            self._save_job_state(job_id)
            
        return next_run
    
    def _update_next_run_without_save(self, job_id: str) -> Optional[datetime]:
        if job_id not in self.job_states:
            raise ValueError(f"Sequence {job_id} not found")
            
        seq_data = self.job_states[job_id]
        base_time = datetime.now()
        
        if seq_data.end_date and base_time > seq_data.end_date:
            # Job has expired
            return None
            
        cron_iter = croniter.croniter(seq_data.seq_run_interval, base_time)
        next_run = cron_iter.get_next(datetime)
        
        # Just update the next_run time without saving
        seq_data.next_run = next_run
        
        return next_run
    
    def get_due_jobs(self) -> List[JobState]:
        now = datetime.now()
        due_jobs = []
        
        for job_id, job_state in self.job_states.items():
            if job_state.next_run and job_state.next_run <= now:
                if job_state.end_date and now > job_state.end_date:
                    continue
                due_jobs.append(job_state)
                
        return due_jobs
    
    def record_execution(self, job_id: str, seq_id: str, execution_result: Dict[str, Any], 
                         status: SequenceStatus, execution_time: float, 
                         error: Optional[str] = None) -> JobState:
        try:
            now = datetime.now()
            
            if job_id not in self.job_states:
                raise ValueError(f"Sequence {job_id} not found")

                
            job_data = self.job_states[job_id]
            job_data.run_state = status
            job_data.latest_result = execution_result
            job_data.total_execution_time += execution_time
            current_runs = job_data.total_runs if job_data.total_runs is not None else 0
            job_data.total_runs = int(current_runs) + 1

            if error:
                if not job_data.error_logs:
                    job_data.error_logs = []    
                job_data.error_logs.append(f"{now.isoformat()}: {error}")
                
            if execution_time < 0:
                execution_time = 0.0
            elif execution_time > 86400:  
                execution_time = 86400.0
                
            start_time = now - timedelta(seconds=int(execution_time))
            job_data.last_run = start_time
            
            job_data.next_run = now
            
            self._update_next_run_without_save(job_id)
            
            self._append_execution_record(job_id, {
                "status": status.value if hasattr(status, "value") else str(status),
                "job_id": job_id,
                "start_time": job_data.last_run.isoformat() if job_data.last_run else None,
                "execution_time": execution_time,
                "error": error
            })
            
            self._save_job_state(job_id)
            return job_data
        except Exception as e:
            raise e
            
    
    def _save_job_state(self, job_id: str):
        job_dir = self._ensure_sequence_directory(job_id)
        state_file = os.path.join(job_dir, "state.json")
        
        with open(state_file, "w") as f:
            job_data = self.job_states[job_id]
           
            try:
               
                if job_data.latest_result is not None:
                    job_data.latest_result = self._clean_nat_values(job_data.latest_result)
                if job_data.parameters is not None:
                    job_data.parameters = self._clean_nat_values(job_data.parameters)
                if job_data.error_logs is not None:
                    job_data.error_logs = self._clean_nat_values(job_data.error_logs)
                    
            except Exception as e:
                raise e
            
            try:
                job_dict = job_data.model_dump()
                
                # Then try with mode='json'
                job_dict_json = job_data.model_dump(mode='json')
                job_dict = job_dict_json
                
            except Exception as e:
                try:
                    # Try to serialize each field individually
                    for field_name, field_value in job_data.__dict__.items():
                        try:
                            if hasattr(job_data, field_name):
                                field_info = job_data.model_fields.get(field_name, None)
                        except Exception as field_error:
                            print(f"DEBUG: Error inspecting field {field_name}: {field_error}")
                except Exception as inspect_error:
                    print(f"DEBUG: Error during field inspection: {inspect_error}")
                
                try:
                    
                    manual_dict = {
                        'job_id': job_data.job_id,
                        'seq_id': job_data.seq_id,
                        'total_runs': int(job_data.total_runs) if job_data.total_runs is not None else 0,
                        'total_execution_time': float(job_data.total_execution_time) if job_data.total_execution_time is not None else 0.0,
                        'retries': int(job_data.retries) if job_data.retries is not None else 0,
                        'retry_delay': int(job_data.retry_delay) if job_data.retry_delay is not None else 0,
                        'seq_run_timeout': int(job_data.seq_run_timeout) if job_data.seq_run_timeout is not None else 300,
                        'run_state': job_data.run_state.value if hasattr(job_data.run_state, 'value') else str(job_data.run_state),
                        'last_run': job_data.last_run.isoformat() if job_data.last_run else None,
                        'next_run': job_data.next_run.isoformat() if job_data.next_run else None,
                        'latest_result': self._clean_for_json(job_data.latest_result),
                        'error_logs': self._clean_for_json(job_data.error_logs),
                        'parameters': self._clean_for_json(job_data.parameters),
                        'seq_run_interval': job_data.seq_run_interval,
                        'start_date': job_data.start_date.isoformat() if job_data.start_date else None,
                        'end_date': job_data.end_date.isoformat() if job_data.end_date else None,
                    }
                    job_dict = manual_dict
                except Exception as e:
                    raise e
            json.dump(job_dict, f, indent=2)
        
        # Also save to MongoDB if available
        if self.mongo_client:
            try:
                self.mongo_client.create_job_state(job_data)
            except Exception:
                # Don't fail if MongoDB write fails
                pass
    
    def _append_execution_record(self, job_id: str, record: Dict[str, Any]):
        """
        Append an execution record to the sequence's run log.
        
        Args:
            seq_id (str): Sequence ID
            record (Dict[str, Any]): Execution record
        """
        job_dir = self._ensure_sequence_directory(job_id)
        run_file = os.path.join(job_dir, "runs.json")
        
        # Load existing records or create new array
        if os.path.exists(run_file):
            try:
                with open(run_file, "r") as f:
                    runs = json.load(f)
            except json.JSONDecodeError:
                runs = []
        else:
            runs = []
            
        # Append new record
        runs.append(record)
        
        # Save updated records
        with open(run_file, "w") as f:
            json.dump(runs, f, indent=2)
        
        # Also save to MongoDB if available
        if self.mongo_client:
            try:
                self.mongo_client.create_job_run(job_id, record)
            except Exception:
                # Don't fail if MongoDB write fails
                pass
