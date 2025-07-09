import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from pydantic import BaseModel, Field
import croniter
from pathlib import Path
from src.core._types import SubmitSequenceData, SequenceData, JobExecutuionData, SequenceResult, SequenceStatus, JobState, JobState
from tabulate import tabulate

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
    
    def get_sequence_state(self, job_id: str) -> Optional[SequenceData]:
        """
        Get the current state for a sequence.
        
        Args:
            seq_id (str): Sequence ID
            
        Returns:
            Optional[SequenceData]: The sequence state if it exists, None otherwise
        """
        return self.job_states.get(job_id)
    
    def print_state(self, loop_interval: int, blaze_name: str, pid: int):
        """
        Print the state of all jobs.

        First print how many jobs are in the state
        Then the time state will be next updated loop_interval + time now
        then a table with job seq id, last run(Parsed into date and time), next run(like <12m), status
        the whole thing should rewrite the screen
        """

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
        # Clear screen and move cursor to top
        print("\033[2J\033[H", end="")
        print(f"{statement1}\n{statement2}\n{statement3}\n")


    
    def add_job(self, job: JobState) -> SequenceData:
        """
        Add a new job to the state or update an existing one.
        
        Args:
            job (JobState): The job data
            
        Returns:
            SequenceData: The updated sequence state
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
                    total_runs=0
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
            # Job has expired
            return None
            
        cron_iter = croniter.croniter(seq_data.seq_run_interval, base_time)
        next_run = cron_iter.get_next(datetime)
        
        if update_time:
            seq_data.next_run = next_run
            self._save_job_state(job_id)
            
        return next_run
    
    def get_due_jobs(self) -> List[JobState]:
        """
        Get all jobs that are due to run now.
        
        Returns:
            List[JobState]: List of jobs due to run
        """
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
        """
        Record the execution of a sequence.
        
        Args:
            seq_id (str): Sequence ID
            execution_result (Dict[str, Any]): The result of the execution
            status (SequenceStatus): The status of the execution
            execution_time (float): The time taken for execution in seconds
            error (Optional[str]): Error message if the execution failed
        """
        try:
            now = datetime.now()
            
            if job_id not in self.job_states:
                raise ValueError(f"Sequence {job_id} not found")

                
            job_data = self.job_states[job_id]
            job_data.run_state = status
            job_data.last_run = now     
            job_data.latest_result = execution_result
            job_data.total_execution_time += execution_time
            job_data.total_runs += 1

            if error:
                if not job_data.error_logs:
                    job_data.error_logs = []    
                job_data.error_logs.append(f"{now.isoformat()}: {error}")
                
            # Update next run time
            self.update_next_run(job_id)
            
            # Calculate start time
            job_data.last_run = now - timedelta(seconds=execution_time)
            
            # Create execution record
            job_data.next_run = now
            
            # Save to sequence log file
            self._append_execution_record(job_id, {
                "status": status,
                "job_id": job_id,
                "start_time": job_data.last_run.isoformat() if job_data.last_run else None,
                "execution_time": execution_time,
                "error": error
            })
            
            # Save updated state
            self._save_job_state(job_id)
            return job_data
        except Exception as e:
            print(f"Error recording execution: {str(e)}")
            raise e
    
    def _save_job_state(self, job_id: str):
        """
        Save the state of a sequence to disk.
        
        Args:
            seq_id (str): Sequence ID
        """
        job_dir = self._ensure_sequence_directory(job_id)
        state_file = os.path.join(job_dir, "state.json")
        
        with open(state_file, "w") as f:
            # Convert SequenceData to dict for serialization
            job_data = self.job_states[job_id]
            job_dict = job_data.model_dump(mode='json')
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
