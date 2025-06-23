from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core.namegen import generate_scheduler_name
from src.core._types import SubmitSequenceData, JobFile, JobExecutuionData, BlazeLock, SequenceStatus, JobState
from typing import List, Dict, Any, Optional
import os, multiprocessing, json, time, logging, hashlib
from datetime import datetime
import croniter
from src.core.logger import BlazeLogger
from src.core.state import BlazeState
from tabulate import tabulate
from concurrent.futures import ThreadPoolExecutor, as_completed

class Blaze:

    def __init__(
            self,
            blaze_blocks: BlazeBlock,
            sequences: BlazeSequence,
            logger: BlazeLogger,
            max_workers: int = 1,
            auto_start: bool = True,
            loop_interval: int = 1,
            job_file_path: str = "/tmp/scheduler_jobs.json",
            job_lock_path: str = "/tmp/scheduler_lock.json",
            state_dir: str = "./log",
    ):
        self.name = generate_scheduler_name()
        self.blocks : BlazeBlock = blaze_blocks
        self._sequences: BlazeSequence = sequences
        self.logger: BlazeLogger = logger

        max_available_processes = multiprocessing.cpu_count()
        self.max_workers: int = max_workers if max_workers and max_workers < max_available_processes else max_available_processes

        self.job_file_path: str = job_file_path
        self.job_lock_path: str = job_lock_path
        self.auto_start: bool = auto_start
        self.loop_interval: int = loop_interval

        self.is_running: bool = False
        self.is_paused: bool = False
        self.is_stopped: bool = False
        
        # Initialize state manager
        self.state_manager = BlazeState(state_dir=state_dir)

        self.logger.info(f"Blaze {self.name} initialized")
        self.logger.info(f"Blaze running on pid:{os.getpid()} | Initialisation Completed")

        self._sequences.set_logger(self.logger)

        if self.auto_start:
            self.start()

    def start(self):
        self.is_running = True
        self.is_paused = False
        self.is_stopped = False
        
        # Create job lock
        job_lock = BlazeLock(
            name=self.name,
            blocks=[block.name for block in self.blocks.get_all_blocks()],
            sequences=[seq.seq_id for seq in self._sequences.get_all_seq()],
            loop_interval=self.loop_interval,
            job_file_path=self.job_file_path,
            is_running=self.is_running,
            is_paused=self.is_paused,
            is_stopped=self.is_stopped,
            last_updated=datetime.now(),
        )
        with open(self.job_lock_path, "w") as f:
            json.dump(job_lock.model_dump(mode='json'), f)

        self.scheduler_loop()

    def stop(self, shutdown: bool = False):
        self.is_running = False
        self.is_paused = True
        self.is_stopped = shutdown

        if shutdown:
            self.remove_jobs()
            self.remove_lock()

    def remove_lock(self):  
        if os.path.exists(self.job_lock_path):
            os.remove(self.job_lock_path)

    def remove_jobs(self):
        if os.path.exists(self.job_file_path):
            os.remove(self.job_file_path)

    
    def read_jobs(self) -> List[SubmitSequenceData]:
        if os.path.exists(self.job_file_path):
            # Check if file was modified in the last {loop_interval + 5} seconds
            file_mod_time = os.path.getmtime(self.job_file_path)
            current_time = time.time()
            time_threshold = self.loop_interval + 5

            if current_time - file_mod_time <= time_threshold:
                with open(self.job_file_path, "r") as f:
                    data = f.read()
                    json_data = json.loads(data)
                    job_file = JobFile(**json_data)
                    if job_file:
                        return job_file.submitted_jobs  
        return []

    def read_and_verify_lock(self) -> BlazeLock:
        if os.path.exists(self.job_lock_path):
            with open(self.job_lock_path, "r") as f:
                data = f.read()
                json_data = json.loads(data)
                lock_data = BlazeLock(**json_data)
                if lock_data.name != self.name:
                    raise ValueError(f"Scheduler name mismatch: {lock_data.name} != {self.name}")
                if lock_data.is_paused:
                    self.stop(shutdown=False)
                if lock_data.is_stopped:
                    self.stop(shutdown=True)
                return lock_data
        return None
    
    def validate_jobs(self, submitted_jobs: List[SubmitSequenceData]):
        for job in submitted_jobs:
            seq_data = self._sequences.get_seq(job.seq_id)
            if job.end_date and job.end_date < datetime.now():
                raise ValueError(f"Sequence {job.seq_id} end date is in the past")
            if job.start_date and job.end_date and job.start_date > job.end_date:
                raise ValueError(f"Sequence {job.seq_id} start date is after end date")

    
    def get_next_run(self, job_execution_data: JobState) -> datetime:
        """
        Get the next run time for a job using the state manager.
        
        Args:
            job_execution_data (JobState): The job data
            
        Returns:
            datetime: The next run time
        """
        seq_id = job_execution_data.seq_id
        seq_state = self.state_manager.get_sequence_state(seq_id)
        
        if seq_state and seq_state.next_run:
            return seq_state.next_run
            
        # If no state exists yet or next_run is not set, calculate it
        cron_iter = croniter.croniter(job_execution_data.seq_run_interval, job_execution_data.start_date)
        next_run = cron_iter.get_next(datetime)
        return next_run


    def execute(self, job_state: JobState):
        """
        Execute a sequence function with the provided parameters.
        
        Args:
            seq_id (str): The ID of the sequence to execute
            parameters (dict, optional): Parameters to pass to the sequence function. Defaults to {}.
            
        Returns:
            dict: The execution context with results from all blocks in the sequence
        """
        execution_func = job_state.execution_func
        
        try:
            start_time = datetime.now()
            result = execution_func(job_state.job_id[:3]+"..."+job_state.job_id[-5:], job_state.parameters)
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Record successful execution in state manager
            self.state_manager.record_execution(
                job_id=job_state.job_id,
                seq_id=job_state.seq_id,
                execution_result=result,
                status=SequenceStatus.COMPLETED,
                execution_time=execution_time
            )
            
            self.logger.info(f"Run {job_state.seq_id}/{job_state.job_id} - success in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Failed to execute sequence {job_state.seq_id}: {error_msg}")
            
            # Record failed execution in state manager
            self.state_manager.record_execution(
                job_id=job_state.job_id,
                seq_id=job_state.seq_id,
                execution_result={},
                status=SequenceStatus.FAILED,
                execution_time=(datetime.now() - start_time).total_seconds(),
                error=error_msg
            )
            
            raise
            
    def _execute_job_with_state_update(self, job: JobState) -> None:
        """
        Execute a single job and update its state.
        This is a helper method for threaded execution.
        
        Args:
            job (JobState): The job to execute
        """
        try:
            self.execute(job)
        except Exception as e:
            self.logger.error(f"Error executing job {job.job_id }: {str(e)}")
            
        # Update next run time after execution
        self.state_manager.update_next_run(job.job_id)

    def check_and_add_jobs(self, submitted_jobs: List[SubmitSequenceData]):
        """
        Check if the jobs are already in the state and add them if they are not.
        """
        sub_jobs = {hashlib.sha256(str(job).encode()).hexdigest():job for job in submitted_jobs }
        sub_keys = set(sub_jobs.keys())
        state_jobs = set(self.state_manager.job_states.keys())
        new_jobs = sub_keys - state_jobs
        for job_id in new_jobs:
            self.state_manager.add_job(JobState(
                job_id=job_id,
                seq_id=sub_jobs[job_id].seq_id,
                parameters=sub_jobs[job_id].parameters,
                seq_run_interval=sub_jobs[job_id].seq_run_interval,
                start_date=sub_jobs[job_id].start_date,
                end_date=sub_jobs[job_id].end_date,
                run_state=SequenceStatus.PENDING,
                execution_func=self._sequences.get_seq(sub_jobs[job_id].seq_id).sequence_func,
            ))
            
    def scheduler_loop(self):
        while self.is_running:
            self.read_and_verify_lock()
            self.state_manager.print_state(self.loop_interval, self.name, os.getpid())
            
            # Process submitted jobs from job file
            submitted_jobs = self.read_jobs()
            if submitted_jobs:
                self.validate_jobs(submitted_jobs)
                self.check_and_add_jobs(submitted_jobs)
            
            # Check for due jobs in state manager
            due_jobs = self.state_manager.get_due_jobs()
            
            if not due_jobs:
                time.sleep(self.loop_interval)
                continue
                
            
            # Execute jobs in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all jobs to the thread pool
                future_to_job = {
                    executor.submit(self._execute_job_with_state_update, job): job 
                    for job in due_jobs
                }
                
                # Wait for all jobs to complete
                for future in as_completed(future_to_job):
                    job = future_to_job[future]
                    try:
                        future.result()  # This will raise any exception that occurred
                    except Exception as e:
                        self.logger.error(f"Job {job.seq_id} failed with error: {str(e)}")
            
            time.sleep(self.loop_interval)
