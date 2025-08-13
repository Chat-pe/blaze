from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core.namegen import generate_scheduler_name
from src.core._types import SubmitSequenceData, JobFile, BlazeLock, SequenceStatus, JobState
from typing import List, Optional
import os, multiprocessing, json, time, hashlib, signal, sys
from datetime import datetime
import croniter
from src.core.logger import BlazeLogger
from src.core.state import BlazeState
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.db.mongo import BlazeMongoClient
from src.core.sync import BlazeSync

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
            mongo_uri: Optional[str] = None,
    ):
        
        self.logger: BlazeLogger = logger
        self.logger.warning(f"Mongo URI:{bool(mongo_uri)}")
        self.name = generate_scheduler_name()
        self.blocks : BlazeBlock = blaze_blocks
        self._sequences: BlazeSequence = sequences

        max_available_processes = multiprocessing.cpu_count()
        self.max_workers: int = max_workers if max_workers and max_workers < max_available_processes else max_available_processes

        self.job_file_path: str = job_file_path
        self.job_lock_path: str = job_lock_path
        self.auto_start: bool = auto_start
        self.loop_interval: int = loop_interval

        self.is_running: bool = False
        self.is_paused: bool = False
        self.is_stopped: bool = False
        self.shutdown_requested: bool = False
        
        # Initialize MongoDB client if URI is provided
        self.mongo_client = None
        if mongo_uri:
            try:
                self.mongo_client = BlazeMongoClient(mongo_uri)
                if self.mongo_client.test_connection():
                    self.logger.info("MongoDB connection established successfully")
                    # Pass mongo client to logger
                    self.logger.set_mongo_client(self.mongo_client)
                else:
                    self.logger.warning("MongoDB connection failed, continuing without MongoDB backup")
                    self.mongo_client = None
            except Exception as e:
                self.logger.error(f"Failed to initialize MongoDB client: {str(e)}")
                self.mongo_client = None
        
        self.state_manager = BlazeState(state_dir=state_dir, mongo_client=self.mongo_client)
        self.sync = BlazeSync(
            mongo_client=self.mongo_client,
            state_manager=self.state_manager,
            job_file_path=self.job_file_path,
            job_lock_path=self.job_lock_path,
            logger=self.logger,
        )


        self.logger.info(f"Blaze {self.name} initialized")
        self.logger.info(f"Blaze running on pid:{os.getpid()} | Initialisation Completed")

        self._sequences.set_logger(self.logger)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        if self.auto_start:
            self.start()

    def _signal_handler(self, signum, frame):
        """Handle SIGINT (Ctrl+C) and SIGTERM signals for graceful shutdown."""
        if self.shutdown_requested:
            self.logger.info("Force shutdown requested. Exiting immediately.")
            sys.exit(1)
        
        self.shutdown_requested = True
        self.logger.info("Shutdown signal received. Choose shutdown type:")
        
        while True:
            try:
                print("\nShutdown Options:")
                print("1. Hard Stop - Delete all files and clear MongoDB")
                print("2. Soft Stop - Leave files intact for resume")
                print("3. Cancel - Continue running")
                
                choice = input("\nEnter your choice (1/2/3): ").strip()
                
                if choice == '1':
                    self.logger.info("Hard stop selected. Performing complete cleanup...")
                    self.hard_stop()
                    break
                elif choice == '2':
                    self.logger.info("Soft stop selected. Leaving files intact...")
                    self.soft_stop()
                    break
                elif choice == '3':
                    self.logger.info("Shutdown cancelled. Continuing operation...")
                    self.shutdown_requested = False
                    return
                else:
                    print("Invalid choice. Please enter 1, 2, or 3.")
                    
            except (EOFError, KeyboardInterrupt):
                self.soft_stop()
                self.logger.info("Force shutdown requested. Exiting immediately.")
                sys.exit(1)

    def _check_existing_state(self, intended_lock: Optional[BlazeLock] = None):
        """Check and reconcile existing state using BlazeSync."""
        try:
            resumed = self.sync.reconcile_on_start(self.name, intended_lock=intended_lock)
            if resumed:
                self.logger.info("Resuming from existing state...")
            return resumed
        except Exception as e:
            self.logger.warning(f"Failed to reconcile state on start: {e}")
            return False

    def _restore_from_mongo_lock(self, mongo_lock: BlazeLock):
        """Deprecated: Kept for compatibility; state restore handled by BlazeSync."""
        with open(self.job_lock_path, "w", encoding="utf-8") as f:
            json.dump(mongo_lock.model_dump(mode='json'), f)

    def hard_stop(self):
        """Perform hard stop: delete all files and clear MongoDB."""
        self.is_running = False
        self.is_stopped = True
        
        # Synchronous cleanup across local and Mongo
        try:
            self.sync.hard_stop_cleanup(self.name)
        except Exception as e:
            self.logger.warning(f"Hard stop cleanup encountered issues: {e}")
        
        self.logger.info("Hard stop completed. All data cleared.")
        sys.exit(0)

    def soft_stop(self):
        """Perform soft stop: leave files intact for resume."""
        self.is_running = False
        self.is_paused = True
        
        # Update lock file to indicate paused state
        if os.path.exists(self.job_lock_path):
            with open(self.job_lock_path, "r", encoding="utf-8") as f:
                lock_data = json.load(f)
                lock_data['is_paused'] = True
                lock_data['is_running'] = False
                lock_data['last_updated'] = datetime.now().isoformat()
            
            with open(self.job_lock_path, "w", encoding="utf-8") as f:
                json.dump(lock_data, f)
        
        # Update MongoDB lock if available
        if self.mongo_client:
            self.mongo_client.update_lock(self.name, {
                'is_paused': True,
                'is_running': False
            })
        
        self.logger.info("Soft stop completed. Files left intact for resume.")
        sys.exit(0)

    def _remove_log_files(self):
        """Remove all log files from the state directory."""
        if os.path.exists(self.state_manager.state_dir):
            import shutil
            shutil.rmtree(self.state_manager.state_dir)
            self.logger.info(f"Removed log directory: {self.state_manager.state_dir}")

    def start(self):
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
        # Check for existing state and resume if found
        if self._check_existing_state(job_lock):
            existing_lock = self.read_and_verify_lock(job_lock)
            if existing_lock:
                self.name = existing_lock.name
                job_lock.name = self.name
                
                if existing_lock.is_paused:
                    self.logger.info("Resuming from paused state...")
                elif existing_lock.is_stopped:
                    self.logger.info("Previous session was stopped. Starting fresh...")
                else:
                    self.logger.info("Resuming from previous session...")
        
        # Always set to running when starting/resuming
        self.is_running = True
        self.is_paused = False
        self.is_stopped = False
        
        # Update job_lock with current running state
        job_lock.is_running = True
        job_lock.is_paused = False
        job_lock.is_stopped = False
        job_lock.last_updated = datetime.now()
        
        # Save updated lock state to local file
        with open(self.job_lock_path, "w", encoding="utf-8") as f:
            json.dump(job_lock.model_dump(mode='json'), f)
        
        # Also save to MongoDB if available
        if self.mongo_client:
            self.mongo_client.create_lock(job_lock)
            
        self.logger.info(f"Scheduler {self.name} is now running (PID: {os.getpid()})")

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
                with open(self.job_file_path, "r", encoding="utf-8") as f:
                    data = f.read()
                    json_data = json.loads(data)
                    job_file = JobFile(**json_data)
                    if job_file:
                        return job_file.submitted_jobs  
        return []

    def read_and_verify_lock(self, job_lock = None) -> BlazeLock:
        if os.path.exists(self.job_lock_path):
            with open(self.job_lock_path, "r", encoding="utf-8") as f:
                data = f.read()
                json_data = json.loads(data)
                lock_data = BlazeLock(**json_data)
                if lock_data.name != self.name:
                    if job_lock:
                        if lock_data.sequences == job_lock.sequences and lock_data.blocks == job_lock.blocks and lock_data.loop_interval == job_lock.loop_interval:
                            self.name = lock_data.name 
                        else:
                            raise ValueError(f"Scheduler (Sequence, block & loop interval) mismatch: {lock_data.name} != {self.name}")
                    else:
                        self.name = lock_data.name 
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
            
            self.logger.info(f"Run {job_state.seq_id}/{job_state.job_id} - success in {execution_time:.2f} seconds", job_id=job_state.job_id)
            return result
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Failed to execute sequence {job_state.seq_id}: {error_msg}", job_id=job_state.job_id)
            
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
            self.logger.error(f"Error executing job {job.job_id }: {str(e)}", job_id=job.job_id)
            
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
        while self.is_running and not self.shutdown_requested:
            self.read_and_verify_lock()
            self.state_manager.print_state(self.loop_interval, self.name, os.getpid())
            
            # Process submitted jobs from job file
            # Ensure job file is reconciled with Mongo before reading
            try:
                self.sync.reconcile_job_file(self.name)
            except Exception:
                pass
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
                        self.logger.error(f"Job {job.seq_id} failed with error: {str(e)}", job_id=job.job_id)
            
            time.sleep(self.loop_interval)
