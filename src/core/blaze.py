from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core.namegen import generate_scheduler_name
from src.core._types import SubmitSequenceData, JobFile, JobExecutuionData, BlazeLock
from typing import List
import os, multiprocessing, json, time, logging, datetime
from datetime import datetime
import croniter
from src.core.logger import BlazeLogger

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

        self.logger.info(f"Blaze {self.name} initialized")
        self.logger.info(f"Blaze running on pid:{os.getpid()} | Initialisation Completed")

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
            with open(self.job_file_path, "r") as f:
                data = f.read()
                json_data = json.loads(data)
                print(json_data)
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
    
    def validate_jobs(self, submitted_jobs: List[SubmitSequenceData]) -> List[JobExecutuionData]:
        all_job_execution_data: List[JobExecutuionData] = []
        for job in submitted_jobs:
            seq_data = self._sequences.get_seq(job.seq_id)
            if job.end_date and job.end_date < datetime.now():
                raise ValueError(f"Sequence {job.seq_id} end date is in the past")
            if job.start_date and job.end_date and job.start_date > job.end_date:
                raise ValueError(f"Sequence {job.seq_id} start date is after end date")
            execution_func = seq_data.sequence_func
            job_execution_data = JobExecutuionData(
                seq_id=job.seq_id,
                parameters=job.parameters,
                seq_run_interval=job.seq_run_interval,
                start_date=job.start_date,
                end_date=job.end_date,
                execution_func=execution_func,
            )
            all_job_execution_data.append(job_execution_data)
        return all_job_execution_data
    
    def get_next_run(self, job_execution_data: JobExecutuionData) -> datetime:
        cron_iter = croniter.croniter(job_execution_data.seq_run_interval, job_execution_data.start_date)
        return cron_iter.get_next(datetime)

    def scheduler_loop(self):
        while self.is_running:
            self.read_and_verify_lock()
            submitted_jobs = self.read_jobs()
            job_execution_data = self.validate_jobs(submitted_jobs)
            if len(job_execution_data) == 0:
                self.logger.info(f"No jobs to execute | Sleeping for {self.loop_interval} seconds")
                time.sleep(self.loop_interval)
                continue
            for job in job_execution_data:
                self.logger.info(f"Executing job {job.seq_id} with parameters next running at {self.get_next_run(job)}")
            time.sleep(self.loop_interval)
