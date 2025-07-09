from src.core.logger import BlazeLogger
from src.core._types import SubmitSequenceData, JobFile, BlazeLock
from typing import List, Optional, TYPE_CHECKING
import os
import json
from datetime import datetime

if TYPE_CHECKING:
    from src.db.mongo import BlazeMongoClient

class BlazeJobs:

    def __init__(self, lock_path: str, logger: BlazeLogger, mongo_client: Optional['BlazeMongoClient'] = None):

        self.lock_path = lock_path
        self.logger = logger
        self.mongo_client = mongo_client

        #check if lock file exists
        if os.path.exists(self.lock_path):
            with open(self.lock_path, "r") as f:
                    self.blaze_lock: BlazeLock = BlazeLock.model_validate_json(f.read())
                    if not self.blaze_lock.is_running:
                        raise ValueError(f"Scheduler is not running: {self.blaze_lock.name} | Paused: {self.blaze_lock.is_paused} | Stopped: {self.blaze_lock.is_stopped}")
        else:
            raise ValueError(f"Lock file not found: {self.lock_path} | Please start the scheduler first")



    def update_jobs(self, submitted_jobs: List[SubmitSequenceData]):
        if os.path.exists(self.blaze_lock.job_file_path):
            with open(self.blaze_lock.job_file_path, "r") as f:
                read_file = json.loads(f.read())
                job_file = JobFile(**read_file)
                if job_file.scheduler_name != self.blaze_lock.name:
                    raise ValueError(f"Scheduler name mismatch: {job_file.scheduler_name} != {self.blaze_lock.name}")
                job_file.submitted_jobs.extend(submitted_jobs)
                job_file.last_updated = datetime.now()
        else:
            job_file = JobFile(
                scheduler_name=self.blaze_lock.name,
                submitted_jobs=submitted_jobs,
            )
        try:
            with open(self.blaze_lock.job_file_path, "w") as f:
                json.dump(job_file.model_dump(mode='json'), f)
            
            # Also save to MongoDB if available
            if self.mongo_client:
                try:
                    self.mongo_client.create_job_file(job_file)
                except Exception as mongo_e:
                    self.logger.warning(f"Failed to save job file to MongoDB: {mongo_e}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error creating job file: {e}")
            raise e
        return False
        