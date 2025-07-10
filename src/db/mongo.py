from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

from src.core._types import (
    JobFile, 
    JobState, 
    BlazeLock, 
    SubmitSequenceData,
    SequenceStatus
)


class BlazeMongoClient:
    """
    MongoDB client for Blaze system that mirrors the local JSON file storage structure.
    Provides backup and persistence for jobs, job states, execution logs, and system locks.
    """
    
    def __init__(self, connection_string: str, database_name: str = "blaze_db"):
        """
        Initialize MongoDB client with connection string.
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Name of the database to use (default: blaze_db)
        """
        self.client = MongoClient(connection_string)
        self.db: Database = self.client[database_name]
        
        # Collections corresponding to local storage structure
        self.jobs_collection: Collection = self.db["jobs"]  # JobFile data
        self.job_states_collection: Collection = self.db["job_states"]  # Individual JobState data  
        self.job_runs_collection: Collection = self.db["job_runs"]  # Execution logs
        self.locks_collection: Collection = self.db["locks"]  # BlazeLock data
        self.logs_collection: Collection = self.db["logs"]  # Application logs
        
        # Create indexes for better query performance
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for optimized querying."""
        # Jobs collection indexes
        self.jobs_collection.create_index("scheduler_name", unique=True)
        self.jobs_collection.create_index("last_updated")
        
        # Job states collection indexes  
        self.job_states_collection.create_index("job_id", unique=True)
        self.job_states_collection.create_index("run_state")
        self.job_states_collection.create_index("last_run")
        self.job_states_collection.create_index("next_run")
        
        # Job runs collection indexes
        self.job_runs_collection.create_index("job_id")
        self.job_runs_collection.create_index("timestamp")
        self.job_runs_collection.create_index([("job_id", 1), ("timestamp", -1)])  # Compound index for latest runs
        
        # Locks collection indexes
        self.locks_collection.create_index("name", unique=True)
        self.locks_collection.create_index("last_updated")
        
        # Logs collection indexes
        self.logs_collection.create_index("timestamp")
        self.logs_collection.create_index("level")
        self.logs_collection.create_index("job_id")
        self.logs_collection.create_index([("timestamp", -1), ("level", 1)])  # Compound index for time-based queries

    # === JOBS COLLECTION OPERATIONS (JobFile) ===
    
    def create_job_file(self, job_file: JobFile) -> bool:
        """
        Create or replace a JobFile document.
        
        Args:
            job_file (JobFile): The job file data to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            job_data = job_file.model_dump(mode='json')
            self.jobs_collection.replace_one(
                {"scheduler_name": job_file.scheduler_name},
                job_data,
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Error creating job file: {e}")
            return False
    
    def fetch_job_file(self, scheduler_name: str) -> Optional[JobFile]:
        """
        Fetch a JobFile by scheduler name.
        
        Args:
            scheduler_name (str): Name of the scheduler
            
        Returns:
            Optional[JobFile]: The job file if found, None otherwise
        """
        try:
            doc = self.jobs_collection.find_one({"scheduler_name": scheduler_name})
            if doc:
                # Remove MongoDB _id field before validation
                doc.pop('_id', None)
                return JobFile(**doc)
            return None
        except Exception as e:
            print(f"Error fetching job file: {e}")
            return None
    
    def update_job_file(self, scheduler_name: str, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields in a JobFile.
        
        Args:
            scheduler_name (str): Name of the scheduler
            updates (Dict[str, Any]): Fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            updates["last_updated"] = datetime.now().isoformat()
            result = self.jobs_collection.update_one(
                {"scheduler_name": scheduler_name},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating job file: {e}")
            return False
    
    def delete_job_file(self, scheduler_name: str) -> bool:
        """
        Delete a JobFile by scheduler name.
        
        Args:
            scheduler_name (str): Name of the scheduler
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.jobs_collection.delete_one({"scheduler_name": scheduler_name})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting job file: {e}")
            return False

    # === JOB STATES COLLECTION OPERATIONS (JobState) ===
    
    def create_job_state(self, job_state: JobState) -> bool:
        """
        Create or replace a JobState document.
        
        Args:
            job_state (JobState): The job state data to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            job_data = job_state.model_dump(mode='json')
            self.job_states_collection.replace_one(
                {"job_id": job_state.job_id},
                job_data,
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Error creating job state: {e}")
            return False
    
    def fetch_job_state(self, job_id: str) -> Optional[JobState]:
        """
        Fetch a JobState by job ID.
        
        Args:
            job_id (str): The job ID
            
        Returns:
            Optional[JobState]: The job state if found, None otherwise
        """
        try:
            doc = self.job_states_collection.find_one({"job_id": job_id})
            if doc:
                doc.pop('_id', None)
                return JobState(**doc)
            return None
        except Exception as e:
            print(f"Error fetching job state: {e}")
            return None
    
    def fetch_all_job_states(self, status_filter: Optional[SequenceStatus] = None) -> List[JobState]:
        """
        Fetch all job states, optionally filtered by status.
        
        Args:
            status_filter (Optional[SequenceStatus]): Filter by specific status
            
        Returns:
            List[JobState]: List of job states
        """
        try:
            query = {}
            if status_filter:
                query["run_state"] = status_filter.value
                
            docs = self.job_states_collection.find(query)
            job_states = []
            for doc in docs:
                doc.pop('_id', None)
                job_states.append(JobState(**doc))
            return job_states
        except Exception as e:
            print(f"Error fetching job states: {e}")
            return []
    
    def update_job_state(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields in a JobState.
        
        Args:
            job_id (str): The job ID
            updates (Dict[str, Any]): Fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.job_states_collection.update_one(
                {"job_id": job_id},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating job state: {e}")
            return False
    
    def delete_job_state(self, job_id: str) -> bool:
        """
        Delete a JobState by job ID.
        
        Args:
            job_id (str): The job ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.job_states_collection.delete_one({"job_id": job_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting job state: {e}")
            return False

    # === JOB RUNS COLLECTION OPERATIONS (Execution Logs) ===
    
    def create_job_run(self, job_id: str, run_record: Dict[str, Any]) -> bool:
        """
        Create a job run record (execution log).
        
        Args:
            job_id (str): The job ID
            run_record (Dict[str, Any]): The execution record
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Add metadata to the run record
            run_data = {
                "job_id": job_id,
                "timestamp": datetime.now().isoformat(),
                **run_record
            }
            self.job_runs_collection.insert_one(run_data)
            return True
        except Exception as e:
            print(f"Error creating job run: {e}")
            return False
    
    def fetch_job_runs(self, job_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch job run records for a specific job.
        
        Args:
            job_id (str): The job ID
            limit (Optional[int]): Maximum number of records to return
            
        Returns:
            List[Dict[str, Any]]: List of run records
        """
        try:
            query = {"job_id": job_id}
            cursor = self.job_runs_collection.find(query).sort("timestamp", -1)
            
            if limit:
                cursor = cursor.limit(limit)
                
            runs = []
            for doc in cursor:
                doc.pop('_id', None)
                runs.append(doc)
            return runs
        except Exception as e:
            print(f"Error fetching job runs: {e}")
            return []
    
    def delete_job_runs(self, job_id: str) -> bool:
        """
        Delete all run records for a specific job.
        
        Args:
            job_id (str): The job ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.job_runs_collection.delete_many({"job_id": job_id})
            return result.deleted_count >= 0
        except Exception as e:
            print(f"Error deleting job runs: {e}")
            return False

    # === LOCKS COLLECTION OPERATIONS (BlazeLock) ===
    
    def create_lock(self, lock: BlazeLock) -> bool:
        """
        Create or replace a BlazeLock document.
        
        Args:
            lock (BlazeLock): The lock data to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            lock_data = lock.model_dump(mode='json')
            self.locks_collection.delete_many({})
            self.locks_collection.insert_one(
                lock_data
            )
            return True
        except Exception as e:
            print(f"Error creating lock: {e}")
            return False
    
    def fetch_lock(self, name: str) -> Optional[BlazeLock]:
        """
        Fetch a BlazeLock by name.
        
        Args:
            name (str): Name of the lock
            
        Returns:
            Optional[BlazeLock]: The lock if found, None otherwise
        """
        try:
            doc = self.locks_collection.find_one({"name": name})
            if doc:
                doc.pop('_id', None)
                return BlazeLock(**doc)
            return None
        except Exception as e:
            print(f"Error fetching lock: {e}")
            return None
    
    def fetch_all_locks(self) -> List[BlazeLock]:
        """
        Fetch all BlazeLock documents.
        
        Returns:
            List[BlazeLock]: List of all locks
        """
        try:
            docs = self.locks_collection.find()
            locks = []
            for doc in docs:
                doc.pop('_id', None)
                locks.append(BlazeLock(**doc))
            return locks
        except Exception as e:
            print(f"Error fetching locks: {e}")
            return []
    
    def update_lock(self, name: str, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields in a BlazeLock.
        
        Args:
            name (str): Name of the lock
            updates (Dict[str, Any]): Fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            updates["last_updated"] = datetime.now().isoformat()
            result = self.locks_collection.update_one(
                {"name": name},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating lock: {e}")
            return False
    
    def delete_lock(self, name: str) -> bool:
        """
        Delete a BlazeLock by name.
        
        Args:
            name (str): Name of the lock
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.locks_collection.delete_one({"name": name})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting lock: {e}")
            return False

    # === LOGS COLLECTION OPERATIONS ===
    
    def create_log(self, level: str, message: str, job_id: Optional[str] = None, 
                   module: Optional[str] = None, extra_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create a log entry.
        
        Args:
            level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message (str): Log message
            job_id (Optional[str]): Associated job ID if applicable
            module (Optional[str]): Module/component that generated the log
            extra_data (Optional[Dict[str, Any]]): Additional log data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "level": level.upper(),
                "message": message,
                "job_id": job_id,
                "module": module,
                "extra_data": extra_data or {}
            }
            self.logs_collection.insert_one(log_entry)
            return True
        except Exception as e:
            print(f"Error creating log entry: {e}")
            return False
    
    def fetch_logs(self, job_id: Optional[str] = None, level: Optional[str] = None,
                   module: Optional[str] = None, limit: Optional[int] = 100,
                   start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch log entries with optional filtering.
        
        Args:
            job_id (Optional[str]): Filter by job ID
            level (Optional[str]): Filter by log level
            module (Optional[str]): Filter by module
            limit (Optional[int]): Maximum number of records to return (default: 100)
            start_time (Optional[datetime]): Start time for filtering
            end_time (Optional[datetime]): End time for filtering
            
        Returns:
            List[Dict[str, Any]]: List of log entries
        """
        try:
            query = {}
            
            if job_id:
                query["job_id"] = job_id
            if level:
                query["level"] = level.upper()
            if module:
                query["module"] = module
                
            # Time range filtering
            if start_time or end_time:
                time_query = {}
                if start_time:
                    time_query["$gte"] = start_time.isoformat()
                if end_time:
                    time_query["$lte"] = end_time.isoformat()
                query["timestamp"] = time_query
            
            cursor = self.logs_collection.find(query).sort("timestamp", -1)
            
            if limit:
                cursor = cursor.limit(limit)
                
            logs = []
            for doc in cursor:
                doc.pop('_id', None)
                logs.append(doc)
            return logs
        except Exception as e:
            print(f"Error fetching logs: {e}")
            return []
    
    def fetch_recent_logs(self, minutes: int = 60, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch recent log entries within specified time window.
        
        Args:
            minutes (int): Number of minutes to look back (default: 60)
            level (Optional[str]): Filter by log level
            
        Returns:
            List[Dict[str, Any]]: List of recent log entries
        """
        try:
            start_time = datetime.now() - timedelta(minutes=minutes)
            return self.fetch_logs(start_time=start_time, level=level)
        except Exception as e:
            print(f"Error fetching recent logs: {e}")
            return []
    
    def delete_logs(self, job_id: Optional[str] = None, older_than: Optional[datetime] = None) -> bool:
        """
        Delete log entries based on criteria.
        
        Args:
            job_id (Optional[str]): Delete logs for specific job ID
            older_than (Optional[datetime]): Delete logs older than specified time
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            query = {}
            
            if job_id:
                query["job_id"] = job_id
            if older_than:
                query["timestamp"] = {"$lt": older_than.isoformat()}
                
            if not query:
                return False  # Prevent deleting all logs accidentally
                
            result = self.logs_collection.delete_many(query)
            return result.deleted_count >= 0
        except Exception as e:
            print(f"Error deleting logs: {e}")
            return False
    
    def get_log_stats(self) -> Dict[str, Any]:
        """
        Get statistics about log entries.
        
        Returns:
            Dict[str, Any]: Log statistics including counts by level
        """
        try:
            total_logs = self.logs_collection.count_documents({})
            
            # Count by level
            level_counts = {}
            for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                count = self.logs_collection.count_documents({"level": level})
                if count > 0:
                    level_counts[level] = count
            
            # Recent logs count (last 24 hours)
            recent_time = datetime.now() - timedelta(hours=24)
            recent_count = self.logs_collection.count_documents({
                "timestamp": {"$gte": recent_time.isoformat()}
            })
            
            return {
                "total_logs": total_logs,
                "level_counts": level_counts,
                "recent_logs_24h": recent_count
            }
        except Exception as e:
            print(f"Error getting log stats: {e}")
            return {"total_logs": 0, "level_counts": {}, "recent_logs_24h": 0}

    # === UTILITY METHODS ===
    
    def close(self):
        """Close the MongoDB connection."""
        self.client.close()
    
    def test_connection(self) -> bool:
        """
        Test the MongoDB connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def get_collection_stats(self) -> Dict[str, int]:
        """
        Get document counts for all collections.
        
        Returns:
            Dict[str, int]: Document counts by collection name
        """
        try:
            return {
                "jobs": self.jobs_collection.count_documents({}),
                "job_states": self.job_states_collection.count_documents({}),
                "job_runs": self.job_runs_collection.count_documents({}),
                "locks": self.locks_collection.count_documents({}),
                "logs": self.logs_collection.count_documents({})
            }
        except Exception as e:
            print(f"Error getting collection stats: {e}")
            return {"jobs": 0, "job_states": 0, "job_runs": 0, "locks": 0, "logs": 0}
