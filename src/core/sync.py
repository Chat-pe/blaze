from __future__ import annotations

from typing import Optional, Dict, TYPE_CHECKING
import os
import json
from datetime import datetime

from src.core._types import BlazeLock, JobFile, JobState
from pymongo.errors import PyMongoError

if TYPE_CHECKING:
    from src.db.mongo import BlazeMongoClient
    from src.core.state import BlazeState
    from src.core.logger import BlazeLogger


class BlazeSync:
    """
    Keeps local storage state (lock, job file, job states) in sync with MongoDB.
    Also provides a synchronous hard-stop cleanup across both stores.
    """

    def __init__(
        self,
        mongo_client: Optional["BlazeMongoClient"],
        state_manager: "BlazeState",
        job_file_path: str,
        job_lock_path: str,
        logger: "BlazeLogger",
    ) -> None:
        self.mongo_client = mongo_client
        self.state_manager = state_manager
        self.job_file_path = job_file_path
        self.job_lock_path = job_lock_path
        self.logger = logger

    # ---------------------------
    # Reconcile helpers
    # ---------------------------
    def _read_local_lock(self) -> Optional[BlazeLock]:
        if not os.path.exists(self.job_lock_path):
            return None
        try:
            with open(self.job_lock_path, "r", encoding="utf-8") as f:
                return BlazeLock(**json.load(f))
        except (OSError, ValueError, json.JSONDecodeError) as e:
            self.logger.warning(f"Failed to read local lock: {e}")
            return None

    def _write_local_lock(self, lock: BlazeLock) -> None:
        with open(self.job_lock_path, "w", encoding="utf-8") as f:
            json.dump(lock.model_dump(mode='json'), f)

    def _read_local_job_file(self) -> Optional[JobFile]:
        if not os.path.exists(self.job_file_path):
            return None
        try:
            with open(self.job_file_path, "r", encoding="utf-8") as f:
                return JobFile(**json.load(f))
        except (OSError, ValueError, json.JSONDecodeError) as e:
            self.logger.warning(f"Failed to read local job file: {e}")
            return None

    def _write_local_job_file(self, job_file: JobFile) -> None:
        with open(self.job_file_path, "w", encoding="utf-8") as f:
            json.dump(job_file.model_dump(mode='json'), f)

    def _choose_newer_lock(self, a: Optional[BlazeLock], b: Optional[BlazeLock]) -> Optional[BlazeLock]:
        def _as_dt(val: Optional[datetime]) -> Optional[datetime]:
            # BlazeLock.last_updated is expected to be datetime already
            if isinstance(val, datetime):
                return val
            return None

        if a and b:
            a_dt = _as_dt(a.last_updated)
            b_dt = _as_dt(b.last_updated)
            if a_dt and b_dt:
                return a if a_dt >= b_dt else b
            if a_dt and not b_dt:
                return a
            if b_dt and not a_dt:
                return b
            return b
        return a or b

    def _choose_newer_job_file(self, a: Optional[JobFile], b: Optional[JobFile]) -> Optional[JobFile]:
        def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
            if not ts:
                return None
            try:
                return datetime.fromisoformat(ts)
            except ValueError:
                return None

        if a and b:
            a_time = _parse_iso(a.last_updated)
            b_time = _parse_iso(b.last_updated)
            if a_time and b_time:
                return a if a_time >= b_time else b
            if a_time and not b_time:
                return a
            if b_time and not a_time:
                return b
            return b
        return a or b

    def _choose_better_job_state(self, local: Optional[JobState], remote: Optional[JobState]) -> Optional[JobState]:
        if local and remote:
            # Prefer the one with higher total_runs; tie-breaker with later last_run
            if local.total_runs != remote.total_runs:
                return local if local.total_runs > remote.total_runs else remote
            local_last = local.last_run
            remote_last = remote.last_run
            if local_last and remote_last:
                return local if local_last >= remote_last else remote
            if local_last and not remote_last:
                return local
            if remote_last and not local_last:
                return remote
            # Fallback: prefer remote (Mongo)
            return remote
        return local or remote

    # ---------------------------
    # Public sync API
    # ---------------------------
    def reconcile_on_start(self, scheduler_name: Optional[str] = None, intended_lock: Optional[BlazeLock] = None) -> bool:
        """
        Pull records from MongoDB and overwrite local files on startup.
        Returns True if a prior remote state was found, else False.
        """
        if not self.mongo_client:
            return os.path.exists(self.job_lock_path)

        # Determine the authoritative remote lock
        remote_lock: Optional[BlazeLock] = None
        if scheduler_name:
            remote_lock = self.mongo_client.fetch_lock(scheduler_name)
        if not remote_lock:
            try:
                locks = self.mongo_client.fetch_all_locks()
            except PyMongoError:
                locks = []
            if locks:
                # Choose the most recently updated lock
                remote_lock = max(
                    locks,
                    key=lambda l: l.last_updated if isinstance(l.last_updated, datetime) else datetime.min
                )

        if not remote_lock and intended_lock:
            # No remote lock; if we have an intended lock, create it remotely and locally
            try:
                self.mongo_client.create_lock(intended_lock)
            except PyMongoError:
                pass
            try:
                self._write_local_lock(intended_lock)
            except Exception as e:
                self.logger.warning(f"Failed to write local intended lock: {e}")
            return False

        # If we found a remote lock, write it locally (authoritative)
        if remote_lock:
            try:
                self._write_local_lock(remote_lock)
            except Exception as e:
                self.logger.warning(f"Failed to write local lock during reconcile: {e}")

            # Job file: pull from Mongo for this scheduler and overwrite local
            try:
                remote_job_file = self.mongo_client.fetch_job_file(remote_lock.name)
            except PyMongoError:
                remote_job_file = None
            if remote_job_file:
                try:
                    self._write_local_job_file(remote_job_file)
                except Exception as e:
                    self.logger.warning(f"Failed to write local job file during reconcile: {e}")

            # Job states: refresh local memory/disk from Mongo authoritative
            try:
                remote_states = self.mongo_client.fetch_all_job_states()
            except PyMongoError:
                remote_states = []

            # Clear current local memory and state directory
            self.state_manager.job_states = {}
            try:
                if os.path.exists(self.state_manager.state_dir):
                    import shutil
                    shutil.rmtree(self.state_manager.state_dir)
            except OSError as e:
                self.logger.warning(f"Failed to clear local state dir: {e}")

            # Rehydrate from remote
            for job_state in remote_states:
                try:
                    self.state_manager.add_job(job_state)
                except Exception:
                    continue

            return True

        return False

    def get_last_lock(self) -> Optional[BlazeLock]:
        """Return the most recently updated lock from MongoDB, if available."""
        if not self.mongo_client:
            return None
        try:
            locks = self.mongo_client.fetch_all_locks()
            if not locks:
                return None
            return max(
                locks,
                key=lambda l: l.last_updated if isinstance(l.last_updated, datetime) else datetime.min
            )
        except PyMongoError:
            return None

    def reconcile_job_file(self, scheduler_name: str) -> None:
        if not self.mongo_client:
            return
        local_job_file = self._read_local_job_file()
        mongo_job_file = self.mongo_client.fetch_job_file(scheduler_name)
        chosen_job_file = self._choose_newer_job_file(local_job_file, mongo_job_file)
        if chosen_job_file:
            self._write_local_job_file(chosen_job_file)
            try:
                self.mongo_client.create_job_file(chosen_job_file)
            except PyMongoError:
                pass

    # ---------------------------
    # Hard stop cleanup
    # ---------------------------
    def hard_stop_cleanup(self, scheduler_name: str) -> None:
        """
        Remove local storage and MongoDB artifacts synchronously.
        """
        # Local cleanup
        try:
            if os.path.exists(self.job_lock_path):
                os.remove(self.job_lock_path)
        except OSError as e:
            self.logger.warning(f"Failed to remove local lock: {e}")

        try:
            if os.path.exists(self.job_file_path):
                os.remove(self.job_file_path)
        except OSError as e:
            self.logger.warning(f"Failed to remove local job file: {e}")

        # Remove state directory via state manager helper if present
        try:
            if os.path.exists(self.state_manager.state_dir):
                import shutil
                shutil.rmtree(self.state_manager.state_dir)
        except OSError as e:
            self.logger.warning(f"Failed to remove local state dir: {e}")

        # Mongo cleanup
        if self.mongo_client:
            try:
                self.mongo_client.delete_lock(scheduler_name)
            except PyMongoError:
                pass
            try:
                self.mongo_client.delete_job_file(scheduler_name)
            except PyMongoError:
                pass
            # Delete all job states and runs
            try:
                job_states = self.mongo_client.fetch_all_job_states()
                for job_state in job_states:
                    try:
                        self.mongo_client.delete_job_state(job_state.job_id)
                    except PyMongoError:
                        pass
                    try:
                        self.mongo_client.delete_job_runs(job_state.job_id)
                    except PyMongoError:
                        pass
            except PyMongoError:
                pass
            # Delete logs for this scheduler (best-effort)
            try:
                self.mongo_client.delete_logs()
            except PyMongoError:
                pass


