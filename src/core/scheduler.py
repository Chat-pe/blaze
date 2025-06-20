import queue
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

from src.db.main import get_db_handler
from src.core.scheduler_daemon import ScheduleConfig, Schedule

class Scheduler:

    DEFAULT_DB_HANDLER = 'postgres'
    DEFAULT_SCHEDULE_INTERVAL = '0 0 * * *'

    def __init__(
            self, 
            max_workers: int,
            auto_start: bool = True,
        ):
        self.max_workers = max_workers
        self.db_handler = get_db_handler[self.DEFAULT_DB_HANDLER]
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
        self.scheduled_pipelines: Dict[str, Schedule] = {}
        self.next_run_time: Dict[str, datetime] = {}

        self.main_thread: Optional[threading.Thread] = None
        self.worker_threads: list[threading.Thread] = []
        
        self.is_running = False
        self.is_stopped = False
        self.is_paused = False
        self.is_resumed = False

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Automatically register all components in the database
        self._auto_register_components()
        
        # Load scheduled jobs from database
        self._load_from_db()
        
        # Auto-start if requested
        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the scheduler."""
        if self.is_running:
            return

        self.is_running = True
        self.is_stopped = False
        self.is_paused = False
        self.is_resumed = False

        # Start the main scheduler thread
        self.main_thread = threading.Thread(target=self._scheduler_loop)
        self.main_thread.daemon = True
        self.main_thread.start()

        # Start worker threads
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop)
            worker.daemon = True
            worker.start()
            self.worker_threads.append(worker)

    def stop(self) -> None:
        """Stop the scheduler."""
        self.is_running = False
        self.is_stopped = True
        
        if self.main_thread:
            self.main_thread.join()
        
        for worker in self.worker_threads:
            worker.join()

    def pause(self) -> None:
        """Pause the scheduler."""
        self.is_paused = True
        self.is_resumed = False

    def resume(self) -> None:
        """Resume the scheduler."""
        self.is_paused = False
        self.is_resumed = True

    def _handle_shutdown(self, signum, frame):
        self.stop()
        sys.exit(0)

    def _auto_register_components(self):
        pass

    def _load_from_db(self):
        pass

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that checks and schedules tasks."""
        while self.is_running and not self.is_stopped:
            if self.is_paused:
                time.sleep(1)
                continue

            current_time = datetime.now()
            
            # Check each scheduled pipeline
            for pipeline_id, schedule in self.scheduled_pipelines.items():
                if schedule.should_run(current_time):
                    self.task_queue.put({
                        'pipeline_id': pipeline_id,
                        'scheduled_time': current_time
                    })
                    self.next_run_time[pipeline_id] = schedule.get_next_run()

            # Sleep for a short interval to prevent CPU overuse
            time.sleep(1)

    def _worker_loop(self) -> None:
        """Worker loop that processes tasks from the queue."""
        while self.is_running and not self.is_stopped:
            try:
                task = self.task_queue.get(timeout=1)
                # Process the task here
                # TODO: Implement task processing logic
                self.task_queue.task_done()
            except queue.Empty:
                continue

    def add_schedule(self, pipeline_id: str, config: ScheduleConfig) -> None:
        """Add a new schedule for a pipeline."""
        schedule = Schedule(config)
        self.scheduled_pipelines[pipeline_id] = schedule
        self.next_run_time[pipeline_id] = schedule.get_next_run()

    def remove_schedule(self, pipeline_id: str) -> None:
        """Remove a schedule for a pipeline."""
        if pipeline_id in self.scheduled_pipelines:
            del self.scheduled_pipelines[pipeline_id]
            del self.next_run_time[pipeline_id]

    def get_next_run_time(self, pipeline_id: str) -> Optional[datetime]:
        """Get the next run time for a pipeline."""
        return self.next_run_time.get(pipeline_id)
    