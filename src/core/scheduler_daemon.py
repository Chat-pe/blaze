import multiprocessing
import signal
import sys
import time
import os
import json
import tempfile
import random
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from croniter import croniter
from pydantic import BaseModel

from src.core.registrar import BlazeRegistrar
from src.core.scheduler_state import ScheduleState, SequenceStatus, SequenceResult
from src.core._types import SeqData, SeqBlockData, SequenceData
from src.core.name_generator import generate_daemon_name
from src.core.persistence import BlazePersistence

__all__ = ['SchedulerDaemon']

class ScheduleConfig:
    def __init__(self, cron: str, seq_id: str):
        self.cron = cron
        self.seq_id = seq_id

class Schedule:
    def __init__(self, config: ScheduleConfig):
        self.config = config
        self.last_run = None

    def should_run(self, current_time: datetime) -> bool:
        if self.last_run is None:
            # First run
            self.last_run = current_time
            return True
        next_run = croniter(self.config.cron, self.last_run).get_next(datetime)
        if current_time >= next_run:
            self.last_run = current_time
            return True
        return False

    def get_next_run(self) -> datetime:
        if self.last_run is None:
            return croniter(self.config.cron, datetime.now()).get_next(datetime)
        return croniter(self.config.cron, self.last_run).get_next(datetime)

class SchedulerDaemon:
    # Paths for persistence
    _SEQUENCES_FILE = os.path.join(tempfile.gettempdir(), "blaze_sequences.json")
    _STATE_FILE = os.path.join(tempfile.gettempdir(), "blaze_state.json")
    
    def __init__(
        self,
        max_workers: int = multiprocessing.cpu_count(),
        auto_start: bool = True,
        persistence_enabled: bool = True,
        name: str = None,
        instance_id: str = None,
        connect_to_existing: bool = False
    ):
        self.max_workers = max_workers
        self.registrar = BlazeRegistrar()
        self.state = ScheduleState()
        self.persistence_enabled = persistence_enabled
        self.persistence = BlazePersistence.get_instance()
        self._connecting_to_existing = connect_to_existing
        
        # Generate or use a provided name for this daemon instance
        self.instance_name = name or generate_daemon_name()
        
        # Use provided instance_id if given, otherwise generate a new one
        if instance_id and connect_to_existing:
            self.instance_id = instance_id  # Reuse existing ID for continuity
        else:
            self.instance_id = str(uuid.uuid4())[:8]  # Short UUID for uniqueness
        
        self._sequences: Dict[str, SeqData] = {}
        self._processes: Dict[str, multiprocessing.Process] = {}
        self._last_runs: Dict[str, datetime] = {}  # Track last run time for each sequence
        self._next_runs: Dict[str, datetime] = {}  # Track next run time for each sequence
        self._is_running = False
        self._main_process: Optional[multiprocessing.Process] = None
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Load any persisted sequences
        if persistence_enabled:
            self._load_persisted_data()
        
            print(f"Connected to existing daemon: {self.instance_name} ({self.instance_id})")
        else:
            print(f"Created daemon instance: {self.instance_name} ({self.instance_id})")
        
        if auto_start:
            self.start()
            
    def _save_sequences(self):
        """Save sequences to persistence."""
        if not self.persistence_enabled:
            return
        try:
            for seq_id in self._sequences:
                self.persistence.store_sequence(seq_id, SequenceData(seq_data=self._sequences[seq_id], sequence_id=seq_id, next_run=self._next_runs.get(seq_id), last_run=self._last_runs.get(seq_id)))
        except Exception as e:
            print(f"Warning: Failed to save sequences: {e}")
    
    def managed_loaded_data(self, persisted_sequences: Dict[str, SequenceData]):
        self._sequences = persisted_sequences
        self._last_runs = {seq_id: persisted_sequences[seq_id].last_run for seq_id in persisted_sequences}
        self._next_runs = {seq_id: croniter(persisted_sequences[seq_id].seq_run_interval, start_time=datetime.now(), ret_type=datetime).get_next(datetime) for seq_id in persisted_sequences}
        self.state.add_bulk_sequences(list(persisted_sequences.keys()))
        self.state.add_bulk_logs(list(persisted_sequences.keys()), ["Sequence restored from persistence" for _ in persisted_sequences])
            
    def _load_persisted_data(self):
        """Load persisted data from storage."""
        try:
            # Get all sequences from persistence
            persisted_sequences = self.persistence.get_all_sequences()
            if persisted_sequences:
                self.managed_loaded_data(persisted_sequences)
        except Exception as e:
            print(f"Warning: Failed to load persisted data: {e}")
            import traceback
            traceback.print_exc()
            
    
    def _check_for_new_sequences(self):
        """Check for new sequences added by other processes."""
        try:
            persisted_sequences = self.persistence.get_all_sequences()
            if not persisted_sequences:
                return
            current_ids = set(self._sequences.keys())
            persisted_ids = set(persisted_sequences.keys())
            new_ids = persisted_ids - current_ids
            
            if new_ids:
                self.managed_loaded_data({seq_id: persisted_sequences[seq_id] for seq_id in new_ids})
                
        except Exception as e:
            print(f"Error checking for new sequences: {e}")
            import traceback
            traceback.print_exc()

    def start(self) -> None:
        """Start the scheduler daemon."""
        if self._is_running:
            return
            
        # Check if we're connecting to an existing daemon or starting a new one
        is_connecting = hasattr(self, '_connecting_to_existing') and self._connecting_to_existing
            
        if is_connecting:
            # If connecting to existing daemon, just mark as running but don't start a new thread
            self._is_running = True
            print(f"Connected to scheduler daemon '{self.instance_name}' (ID: {self.instance_id})")
            return

        self._is_running = True
        # Use threading instead of multiprocessing for simpler communication
        import threading
        self._main_process = threading.Thread(target=self._scheduler_loop)
        self._main_process.daemon = True
        self._main_process.start()
        print(f"Scheduler daemon '{self.instance_name}' started (PID: {os.getpid()}, ID: {self.instance_id})")

    def stop(self) -> None:
        """Stop the scheduler daemon and all running sequences."""
        print(f"Stopping scheduler daemon '{self.instance_name}' ({self.instance_id})...")
        self._is_running = False
        
        # Stop all running processes
        for process in self._processes.values():
            if hasattr(process, 'is_alive') and process.is_alive():
                if hasattr(process, 'terminate'):
                    process.terminate()
                if hasattr(process, 'join'):
                    process.join()
        
        # For thread-based main process
        if self._main_process and hasattr(self._main_process, 'is_alive') and self._main_process.is_alive():
            # Can't terminate threads, but they should exit when self._is_running becomes False
            if hasattr(self._main_process, 'join'):
                self._main_process.join(timeout=2)
        print(f"Scheduler daemon '{self.instance_name}' stopped.")

    def submit_sequence(
        self,
        sequence_id: str,
        seq_data: SeqData
    ) -> None:
        """
        Submit a new sequence to be scheduled and executed.
        
        Args:
            sequence_id: Unique identifier for the sequence
            sequence: List of blocks to execute
            seq_data: Sequence data containing schedule information
        """
        if sequence_id in self._sequences:
            raise ValueError(f"Sequence {sequence_id} already exists")
        
        # Register the sequence
        self._sequences[sequence_id] = seq_data
        print(f"Sequence {self._sequences[sequence_id]} added to scheduler")
        current_time = datetime.now()
        self._last_runs[sequence_id] = current_time  # Initialize last run time
        
        # Calculate and store the next run time immediately
        try:
            cron = croniter(
                seq_data.seq_run_interval,
                start_time=current_time,
                ret_type=datetime
            )
            next_run = cron.get_next(datetime)
            self._next_runs[sequence_id] = next_run
            print(f"Next run for {sequence_id} scheduled at: {next_run}")
        except Exception as e:
            print(f"Error calculating initial next run time for {sequence_id}: {e}")
        
        # Initialize sequence state
        print(f"Storing sequence {sequence_id} to persistence")
        self.persistence.store_sequence(sequence_id, SequenceData(seq_data=seq_data, sequence_id=sequence_id, next_run=self._next_runs.get(sequence_id), last_run=self._last_runs.get(sequence_id)))
        self.state.add_sequence(sequence_id)
        self.state.update_sequence_status(sequence_id, SequenceStatus.PENDING)
        print(f"Sequence {sequence_id} state updated to PENDING")
        self.state.add_log(sequence_id, f"Sequence submitted with schedule: {seq_data.seq_run_interval}")
        
        print(f"Successfully registered sequence: {sequence_id}")
        
        # Save sequences to file for persistence
        if self.persistence_enabled:
            # Flush to persistence immediately with fsync to ensure other processes can see it
            self._save_sequences()
            print(f"Sequence {sequence_id} saved to persistence store")
            

    def cancel_sequence(self, sequence_id: str) -> None:
        """Cancel a running sequence."""
        if sequence_id in self._processes:
            process = self._processes[sequence_id]
            if process.is_alive():
                process.terminate()
                process.join()
            del self._processes[sequence_id]
        
        if sequence_id in self._sequences:
            del self._sequences[sequence_id]
        
        if sequence_id in self._last_runs:
            del self._last_runs[sequence_id]
        
        self.state.update_sequence_status(sequence_id, SequenceStatus.CANCELLED)
        self.state.add_log(sequence_id, "Sequence cancelled")
        
        # Update the persistence file
        if self.persistence_enabled:
            self.persistence.remove_sequence(sequence_id)

    def get_sequence_status(self, sequence_id: str) -> Optional[SequenceResult]:
        """Get the current status of a sequence."""
        return self.state.get_sequence_status(sequence_id)

    def get_sequence_logs(self, sequence_id: str) -> List[str]:
        """Get all logs for a sequence."""
        return self.state.get_sequence_logs(sequence_id)

    def get_active_sequences(self) -> Dict[str, SequenceResult]:
        """Get all currently running sequences."""
        return self.state.get_active_sequences()

    def _handle_shutdown(self, signum, frame):
        """
        Handle shutdown signals more gracefully with cross-process coordination.
        
        Args:
            signum: Signal number received
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else f"Signal {signum}"
        print(f"Received {signal_name} signal. Initiating graceful shutdown...")
        
        # Mark that we're shutting down to prevent new tasks
        self._is_running = False
        
        # First terminate all child processes gracefully
        for seq_id, process in list(self._processes.items()):
            try:
                print(f"Terminating process for sequence {seq_id}...")
                if hasattr(process, 'terminate') and process.is_alive():
                    process.terminate()
                    # Give each process a small amount of time to terminate
                    process.join(timeout=1.0)
                    
                    # If still alive, force kill (for stubborn processes)
                    if process.is_alive() and hasattr(process, 'kill'):
                        print(f"Force killing process for sequence {seq_id}...")
                        process.kill()
                
                # Update sequence status to show it was interrupted
                self.state.update_sequence_status(seq_id, SequenceStatus.CANCELLED)
                self.state.add_log(seq_id, f"Sequence cancelled due to daemon shutdown ({signal_name})")
            except Exception as e:
                print(f"Error during process termination for {seq_id}: {e}")
        
        # Save state one final time before exiting
        if self.persistence_enabled:
            print(f"Saving final state before shutdown...")
            self._save_sequences()
        
        # Now call the regular stop method to clean up main process
        self.stop()
        
        # Exit process with appropriate code (0 for SIGINT/SIGTERM, 1 for others)
        exit_code = 0 if signum in (signal.SIGINT, signal.SIGTERM) else 1
        print(f"Scheduler daemon '{self.instance_name}' shutdown complete.")
        sys.exit(exit_code)

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that checks and schedules sequences."""
        print("Scheduler loop starting...")
        
        # Print these messages only once at startup
        first_iteration = True
        last_hashed_sequences = self.persistence.get_hashed_sequences()
        # Track when we last checked for new sequences
        last_refresh_time = time.time()
        refresh_interval = 10  # Check for new sequences every 10 seconds


        
        while self._is_running:
            print(f"loop is looping, {self._is_running}, total processes: {len(self._processes)}, total sequences: {len(self._sequences)}", end="\r")
            current_time = datetime.now()
            
            # Only print these debug messages on the first iteration
            if first_iteration:
                print(f"Scheduler loop running at {current_time}")
                first_iteration = False
            
            # Periodically check for new or updated sequences from persistence
            # but don't spam the logs with messages if no new sequences are found
            current_time_secs = time.time()
            if self.persistence_enabled and (current_time_secs - last_refresh_time) >= refresh_interval:
                current_hashed_sequences = self.persistence.get_hashed_sequences()
                print(f"current_hashed_sequences: {current_hashed_sequences}, last_hashed_sequences: {last_hashed_sequences}")
                if current_hashed_sequences != last_hashed_sequences:
                    last_hashed_sequences = current_hashed_sequences
                    self._check_for_new_sequences()
                
                last_refresh_time = current_time_secs   
                
            # Check each sequence without verbose logging
            for sequence_id, seq_data in self._sequences.items():
                # Skip if sequence is already running, but first check if the process is actually alive
                if sequence_id in self._processes:
                    process = self._processes[sequence_id]
                    if hasattr(process, 'is_alive') and process.is_alive():
                        # Process is still running, skip this sequence
                        continue
                    else:
                        # Process is no longer alive, clean it up
                        if hasattr(process, 'join'):
                            process.join(timeout=0.1)
                        # Remove from processes dict so we can run again
                        del self._processes[sequence_id]
                        print(f"Cleaned up completed process for {sequence_id}")
                
                # Get the last run time for this sequence or initialize it
                last_run = self._last_runs.get(sequence_id)
                
                # If this is the first check or we need to recalculate due to time passing
                if last_run is None:
                    # For a new sequence, use current time as start reference
                    last_run = seq_data.start_date if seq_data.start_date else current_time
                    self._last_runs[sequence_id] = last_run
                    
                # Check if it's time to run
                try:
                    # Get the next run time for checking if we should run
                    # This could be in the past for the first run or if we missed runs
                    # We'll recalculate the *next* next run time only after deciding to run
                    cron = croniter(
                        seq_data.seq_run_interval,
                        start_time=last_run,
                        ret_type=datetime
                    )
                    next_run = cron.get_next(datetime)
                    
                    # For debugging
                    self.state.add_log(sequence_id, f"Current time: {current_time}")
                    self.state.add_log(sequence_id, f"Next run scheduled for: {next_run}")
                    if next_run:
                        self.state.add_log(sequence_id, f"Time until next run: {(next_run - current_time).total_seconds()} seconds")
                    
                    # Check if it's time to run - before updating any times!
                    should_run = (next_run and 
                                 current_time >= next_run and
                                 (not seq_data.start_date or current_time >= seq_data.start_date) and
                                 (not seq_data.end_date or current_time <= seq_data.end_date))
                    
                    
                    if should_run:
                        # We're going to run, so update the next run time from the current time
                        # This ensures we don't miss the execution
                        future_cron = croniter(
                            seq_data.seq_run_interval,
                            start_time=current_time,
                            ret_type=datetime
                        )
                        future_next_run = future_cron.get_next(datetime)
                        
                        # Update the next run time for CLI display - but only AFTER we decide to run
                        self._next_runs[sequence_id] = future_next_run
                        print(f"After execution, next run will be at: {future_next_run}")
                    else:
                        # If next_run is in the past but we're not running (maybe due to date constraints),
                        # we should still update the next run time to prevent constant checking
                        if next_run > seq_data.end_date:
                            self.state.update_sequence_status(sequence_id, SequenceStatus.COMPLETED)
                            self.state.add_log(sequence_id, f"Sequence completed at {current_time}")
                            self.cancel_sequence(sequence_id)
                        if next_run < current_time:
                            recalc_message_key = f"recalc_{sequence_id}"
                            current_time_secs = int(time.time())
                            last_message_time = getattr(self, recalc_message_key, 0)
                            
                            # Only print messages once every 5 minutes (300 seconds)
                            if current_time_secs - last_message_time > 300:
                                print(f"Recalculating next run for {sequence_id} - previous run time in past")
                                setattr(self, recalc_message_key, current_time_secs)
                                
                            # Find the next future execution time
                            cron = croniter(
                                seq_data.seq_run_interval,
                                start_time=current_time,
                                ret_type=datetime
                            )
                            future_next_run = cron.get_next(datetime)
                            
                            # Update the next run time for CLI display
                            self._next_runs[sequence_id] = future_next_run
                            
                            if current_time_secs - last_message_time > 300:
                                print(f"Updated next run time to {future_next_run}")
                        else:
                            # Not time to run yet, but update the display time
                            self._next_runs[sequence_id] = next_run
                
                except Exception as e:
                    self.state.add_log(sequence_id, f"Error calculating next run time: {str(e)}")
                    import traceback
                    print(f"Error in run time calculation: {e}")
                    traceback.print_exc()
                    next_run = None
                    should_run = False
                
                # Execute the sequence if it's time
                if should_run:
                    print(f"\n\n**** EXECUTION TRIGGER: Sequence {sequence_id} is due to run! ****")
                    
                    
                    
                    # Run sequence in a separate thread for better isolation
                    import threading
                    import traceback
                    
                    # Define a wrapper that catches and prints all exceptions
                    def execute_with_traceback():
                        try:
                            self._execute_sequence(sequence_id)
                        except Exception as e:
                            print(f"ERROR IN SEQUENCE EXECUTION: {e}")
                            traceback.print_exc()
                    
                    # Create a dedicated thread for this sequence
                    process = threading.Thread(
                        target=execute_with_traceback
                    )
                    
                    # Set daemon to True so it doesn't prevent program exit
                    process.daemon = True
                    
                    # Start the thread
                    process.start()
                    print(f"Started thread for sequence: {sequence_id} (Thread ID: {process.ident})")
                    
                    # Update tracking information
                    self._processes[sequence_id] = process
                    self._last_runs[sequence_id] = current_time  # Update last run time
                    self.state.update_sequence_status(sequence_id, SequenceStatus.RUNNING)
                    self.state.add_log(sequence_id, f"Sequence started at {current_time}")
                    
                
            # Clean up completed processes
            for sequence_id, process in list(self._processes.items()):
                if not process.is_alive():
                    process.join()
                    del self._processes[sequence_id]
                    # Update status to completed when process finishes
                    self.state.update_sequence_status(sequence_id, SequenceStatus.COMPLETED)
                    self.state.add_log(sequence_id, "Process completed")
            
            # Sleep to prevent CPU overuse
            time.sleep(2)

    def _execute_sequence(self, sequence_id: str) -> None:
        """Execute a sequence in a separate thread."""
        try:
            current_time = datetime.now()
            
            seq_data = self._sequences[sequence_id]
            

            
            # Also print to stderr for better visibility
            import sys
            sys.stderr.write(f"\n\nEXECUTING SEQUENCE: {sequence_id} at {current_time}\n")
            sys.stderr.flush()
            
            self.state.add_log(sequence_id, f"Starting sequence execution at {current_time}")
            
            # Execute the sequence function
            try:
                # Get the sequence function from the seq_data
                sequence_func = seq_data.sequence_func
                # This is the actual execution of the sequence function
                start_time = datetime.now()
                self.state.add_log(sequence_id, f"Starting sequence execution at {start_time}")
                
                result = sequence_func({})
                end_time = datetime.now()

                
                execution_time = (end_time - start_time).total_seconds()
                self.persistence.add_execution_time(sequence_id, execution_time)
                self.persistence.add_latest_result(sequence_id, result)
                print(f"Execution function returned: {result}")
                print(f"Execution took {execution_time:.2f} seconds")
                
                self.state.add_log(sequence_id, f"Sequence completed successfully in {execution_time:.2f} seconds")
                self.state.add_log(sequence_id, f"Result: {result}")
                self.state.update_sequence_status(sequence_id, SequenceStatus.COMPLETED)
                
                # Store results for each block
                if isinstance(result, dict):
                    for block_name, block_result in result.items():
                        self.state.add_block_result(sequence_id, block_name, {"result": block_result})
                
            except Exception as e:
                import traceback
                error_msg = f"Sequence failed: {str(e)}\n{traceback.format_exc()}"
                print(f"\nERROR EXECUTING SEQUENCE: {error_msg}")
                sys.stderr.write(f"ERROR EXECUTING SEQUENCE: {str(e)}\n")
                sys.stderr.flush()
                self.state.add_log(sequence_id, error_msg)
                self.state.update_sequence_status(sequence_id, SequenceStatus.FAILED)
            
        except Exception as e:
            import traceback
            error_msg = f"Sequence execution wrapper failed: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            import sys
            sys.stderr.write(f"WRAPPER ERROR: {str(e)}\n")
            sys.stderr.flush()
            self.state.add_log(sequence_id, error_msg)
            self.state.update_sequence_status(sequence_id, SequenceStatus.FAILED)