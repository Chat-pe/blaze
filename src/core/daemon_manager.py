"""
Daemon manager module that implements a singleton pattern for the scheduler daemon.
This ensures only one daemon instance is running at any time and provides global access to it.
"""

import atexit
import threading
import os
import json
import time
import socket
import signal
import tempfile
from typing import Optional, Dict, Any
from pathlib import Path

from src.core.scheduler_daemon import SchedulerDaemon
from src.core.persistence import BlazePersistence

# Global variable to hold the singleton instance
_scheduler_instance: Optional[SchedulerDaemon] = None
_scheduler_lock = threading.Lock()  # Lock for thread-safe access
_is_initialized = False

# File-based persistence for cross-process communication
_DAEMON_FILE = os.path.join(tempfile.gettempdir(), "blaze_daemon.json")
_PID_FILE = os.path.join(tempfile.gettempdir(), "blaze_daemon.pid")
_DAEMON_PORT = 43201  # Default port for daemon socket check

def _write_daemon_info():
    """Write daemon information to file for cross-process communication."""
    if _scheduler_instance is None:
        return
    
    daemon_info = {
        "pid": os.getpid(),
        "port": _DAEMON_PORT,
        "start_time": time.time(),
        "max_workers": _scheduler_instance.max_workers,
        "running": _scheduler_instance._is_running,
        "name": getattr(_scheduler_instance, 'instance_name', 'unnamed'),
        "id": getattr(_scheduler_instance, 'instance_id', 'unknown')
    }
    
    try:
        with open(_DAEMON_FILE, 'w') as f:
            json.dump(daemon_info, f)
        
        # Also write PID to a separate file for easier checking
        with open(_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        print(f"Warning: Failed to write daemon info: {e}")

def _read_daemon_info():
    """Read daemon information from file."""
    if not os.path.exists(_DAEMON_FILE):
        return None
    
    try:
        with open(_DAEMON_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return None

def _is_process_running(pid):
    """Check if a process with the given PID is running."""
    try:
        # This works on both Unix and Windows
        pid = int(pid)  # Make sure pid is an integer
        os.kill(pid, 0)
        
        # On Unix/Linux/macOS, additional check to make sure the process is actually our daemon
        # and not a different process that got the same PID after our daemon died
        if os.name == 'posix':
            try:
                import subprocess
                # Use ps command which works on macOS, Linux, and other Unix systems
                result = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "command="],
                    capture_output=True,
                    text=True,
                    timeout=1
                )
                
                if result.returncode == 0:
                    cmd_output = result.stdout.lower()
                    if "python" in cmd_output and "blaze" in cmd_output:
                        return True
                    else:
                        print(f"Warning: Found process with PID {pid} but it's not a Blaze daemon")
                        print(f"Process command: {cmd_output.strip()}")
                        return False
                else:
                    # Process doesn't exist according to ps command
                    return False
            except Exception as e:
                # If ps command fails, fall back to just PID check
                print(f"Warning: Error checking process command: {e}")
                return True
        return True
    except OSError:
        return False
    except Exception as e:
        print(f"Warning: Error checking if process is running: {e}")
        return False

def _check_daemon_socket():
    """Try to connect to the daemon socket to check if it's running."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        result = s.connect_ex(('127.0.0.1', _DAEMON_PORT))
        s.close()
        return result == 0
    except Exception:
        return False

def get_scheduler(
    max_workers: int = None,
    auto_start: bool = True,
    init_args: Dict[str, Any] = None,
    force_new: bool = False,
    auto_shutdown: bool = False
) -> SchedulerDaemon:
    """
    Get or create the scheduler daemon instance.
    
    Args:
        max_workers: Number of worker threads/processes (only used on first initialization)
        auto_start: Whether to automatically start the daemon (only used on first initialization)
        init_args: Additional initialization arguments for the scheduler
        force_new: Force creation of a new instance, ignoring existing daemon
        auto_shutdown: Whether to automatically shut down the daemon when the process exits
                      (default: False - daemon will continue running after script exits)
        
    Returns:
        The scheduler daemon instance
    """
    global _scheduler_instance, _is_initialized
    
    # First check if we already have an instance in this process
    if _scheduler_instance is not None and not force_new:
        return _scheduler_instance
    
    with _scheduler_lock:  # Thread-safe initialization
        # Double-check after acquiring the lock
        if _scheduler_instance is not None and not force_new:
            return _scheduler_instance
            
        # If we're forcing a new instance, clean up the old one first
        if force_new and _scheduler_instance is not None:
            shutdown_scheduler()
        
        # If not forcing a new instance, check if there's a running daemon
        if not force_new:
            # Check if there's daemon info from a previous run
            daemon_info = _read_daemon_info()
            
            if daemon_info:
                pid = daemon_info.get("pid")
                
                # Check if the process is still running
                if pid and _is_process_running(pid):
                    # Get the daemon name and ID if available
                    daemon_name = daemon_info.get("name", "unnamed")
                    daemon_id = daemon_info.get("id", "unknown")
                    
                    print(f"Found existing daemon: '{daemon_name}' (PID: {pid}, ID: {daemon_id})")
                    
                    # We found an existing daemon, but we can't access it directly
                    # from this process. Create a new instance that will connect to it.
                    init_args = init_args or {}
                    
                    if max_workers is not None:
                        init_args['max_workers'] = max_workers
                        
                    init_args['auto_start'] = auto_start
                    
                    # Use the same name AND ID for the connected instance to ensure continuity
                    init_args['name'] = daemon_name
                    init_args['instance_id'] = daemon_id
                    init_args['connect_to_existing'] = True
                    
                    _scheduler_instance = SchedulerDaemon(**init_args)
                    _is_initialized = True
                    
                    # Register cleanup on application exit if requested
                    if auto_shutdown:
                        atexit.register(shutdown_scheduler)
                    
                    # Return the instance - it will use the same persistence files
                    return _scheduler_instance
        
        # No existing daemon found or forcing new instance, create a new one
        init_args = init_args or {}
        
        if max_workers is not None:
            init_args['max_workers'] = max_workers
            
        init_args['auto_start'] = auto_start
        
        _scheduler_instance = SchedulerDaemon(**init_args)
        _is_initialized = True
        
        # Write daemon info to file
        _write_daemon_info()
        
        # Register cleanup on application exit if requested
        if auto_shutdown:
            atexit.register(shutdown_scheduler)
            print("Note: Daemon will be automatically stopped when this script exits")
        else:
            print("Note: Daemon will continue running after this script exits")
            
        return _scheduler_instance

def is_scheduler_running() -> bool:
    """
    Check if the scheduler daemon is running.
    
    Returns:
        True if a scheduler instance exists and is running, False otherwise
    """
    global _scheduler_instance
    
    # First check the current process
    if _scheduler_instance is not None and _scheduler_instance._is_running:
        return True
    
    # Then check for an external daemon process
    daemon_info = _read_daemon_info()
    if daemon_info:
        pid = daemon_info.get("pid")
        if pid and _is_process_running(pid):
            # Additional check: verify the daemon files exist and are valid
            if not os.path.exists(_DAEMON_FILE) or not os.path.exists(_PID_FILE):
                print("Warning: Daemon info files are missing even though process is running.")
                return False
                
            # Check if the daemon file contains the expected PID
            try:
                with open(_PID_FILE, 'r') as f:
                    file_pid = f.read().strip()
                    if str(pid) != file_pid:
                        print(f"Warning: PID mismatch - daemon file: {file_pid}, info: {pid}")
                        return False
            except Exception as e:
                print(f"Warning: Error reading PID file: {e}")
                return False
                
            # All checks passed, daemon is running
            return True
    
    # Check if the daemon file exists but no valid process was found
    # This indicates a stale daemon file that should be cleaned up
    if os.path.exists(_DAEMON_FILE) or os.path.exists(_PID_FILE):
        print("Warning: Found stale daemon files. Cleaning up...")
        try:
            if os.path.exists(_DAEMON_FILE):
                os.remove(_DAEMON_FILE)
            if os.path.exists(_PID_FILE):
                os.remove(_PID_FILE)
        except Exception as e:
            print(f"Warning: Failed to clean up stale daemon files: {e}")
    
    return False

def shutdown_scheduler(clear_sequences: bool = False) -> None:
    """
    Shutdown the scheduler daemon if it's running.
    
    Args:
        clear_sequences: If True, all sequence data will be cleared. Otherwise,
                         sequences will be persisted for the next daemon instance.
    """
    global _scheduler_instance, _is_initialized
    
    # Get the daemon info first to handle external daemon shutdowns
    daemon_info = _read_daemon_info()
    
    # First handle the local instance if it exists
    if _scheduler_instance is not None:
        with _scheduler_lock:
            if _scheduler_instance is not None:
                # Stop the scheduler daemon
                _scheduler_instance.stop()
                _scheduler_instance = None
                _is_initialized = False
    
    # Now handle external daemons if our local instance didn't exist
    elif daemon_info:
        pid = daemon_info.get("pid")
        if pid and _is_process_running(pid):
            print(f"Stopping external daemon process (PID: {pid})...")
            try:
                # Try to send a terminate signal to the process
                os.kill(int(pid), signal.SIGTERM)
                # Wait a moment for the process to shut down
                time.sleep(1)
            except Exception as e:
                print(f"Warning: Failed to terminate external daemon: {e}")
    

        # Optionally clear sequence persistence
        if clear_sequences:
            print("Clearing all persisted sequence data...")
            BlazePersistence.get_instance().clear_all()
