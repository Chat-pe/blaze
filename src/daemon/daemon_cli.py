#!/usr/bin/env python
"""
Blaze Daemon Command Line Interface

This CLI provides commands to manage the Blaze daemon process.
"""

import argparse
import sys
import os
import json
import time
from typing import List, Optional
from datetime import datetime

# Import the manager functions
from src.daemon.manager import (
    get_scheduler,
    get_or_create_scheduler,
    start_scheduler,
    stop_scheduler,
    update_jobs,
    _LOCK_PATH,
    _JOBS_PATH
)

# Import core types
from src.core import (
    BlazeBlock,
    BlazeSequence,
    SubmitSequenceData,
    BlazeLock,
    BlazeLogger
)


class DaemonCLI:
    """Main CLI class for the Blaze daemon."""
    
    def __init__(self):
        self.logger = BlazeLogger()
    
    def status(self, args) -> None:
        """Check daemon status."""
        try:
            if not os.path.exists(_LOCK_PATH):
                print("❌ Daemon is not running (no lock file found)")
                return
            
            with open(_LOCK_PATH, "r") as f:
                lock_data = BlazeLock(**json.loads(f.read()))
            
            status_emoji = "✅" if lock_data.is_running else "⏸️" if lock_data.is_paused else "❌"
            
            print(f"{status_emoji} Daemon Status:")
            print(f"  Name: {lock_data.name}")
            print(f"  Running: {lock_data.is_running}")
            print(f"  Paused: {lock_data.is_paused}")
            print(f"  Stopped: {lock_data.is_stopped}")
            print(f"  Loop Interval: {lock_data.loop_interval}s")
            print(f"  Last Updated: {lock_data.last_updated}")
            print(f"  Blocks: {len(lock_data.blocks)} registered")
            print(f"  Sequences: {len(lock_data.sequences)} registered")
            
            # Show jobs if they exist
            if os.path.exists(_JOBS_PATH):
                with open(_JOBS_PATH, "r") as f:
                    jobs_data = json.loads(f.read())
                print(f"  Active Jobs: {len(jobs_data.get('jobs', []))}")
            
        except Exception as e:
            print(f"❌ Error checking daemon status: {e}")
    
    def start(self, args) -> None:
        """Start the daemon."""
        try:
            # Check if already running
            if os.path.exists(_LOCK_PATH):
                with open(_LOCK_PATH, "r") as f:
                    lock_data = BlazeLock(**json.loads(f.read()))
                if lock_data.is_running:
                    print("⚠️  Daemon is already running")
                    return
            
            # Import the blocks and sequences from definition
            if args.definition:
                definition_path = args.definition
            else:
                # Default to examples/definition.py
                definition_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                             "examples", "definition.py")
            
            if not os.path.exists(definition_path):
                print(f"❌ Definition file not found: {definition_path}")
                print("   Use --definition to specify a custom definition file")
                return
            
            # Load the definition module
            import importlib.util
            spec = importlib.util.spec_from_file_location("definition", definition_path)
            definition_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(definition_module)
            
            # Get blocks and sequences from the module
            if not hasattr(definition_module, 'blocks') or not hasattr(definition_module, 'sequences'):
                print("❌ Definition file must contain 'blocks' and 'sequences' variables")
                return
            
            blocks = definition_module.blocks
            sequences = definition_module.sequences
            
            print("🚀 Starting Blaze daemon...")
            
            # Create or get the scheduler
            scheduler = get_or_create_scheduler(blocks, sequences)
            
            if args.detach:
                # Start as background process
                pid = os.fork()
                if pid == 0:
                    # Child process
                    os.setsid()  # Create new session
                    start_scheduler()
                else:
                    # Parent process
                    print(f"✅ Daemon started in background with PID: {pid}")
            else:
                # Start in foreground
                print("✅ Starting daemon in foreground...")
                print("   Press Ctrl+C to stop")
                start_scheduler()
                
        except KeyboardInterrupt:
            print("\n🛑 Daemon stopped by user")
        except Exception as e:
            print(f"❌ Error starting daemon: {e}")
    
    def stop(self, args) -> None:
        """Stop the daemon."""
        try:
            print("🛑 Stopping Blaze daemon...")
            stop_scheduler(shutdown=args.shutdown)
            
            if args.shutdown:
                print("✅ Daemon shutdown complete")
            else:
                print("⏸️  Daemon paused (can be resumed)")
                
        except FileNotFoundError as e:
            print(f"⚠️  {e}")
        except Exception as e:
            print(f"❌ Error stopping daemon: {e}")
    
    def pause(self, args) -> None:
        """Pause the daemon."""
        try:
            print("⏸️  Pausing Blaze daemon...")
            stop_scheduler(shutdown=False)
            print("✅ Daemon paused successfully")
        except Exception as e:
            print(f"❌ Error pausing daemon: {e}")
    
    def jobs(self, args) -> None:
        """Manage jobs."""
        try:
            if args.action == "list":
                self._list_jobs()
            elif args.action == "submit":
                self._submit_jobs(args.job_file)
            elif args.action == "clear":
                self._clear_jobs()
            else:
                print("❌ Invalid jobs action. Use: list, submit, or clear")
                
        except Exception as e:
            print(f"❌ Error managing jobs: {e}")
    
    def _list_jobs(self) -> None:
        """List current jobs."""
        if not os.path.exists(_JOBS_PATH):
            print("📝 No jobs file found")
            return
        
        with open(_JOBS_PATH, "r") as f:
            jobs_data = json.loads(f.read())
        
        jobs = jobs_data.get('jobs', [])
        if not jobs:
            print("📝 No jobs found")
            return
        
        print(f"📋 Found {len(jobs)} job(s):")
        for i, job in enumerate(jobs, 1):
            print(f"  {i}. Sequence ID: {job.get('seq_id', 'Unknown')}")
            print(f"     Status: {job.get('status', 'Unknown')}")
            print(f"     Submitted: {job.get('submitted_at', 'Unknown')}")
            print()
    
    def _submit_jobs(self, job_file: str) -> None:
        """Submit jobs from file."""
        if not job_file:
            print("❌ Job file path required")
            return
        
        if not os.path.exists(job_file):
            print(f"❌ Job file not found: {job_file}")
            return
        
        with open(job_file, "r") as f:
            jobs_data = json.loads(f.read())
        
        # Convert to SubmitSequenceData objects
        submitted_jobs = []
        for job_data in jobs_data.get('jobs', []):
            submit_data = SubmitSequenceData(**job_data)
            submitted_jobs.append(submit_data)
        
        success = update_jobs(submitted_jobs)
        if success:
            print(f"✅ Successfully submitted {len(submitted_jobs)} job(s)")
        else:
            print("❌ Failed to submit jobs")
    
    def _clear_jobs(self) -> None:
        """Clear all jobs."""
        if os.path.exists(_JOBS_PATH):
            os.remove(_JOBS_PATH)
            print("✅ Jobs cleared successfully")
        else:
            print("📝 No jobs file to clear")
    
    def logs(self, args) -> None:
        """View daemon logs."""
        # This is a placeholder - you might want to implement proper log file handling
        print("📄 Daemon logs:")
        print("   (Log viewing functionality to be implemented)")
        if args.follow:
            print("   --follow mode requested")
        if args.lines:
            print(f"   --lines {args.lines} requested")


def main():
    """Main entry point for the daemon CLI."""
    parser = argparse.ArgumentParser(
        description="Blaze Daemon Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start                    # Start daemon in foreground
  %(prog)s start --detach           # Start daemon in background
  %(prog)s status                   # Check daemon status
  %(prog)s stop                     # Pause daemon
  %(prog)s stop --shutdown          # Shutdown daemon completely
  %(prog)s jobs list                # List current jobs
  %(prog)s jobs submit jobs.json    # Submit jobs from file
        """
    )
    
    # Create subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Check daemon status')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start the daemon')
    start_parser.add_argument('--detach', '-d', action='store_true',
                            help='Start daemon in background')
    start_parser.add_argument('--definition', '-f', type=str,
                            help='Path to definition file (default: examples/definition.py)')
    
    # Stop command  
    stop_parser = subparsers.add_parser('stop', help='Stop the daemon')
    stop_parser.add_argument('--shutdown', action='store_true',
                           help='Shutdown completely (default: pause)')
    
    # Pause command
    pause_parser = subparsers.add_parser('pause', help='Pause the daemon')
    
    # Jobs command
    jobs_parser = subparsers.add_parser('jobs', help='Manage jobs')
    jobs_parser.add_argument('action', choices=['list', 'submit', 'clear'],
                           help='Jobs action to perform')
    jobs_parser.add_argument('job_file', nargs='?',
                           help='Job file path (required for submit action)')
    
    # Logs command
    logs_parser = subparsers.add_parser('logs', help='View daemon logs')
    logs_parser.add_argument('--follow', '-f', action='store_true',
                           help='Follow log output')
    logs_parser.add_argument('--lines', '-n', type=int, default=50,
                           help='Number of lines to show')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Create CLI instance and execute command
    cli = DaemonCLI()
    
    if args.command == 'status':
        cli.status(args)
    elif args.command == 'start':
        cli.start(args)
    elif args.command == 'stop':
        cli.stop(args)
    elif args.command == 'pause':
        cli.pause(args)
    elif args.command == 'jobs':
        cli.jobs(args)
    elif args.command == 'logs':
        cli.logs(args)
    else:
        print(f"❌ Unknown command: {args.command}")
        parser.print_help()


if __name__ == "__main__":
    main()
