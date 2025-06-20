#!/usr/bin/env python
"""
CLI entrypoint for the Blaze scheduler daemon.
This script can be used to start, stop, and manage the scheduler daemon.
"""

import argparse
import logging
import signal
import sys
import time
import os
import socket
import tempfile
from datetime import datetime

from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("blaze-daemon")

def handle_signals():
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down daemon...")
        # For signals, we preserve sequence data (soft stop)
        shutdown_scheduler(clear_sequences=False)
        logger.info("Daemon stopped with sequence data preserved (soft stop)")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def start_daemon(args):
    """Start the scheduler daemon."""
    logger.info("Starting Blaze scheduler daemon...")
    
    # Get or create the scheduler instance with persistence enabled
    print(args.force_new)
    scheduler = get_scheduler(
        max_workers=args.workers,
        auto_start=True,
        force_new=args.force_new,
        auto_shutdown=True,  # CLI tool should shut down the daemon when it exits
        init_args={"persistence_enabled": True, "name": args.name}
    )
    
    if not scheduler._is_running:
        scheduler.start()
    print(f"Scheduler is running: loop started")
    
    # Write PID info to make it easier to find this process
    pid = os.getpid()
    logger.info(f"Daemon '{scheduler.instance_name}' started")
    logger.info(f"  ID: {scheduler.instance_id}")
    logger.info(f"  PID: {pid}")
    logger.info(f"  Workers: {args.workers}")
    
    # Keep the process running
    try:
        # Import required modules
        import shutil
        from datetime import datetime, timedelta
        from croniter import croniter
        
        # Try to import tabulate, but don't fail if it's not available
        try:
            from tabulate import tabulate
            has_tabulate = True
        except ImportError:
            has_tabulate = False
            logger.warning("tabulate package not found. Install with: pip install tabulate")
        
        # Get terminal width for formatting
        term_width = shutil.get_terminal_size().columns
        
        # Define a function to get the next run time
        def get_next_run(seq_id, seq_data, last_run=None):
            try:
                # First check if scheduler has already calculated next run time
                if hasattr(scheduler, '_next_runs') and scheduler._next_runs and seq_id in scheduler._next_runs:
                    return scheduler._next_runs[seq_id]
                    
                # If not available, calculate it ourselves
                current_time = datetime.now()
                
                # Use last_run if provided, otherwise use current time
                if last_run is None:
                    last_run = current_time
                    
                # If last_run is in the past, we need to find the next future run
                cron = croniter(seq_data.seq_run_interval, start_time=last_run)
                next_run = cron.get_next(datetime)
                
                # If next_run is in the past, recalculate from current time
                if next_run < current_time:
                    cron = croniter(seq_data.seq_run_interval, start_time=current_time)
                    next_run = cron.get_next(datetime)
                    
                return next_run
            except Exception as e:
                logger.debug(f"Error calculating next run time: {e}")
                return "Unknown"
        
        while True:
            # Check if the daemon is still running (external shutdown detection)
            print(f"Running this again")
            if not is_scheduler_running():

                logger.info("Daemon has been stopped from another process.")
                logger.info("Exiting monitoring loop.")
                break
                
            current_time = datetime.now()
            
            # First check if scheduler is properly updating from persistence
            # if not, force a refresh every 5 seconds
            if hasattr(scheduler, '_check_for_new_sequences'):
                print("Checking for new sequences")
                scheduler._check_for_new_sequences()
                
            # Get the latest data from the scheduler
            active_sequences = scheduler.get_active_sequences()
            all_sequences = scheduler._sequences
            
            # Build sequence table data
            table_data = []
            headers = ["ID", "Status", "Blocks", "Schedule", "Next Run", "Description"]
            
            # Check if there are any sequences
            if not all_sequences:
                logger.info("No sequences registered with the scheduler yet.")
                logger.info("Waiting for sequences to be submitted...")
            else:
                for seq_id, seq_data in all_sequences.items():
                    # Get sequence status
                    status_obj = scheduler.get_sequence_status(seq_id)
                    status = status_obj.status.value if status_obj else "PENDING"
                    
                    # Get next run time
                    last_run = scheduler._last_runs.get(seq_id)
                    next_run = get_next_run(seq_id, seq_data, last_run)
                    
                    if isinstance(next_run, datetime):
                        next_run_str = next_run.strftime("%H:%M:%S")
                        # Add relative time
                        delta = (next_run - current_time).total_seconds()
                        if delta < 60:
                            next_run_str += " (< 1m)"
                        elif delta < 3600:
                            next_run_str += f" (~{int(delta/60)}m)"
                        else:
                            next_run_str += f" (~{int(delta/3600)}h)"
                    else:
                        next_run_str = str(next_run)
                    
                    # Get block count
                    block_count = len(seq_data.sequence)
                    
                    # Add to table data
                    description = seq_data.description
                    if description and len(description) > 30:
                        description = description[:27] + "..."
                        
                    # Truncate sequence ID if too long
                    if len(seq_id) > 20:
                        display_id = seq_id[:17] + "..."
                    else:
                        display_id = seq_id
                    
                    table_data.append([
                        display_id,
                        status,
                        block_count,
                        seq_data.seq_run_interval,
                        next_run_str,
                        description
                    ])
            
            # Clear the previous output
            # Print ANSI escape code to clear the screen
            print("\033[H\033[J", end="")
            
            # Print daemon info
            logger.info(f"Daemon '{scheduler.instance_name}' status at {current_time.strftime('%H:%M:%S')} (refreshes every {args.interval}s)")
            logger.info(f"  PID: {os.getpid()}, ID: {scheduler.instance_id}")
            logger.info(f"  Sequences: {len(all_sequences)} total, {len(active_sequences)} active")
            
            # Check for any diagnostic log files
            import glob
            log_files = glob.glob("/tmp/blaze_sequence_*.log")
            if log_files:
                logger.info(f"  Found {len(log_files)} sequence execution logs in /tmp/")
                for log_file in log_files:
                    logger.info(f"    {log_file}")
            
            # Print the table
            if table_data:
                if has_tabulate:
                    try:
                        # Use tabulate for nice formatting
                        table = tabulate(table_data, headers=headers, tablefmt="pretty")
                        for line in table.split("\n"):
                            logger.info(line)
                    except Exception as e:
                        logger.warning(f"Error using tabulate: {e}")
                        # Fall back to simple formatting
                        logger.info("Sequences:")
                        for row in table_data:
                            logger.info(f"  {row[0]} ({row[1]}) - {row[2]} blocks - Next: {row[4]}")
                else:
                    # Simple formatting without tabulate
                    logger.info("SEQUENCES:")
                    # Print header
                    header_fmt = "{:<20} {:<10} {:<7} {:<15} {:<15} {:<30}"
                    logger.info(header_fmt.format(*headers))
                    logger.info("-" * min(100, term_width))
                    
                    # Print rows
                    row_fmt = "{:<20} {:<10} {:<7} {:<15} {:<15} {:<30}"
                    for row in table_data:
                        logger.info(row_fmt.format(*[str(x) for x in row]))
            else:
                logger.info("No sequences registered yet")

            
            # Sleep in smaller increments while checking if the daemon is still running
            # This allows us to detect if the daemon was stopped from another process
            check_interval = 1  # Check every second
            for _ in range(int(args.interval / check_interval)):
                time.sleep(check_interval)
                # Check if the daemon has been stopped externally
                if not is_scheduler_running():
                    logger.info("Daemon has been stopped from another process.")
                    logger.info("Exiting monitoring loop.")
                    break
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    finally:
        # For keyboard interrupt, we preserve sequence data (soft stop)
        shutdown_scheduler(clear_sequences=False)
        logger.info("Daemon stopped with sequence data preserved (soft stop)")

def stop_daemon(args):
    """Stop the scheduler daemon."""
    logger.info("Stopping Blaze scheduler daemon...")
    
    # Determine if sequences should be cleared based on stop mode
    clear_sequences = False
    
    if hasattr(args, 'mode'):
        if args.mode == 'h':
            # Hard stop - clear all persistence
            clear_sequences = True
            logger.info("Hard stop requested - all sequence data will be cleared")
        elif args.mode == 's':
            # Soft stop - keep persistence
            clear_sequences = False
            logger.info("Soft stop requested - sequence data will be preserved")
    elif hasattr(args, 'clear_sequences') and args.clear_sequences:
        # Legacy support for --clear-sequences
        clear_sequences = True
        logger.info("All sequence data will be cleared")
    
    shutdown_scheduler(clear_sequences=clear_sequences)
    if clear_sequences:
        logger.info("Daemon stopped with all data cleared (hard stop)")
    else:
        logger.info("Daemon stopped with sequence data preserved (soft stop)")

def status_daemon(args):
    """Check the status of the scheduler daemon."""
    if is_scheduler_running():
        scheduler = get_scheduler()
        
        # Show daemon information
        logger.info(f"Daemon '{scheduler.instance_name}' is running")
        logger.info(f"  ID: {scheduler.instance_id}")
        logger.info(f"  PID: {os.getpid()}")
        logger.info(f"  Workers: {scheduler.max_workers}")
        
        if hasattr(args, 'sequence') and args.sequence:
            # Show status for a specific sequence
            sequence_id = args.sequence
            status = scheduler.get_sequence_status(sequence_id)
            
            logger.info("\nSEQUENCE STATUS:")
            if status:
                logger.info(f"Sequence: {sequence_id}")
                logger.info(f"Status: {status.status}")
                logger.info(f"Start time: {status.start_time}")
                if status.end_time:
                    logger.info(f"End time: {status.end_time}")
                
                if args.verbose:
                    logs = scheduler.get_sequence_logs(sequence_id)
                    logger.info(f"\nLogs ({len(logs)}):")
                    for log in logs[-10:]:  # Show the last 10 logs
                        logger.info(f"  {log}")
            else:
                logger.info(f"No status found for sequence '{sequence_id}'")
        else:
            # Show general daemon status
            active_sequences = scheduler.get_active_sequences()
            all_sequences = scheduler._sequences
            
            logger.info(f"\nSEQUENCES:")
            logger.info(f"Total sequences: {len(all_sequences)}")
            logger.info(f"Active sequences: {len(active_sequences)}")
            
            if args.verbose:
                if all_sequences:
                    logger.info("\nAll sequences:")
                    for seq_id in all_sequences.keys():
                        status = scheduler.get_sequence_status(seq_id)
                        status_str = status.status.value if status else "UNKNOWN"
                        logger.info(f"  {seq_id}: {status_str}")
                
                if active_sequences:
                    logger.info("\nActive sequences:")
                    for seq_id, status in active_sequences.items():
                        logger.info(f"  {seq_id}: {status.status}")
    else:
        logger.info("Daemon is not running")

def list_sequences(args):
    """List all registered sequences."""
    if is_scheduler_running():
        scheduler = get_scheduler()
        
        # Show daemon information
        logger.info(f"Daemon '{scheduler.instance_name}' is running")
        logger.info(f"  ID: {scheduler.instance_id}")
        logger.info(f"  PID: {os.getpid()}")
        
        # Get all sequences from the scheduler
        sequences = scheduler._sequences
        
        if sequences:
            logger.info(f"\nFound {len(sequences)} registered sequences:")
            for seq_id, seq_data in sequences.items():
                status = scheduler.get_sequence_status(seq_id)
                status_str = status.status.value if status else "UNKNOWN"
                
                logger.info(f"\n- {seq_id}")
                logger.info(f"  Description: {seq_data.description}")
                logger.info(f"  Schedule: {seq_data.seq_run_interval}")
                logger.info(f"  Status: {status_str}")
                
                if seq_data.start_date:
                    logger.info(f"  Start date: {seq_data.start_date}")
                if seq_data.end_date:
                    logger.info(f"  End date: {seq_data.end_date}")
                    
                logger.info(f"  Blocks: {len(seq_data.sequence)}")
                
                # Show block information if verbose
                if hasattr(args, 'verbose') and args.verbose:
                    logger.info("\n  Blocks:")
                    for i, block in enumerate(seq_data.sequence):
                        logger.info(f"    {i+1}. {block.block_name}")
                        if block.dependencies:
                            logger.info(f"       Dependencies: {', '.join(block.dependencies)}")
        else:
            logger.info("\nNo sequences registered with the scheduler")
    else:
        logger.info("Daemon is not running")

def reset_data(args):
    """Reset all persisted data."""
    from src.core.persistence import BlazePersistence
    
    if args.force or input("Are you sure you want to clear all persisted data? (y/N): ").lower() == 'y':
        logger.info("Clearing all persisted data...")
        BlazePersistence.get_instance().clear_all()
        logger.info("All persisted data has been cleared.")
    else:
        logger.info("Operation cancelled.")

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Blaze Scheduler Daemon CLI",
        epilog="For more information, see the documentation."
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Start command
    start_parser = subparsers.add_parser(
        "start", 
        help="Start the scheduler daemon",
        description="Start the Blaze scheduler daemon and keep it running in the foreground."
    )
    start_parser.add_argument(
        "--workers", 
        type=int, 
        default=4, 
        help="Number of worker threads (default: 4)"
    )
    start_parser.add_argument(
        "--interval", 
        type=int, 
        default=20, 
        help="Status reporting interval in seconds (default: 20)"
    )
    start_parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Enable verbose logging"
    )
    start_parser.add_argument(
        "--log-file",
        type=str,
        help="Path to log file (if not specified, logs to stdout)"
    )
    start_parser.add_argument(
        "--force-new",
        action="store_true",
        help="Force creation of a new daemon instance, even if one is already running"
    )
    start_parser.add_argument(
        "--name",
        type=str,
        help="Custom name for the daemon instance (if not provided, a random name will be generated)"
    )
    
    # Stop command
    stop_parser = subparsers.add_parser(
        "stop", 
        help="Stop the scheduler daemon",
        description="Stop the running Blaze scheduler daemon. By default, sequence data is preserved for the next run.\n\n"
                  "Examples:\n"
                  "  blazed stop      # Soft stop (preserves sequences)\n"
                  "  blazed stop --s  # Soft stop (preserves sequences, explicit)\n"
                  "  blazed stop --h  # Hard stop (clears all sequences and data)"
    )
    
    # Define mutually exclusive group for stop mode options
    stop_mode_group = stop_parser.add_mutually_exclusive_group()
    stop_mode_group.add_argument(
        "--h", 
        dest="mode",
        action="store_const",
        const="h",
        help="Hard stop - completely remove all persistence data (sequences and daemon info)"
    )
    stop_mode_group.add_argument(
        "--s", 
        dest="mode",
        action="store_const",
        const="s",
        help="Soft stop - preserve all sequence data for the next run (default behavior)"
    )
    
    # Keep for backward compatibility
    stop_parser.add_argument(
        "--clear-sequences",
        action="store_true",
        help="Clear all persisted sequence data when stopping the daemon (same as --h)"
    )
    
    # Status command
    status_parser = subparsers.add_parser(
        "status", 
        help="Check the status of the scheduler daemon",
        description="Check if the Blaze scheduler daemon is running and show active sequences."
    )
    status_parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Show detailed status information including logs"
    )
    status_parser.add_argument(
        "--sequence", 
        type=str, 
        help="Show information for a specific sequence by ID"
    )
    
    # List command
    list_parser = subparsers.add_parser(
        "list", 
        help="List all registered sequences",
        description="List all sequences registered with the scheduler."
    )
    
    # Reset command
    reset_parser = subparsers.add_parser(
        "reset",
        help="Reset all persisted data",
        description="Clear all persisted sequence data and daemon information."
    )
    reset_parser.add_argument(
        "--force",
        action="store_true",
        help="Force reset without confirmation"
    )
    
    args = parser.parse_args()
    
    # Set up signal handlers
    handle_signals()
    
    # Configure logging if a log file was specified
    if hasattr(args, 'log_file') and args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {args.log_file}")
    
    # Execute the appropriate command
    try:
        if args.command == "start":
            print(f"Starting daemon")
            start_daemon(args)
        elif args.command == "stop":
            stop_daemon(args)
        elif args.command == "status":
            status_daemon(args)
        elif args.command == "list":
            list_sequences(args)
        elif args.command == "reset":
            reset_data(args)
        else:
            parser.print_help()
    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()