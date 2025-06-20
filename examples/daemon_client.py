#!/usr/bin/env python
"""
A simple client that connects to a running Blaze daemon.

This script demonstrates how to connect to an existing daemon
from a separate process, and how to submit and monitor sequences.
"""

import sys
import time
import os
from datetime import datetime, timedelta

from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

def print_separator():
    print("\n" + "="*50 + "\n")

def main():
    print_separator()
    print("BLAZE DAEMON CLIENT")
    print_separator()
    
    # Check if a daemon is already running
    if not is_scheduler_running():
        print("No scheduler daemon is currently running.")
        print("Please start the daemon first using:")
        print("  blazed start")
        print_separator()
        sys.exit(1)
    
    print("✓ Found a running scheduler daemon")
    
    # Connect to the existing daemon (don't shut it down when we exit)
    scheduler = get_scheduler(auto_shutdown=False)
    print(f"Successfully connected to daemon '{scheduler.instance_name}'")
    print(f"  ID: {scheduler.instance_id}")
    print(f"  PID: {os.getpid()}")
    print(f"  Workers: {scheduler.max_workers}")
    
    # Check for registered sequences
    sequences = scheduler._sequences
    print(f"Found {len(sequences)} registered sequences")
    
    if sequences:
        print("\nRegistered sequences:")
        for seq_id in sequences.keys():
            status = scheduler.get_sequence_status(seq_id)
            status_str = status.status.value if status else "UNKNOWN"
            print(f"- {seq_id}: {status_str}")
    
    # Monitor for a few seconds
    print_separator()
    print("MONITORING FOR 10 SECONDS")
    print_separator()
    
    print("Checking daemon activity...")
    for i in range(5):
        active_sequences = scheduler.get_active_sequences()
        current_time = datetime.now().strftime("%H:%M:%S")
        
        print(f"[{current_time}] Active sequences: {len(active_sequences)}")
        if active_sequences:
            for seq_id, result in active_sequences.items():
                print(f"  - {seq_id}: {result.status}")
                
        # Sleep for a couple of seconds
        time.sleep(2)
    
    print_separator()
    print("CLIENT COMPLETE")
    print("The daemon continues running in the background")
    print("\nTo stop the daemon:")
    print("  - blazed stop      # Soft stop (preserves sequences)")
    print("  - blazed stop --s  # Soft stop (explicit)")
    print("  - blazed stop --h  # Hard stop (clears all sequences)")
    print_separator()
    
    # Note that we don't call shutdown_scheduler() here because we
    # want the daemon to continue running after this client exits

if __name__ == "__main__":
    main()