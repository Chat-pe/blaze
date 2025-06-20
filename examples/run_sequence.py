#!/usr/bin/env python
"""
Script to manually run a sequence in the scheduler.
"""

import sys
from datetime import datetime

from src.core.daemon_manager import get_scheduler, is_scheduler_running

def main():
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python run_sequence.py <sequence_id>")
        return
    
    # Get the sequence ID from command line
    sequence_id = sys.argv[1]
    print(f"Attempting to run sequence: {sequence_id}")
    
    # Check if a daemon is running
    if not is_scheduler_running():
        print("No scheduler daemon is running. Start a daemon first with 'blazed start'")
        return
    
    # Connect to the daemon
    scheduler = get_scheduler(auto_shutdown=False)
    print(f"Connected to daemon '{scheduler.instance_name}'")
    
    # Check if the sequence exists
    if sequence_id not in scheduler._sequences:
        print(f"Sequence '{sequence_id}' not found in scheduler")
        print("Available sequences:")
        for seq_id in scheduler._sequences.keys():
            print(f"- {seq_id}")
        return
    
    # Get the sequence data
    seq_data = scheduler._sequences[sequence_id]
    print(f"Found sequence: {sequence_id}")
    print(f"Description: {seq_data.description}")
    print(f"Schedule: {seq_data.seq_run_interval}")
    
    # Run the sequence manually
    print(f"Executing sequence...")
    try:
        # Get the sequence function
        sequence_func = seq_data.sequence_func
        
        # Execute the sequence
        result = sequence_func({})
        
        print(f"Sequence executed successfully!")
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error executing sequence: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()