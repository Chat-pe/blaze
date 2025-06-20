#!/usr/bin/env python
"""
Simple test script to run the math ETL pipeline directly.
"""

import sys
import os
import time
from datetime import datetime

# Add the src directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import required modules
from src.core.daemon_manager import get_scheduler, is_scheduler_running
from examples.math_etl import math_pipeline

def main():
    print("=" * 50)
    print("TESTING MATH PIPELINE SUBMISSION")
    print("=" * 50)
    
    # Check if daemon is already running
    daemon_running = is_scheduler_running()
    if daemon_running:
        print("Daemon is already running.")
    else:
        print("Daemon is not running, starting a new one.")
    
    # Get or create a scheduler instance
    scheduler = get_scheduler(auto_shutdown=False)
    print(f"Scheduler instance: {scheduler.instance_name} (ID: {scheduler.instance_id})")
    
    # Submit the sequence directly from our script
    from examples.math_etl import math_pipeline
    
    # Get the sequence data and blocks
    seq_data = math_pipeline.seq_data
    sequence = math_pipeline.sequence
    
    # Submit the sequence to the scheduler
    try:
        print(f"Submitting sequence 'math_pipeline_1' to scheduler...")
        scheduler.submit_sequence(
            sequence_id="math_pipeline_1",
            sequence=sequence,
            seq_data=seq_data
        )
        print("Sequence submitted successfully!")
        
        # Wait for the sequence to run
        print("\nWaiting for sequence to run (checking status every 5 seconds)...")
        for _ in range(30):  # Check for up to 2.5 minutes
            status = scheduler.get_sequence_status("math_pipeline_1")
            print(f"Current status: {status}")
            
            # Check if the sequence has completed or failed
            if status and status.status in ["COMPLETED", "FAILED"]:
                print(f"Sequence {status.status.lower()}!")
                break
                
            # Sleep for 5 seconds before checking again
            time.sleep(5)
        
        # Display the logs
        logs = scheduler.get_sequence_logs("math_pipeline_1")
        print("\nSequence logs:")
        for log in logs:
            print(f"  {log}")
        
    except Exception as e:
        print(f"Error submitting sequence: {e}")
    
    print("\nDone! The daemon will continue running in the background.")
    print("You can stop it with: 'blazed stop'")

if __name__ == "__main__":
    main()