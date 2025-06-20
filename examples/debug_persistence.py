#!/usr/bin/env python
"""
Debugging script for Blaze sequence persistence.
This script directly inspects and manipulates persisted data.
"""

import os
import sys
import pickle
import json
import tempfile
from datetime import datetime
from pprint import pprint

# Import Blaze modules
from src.core.persistence import SEQUENCES_FILE, REGISTRY_FILE, load_sequences, save_sequences, clear_persistence_files
from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler
from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core._types import SeqBlockData

def print_separator(text=""):
    print("\n" + "="*50)
    if text:
        print(text)
        print("="*50)
    print()

def inspect_persistence_files():
    """Inspect all persistence files and their contents."""
    print_separator("PERSISTENCE FILES")
    
    # Check if files exist
    print(f"Sequences file: {SEQUENCES_FILE}")
    print(f"  Exists: {os.path.exists(SEQUENCES_FILE)}")
    if os.path.exists(SEQUENCES_FILE):
        print(f"  Size: {os.path.getsize(SEQUENCES_FILE)} bytes")
        print(f"  Modified: {datetime.fromtimestamp(os.path.getmtime(SEQUENCES_FILE))}")
    
    print(f"\nRegistry file: {REGISTRY_FILE}")
    print(f"  Exists: {os.path.exists(REGISTRY_FILE)}")
    if os.path.exists(REGISTRY_FILE):
        print(f"  Size: {os.path.getsize(REGISTRY_FILE)} bytes")
        print(f"  Modified: {datetime.fromtimestamp(os.path.getmtime(REGISTRY_FILE))}")
        
        # Display registry contents
        try:
            with open(REGISTRY_FILE, 'r') as f:
                registry_data = json.load(f)
                print("\nRegistry contents:")
                pprint(registry_data)
        except Exception as e:
            print(f"Error reading registry file: {e}")
    
    # Try to load the sequences
    print_separator("LOADING SEQUENCES")
    try:
        sequences, daemon_info = load_sequences()
        print(f"Loaded {len(sequences)} sequences")
        
        if sequences:
            print("\nSequence IDs:")
            for seq_id in sequences.keys():
                print(f"- {seq_id}")
            
            # Print details of first sequence
            first_seq_id = next(iter(sequences.keys()))
            first_seq = sequences[first_seq_id]
            print(f"\nDetails of sequence '{first_seq_id}':")
            print(f"  Description: {first_seq.description}")
            print(f"  Schedule: {first_seq.seq_run_interval}")
            print(f"  Blocks: {len(first_seq.sequence)}")
        
        print("\nDaemon info:")
        pprint(daemon_info)
    except Exception as e:
        print(f"Error loading sequences: {e}")
        import traceback
        traceback.print_exc()

def create_test_sequence():
    """Create a test sequence for debugging."""
    print_separator("CREATING TEST SEQUENCE")
    
    # Create a block manager
    blocks = BlazeBlock()
    
    # Define a simple test block
    @blocks.block(
        name="debug_test",
        description="A test block for debugging",
        tags=["debug"]
    )
    def debug_test(value: str) -> str:
        """Test block for debugging."""
        result = f"Debug test: {value}"
        print(result)
        return result
    
    # Create a sequence manager
    sequences = BlazeSequence()
    
    # Generate a unique ID with timestamp
    seq_id = f"debug_sequence_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Define a simple sequence
    @sequences.sequence(
        blocks=blocks,
        sequence=[
            SeqBlockData(
                block_name="debug_test",
                parameters={"value": "test value"},
                dependencies=[]
            )
        ],
        seq_id=seq_id,
        description="Debug test sequence",
        seq_run_interval="* * * * *"  # Run every minute
    )
    def debug_sequence(context=None):
        """Debug test sequence."""
        return context or {}
    
    print(f"Created test sequence: {seq_id}")
    
    # Connect to the scheduler and submit the sequence
    if is_scheduler_running():
        print("Connecting to running scheduler daemon...")
        scheduler = get_scheduler(auto_shutdown=False)
        
        try:
            print(f"Submitting sequence {seq_id} to scheduler...")
            scheduler.submit_sequence(
                sequence_id=seq_id,
                sequence=debug_sequence.sequence,
                seq_data=debug_sequence.seq_data
            )
            print("Sequence submitted successfully")
            
            # Wait a moment to ensure it's saved
            import time
            time.sleep(1)
        except Exception as e:
            print(f"Error submitting sequence: {e}")
    else:
        print("No scheduler is running. Start a daemon first with 'blazed start'")

def clear_all_data():
    """Clear all persisted data."""
    print_separator("CLEARING ALL DATA")
    
    try:
        # This is equivalent to doing a hard stop (--h) when the daemon is running
        if is_scheduler_running():
            print("Daemon is running. Performing a hard stop (--h)")
            shutdown_scheduler(clear_sequences=True)
            print("Daemon stopped with all data cleared (hard stop)")
        else:
            # Just clear the files directly if no daemon is running
            clear_persistence_files()
            print("All persistence files cleared")
    except Exception as e:
        print(f"Error clearing persistence files: {e}")

def check_daemon_status():
    """Check daemon status and sequences."""
    print_separator("DAEMON STATUS")
    
    if is_scheduler_running():
        print("Scheduler daemon is running")
        scheduler = get_scheduler(auto_shutdown=False)
        
        # Get sequence information
        sequences = scheduler._sequences
        print(f"Active sequences: {len(sequences)}")
        
        if sequences:
            print("\nSequence IDs:")
            for seq_id in sequences.keys():
                print(f"- {seq_id}")
        else:
            print("\nNo sequences found in scheduler instance")
    else:
        print("No scheduler daemon is running")

def main():
    """Main function to run debug operations."""
    if len(sys.argv) < 2:
        print("Please specify a command: inspect, create, clear, status, all")
        print("\nAdditional commands for the daemon:")
        print("  stop-soft  - Stop the daemon preserving sequences (--s)")
        print("  stop-hard  - Stop the daemon clearing all data (--h)")
        return
    
    command = sys.argv[1].lower()
    
    if command == "inspect" or command == "all":
        inspect_persistence_files()
    
    if command == "create" or command == "all":
        create_test_sequence()
    
    if command == "status" or command == "all":
        check_daemon_status()
    
    if command == "clear":
        clear_all_data()
        
    if command == "stop-soft":
        if is_scheduler_running():
            print_separator("STOPPING DAEMON (SOFT)")
            shutdown_scheduler(clear_sequences=False)
            print("Daemon stopped with sequence data preserved (soft stop)")
        else:
            print("No daemon is running")
            
    if command == "stop-hard":
        if is_scheduler_running():
            print_separator("STOPPING DAEMON (HARD)")
            shutdown_scheduler(clear_sequences=True)
            print("Daemon stopped with all data cleared (hard stop)")
        else:
            print("No daemon is running")

if __name__ == "__main__":
    main()