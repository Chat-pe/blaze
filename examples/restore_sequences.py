#!/usr/bin/env python
"""
Tool to manually restore sequences to a running daemon.
This is useful if sequences were not properly restored on daemon startup.
"""

import os
import sys
import pickle
import json
import tempfile
from datetime import datetime

# Import Blaze modules
from src.core.persistence import SEQUENCES_FILE, REGISTRY_FILE
from src.core.daemon_manager import get_scheduler, is_scheduler_running

def print_separator(text=""):
    print("\n" + "="*50)
    if text:
        print(text)
        print("="*50)
    print()

def restore_sequences():
    """Restore sequences from persistence files to a running daemon."""
    # Check if a daemon is running
    if not is_scheduler_running():
        print("No scheduler daemon is running. Start a daemon first with 'blazed start'")
        return False
    
    # Check if sequence file exists
    if not os.path.exists(SEQUENCES_FILE):
        print(f"No sequence file found at {SEQUENCES_FILE}")
        return False
    
    # Load sequences from file
    try:
        print(f"Loading sequences from {SEQUENCES_FILE}...")
        with open(SEQUENCES_FILE, 'rb') as f:
            sequences = pickle.load(f)
        
        print(f"Found {len(sequences)} sequences to restore")
        
        # Connect to the running daemon
        print("Connecting to running scheduler daemon...")
        scheduler = get_scheduler(auto_shutdown=False)
        
        # Get existing sequences in the daemon
        existing_sequences = scheduler._sequences
        print(f"Daemon currently has {len(existing_sequences)} sequences")
        
        # Identify sequences to restore (not already in daemon)
        sequences_to_restore = {}
        for seq_id, seq_data in sequences.items():
            if seq_id not in existing_sequences:
                sequences_to_restore[seq_id] = seq_data
        
        print(f"Found {len(sequences_to_restore)} sequences to restore")
        
        # Restore each sequence
        restored_count = 0
        for seq_id, seq_data in sequences_to_restore.items():
            try:
                print(f"Restoring sequence: {seq_id}")
                
                # Extract sequence blocks from the seq_data
                sequence_blocks = seq_data.sequence
                
                # Submit to scheduler
                scheduler.submit_sequence(
                    sequence_id=seq_id,
                    sequence=sequence_blocks,
                    seq_data=seq_data
                )
                restored_count += 1
                print(f"Successfully restored {seq_id}")
            except Exception as e:
                print(f"Error restoring sequence {seq_id}: {e}")
        
        print(f"\nRestoration complete. Restored {restored_count} out of {len(sequences_to_restore)} sequences.")
        return True
    
    except Exception as e:
        print(f"Error during restoration: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print_separator("SEQUENCE RESTORATION TOOL")
    
    if restore_sequences():
        print("\nSequences were successfully restored to the running daemon.")
        print("You can verify with: blazed list")
    else:
        print("\nFailed to restore sequences. See errors above for details.")

if __name__ == "__main__":
    main()