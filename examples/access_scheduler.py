"""
Example demonstrating how to access the scheduler from different parts of your application.
This shows how the daemon manager provides a global access point to the scheduler.
"""

import time
from datetime import datetime

from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

def print_separator():
    print("\n" + "="*50 + "\n")

def main():
    import sys
    
    print_separator()
    print("CHECKING SCHEDULER STATUS")
    print_separator()
    
    # Check if a scheduler is already running
    daemon_running = is_scheduler_running()
    
    if not daemon_running:
        print("No scheduler daemon is currently running.")
        print("Attempting to start a new daemon...")
        scheduler = get_scheduler(auto_start=True)
        print("Started a new scheduler daemon")
        print("Note: For production use, it's better to start the daemon separately:")
        print("  blazed start")
    else:
        print("✓ Scheduler daemon is running")
        # Connect to the existing scheduler
        scheduler = get_scheduler()
        print("Successfully connected to existing daemon")
    
    print_separator()
    print("REGISTERED SEQUENCES")
    print_separator()
    
    # Get all registered sequences
    all_sequences = scheduler._sequences
    
    if not all_sequences:
        print("No sequences are currently registered with the scheduler.")
        print("Run one of the example scripts to register a sequence:")
        print("  - python examples/math_etl.py")
        print("  - python examples/managed_etl.py")
        print_separator()
        sys.exit(0)
    
    print(f"Found {len(all_sequences)} registered sequences:")
    
    for seq_id, seq_data in all_sequences.items():
        status = scheduler.get_sequence_status(seq_id)
        status_str = status.status.value if status else "UNKNOWN"
        
        print(f"\n• {seq_id}")
        print(f"  Description: {seq_data.description}")
        print(f"  Schedule: {seq_data.seq_run_interval}")
        print(f"  Status: {status_str}")
        print(f"  Blocks: {len(seq_data.sequence)}")
        
        # Show the most recent log entries
        logs = scheduler.get_sequence_logs(seq_id)
        if logs:
            print(f"  Recent logs ({len(logs)} total):")
            for log in logs[-3:]:
                print(f"    {log}")
    
    print_separator()
    print("QUICK STATUS CHECK")
    print_separator()
    
    # Get active sequences for a quick status check
    active_sequences = scheduler.get_active_sequences()
    print(f"Currently active sequences: {len(active_sequences)}")
    
    if active_sequences:
        print("Active sequence IDs:")
        for seq_id, status in active_sequences.items():
            print(f"  • {seq_id}: {status.status}")
    
    print_separator()
    print("For more detailed monitoring, use:")
    print("  blazed status --verbose")
    print("  blazed status --sequence <sequence_id> --verbose")
    
    print("\nTo stop the daemon:")
    print("  blazed stop      # Soft stop (preserves sequences)")
    print("  blazed stop --s  # Soft stop (explicit)")
    print("  blazed stop --h  # Hard stop (clears all sequences)")
    print_separator()

if __name__ == "__main__":
    main()