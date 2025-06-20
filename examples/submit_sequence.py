"""
Example demonstrating how to create and submit a sequence to a running daemon.
This script can be used as a template for creating new ETL sequences.
"""

from datetime import datetime, timedelta
import random
import sys

from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core._types import SeqBlockData
from src.core.daemon_manager import get_scheduler, is_scheduler_running

def print_separator():
    print("\n" + "="*50 + "\n")

def main():
    # Check if daemon is running
    daemon_running = is_scheduler_running()
    if not daemon_running:
        print("No scheduler daemon is currently running.")
        print("Starting a new daemon...")
        scheduler = get_scheduler(auto_start=True, auto_shutdown=False)  # Don't shut down daemon when script exits
        print("Daemon started, but it's recommended to run the daemon separately:")
        print("  blazed start")
    else:
        print("✓ Scheduler daemon is running")
        
        # Get information about the daemon
        scheduler = get_scheduler(auto_shutdown=False)  # Don't shut down daemon when script exits
        print(f"Connected to daemon")
    
    # Create a block manager
    blocks = BlazeBlock()
    
    # Define example blocks
    @blocks.block(
        name="generate_id",
        description="Generates a unique job ID",
        tags=["utility"]
    )
    def generate_id() -> str:
        """Generate a unique job ID."""
        job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"
        print(f"Generated job ID: {job_id}")
        return job_id
    
    @blocks.block(
        name="process_job",
        description="Processes a job",
        tags=["process"]
    )
    def process_job(job_id: str) -> dict:
        """Process a job by ID."""
        print(f"Processing job: {job_id}")
        
        # Simulate processing
        result = {
            "job_id": job_id,
            "status": "completed",
            "processed_at": datetime.now().isoformat(),
            "items_processed": random.randint(10, 100)
        }
        
        print(f"Job processed: {result}")
        return result
    
    @blocks.block(
        name="notify_completion",
        description="Sends a notification that the job is complete",
        tags=["notification"]
    )
    def notify_completion(job_result: dict) -> dict:
        """Send a notification that the job is complete."""
        job_id = job_result["job_id"]
        print(f"Sending notification for job: {job_id}")
        
        # Simulate sending a notification
        notification = {
            "job_id": job_id,
            "sent_at": datetime.now().isoformat(),
            "recipient": "admin@example.com",
            "message": f"Job {job_id} completed with {job_result['items_processed']} items processed"
        }
        
        print(f"Notification sent: {notification}")
        return notification
    
    # Create a sequence manager
    sequences = BlazeSequence()
    
    # Generate a unique sequence ID for this run
    sequence_id = f"job_sequence_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Define the sequence
    @sequences.sequence(
        blocks=blocks,
        sequence=[
            SeqBlockData(
                block_name="generate_id",
                parameters={},
                dependencies=[]
            ),
            SeqBlockData(
                block_name="process_job",
                parameters={"job_id": "@generate_id"},
                dependencies=["generate_id"]
            ),
            SeqBlockData(
                block_name="notify_completion",
                parameters={"job_result": "@process_job"},
                dependencies=["process_job"]
            )
        ],
        seq_id=sequence_id,
        description="Job processing sequence with notification",
        seq_run_interval="*/10 * * * *",  # Run every 10 minutes
        start_date=datetime.now(),
        end_date=datetime.now() + timedelta(hours=2),
        retries=1,
        retry_delay=30,
        auto_start=True,
        fail_stop=True
    )
    def job_sequence(context: dict = None) -> dict:
        """Job processing sequence."""
        return context or {}
    
    print_separator()
    print(f"CREATED NEW SEQUENCE: {sequence_id}")
    print_separator()
    
    # Re-use the scheduler instance from above, don't need to get it again
    # scheduler = get_scheduler()
    
    print("Submitting sequence to scheduler...")
    scheduler.submit_sequence(
        sequence_id=sequence_id,
        sequence=job_sequence.sequence,
        seq_data=job_sequence.seq_data
    )
    print("Sequence submitted successfully!")
    
    print_separator()
    print("SEQUENCE DETAILS")
    print_separator()
    print(f"ID: {sequence_id}")
    print(f"Description: {job_sequence.seq_data.description}")
    print(f"Schedule: {job_sequence.seq_data.seq_run_interval}")
    print(f"Start date: {job_sequence.seq_data.start_date}")
    print(f"End date: {job_sequence.seq_data.end_date}")
    print(f"Number of blocks: {len(job_sequence.sequence)}")
    
    print_separator()
    print("MONITORING COMMANDS")
    print_separator()
    print(f"To monitor this sequence, run:")
    print(f"  blazed status --sequence {sequence_id} --verbose")
    print(f"To list all sequences:")
    print(f"  blazed list")
    print_separator()

if __name__ == "__main__":
    main()