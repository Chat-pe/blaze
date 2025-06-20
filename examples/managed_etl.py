"""
Example demonstrating the use of the daemon manager to run ETL tasks.
This approach uses the singleton pattern for accessing the scheduler daemon.
"""

from datetime import datetime, timedelta
import time
import random
import signal
import sys

from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core._types import SeqBlockData
from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

# Create a block manager
blocks = BlazeBlock()

# Define the math operation blocks
@blocks.block(
    name="fetch_data",
    description="Fetches simulated data",
    tags=["etl", "data"]
)
def fetch_data(records: int = 5) -> list:
    """Simulates fetching data from a source."""
    print(f"Fetching {records} records...")
    data = [
        {
            "id": i,
            "value": random.randint(1, 100),
            "timestamp": datetime.now().isoformat()
        }
        for i in range(records)
    ]
    print(f"Fetched {len(data)} records")
    return data

@blocks.block(
    name="process_data",
    description="Processes the fetched data",
    tags=["etl", "transform"]
)
def process_data(data: list) -> list:
    """Processes the fetched data."""
    print(f"Processing {len(data)} records...")
    
    processed = []
    for record in data:
        processed.append({
            **record,
            "processed_value": record["value"] * 2,
            "status": "processed"
        })
    
    print(f"Processed {len(processed)} records")
    return processed

@blocks.block(
    name="load_data",
    description="Loads the processed data",
    tags=["etl", "load"]
)
def load_data(data: list) -> dict:
    """Simulates loading data to a destination."""
    print(f"Loading {len(data)} records...")
    
    # In a real scenario, this would write to a database or file
    total_value = sum(record["processed_value"] for record in data)
    average_value = total_value / len(data) if data else 0
    
    result = {
        "records_loaded": len(data),
        "total_value": total_value,
        "average_value": average_value,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"Loaded data with summary: {result}")
    return result

# Create a sequence manager
sequences = BlazeSequence()

# Define a sequence that runs every 5 minutes
@sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(
            block_name="fetch_data",
            parameters={"records": 10},
            dependencies=[]
        ),
        SeqBlockData(
            block_name="process_data",
            parameters={"data": "@fetch_data"},
            dependencies=["fetch_data"]
        ),
        SeqBlockData(
            block_name="load_data",
            parameters={"data": "@process_data"},
            dependencies=["process_data"]
        )
    ],
    seq_id="managed_etl_pipeline",
    description="An ETL pipeline managed by the daemon manager",
    seq_run_interval="*/5 * * * *",  # Run every 5 minutes
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(hours=1),
    retries=2,
    retry_delay=10,
    auto_start=True,
    fail_stop=False
)
def managed_etl_pipeline(context: dict = None) -> dict:
    """This is the main ETL pipeline function that will be executed."""
    return context or {}

def print_separator():
    print("\n" + "="*50 + "\n")

def setup_signal_handlers():
    def signal_handler(sig, frame):
        print("\nShutting down gracefully...")
        # Use soft stop to preserve sequence data
        shutdown_scheduler(clear_sequences=False)
        print("Daemon stopped with sequence data preserved (soft stop)")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    print_separator()
    print("CONNECTING TO SCHEDULER DAEMON")
    print_separator()
    
    # Check if a daemon is already running
    daemon_running = is_scheduler_running()
    
    # Connect to the existing scheduler or create a new one
    scheduler = get_scheduler(auto_shutdown=False)  # Don't shut down daemon when script exits
    
    if daemon_running:
        print("Successfully connected to existing scheduler daemon")
    else:
        print("Started a new scheduler daemon instance")
        print("Note: For production use, it's better to start the daemon separately:")
        print("  blazed start")
    
    print_separator()
    print("SUBMITTING ETL SEQUENCE")
    print_separator()
    
    # Submit the sequence to the scheduler
    try:
        scheduler.submit_sequence(
            sequence_id="managed_etl_pipeline",
            sequence=managed_etl_pipeline.sequence,
            seq_data=managed_etl_pipeline.seq_data
        )
        print("Sequence 'managed_etl_pipeline' submitted successfully")
    except ValueError as e:
        if "already exists" in str(e):
            print("Note: This sequence was already registered with the scheduler")
            print("The existing sequence will continue to run according to its schedule")
        else:
            raise
    
    # Set up signal handlers for graceful shutdown
    setup_signal_handlers()
    
    # Display sequence information
    print_separator()
    print("SEQUENCE DETAILS")
    print_separator()
    
    print(f"ID: managed_etl_pipeline")
    print(f"Description: {managed_etl_pipeline.seq_data.description}")
    print(f"Schedule: {managed_etl_pipeline.seq_data.seq_run_interval}")
    print(f"Start date: {managed_etl_pipeline.seq_data.start_date}")
    print(f"End date: {managed_etl_pipeline.seq_data.end_date}")
    
    print("\nBlocks:")
    for i, block in enumerate(managed_etl_pipeline.sequence):
        print(f"  {i+1}. {block.block_name}")
        if block.parameters:
            params = ', '.join(f"{k}={v}" for k, v in block.parameters.items())
            print(f"     Parameters: {params}")
        if block.dependencies:
            print(f"     Dependencies: {', '.join(block.dependencies)}")
    
    print_separator()
    print("SEQUENCE REGISTRATION COMPLETE")
    print("The daemon will execute this sequence according to its schedule")
    print_separator()
    
    print("You can monitor this sequence using:")
    print("  blazed status --sequence managed_etl_pipeline --verbose")
    print("  blazed list")
    
    print("\nTo stop the daemon:")
    print("  blazed stop      # Soft stop (preserves sequences)")
    print("  blazed stop --s  # Soft stop (explicit)")
    print("  blazed stop --h  # Hard stop (clears all sequences)")
    print_separator()

if __name__ == "__main__":
    main()