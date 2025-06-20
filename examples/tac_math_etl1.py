from datetime import datetime, timedelta
import time
import random
import signal
import sys
import os
import json
from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core._types import SeqBlockData
from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

# Create a block manager
blocks = BlazeBlock()

# Define the math operation blocks
@blocks.block(
    name="generate_numbers",
    description="Generates random numbers for processing",
    tags=["math", "data"]
)
def generate_numbers(count: int = 5, min_val: int = 1, max_val: int = 100) -> list:
    """Generates a list of random numbers."""
    print(f"Generating {count} random numbers between {min_val} and {max_val}")
    numbers = [random.randint(min_val, max_val) for _ in range(count)]
    print(f"Generated numbers: {numbers}")
    # Save to file for visibility
    with open("/tmp/generated_numbers.txt", "a") as f:
        f.write(f"Generated at {datetime.now()}: {numbers}\n")
    return numbers

@blocks.block(
    name="calculate_statistics",
    description="Calculates basic statistics on a list of numbers",
    tags=["math", "stats"]
)
def calculate_statistics(numbers: list) -> dict:
    """Calculates mean, median, min, max, and sum of a list of numbers."""
    print(f"Calculating statistics for: {numbers}")
    
    # Sort the numbers for median calculation
    sorted_nums = sorted(numbers)
    n = len(sorted_nums)
    
    # Calculate median
    if n % 2 == 0:
        median = (sorted_nums[n//2 - 1] + sorted_nums[n//2]) / 2
    else:
        median = sorted_nums[n//2]
    
    stats = {
        "mean": sum(numbers) / len(numbers),
        "median": median,
        "min": min(numbers),
        "max": max(numbers),
        "sum": sum(numbers)
    }
    print(f"Statistics: {stats}")
    return stats

@blocks.block(
    name="transform_data",
    description="Performs additional transformations on statistical data",
    tags=["math", "transform"]
)
def transform_data(stats: dict) -> dict:
    """Performs additional transformations on statistics."""
    print(f"Transforming statistics: {stats}")
    json.dump(stats, open("stats.json", "w"))
    transformed = {
        "range": stats["max"] - stats["min"],
        "normalized_mean": stats["mean"] / stats["max"] if stats["max"] != 0 else 0,
        "squared_sum": stats["sum"] ** 2,
        "doubled_values": {k: v * 2 for k, v in stats.items() if isinstance(v, (int, float))}
    }
    
    # Merge original stats with transformed data
    result = {**stats, **transformed}
    print(f"Transformation complete: {result}")
    return result

# Create a sequence manager
sequences = BlazeSequence()

# Define a sequence that runs every 5 minutes
@sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(
            block_name="generate_numbers",
            parameters={"count": 10, "min_val": 1, "max_val": 100},
            dependencies=[]
        ),
        SeqBlockData(
            block_name="calculate_statistics",
            parameters={"numbers": "@generate_numbers"},
            dependencies=["generate_numbers"]
        ),
        SeqBlockData(
            block_name="transform_data",
            parameters={"stats": "@calculate_statistics"},
            dependencies=["calculate_statistics"]
        )
    ],
    seq_id="math_pipeline_1",
    description="A pipeline that performs mathematical operations every 5 minutes",
    seq_run_interval="*/1 * * * *",  # Run every 5 minutes
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(hours=1),
    retries=2,
    retry_delay=10,
    auto_start=True,
    fail_stop=False
)
def math_pipeline(context: dict = None) -> dict:
    """This is the main math pipeline function that will be executed."""
    print("Running math_pipeline with direct block execution")
    # result = {}
    
    # # Generate numbers
    # numbers = generate_numbers(count=10, min_val=1, max_val=100)
    # result["generate_numbers"] = numbers
    
    # # Calculate statistics
    # stats = calculate_statistics(numbers=numbers)
    # result["calculate_statistics"] = stats
    
    # # Transform data
    # transformed = transform_data(stats=stats)
    # result["transform_data"] = transformed
    
    # print(f"Math pipeline complete with results: {result}")
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
    # Set up signal handlers for graceful shutdown
    setup_signal_handlers()
    
    # Get the existing scheduler or create a new one
    print("Connecting to scheduler daemon...")
    daemon_running = is_scheduler_running()
    scheduler = get_scheduler(auto_shutdown=False)  # Don't shut down daemon when script exits
    
    if not daemon_running:
        print(f"Started a new scheduler daemon instance: '{scheduler.instance_name}'")
        print(f"  ID: {scheduler.instance_id}")
        print(f"  PID: {os.getpid()}")
        print("For production use, it's recommended to start the daemon separately:")
        print("  blazed start")
    else:
        print(f"Connected to existing daemon: '{scheduler.instance_name}'")
        print(f"  ID: {scheduler.instance_id}")
        print(f"  PID: {os.getpid()}")
    
    try:
        print_separator()
        print("SUBMITTING SEQUENCE TO SCHEDULER")
        print_separator()
        
        # Submit the sequence to the scheduler
        scheduler.submit_sequence(
            sequence_id="math_pipeline_1",
            sequence=math_pipeline.sequence,
            seq_data=math_pipeline.seq_data
        )
        print(f"Sequence 'math_pipeline' submitted successfully")
        print(f"Schedule: {math_pipeline.seq_data.seq_run_interval}")
        print(f"Next execution will occur automatically according to the schedule")
        
        print_separator()
        print("SEQUENCE INFORMATION")
        print_separator()
        print("Sequence ID: math_pipeline")
        print(f"Description: {math_pipeline.seq_data.description}")
        print(f"Number of blocks: {len(math_pipeline.sequence)}")
        print(f"Blocks in sequence:")
        for i, block in enumerate(math_pipeline.sequence):
            print(f"  {i+1}. {block.block_name}")
            if block.dependencies:
                print(f"     Dependencies: {', '.join(block.dependencies)}")
        
        print_separator()
        print("SEQUENCE SUBMITTED SUCCESSFULLY")
        print("The daemon will continue running this sequence in the background")
        print_separator()
        print("You can monitor the sequence using:")
        print("  - blazed status --sequence math_pipeline")
        print("  - blazed list")
        print("\nTo stop the daemon:")
        print("  - blazed stop      # Soft stop (preserves sequences)")
        print("  - blazed stop --s  # Soft stop (explicit)")
        print("  - blazed stop --h  # Hard stop (clears all sequences)")
        print_separator()
            
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()