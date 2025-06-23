from datetime import datetime, timedelta
import time
import random
import signal
import sys
import os
import json
from blaze.src.core.block import BlazeBlock
from blaze.src.core.seq import BlazeSequence
from blaze.src.core._types import SeqBlockData

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
math_pipeline = sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(
            block_name="generate_numbers",
            dependencies=[]
        ),
        SeqBlockData(
            block_name="calculate_statistics",
            dependencies=["generate_numbers"]
        ),
        SeqBlockData(
            block_name="transform_data",
            dependencies=["calculate_statistics"]
        )
    ],
    seq_id="math_pipeline_1",
    description="A pipeline that performs mathematical operations every 5 minutes",
    retries=2,
    retry_delay=10,
    auto_start=True,
    fail_stop=False
)

