# Blaze

A flexible and powerful ETL scheduling and pipeline framework with persistent daemon capabilities.

## Features

- Block-based architecture for modular and reusable components
- Sequence-based pipeline definition with dependency management
- Scheduler daemon for automated execution based on cron schedules
- Robust logging and status tracking
- Singleton pattern for daemon management
- Cross-process communication with daemon persistence
- Named daemon instances with unique IDs
- Tabular view for monitoring sequence status
- Hard and soft stop modes for daemon management

## Installation

```bash
# Install using Poetry
poetry install

# Or using pip
pip install -e .
```

## Basic Usage

### Defining Blocks

```python
from src.core import BlazeBlock

# Create a block manager
blocks = BlazeBlock()

# Define a block
@blocks.block(
    name="process_data",
    description="Processes input data",
    tags=["data", "transform"]
)
def process_data(data: list) -> dict:
    """Process the input data and return results."""
    # Your processing logic here
    result = {}
    for item in data:
        # Process each item
        pass
    return result
```

### Creating Sequences

```python
from src.core import BlazeSequence, SeqBlockData

# Create a sequence manager
sequences = BlazeSequence()

# Define a sequence
@sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(
            block_name="fetch_data",
            parameters={"source": "api"},
            dependencies=[]
        ),
        SeqBlockData(
            block_name="process_data",
            parameters={"data": "@fetch_data"},
            dependencies=["fetch_data"]
        )
    ],
    seq_id="my_pipeline",
    description="My data pipeline",
    seq_run_interval="*/15 * * * *"  # Run every 15 minutes
)
def my_pipeline(context: dict = None) -> dict:
    """Pipeline function."""
    return context or {}
```

### Running the Scheduler

#### Using the CLI

```bash
# Start the daemon
blazed start

# Start with a custom name
blazed start --name my-custom-daemon

# Start with more worker threads
blazed start --workers 8

# Check daemon status
blazed status

# View detailed status for a specific sequence
blazed status --sequence my_pipeline --verbose

# List all registered sequences
blazed list

# Stop the daemon (soft stop - preserves sequences)
blazed stop

# Stop the daemon with explicit soft stop (preserves sequences)
blazed stop --s

# Stop the daemon with hard stop (clears all sequences)
blazed stop --h

# Clear all persisted data
blazed reset
```

#### From Python Code

```python
from src.core.daemon_manager import get_scheduler, is_scheduler_running, shutdown_scheduler

# Check if a daemon is already running
if is_scheduler_running():
    print("Connecting to existing daemon...")
else:
    print("No daemon running, will create a new one")

# Get or create the scheduler (connects to existing daemon if one is running)
scheduler = get_scheduler(
    max_workers=4,              # Number of worker threads
    auto_start=True,            # Start the daemon automatically
    auto_shutdown=False,        # Don't shut down when script exits
    init_args={
        "persistence_enabled": True,  # Enable persistence of sequences
        "name": "my-daemon"           # Custom name for the daemon
    }
)

# Submit a sequence
scheduler.submit_sequence(
    sequence_id="my_pipeline",
    sequence=my_pipeline.sequence,
    seq_data=my_pipeline.seq_data
)

# Check status
status = scheduler.get_sequence_status("my_pipeline")
logs = scheduler.get_sequence_logs("my_pipeline")
active_sequences = scheduler.get_active_sequences()

# Shutdown the daemon
# - Use clear_sequences=False for soft stop (preserves sequences)
# - Use clear_sequences=True for hard stop (clears all sequences)
shutdown_scheduler(clear_sequences=False)
```

## Examples

See the `examples/` directory for complete working examples:

- `math_etl.py` - Mathematical ETL pipeline with daemon management
- `managed_etl.py` - Example using the daemon manager
- `access_scheduler.py` - Accessing a running scheduler from different parts of your code
- `daemon_client.py` - Client for connecting to a running daemon
- `debug_persistence.py` - Tools for debugging persistence issues
- `restore_sequences.py` - Example of restoring sequences from persistence
- `submit_sequence.py` - Submitting a sequence to a running daemon

## Development

```bash
# Install development dependencies
poetry install --with dev

# Run tests
pytest

# Format code
black .
isort .

# Lint
ruff check .
mypy .
```

## License

[MIT](LICENSE)