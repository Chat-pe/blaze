# Blaze ETL Framework

A powerful, flexible, and modern ETL (Extract, Transform, Load) framework for building modular data pipelines with automatic scheduling, execution, and daemon-based architecture. Blaze enables you to create robust data processing workflows using a block-based system with intelligent dependency management and cron-based scheduling.

## 🚀 Key Features

- **🧱 Block-based Architecture**: Create reusable, modular processing units with decorators
- **🔗 Dependency Management**: Define execution order with intelligent dependency resolution
- **⏰ Cron Scheduling**: Schedule sequences using standard cron expressions
- **🔄 Persistent Daemon**: Background daemon that survives process restarts
- **🔀 Cross-process Communication**: Access the daemon from multiple processes
- **📊 Status Monitoring**: Real-time tracking with logs and execution status
- **🛡️ Error Handling**: Configurable retries, fail-stop modes, and comprehensive logging
- **🎯 Named Instances**: Readable daemon names with unique identifiers
- **💾 State Management**: Persistent job state with execution history
- **🧵 Concurrent Execution**: Multi-threaded execution with configurable worker pools

## 🏗️ Architecture

### Core Components

1. **BlazeBlock**: Decorator-based system for registering reusable function blocks
2. **BlazeSequence**: Manager for creating and orchestrating sequences of blocks  
3. **Blaze Scheduler**: Core scheduler that manages execution timing and state
4. **Daemon Manager**: Singleton pattern for cross-process daemon communication
5. **State Manager**: Persistent storage for job states and execution history
6. **CLI Interface**: Command-line tools for daemon management

### Execution Flow

```
Block Definition → Sequence Creation → Daemon Submission → Scheduled Execution
     ↓                    ↓                   ↓                    ↓
  @blocks.block()    @sequences.sequence()   blazed start      Cron Trigger
```

## 🛠️ Installation

### Using Poetry (Recommended)
```bash
git clone <repository-url>
cd blaze
poetry install
```

### Using pip
```bash
git clone <repository-url>
cd blaze
pip install -e .
```

### Dependencies
- Python 3.9+
- pydantic 2.5.0+
- croniter 2.0.0+
- loguru 0.7.2+
- tabulate 0.9.0+

## 📚 Quick Start

### 1. Define Blocks

```python
from src.core import BlazeBlock

# Create a block manager
blocks = BlazeBlock()

@blocks.block(
    name="fetch_data",
    description="Fetches data from external API",
    tags=["extract", "api"]
)
def fetch_data(endpoint: str, limit: int = 100) -> list:
    """Fetch data from an external API endpoint."""
    # Your data fetching logic here
    data = []  # fetch from API
    return data

@blocks.block(
    name="transform_data", 
    description="Applies transformations to raw data",
    tags=["transform", "clean"]
)
def transform_data(raw_data: list) -> dict:
    """Clean and transform the raw data."""
    # Your transformation logic here
    transformed = {"processed_count": len(raw_data), "data": raw_data}
    return transformed

@blocks.block(
    name="load_data",
    description="Loads processed data to destination",
    tags=["load", "database"]
)
def load_data(processed_data: dict) -> bool:
    """Load the processed data to the final destination."""
    # Your loading logic here
    print(f"Loaded {processed_data['processed_count']} records")
    return True
```

### 2. Create Sequences

```python
from src.core import BlazeSequence, SeqBlockData

# Create a sequence manager
sequences = BlazeSequence()

@sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(
            block_name="fetch_data",
            dependencies=[]
        ),
        SeqBlockData(
            block_name="transform_data", 
            dependencies=["fetch_data"]  # Depends on fetch_data completion
        ),
        SeqBlockData(
            block_name="load_data",
            dependencies=["transform_data"]  # Depends on transform_data completion
        )
    ],
    seq_id="etl_pipeline",
    description="Complete ETL pipeline running every 15 minutes",
    retries=3,                    # Retry failed blocks 3 times
    retry_delay=30,               # Wait 30 seconds between retries
    auto_start=True,              # Start immediately when registered
    fail_stop=False               # Continue sequence even if a block fails
)
def etl_pipeline(context: dict = None) -> dict:
    """Main ETL pipeline orchestration function."""
    return context or {}
```

### 3. Submit Jobs to Daemon

```python
from src.core import SubmitSequenceData
from src.daemon.manager import update_jobs
from datetime import datetime, timedelta

# Submit the sequence with parameters
update_jobs([
    SubmitSequenceData(
        seq_id="etl_pipeline",
        parameters={
            "fetch_data": {
                "endpoint": "https://api.example.com/data",
                "limit": 500
            },
            "transform_data": {
                "raw_data": "@fetch_data"  # Use output from fetch_data block
            },
            "load_data": {
                "processed_data": "@transform_data"  # Use output from transform_data
            }
        },
        seq_run_interval="*/15 * * * *",  # Run every 15 minutes
        start_date=datetime.now(),
        end_date=datetime.now() + timedelta(days=30)  # Run for 30 days
    )
])
```

## 🖥️ CLI Usage

### Daemon Management

```bash
# Start the daemon
blazed start

# Start with custom configuration
blazed start --name my-etl-daemon --workers 8 --definition ./my_definition.py

# Start in background (detached mode)
blazed start --detach

# Check daemon status
blazed status

# Stop daemon (soft stop - preserves job data)
blazed stop

# Stop daemon (hard stop - clears all data)
blazed stop --shutdown

# Pause daemon (can be resumed)
blazed pause
```

### Job Management

```bash
# List all jobs
blazed jobs list

# Submit jobs from file
blazed jobs submit --job-file ./jobs.json

# Clear all jobs
blazed jobs clear

# View logs
blazed logs --follow
```

### Monitoring

```bash
# View detailed status for specific sequence
blazed status --sequence etl_pipeline --verbose

# List all registered sequences
blazed list

# Monitor real-time status
watch -n 5 blazed status
```

## 🐍 Python API

### Daemon Management

```python
from src.daemon.manager import get_or_create_scheduler, start_scheduler, stop_scheduler

# Create or connect to existing daemon
scheduler = get_or_create_scheduler(blocks, sequences)

# Start the scheduler loop
start_scheduler()

# Stop with different modes
stop_scheduler(shutdown=False)  # Soft stop
stop_scheduler(shutdown=True)   # Hard stop
```

### Advanced Usage

```python
from src.core.blaze import Blaze
from src.core.logger import BlazeLogger

# Create custom scheduler instance
logger = BlazeLogger(silent=False)
scheduler = Blaze(
    blaze_blocks=blocks,
    sequences=sequences,
    logger=logger,
    max_workers=8,              # Number of concurrent workers
    auto_start=False,           # Don't start automatically
    loop_interval=5,            # Check for jobs every 5 seconds
    state_dir="./custom_logs"   # Custom state directory
)

# Manual control
scheduler.start()
status_report = scheduler.status()
print(status_report)
scheduler.stop(shutdown=False)
```

## ⚙️ Configuration

### Block Configuration

```python
@blocks.block(
    name="custom_block",
    description="Custom processing block", 
    tags=["custom", "processing"],
    data_model=MyCustomModel  # Optional Pydantic model for validation
)
def custom_block(data: dict) -> dict:
    # Block implementation
    return processed_data
```

### Sequence Configuration

```python
@sequences.sequence(
    blocks=blocks,
    sequence=[...],
    seq_id="my_sequence",
    description="Description of what this sequence does",
    seq_run_timeout=3600,       # Maximum execution time (seconds)
    retries=3,                  # Number of retry attempts
    retry_delay=60,             # Delay between retries (seconds)
    auto_start=True,            # Auto-start when daemon starts
    fail_stop=True              # Stop sequence on first failure
)
```

### Cron Schedule Examples

```python
# Every 5 minutes
seq_run_interval="*/5 * * * *"

# Every hour at minute 30
seq_run_interval="30 * * * *"

# Every day at 2:30 AM
seq_run_interval="30 2 * * *"

# Every Monday at 9:00 AM
seq_run_interval="0 9 * * 1"

# Every first day of the month at midnight
seq_run_interval="0 0 1 * *"
```

## 📊 Monitoring and Logging

### State Management

Blaze automatically tracks:
- ✅ Job execution status and results
- ⏱️ Execution times and performance metrics
- 🔄 Next run times based on cron schedules
- ❌ Error logs and failure reasons
- 📈 Historical execution data

### Log Files

Logs are written to timestamped files:
```
/tmp/log/run_{hour}_{date}.log
./log/{sequence_id}/state.json
./log/{sequence_id}/runs.json
```

### Status Table

```bash
$ blazed status

Blaze etl-daemon-x1y2z3 - PID 12345
There are 3 jobs in the state
The state will be next updated in 01/15/24 14:32:15

┌─────────────────┬─────────────────────┬─────────────────────────┬───────────┬─────────────────────────┐
│ Seq ID          │ Last Run            │ Next Run                │ Status    │ Total Execution Time    │
├─────────────────┼─────────────────────┼─────────────────────────┼───────────┼─────────────────────────┤
│ etl_pipeline    │ 01/15/24 14:15:00   │ 01/15/24 14:30:00 <15m  │ COMPLETED │ 45.23s                  │
│ data_cleanup    │ 01/15/24 13:00:00   │ 01/16/24 02:00:00 <11h  │ COMPLETED │ 120.45s                 │
│ weekly_report   │ Never run           │ 01/22/24 09:00:00 <7d   │ PENDING   │ 0.00s                   │
└─────────────────┴─────────────────────┴─────────────────────────┴───────────┴─────────────────────────┘
```

## 🧪 Examples

The `examples/` directory contains complete working examples:

- **`definition.py`** - Mathematical ETL pipeline with multiple blocks
- **`run.py`** - Simple daemon startup script
- **`submit.py`** - Job submission example

### Mathematical ETL Example

```python
# See examples/definition.py for a complete example
@blocks.block(name="generate_numbers", tags=["math"])
def generate_numbers(count: int = 5) -> list:
    return [random.randint(1, 100) for _ in range(count)]

@blocks.block(name="calculate_statistics", tags=["math", "stats"])  
def calculate_statistics(numbers: list) -> dict:
    return {
        "mean": sum(numbers) / len(numbers),
        "median": sorted(numbers)[len(numbers)//2],
        "sum": sum(numbers)
    }

@sequences.sequence(
    blocks=blocks,
    sequence=[
        SeqBlockData(block_name="generate_numbers", dependencies=[]),
        SeqBlockData(block_name="calculate_statistics", dependencies=["generate_numbers"])
    ],
    seq_id="math_pipeline",
    description="Mathematical processing pipeline"
)
def math_pipeline(context: dict = None) -> dict:
    return context or {}
```

## 🔧 Development

### Setting up Development Environment

```bash
# Install with development dependencies
poetry install --with dev

# Run tests
pytest

# Code formatting
black .
isort .

# Linting
ruff check .
mypy .
```

### Project Structure

```
blaze/
├── src/
│   ├── core/           # Core framework components
│   │   ├── blaze.py    # Main scheduler class
│   │   ├── block.py    # Block management
│   │   ├── seq.py      # Sequence management  
│   │   ├── state.py    # State persistence
│   │   └── logger.py   # Logging utilities
│   └── daemon/         # Daemon management
│       ├── manager.py  # Daemon lifecycle
│       └── daemon_cli.py # CLI interface
├── examples/           # Example pipelines
├── bin/               # Executable scripts
└── log/               # Runtime logs and state
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the test suite (`pytest`)
5. Format your code (`black . && isort .`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This means:
- ✅ You can use, modify, and distribute this software
- ✅ You can use it for commercial purposes
- ⚠️ If you distribute the software or run it as a service, you must make your source code available under the same license
- ⚠️ If you modify the software and provide it as a network service, you must provide the source code to users

See the [LICENSE](LICENSE) file for the full license text, or visit [https://www.gnu.org/licenses/agpl-3.0.html](https://www.gnu.org/licenses/agpl-3.0.html) for more details.

## 🙏 Acknowledgments

- Built with [Pydantic](https://pydantic.dev/) for data validation
- Scheduling powered by [croniter](https://github.com/kiorky/croniter)
- Logging enhanced with [Loguru](https://loguru.readthedocs.io/)
- CLI tables rendered with [Tabulate](https://github.com/astanin/python-tabulate)

---

**Happy ETL-ing with Blaze! 🔥**