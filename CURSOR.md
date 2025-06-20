# Blaze ETL Framework

Blaze is a powerful and flexible ETL (Extract, Transform, Load) framework designed for building modular data pipelines with automatic scheduling and execution. It provides a daemon-based architecture for running sequences of processing blocks according to cron schedules.

## Core Concepts

### Blocks
Functions decorated with `@blocks.block()` to create reusable processing units. Each block takes input data, performs operations, and returns output that can be used by other blocks.

```python
@blocks.block(
    name="fetch_data",
    description="Fetches data from a source",
    tags=["etl", "extract"]
)
def fetch_data(source: str = "api") -> list:
    # Fetch data logic here
    return data_list
```

### Sequences
Pipelines composed of blocks with defined dependencies. Sequences are created using the `@sequences.sequence()` decorator and can be scheduled to run at specific intervals using cron expressions.

```python
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
            parameters={"data": "@fetch_data"},  # Reference output from fetch_data
            dependencies=["fetch_data"]
        )
    ],
    seq_id="my_pipeline",
    seq_run_interval="*/15 * * * *"  # Run every 15 minutes
)
def my_pipeline(context: dict = None) -> dict:
    return context or {}
```

### Scheduler Daemon
A background process that manages and executes sequences according to their schedules. The daemon can:
- Run multiple sequences concurrently
- Persist sequence data across restarts
- Be managed via CLI or Python API
- Handle cross-process communication

## Architecture

The framework consists of several key components:

1. **BlazeBlock**: Decorator-based system for registering function blocks
2. **BlazeSequence**: Manager for creating and tracking sequences of blocks
3. **SchedulerDaemon**: Daemon process that schedules and executes sequences
4. **DaemonManager**: Singleton pattern implementation for daemon access
5. **Persistence**: JSON-based system for storing sequence data
6. **CLI Interface**: Command-line tools for managing the daemon

## Key Features

- **Block-based Architecture**: Create reusable, modular processing units
- **Dependency Management**: Define execution order with dependencies between blocks
- **Cron Scheduling**: Schedule sequences using standard cron expressions
- **Persistence**: Sequences are saved across daemon restarts
- **Cross-process Communication**: Access the daemon from multiple processes
- **Named Instances**: Daemons have readable names and unique IDs
- **Status Monitoring**: Track sequence execution with logs and status updates
- **Hard/Soft Stop Modes**: Control persistence when stopping the daemon

## Daemon Management

### Starting a Daemon

The daemon can be started via CLI:
```bash
blazed start
blazed start --name custom-name --workers 8
```

Or via Python:
```python
from src.core.daemon_manager import get_scheduler
scheduler = get_scheduler(auto_shutdown=False)
```

### Stopping a Daemon

The daemon supports two stop modes:
- **Soft Stop**: Preserves sequence data for the next run
- **Hard Stop**: Clears all sequence data

Via CLI:
```bash
blazed stop      # Soft stop (default)
blazed stop --s  # Explicit soft stop
blazed stop --h  # Hard stop
```

Via Python:
```python
from src.core.daemon_manager import shutdown_scheduler
shutdown_scheduler(clear_sequences=False)  # Soft stop
shutdown_scheduler(clear_sequences=True)   # Hard stop
```

## Sequence Execution

Sequences are automatically executed based on their cron schedules. The scheduler daemon:
1. Checks if it's time to run a sequence
2. Executes the sequence in a separate thread/process
3. Handles dependencies between blocks
4. Tracks execution status and logs
5. Stores results for analysis

## Status Monitoring

The status of sequences can be monitored:

Via CLI:
```bash
blazed status
blazed status --sequence my_pipeline --verbose
blazed list
```

Via Python:
```python
status = scheduler.get_sequence_status("my_pipeline")
logs = scheduler.get_sequence_logs("my_pipeline")
active_sequences = scheduler.get_active_sequences()
```

## Best Practices

1. **Block Design**: 
   - Keep blocks small and focused on a single task
   - Ensure blocks have clear input/output contracts
   - Add descriptive tags for better organization

2. **Sequence Design**:
   - Use meaningful sequence IDs
   - Set appropriate retry configurations
   - Define clear dependencies between blocks

3. **Daemon Management**:
   - For production, start the daemon as a separate process
   - Use soft stops unless you need to clear all data
   - Monitor daemon status regularly

4. **Error Handling**:
   - Set appropriate fail_stop behavior
   - Configure retries for transient failures
   - Log errors comprehensively

## Common Issues and Solutions

1. **Daemon Not Connecting**: Ensure the daemon is running before trying to connect
2. **Sequence Not Running**: Check cron expression and ensure start/end dates are set correctly
3. **Cross-Process Issues**: Use `get_scheduler()` to connect to existing daemon
4. **Missing Dependencies**: Ensure all blocks referenced in dependencies exist
5. **Persistence Problems**: Use debug_persistence.py to debug persistence issues