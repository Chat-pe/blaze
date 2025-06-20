"""
Blaze core module exports
"""

# Export main classes
from src.core.block import BlazeBlock
from src.core.seq import BlazeSequence
from src.core.scheduler_daemon import SchedulerDaemon

# Export daemon manager functions for easy access
from src.core.daemon_manager import (
    get_scheduler,
    is_scheduler_running,
    shutdown_scheduler
)

# Export type definitions
from src.core._types import (
    BlockData,
    SeqBlockData,
    SeqData,
    SequenceStatus,
    SequenceResult
)

__all__ = [
    # Main classes
    'BlazeBlock',
    'BlazeSequence',
    'SchedulerDaemon',
    
    # Daemon manager
    'get_scheduler',
    'is_scheduler_running',
    'shutdown_scheduler',
    
    # Type definitions
    'BlockData',
    'SeqBlockData',
    'SeqData',
    'SequenceStatus',
    'SequenceResult'
]