"""
Blaze package - centralized imports and exports
Provides access to all core, daemon, and web-api components
"""

# Core module exports
from .core.block import BlazeBlock
from .core.seq import BlazeSequence
from .core.blaze import Blaze
from .core.logger import BlazeLogger
from .core.jobs import BlazeJobs
from .core.state import BlazeState
from .core.namegen import generate_scheduler_name

# Core type definitions
from .core._types import (
    BlockData,
    SeqBlockData,
    SeqData,
    SubmitSequenceData,
    SequenceExecutionData,
    SequenceStatus,
    SequenceResult,
    JobFile,
    JobExecutuionData,
    JobState,
    SequenceData,
    BlazeLock
)

# Daemon module exports
from .daemon.manager import (
    get_scheduler,
    get_or_create_scheduler,
    start_scheduler,
    stop_scheduler,
    update_jobs
)
from .daemon.daemon_cli import DaemonCLI

# Web API module exports
try:
    # Import using the correct path with hyphens
    import sys
    import os
    web_api_path = os.path.join(os.path.dirname(__file__), 'web-api')
    if web_api_path not in sys.path:
        sys.path.insert(0, web_api_path)
    
    from main import app as web_app
    from config import (
        API_TITLE,
        API_DESCRIPTION,
        API_VERSION,
        LOCK_FILE_PATH,
        JOBS_FILE_PATH,
        LOG_DIR,
        validate_paths,
        get_system_info
    )
    WEB_API_AVAILABLE = True
except ImportError:
    # Web API dependencies may not be installed
    WEB_API_AVAILABLE = False
    web_app = None

# Package version
__version__ = "1.0.0"

# All exports
__all__ = [
    # Core classes
    'BlazeBlock',
    'BlazeSequence', 
    'Blaze',
    'BlazeLogger',
    'BlazeJobs',
    'BlazeState',
    
    # Core utilities
    'generate_scheduler_name',
    
    # Core types
    'BlockData',
    'SeqBlockData',
    'SeqData',
    'SubmitSequenceData',
    'SequenceExecutionData',
    'SequenceStatus',
    'SequenceResult',
    'JobFile',
    'JobExecutuionData',
    'JobState',
    'SequenceData',
    'BlazeLock',
    
    # Daemon functions
    'get_scheduler',
    'get_or_create_scheduler',
    'start_scheduler',
    'stop_scheduler',
    'update_jobs',
    'DaemonCLI',
    
    # Web API (if available)
    'web_app',
    'WEB_API_AVAILABLE',
    
    # Package info
    '__version__'
]

# Conditional web API exports
if WEB_API_AVAILABLE:
    __all__.extend([
        'API_TITLE',
        'API_DESCRIPTION', 
        'API_VERSION',
        'LOCK_FILE_PATH',
        'JOBS_FILE_PATH',
        'LOG_DIR',
        'validate_paths',
        'get_system_info'
    ])

def get_version():
    """Get the package version."""
    return __version__

def get_available_modules():
    """Get information about available modules."""
    modules = {
        'core': True,
        'daemon': True,
        'web_api': WEB_API_AVAILABLE
    }
    return modules

def create_blaze_system(blocks: BlazeBlock, sequences: BlazeSequence, **kwargs):
    """
    Convenience function to create a complete Blaze system.
    
    Args:
        blocks (BlazeBlock): The blocks registry
        sequences (BlazeSequence): The sequences registry
        **kwargs: Additional arguments for Blaze constructor
        
    Returns:
        Blaze: Configured Blaze system instance
    """
    # Set default logger if not provided
    if 'logger' not in kwargs:
        kwargs['logger'] = BlazeLogger()
    
    return Blaze(
        blaze_blocks=blocks,
        sequences=sequences,
        **kwargs
    )

# Package information
PACKAGE_INFO = {
    'name': 'Blaze',
    'version': __version__,
    'description': 'A powerful job scheduling and workflow management system',
    'modules': get_available_modules(),
    'author': 'Blaze Team',
    'components': {
        'core': 'Core scheduling and workflow engine',
        'daemon': 'Daemon management and CLI tools', 
        'web_api': 'REST API for system management' if WEB_API_AVAILABLE else 'Not available'
    }
}
