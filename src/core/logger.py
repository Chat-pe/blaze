import os
from datetime import datetime
from pathlib import Path
from loguru import logger as loguru_logger
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.db.mongo import BlazeMongoClient


class BlazeLogger:
    """
    Custom logger that wraps loguru's logger and adds timestamped file logging.
    Logs are written to /tmp/log/run_{hour}_{date}.log before being processed by loguru.
    """
    
    def __init__(self, silent: bool = False):
        self._loguru = loguru_logger
        self._silent = silent
        self._mongo_client: Optional['BlazeMongoClient'] = None
        self._ensure_log_directory()
    
    def _ensure_log_directory(self):
        """Ensure the /tmp/log directory exists."""
        log_dir = Path("/tmp/log")
        log_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_log_filename(self) -> str:
        """Generate timestamped log filename in format: run_{hour}_{date}.log"""
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        return f"/tmp/log/run_{hour_str}_{date_str}.log"
    
    def set_mongo_client(self, mongo_client: 'BlazeMongoClient'):
        """Set the MongoDB client for logging to MongoDB."""
        self._mongo_client = mongo_client
    
    def _write_to_file(self, level: str, message: str, job_id: Optional[str] = None):
        """Write log message to timestamped file and optionally to MongoDB."""
        try:
            log_file = self._get_log_filename()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            formatted_message = f"[{timestamp}] [{level.upper()}] {message}\n"
            
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(formatted_message)
            
            # Also write to MongoDB if client is available
            if self._mongo_client:
                try:
                    self._mongo_client.create_log(
                        level=level,
                        message=message,
                        job_id=job_id,
                        module="blaze"
                    )
                except Exception as mongo_e:
                    # Don't fail the logging if MongoDB write fails
                    pass
                    
        except Exception as e:
            # If file logging fails, at least log the error via loguru
            self._loguru.error(f"Failed to write to log file: {e}")
    
    def debug(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log debug message to file and optionally to loguru (if not silent)."""
        self._write_to_file("DEBUG", str(message), job_id)
        if not self._silent:
            return self._loguru.debug(message, *args, **kwargs)
    
    def info(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log info message to file and optionally to loguru (if not silent)."""
        self._write_to_file("INFO", str(message), job_id)
        if not self._silent:
            return self._loguru.info(message, *args, **kwargs)
    
    def warning(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log warning message to file and optionally to loguru (if not silent)."""
        self._write_to_file("WARNING", str(message), job_id)
        if not self._silent:
            return self._loguru.warning(message, *args, **kwargs)
    
    def warn(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Alias for warning."""
        return self.warning(message, job_id, *args, **kwargs)
    
    def error(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log error message to file and optionally to loguru (if not silent)."""
        self._write_to_file("ERROR", str(message), job_id)
        if not self._silent:
            return self._loguru.error(message, *args, **kwargs)
    
    def critical(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log critical message to file and optionally to loguru (if not silent)."""
        self._write_to_file("CRITICAL", str(message), job_id)
        if not self._silent:
            return self._loguru.critical(message, *args, **kwargs)
    
    def exception(self, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log exception message to file and optionally to loguru (if not silent)."""
        self._write_to_file("EXCEPTION", str(message), job_id)
        if not self._silent:
            return self._loguru.exception(message, *args, **kwargs)
    
    def log(self, level: str, message: str, job_id: Optional[str] = None, *args, **kwargs):
        """Log message at specified level to file and optionally to loguru (if not silent)."""
        self._write_to_file(level, str(message), job_id)
        if not self._silent:
            return self._loguru.log(level, message, *args, **kwargs)
    
    # Expose loguru's configuration methods
    def add(self, *args, **kwargs):
        """Add a handler to loguru logger."""
        return self._loguru.add(*args, **kwargs)
    
    def remove(self, *args, **kwargs):
        """Remove a handler from loguru logger."""
        return self._loguru.remove(*args, **kwargs)
    
    def configure(self, *args, **kwargs):
        """Configure loguru logger."""
        return self._loguru.configure(*args, **kwargs)
    
    def bind(self, *args, **kwargs):
        """Bind context to loguru logger."""
        return self._loguru.bind(*args, **kwargs)
    
    def patch(self, *args, **kwargs):
        """Patch loguru logger."""
        return self._loguru.patch(*args, **kwargs)


# Create a singleton instance for easy import
logger = BlazeLogger()
