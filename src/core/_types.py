from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field
import inspect
from enum import Enum

class BlockData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    func: Callable
    signature: inspect.Signature

class SeqBlockData(BaseModel):
    block_name: str
    parameters: Dict[str, Any]
    dependencies: List[str]

class SeqData(BaseModel):
    seq_id: str
    description: Optional[str] = None
    seq_run_timeout: int
    seq_run_interval: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    fail_stop: bool = False
    auto_start: bool = False
    retries: int = 0
    retry_delay: int = 0
    sequence: List[SeqBlockData]
    sequence_func: Callable

class SequenceExecutionData(BaseModel):
    seq_data: SeqData

class SequenceStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class SequenceResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    sequence_id: str
    status: SequenceStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    block_results: Dict[str, Any] = {}

class SequenceData(BaseModel):

    seq_data: SeqData
    sequence_id: str
    next_run: Optional[datetime] = Field(default_factory=datetime.now())
    last_run: Optional[datetime] = None
    latest_result: Optional[Dict[str, Any]] = None
    error_logs: Optional[List[str]] = None
    total_execution_time: Optional[float] = 0.0


