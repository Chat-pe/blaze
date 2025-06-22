from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Type
from pydantic import BaseModel, ConfigDict, Field, field_serializer
import inspect
from enum import Enum

class BlockData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    name: str
    description: Optional[str] = None
    tags: List[str] = []
    func: Callable
    signature: inspect.Signature
    data_model: Optional[Type[BaseModel]] = None

class SeqBlockData(BaseModel):
    block_name: str
    dependencies: List[str]

class SeqData(BaseModel):
    seq_id: str
    description: Optional[str] = None
    seq_run_timeout: int
    fail_stop: bool = False
    auto_start: bool = False
    retries: int = 0
    retry_delay: int = 0
    sequence: List[SeqBlockData]
    sequence_func: Callable

class SubmitSequenceData(BaseModel):

    seq_id: str
    parameters: Optional[Dict[str, Any]] = {}
    seq_run_interval: Optional[str] = "*/5 * * * *"
    start_date: Optional[datetime] = Field(default_factory=datetime.now)
    end_date: Optional[datetime] = Field(default_factory=lambda: datetime.now() + timedelta(days=30))
    
    @field_serializer('start_date', 'end_date', when_used='json')
    def serialize_dates(self, value: Optional[datetime]) -> Optional[str]:
        return value.isoformat() if value else None

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

class JobFile(BaseModel):
    scheduler_name: str
    submitted_jobs: List[SubmitSequenceData]

    last_updated: datetime = Field(default_factory=datetime.now)
    
    @field_serializer('last_updated')
    def serialize_last_updated(self, value: datetime) -> str:
        return value.isoformat()


class JobExecutuionData(SubmitSequenceData):
    execution_func: Callable

    @field_serializer('execution_func')
    def serialize_execution_func(self, value: Callable) -> str:
        return value.__name__

class JobState(JobExecutuionData):

    run_state: SequenceStatus
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    latest_result: Optional[Dict[str, Any]] = None
    error_logs: Optional[List[str]] = None
    total_execution_time: Optional[float] = 0.0

    @field_serializer('last_run', 'next_run')
    def serialize_dates(self, value: Optional[datetime]) -> Optional[str]:
        return value.isoformat() if value else None


class SequenceData(BaseModel):

    seq_data: JobExecutuionData
    next_run: Optional[datetime] = Field(default_factory=datetime.now)
    last_run: Optional[datetime] = None
    latest_result: Optional[Dict[str, Any]] = None
    error_logs: Optional[List[str]] = None
    total_execution_time: Optional[float] = 0.0


class BlazeLock(BaseModel):

    name: str
    blocks: List[str]
    sequences: List[str]
    loop_interval: int
    job_file_path: str

    # State variables
    is_running: bool = False
    is_paused: bool = False
    is_stopped: bool = False

    # Last updated time
    last_updated: datetime = Field(default_factory=datetime.now)
    
    @field_serializer('last_updated')
    def serialize_last_updated(self, value: datetime) -> str:
        return value.isoformat()