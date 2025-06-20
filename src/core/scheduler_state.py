from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

class SequenceStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class SequenceResult(BaseModel):
    sequence_id: str
    status: SequenceStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    block_results: Dict[str, Dict[str, Any]] = {}

class ScheduleState:
    def __init__(self):
        self._sequences: Dict[str, SequenceResult] = {}
        self._logs: Dict[str, List[str]] = {}

    def add_sequence(self, sequence_id: str) -> None:
        """Add a new sequence to track."""
        if sequence_id not in self._sequences:
            self._sequences[sequence_id] = SequenceResult(
                sequence_id=sequence_id,
                status=SequenceStatus.PENDING,
                start_time=datetime.now(),
                block_results={}
            )
            self._logs[sequence_id] = []
    
    def add_bulk_sequences(self, sequence_ids: List[str]) -> None:
        """Add multiple sequences to track."""
        for sequence_id in sequence_ids:
            self.add_sequence(sequence_id)

    def update_sequence_status(self, sequence_id: str, status: SequenceStatus) -> None:
        """Update the status of a sequence."""
        if sequence_id in self._sequences:
            self._sequences[sequence_id].status = status
            if status in [SequenceStatus.COMPLETED, SequenceStatus.FAILED, SequenceStatus.CANCELLED]:
                self._sequences[sequence_id].end_time = datetime.now()

    def add_block_result(self, sequence_id: str, block_id: str, result: Dict[str, Any]) -> None:
        """Add a block execution result to a sequence."""
        if sequence_id in self._sequences:
            self._sequences[sequence_id].block_results[block_id] = result

    def add_log(self, sequence_id: str, message: str) -> None:
        """Add a log message for a sequence."""
        if sequence_id in self._logs:
            timestamp = datetime.now().isoformat()
            self._logs[sequence_id].append(f"[{timestamp}] {message}")
    
    def add_bulk_logs(self, sequence_ids: List[str], messages: List[str]) -> None:
        """Add multiple log messages for a sequence."""
        for sequence_id, message in zip(sequence_ids, messages):
            self.add_log(sequence_id, message)

    def get_sequence_status(self, sequence_id: str) -> Optional[SequenceResult]:
        """Get the current status of a sequence."""
        return self._sequences.get(sequence_id)

    def get_sequence_logs(self, sequence_id: str) -> List[str]:
        """Get all logs for a sequence."""
        return self._logs.get(sequence_id, [])

    def get_all_sequences(self) -> Dict[str, SequenceResult]:
        """Get all tracked sequences."""
        return self._sequences

    def get_active_sequences(self) -> Dict[str, SequenceResult]:
        """Get all currently running sequences."""
        return {
            seq_id: result for seq_id, result in self._sequences.items()
            if result.status == SequenceStatus.RUNNING
        }

    def clear_completed_sequences(self, max_age_hours: int = 24) -> None:
        """Clear completed sequences older than max_age_hours."""
        current_time = datetime.now()
        to_remove = []
        
        for seq_id, result in self._sequences.items():
            if result.status in [SequenceStatus.COMPLETED, SequenceStatus.FAILED, SequenceStatus.CANCELLED]:
                if result.end_time and (current_time - result.end_time).total_seconds() > max_age_hours * 3600:
                    to_remove.append(seq_id)

        for seq_id in to_remove:
            del self._sequences[seq_id]
            del self._logs[seq_id] 