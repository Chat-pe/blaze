"""
Persistence module for Blaze sequences.
Simplified implementation to store sequence information.
"""

from typing import Dict, Any, Optional
from src.core._types import SequenceData
import hashlib

class BlazePersistence:
    """Simple persistence manager for storing sequence information"""
    
    _instance = None
    _sequences: Dict[str, SequenceData] = {}
    _hashed_sequences: str = ""
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance of persistence manager"""
        if cls._instance is None:
            cls._instance = BlazePersistence()
        return cls._instance
    
    def __init__(self):
        """Initialize persistence manager"""
        self._sequences: Dict[str, SequenceData] = {}
    
    def store_sequence(self, seq_id: str, sequence_data: SequenceData) -> None:
        """Store sequence data in memory and persist to disk"""
        self._sequences[seq_id] = sequence_data 
        self._hashed_sequences = hashlib.sha256(str(self._sequences).encode()).hexdigest()
        print(f"Stored sequence '{seq_id}' in persistence")

    def add_execution_time(self, seq_id: str, execution_time: float) -> None:
        """Add execution time to a sequence"""
        if seq_id in self._sequences:
            self._sequences[seq_id].total_execution_time += execution_time
            print(f"Added execution time to sequence '{seq_id}' in persistence")

    def add_latest_result(self, seq_id: str, latest_result: Dict[str, Any]) -> None:
        """Add latest result to a sequence"""
        if seq_id in self._sequences:
            self._sequences[seq_id].latest_result = latest_result
            print(f"Added latest result to sequence '{seq_id}' in persistence")
    
    def get_hashed_sequences(self) -> str:
        """Get the hashed sequences"""
        return self._hashed_sequences if self._sequences else "DNF"
    
    def get_sequence(self, seq_id: str) -> Optional[SequenceData]:
        """Get sequence data by ID"""
        return self._sequences.get(seq_id)
    
    def get_all_sequences(self) -> Dict[str, SequenceData]:
        """Get all stored sequences"""
        return self._sequences
    
    def remove_sequence(self, seq_id: str) -> None:
        """Remove a sequence from persistence"""
        if seq_id in self._sequences:
            del self._sequences[seq_id]
            self._hashed_sequences = hashlib.sha256(str(self._sequences).encode()).hexdigest()
            print(f"Removed sequence '{seq_id}' from persistence")
    
    def clear_all(self) -> None:
        """Clear all persisted sequences"""
        self._sequences = {}
        print("Cleared all persisted sequences")
    


    
