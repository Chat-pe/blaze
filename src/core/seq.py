from typing import Dict, List, TypeVar, Any, Callable
import time
from pydantic import BaseModel
from datetime import datetime

from src.core.block import BlazeBlock
from src.core._types import SeqData, SeqBlockData, SequenceExecutionData
from src.core.logger import BlazeLogger

T = TypeVar('T')

class BlazeSequence:
    NOTSET = "* * * * *"  # Default cron sequence for every minute

    def __init__(self):
        self.seq: Dict[str, SeqData] = {}
        self.logger = None

    def _register_seq(self, seq_id: str, seq_data: SeqData):
        if seq_id in self.seq:
            raise ValueError(f"Sequence {seq_id} already registered")
        self.seq[seq_id] = seq_data
    
    def get_seq(self, seq_id: str) -> SeqData:
        if seq_id not in self.seq:
            raise ValueError(f"Sequence {seq_id} not found | Sequences: {self.seq.keys()}")
        return self.seq[seq_id]
    
    def set_logger(self, logger: BlazeLogger):
        self.logger = logger
    
    def get_all_seq(self) -> List[SeqData]:
        return list(self.seq.values())

    def sequence(
            self,
            blocks: BlazeBlock,
            sequence: List[SeqBlockData],
            seq_id: str,
            description: str | None = "",
            seq_run_timeout: int | None = None,
            retries: int = 0,
            retry_delay: int = 0,
            auto_start: bool = False,
            fail_stop: bool = False
    )-> Callable:
        
        if seq_run_timeout is None:
            seq_run_timeout = 3600
            
        def sequencialise(job_id: str, parameters: Dict[str, Any] = {}) -> Dict[str, Any]:
            local_retries = retries  # Use a local copy to avoid modifying the nonlocal
            execution_context = {}  # Create a new context for this execution
            
            self.logger.info(f"Run {seq_id}/{job_id} - starting sequence with {len(sequence)} blocks")

            for seq_block in sequence:
                self.logger.info(f"Run {seq_id}/{job_id}/{seq_block.block_name} - processing")
                try:
                    block = blocks._get_block(seq_block.block_name)
                    block_func = block.func

                    context_and_parameters = {}
                    for param, value in parameters.get(seq_block.block_name, {}).items():
                        if isinstance(value, str) and value.startswith('@'):
                            # Look for the block result in the execution context
                            block_name = value[1:]
                            if block_name not in execution_context:
                                raise ValueError(f"Invalid parameter value: {value} | Not available in context {execution_context}")
                            context_and_parameters[param] = execution_context[block_name]
                        else:
                            context_and_parameters[param] = value
                    
                    for dep in seq_block.dependencies:
                        if dep not in execution_context:
                            raise ValueError(f"Invalid dependency: {dep} | Not available in context {execution_context}")
                        
                    #run block
                    have_result = False
                    attempt = 0
                    max_attempts = local_retries + 1
                    
                    while not have_result and attempt < max_attempts:
                        try:
                            result = block_func(**context_and_parameters)
                            have_result = True
                            
                            # Store the result under the block name
                            execution_context[seq_block.block_name] = result
                            self.logger.info(f"Run {seq_id}/{job_id}/{seq_block.block_name} - success with result: {str(result)[:10]}...{str(result)[-10:]}")
                                
                        except Exception as e:
                            attempt += 1
                            self.logger.error(f"Run {seq_id}/{job_id}/{seq_block.block_name} - failed with error: {str(e)}")
                            if attempt < max_attempts:
                                self.logger.warning(f"Retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                            elif fail_stop:
                                raise RuntimeError(f"Block {seq_block.block_name} failed with error: {str(e)}") from e
                            else: 
                                execution_context[seq_block.block_name] = False
                                self.logger.warning(f"Run {seq_id}/{job_id}/{seq_block.block_name} - failed, continuing sequence")
                except Exception as e:
                    self.logger.error(f"Run {seq_id}/{job_id}/{seq_block.block_name} - failed with error: {str(e)}")
                    if fail_stop:
                        raise
                    execution_context[seq_block.block_name] = False

            self.logger.info(f"Run {seq_id}/{job_id} - success with context: {execution_context}")
            return execution_context

        seq_data = SeqData(
            seq_id=seq_id,
            description=description,
            seq_run_timeout=seq_run_timeout,
            fail_stop=fail_stop,
            auto_start=auto_start,
            retries=retries,
            retry_delay=retry_delay,
            sequence=sequence,
            sequence_func=sequencialise
        )
        self._register_seq(seq_id, seq_data)

        return SequenceExecutionData(seq_data=seq_data)
