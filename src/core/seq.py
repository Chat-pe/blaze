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
        """
        Initialize the BlazeSequence instance with an empty sequence registry and no logger.
        """
        self.seq: Dict[str, SeqData] = {}
        self.logger = None

    def _register_seq(self, seq_id: str, seq_data: SeqData):
        """
        Register a new sequence with the specified ID.
        
        Raises:
            ValueError: If the sequence ID is already registered.
        """
        if seq_id in self.seq:
            raise ValueError(f"Sequence {seq_id} already registered")
        self.seq[seq_id] = seq_data
    
    def get_seq(self, seq_id: str) -> SeqData:
        """
        Retrieve the sequence data associated with the given sequence ID.
        
        Raises:
            ValueError: If the specified sequence ID is not registered.
        
        Returns:
            SeqData: The sequence data corresponding to the provided sequence ID.
        """
        if seq_id not in self.seq:
            raise ValueError(f"Sequence {seq_id} not found | Sequences: {self.seq.keys()}")
        return self.seq[seq_id]
    
    def set_logger(self, logger: BlazeLogger):
        """
        Assigns a logger instance to the BlazeSequence for structured logging of sequence execution.
        """
        self.logger = logger
    
    def get_all_seq(self) -> List[SeqData]:
        """
        Return a list of all registered sequence data objects.
        """
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
        
        """
            Defines and registers a sequence of computational blocks to be executed in order, with support for retries, dependency resolution, and configurable failure handling.
            
            Parameters:
                blocks (BlazeBlock): The container holding available block functions.
                sequence (List[SeqBlockData]): The ordered list of block execution steps, including dependencies and parameters.
                seq_id (str): Unique identifier for the sequence.
                description (str, optional): Human-readable description of the sequence.
                seq_run_timeout (int, optional): Maximum allowed execution time for the sequence in seconds. Defaults to 3600 if not specified.
                retries (int, optional): Number of times to retry a failed block before marking it as failed.
                retry_delay (int, optional): Delay in seconds between retry attempts.
                auto_start (bool, optional): Whether the sequence should start automatically.
                fail_stop (bool, optional): If True, stops the sequence on the first block failure; otherwise, continues execution.
            
            Returns:
                Callable: A function that, when called with a job ID and optional parameters, executes the sequence and returns a dictionary mapping block names to their results.
            
            The returned function executes each block in the specified order, resolving dependencies and parameters from previous block results, and applies retry logic and error handling as configured. The sequence is registered and can be retrieved or executed later.
            """
            if seq_run_timeout is None:
            seq_run_timeout = 3600
            
        def sequencialise(job_id: str, parameters: Dict[str, Any] = {}) -> Dict[str, Any]:
            """
            Executes a sequence of computational blocks in order, handling dependencies, parameter resolution, retries, and logging.
            
            Parameters:
                job_id (str): Unique identifier for the current sequence execution.
                parameters (Dict[str, Any], optional): Mapping of block names to their input parameters. Supports referencing previous block results using strings prefixed with '@'.
            
            Returns:
                Dict[str, Any]: A dictionary mapping each block name to its execution result or `False` if the block failed and the sequence continued.
            """
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
