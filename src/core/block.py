import functools
import inspect 
import time
from typing import Callable, Optional, List, TypeVar, Dict, Any, Type
from blaze.src.core._types import BlockData
from pydantic import BaseModel

T = TypeVar('T')
class BlazeBlock:
    def __init__(self):
        self.blocks: Dict[str, BlockData] = {}
    
    def _register_block(self, name: str, block_data: BlockData):
        if name in self.blocks:
            raise ValueError(f"Block {name} already registered")
        self.blocks[name] = block_data  

    def _get_block(self, name: str) -> BlockData:
        if name not in self.blocks:
            raise ValueError(f"Block {name} not found")
        return self.blocks[name]
    
    def get_all_blocks(self) -> List[BlockData]:
        return list(self.blocks.values())
    
    def get_block_by_name(self, name: str) -> BlockData:
        return self.blocks[name]
    
    def get_block_by_tags(self, tags: List[str]) -> List[BlockData]:
        return [block for block in self.blocks.values() if set(tags) & set(block.tags)]
            
    def block(
            self,
            name: Optional[str] = None,
            description: Optional[str] = None,
            tags: Optional[List[str]] = None,
            data_model: Optional[Type[BaseModel]] = None

    ) -> Callable[[Callable[...,T]], Callable[...,T]]:
        
        def decorator(func: Callable[...,T]) -> Callable[...,T]:
            sig = inspect.signature(func)
            
            nonlocal name
            if name is None:
                name = func.__name__
                
            nonlocal description
            if description is None and func.__doc__:
                description = inspect.cleandoc(func.__doc__)
                
            nonlocal tags
            if tags is None:
                tags = []

            block_data = BlockData(
                name=name,
                description=description,
                tags=tags,
                func=func,
                signature=sig,
                data_model=data_model
            )

            self._register_block(name, block_data)

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    return result
                except Exception as e:
                    raise e

            return wrapper 
        return decorator