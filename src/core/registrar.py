from typing import Any, Dict, Callable

class BlazeRegistrar:

    def __init__(self):
        self._block_registry : Dict[str, Dict[str, Any]] = {}
        self._pipeline_registry : Dict[str, Dict[str, Any]] = {}
        self._sequence_registry : Dict[str, Dict[str, Any]] = {}
        self._registry_map: Dict[str, Callable] = {
            'block': self._register_block,
            'pipeline': self._register_pipeline,
            'sequence': self._register_sequence,
        }
        self._getter_map: Dict[str, Callable] = {
            'block': self._get_block,
            'pipeline': self._get_pipeline,
            'sequence': self._get_sequence,
        }

    def register(self, name: str, component: Any, type: str):
        self._registry_map[type](name, component)

    def _register_block(self, name: str, component: Any):
        self._block_registry[name] = component

    def _register_pipeline(self, name: str, component: Any):
        self._pipeline_registry[name] = component

    def _register_sequence(self, name: str, component: Any):
        self._sequence_registry[name] = component

    def get_registry(self, name: str, type: str):
        return self._getter_map[type](name)
    
    def _get_block(self, name: str):
        return self._block_registry[name]
    
    def _get_pipeline(self, name: str):
        return self._pipeline_registry[name]
    
    def _get_sequence(self, name: str):
        return self._sequence_registry[name]
    