"""In-memory evaluation backend."""

from search_query_dsl.backends.memory.backend import MemoryBackend
from search_query_dsl.backends.memory.base import MemoryOperator
from search_query_dsl.backends.memory.operators import REGISTRY

__all__ = [
    "MemoryBackend",
    "MemoryOperator",
    "REGISTRY",
]
