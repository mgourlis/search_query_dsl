"""
Null check operators for memory backend.

Operators: is_null, is_not_null
"""

from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator


class IsNullOperator(MemoryOperator):
    """Check if field value is None."""
    name = "is_null"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        return field_value is None


class IsNotNullOperator(MemoryOperator):
    """Check if field value is not None."""
    name = "is_not_null"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        return field_value is not None
