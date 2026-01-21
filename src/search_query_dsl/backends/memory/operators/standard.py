"""
Standard comparison operators for memory backend.

Operators: =, !=, >, <, >=, <=
"""

from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator
from search_query_dsl.core.utils import cast_value


class EqualOperator(MemoryOperator):
    """Equality comparison."""
    name = "="
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        return field_value == cast_value(condition_value, value_type)


class NotEqualOperator(MemoryOperator):
    """Not equal comparison."""
    name = "!="
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        return field_value != cast_value(condition_value, value_type)


class GreaterThanOperator(MemoryOperator):
    """Greater than comparison."""
    name = ">"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return field_value > cast_value(condition_value, value_type)


class LessThanOperator(MemoryOperator):
    """Less than comparison."""
    name = "<"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return field_value < cast_value(condition_value, value_type)


class GreaterThanOrEqualOperator(MemoryOperator):
    """Greater than or equal comparison."""
    name = ">="
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return field_value >= cast_value(condition_value, value_type)


class LessThanOrEqualOperator(MemoryOperator):
    """Less than or equal comparison."""
    name = "<="
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return field_value <= cast_value(condition_value, value_type)


class IsEmptyOperator(MemoryOperator):
    """Check if value is empty (string or collection)."""
    name = "is_empty"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            # SQL 'is_empty' checks usually exclude NULL unless specifically handled.
            # But empty string != NULL. Empty collection != NULL?
            # If relation is None (no relation), is it empty?
            # SA ~any() on None relation -> True (no related items).
            # So if None, it IS empty?
            # Let's say yes for None.
            return True
        
        if isinstance(field_value, str):
            return field_value == ""
        if isinstance(field_value, (list, tuple, dict, set)):
            return len(field_value) == 0
            
        # If it's a number etc, it's not "empty" (0 is 0).
        return False


class IsNotEmptyOperator(MemoryOperator):
    """Check if value is not empty (string or collection)."""
    name = "is_not_empty"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
             return False # None is empty, so it's NOT not empty.
             
        if isinstance(field_value, str):
            return field_value != ""
        if isinstance(field_value, (list, tuple, dict, set)):
            return len(field_value) > 0
            
        return True # Present number etc is not empty.
