"""
Set/list operators for memory backend.

Operators: in, not_in, between, not_between, all
"""

from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator
from search_query_dsl.core.utils import cast_value, _parse_list_value


class InOperator(MemoryOperator):
    """Check if field value is in a list of values."""
    name = "in"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        values_list = _parse_list_value(condition_value)
        
        casted_values = [cast_value(v, value_type) for v in values_list]
        return field_value in casted_values


class NotInOperator(MemoryOperator):
    """Check if field value is not in a list of values."""
    name = "not_in"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        values_list = _parse_list_value(condition_value)
        
        casted_values = [cast_value(v, value_type) for v in values_list]
        return field_value not in casted_values


class BetweenOperator(MemoryOperator):
    """Check if field value is between two values (inclusive)."""
    name = "between"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
            
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("'between' operator requires a list of exactly 2 values [min, max]")
        
        min_val = cast_value(condition_value[0], value_type)
        max_val = cast_value(condition_value[1], value_type)
        
        return min_val <= field_value <= max_val


class NotBetweenOperator(MemoryOperator):
    """Check if field value is NOT between two values (inclusive)."""
    name = "not_between"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False

        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)

        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("'not_between' operator requires a list of exactly 2 values [min, max]")
        
        min_val = cast_value(condition_value[0], value_type)
        max_val = cast_value(condition_value[1], value_type)
        
        return not (min_val <= field_value <= max_val)


class AllOperator(MemoryOperator):
    """Check if list-like field contains all specified values."""
    name = "all"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
            
        if not isinstance(field_value, (list, tuple, set)):
            field_value = [field_value]

        values_list = _parse_list_value(condition_value)
            
        expected = {cast_value(v, value_type) for v in values_list}
        
        if not expected:
            return True
            
        return expected.issubset(set(field_value))
