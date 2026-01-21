"""
String operators for memory backend.

Operators: like, ilike, contains, icontains, startswith, istartswith, endswith, iendswith, regex, iregex
"""

import re
from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator


class LikeOperator(MemoryOperator):
    """SQL LIKE pattern matching (case-sensitive)."""
    name = "like"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        pattern = str(condition_value).replace('%', '.*').replace('_', '.')
        return bool(re.match(f'^{pattern}$', str(field_value)))


class NotLikeOperator(MemoryOperator):
    """SQL NOT LIKE pattern matching (case-sensitive)."""
    name = "not_like"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False  # SQL NULL != LIKE pattern -> NULL (False)
        pattern = str(condition_value).replace('%', '.*').replace('_', '.')
        return not bool(re.match(f'^{pattern}$', str(field_value)))


class ILikeOperator(MemoryOperator):
    """SQL LIKE pattern matching (case-insensitive)."""
    name = "ilike"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        pattern = str(condition_value).replace('%', '.*').replace('_', '.')
        return bool(re.match(f'^{pattern}$', str(field_value), re.IGNORECASE))


class ContainsOperator(MemoryOperator):
    """Check if field contains value (case-sensitive)."""
    name = "contains"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(condition_value) in str(field_value)


class IContainsOperator(MemoryOperator):
    """Check if field contains value (case-insensitive)."""
    name = "icontains"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(condition_value).lower() in str(field_value).lower()


class StartsWithOperator(MemoryOperator):
    """Check if field starts with value (case-sensitive)."""
    name = "startswith"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(field_value).startswith(str(condition_value))


class IStartsWithOperator(MemoryOperator):
    """Check if field starts with value (case-insensitive)."""
    name = "istartswith"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(field_value).lower().startswith(str(condition_value).lower())


class EndsWithOperator(MemoryOperator):
    """Check if field ends with value (case-sensitive)."""
    name = "endswith"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(field_value).endswith(str(condition_value))


class IEndsWithOperator(MemoryOperator):
    """Check if field ends with value (case-insensitive)."""
    name = "iendswith"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        return str(field_value).lower().endswith(str(condition_value).lower())


class RegexOperator(MemoryOperator):
    """Regular expression match (case-sensitive)."""
    name = "regex"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        try:
            return bool(re.search(str(condition_value), str(field_value)))
        except re.error:
            return False


class IRegexOperator(MemoryOperator):
    """Regular expression match (case-insensitive)."""
    name = "iregex"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        try:
            return bool(re.search(str(condition_value), str(field_value), re.IGNORECASE))
        except re.error:
            return False
