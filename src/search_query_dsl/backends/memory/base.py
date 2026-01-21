"""
Base class for memory backend operators.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class MemoryOperator(ABC):
    """
    Base class for memory backend operators.
    
    Each operator evaluates a condition against a Python value.
    Subclasses must define `name` and implement `evaluate()`.
    
    Example:
        class EqualOperator(MemoryOperator):
            name = "="
            
            def evaluate(self, field_value, condition_value, value_type=None):
                return field_value == cast_value(condition_value, value_type)
    """
    
    name: str  # Must be defined by subclass
    
    @abstractmethod
    def evaluate(
        self,
        field_value: Any,
        condition_value: Any,
        value_type: Optional[str] = None,
    ) -> bool:
        """
        Evaluate condition against resolved field value.
        
        Args:
            field_value: Value from the Python object
            condition_value: Value from the search condition
            value_type: Optional type hint for casting
        
        Returns:
            True if condition is satisfied, False otherwise
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
