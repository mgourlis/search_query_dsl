"""
Base class for SQLAlchemy backend operators.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ColumnElement


class SQLAlchemyOperator(ABC):
    """
    Base class for SQLAlchemy backend operators.
    
    Each operator generates a SQLAlchemy expression.
    Subclasses must define `name` and implement `apply()`.
    
    Example:
        class EqualOperator(SQLAlchemyOperator):
            name = "="
            
            def apply(self, column, condition_value, value_type=None):
                return column == cast_value(condition_value, value_type)
    """
    
    name: str  # Must be defined by subclass
    supports_relationship: bool = False
    
    @abstractmethod
    def apply(
        self,
        column: "ColumnElement",
        condition_value: Any,
        value_type: Optional[str] = None,
        **kwargs
    ) -> "ColumnElement":
        """
        Generate SQLAlchemy expression for this operator.
        
        Args:
            column: SQLAlchemy column (resolved by backend)
            condition_value: Value from the search condition
            value_type: Optional type hint for casting
            **kwargs: Additional context (model, stmt, etc.)
        
        Returns:
            SQLAlchemy expression (e.g., column == value)
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
