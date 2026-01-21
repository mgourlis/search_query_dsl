"""
Standard comparison operators for SQLAlchemy backend.

Operators: =, !=, >, <, >=, <=
"""

from typing import Any, Optional

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator
from search_query_dsl.core.utils import cast_value


class EqualOperator(SQLAlchemyOperator):
    """Equality comparison."""
    name = "="
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column == cast_value(condition_value, value_type)


class NotEqualOperator(SQLAlchemyOperator):
    """Not equal comparison."""
    name = "!="
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column != cast_value(condition_value, value_type)


class GreaterThanOperator(SQLAlchemyOperator):
    """Greater than comparison."""
    name = ">"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column > cast_value(condition_value, value_type)


class LessThanOperator(SQLAlchemyOperator):
    """Less than comparison."""
    name = "<"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column < cast_value(condition_value, value_type)


class GreaterThanOrEqualOperator(SQLAlchemyOperator):
    """Greater than or equal comparison."""
    name = ">="
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column >= cast_value(condition_value, value_type)


class LessThanOrEqualOperator(SQLAlchemyOperator):
    """Less than or equal comparison."""
    name = "<="
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column <= cast_value(condition_value, value_type)


class IsEmptyOperator(SQLAlchemyOperator):
    """Check if value is empty (string or relationship)."""
    name = "is_empty"
    supports_relationship = True
    
    def apply(self, column, condition_value, value_type=None, **kwargs):
        # Check if relationship property
        if hasattr(column, "property") and hasattr(column.property, "mapper"):
            # It is a relationship. Use ~any()
            # Note: Strategy used outerjoin explicitly. 
            # column.any() uses EXISTS. ~column.any() means NOT EXISTS.
            # This is equivalent to checking empty collection.
            return ~column.any()
        return column == ""


class IsNotEmptyOperator(SQLAlchemyOperator):
    """Check if value is not empty (string or relationship)."""
    name = "is_not_empty"
    supports_relationship = True
    
    def apply(self, column, condition_value, value_type=None, **kwargs):
        if hasattr(column, "property") and hasattr(column.property, "mapper"):
             return column.any()
        return column != ""
