"""
Null check operators for SQLAlchemy backend.

Operators: is_null, is_not_null
"""

from typing import Any, Optional

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator


class IsNullOperator(SQLAlchemyOperator):
    """Check if column value is NULL."""
    name = "is_null"
    supports_relationship = True  # Strategy hints it supports rel
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.is_(None)


class IsNotNullOperator(SQLAlchemyOperator):
    """Check if column value is not NULL."""
    name = "is_not_null"
    supports_relationship = True
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.is_not(None)
