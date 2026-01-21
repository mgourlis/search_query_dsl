"""
String operators for SQLAlchemy backend.

Operators: like, ilike, contains, icontains, startswith, istartswith, endswith, iendswith, regex, iregex
"""

from typing import Any, Optional

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator


class LikeOperator(SQLAlchemyOperator):
    """SQL LIKE pattern matching (case-sensitive)."""
    name = "like"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.like(str(condition_value))


class NotLikeOperator(SQLAlchemyOperator):
    """SQL NOT LIKE pattern matching (case-sensitive)."""
    name = "not_like"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return ~column.like(str(condition_value))


class ILikeOperator(SQLAlchemyOperator):
    """SQL ILIKE pattern matching (case-insensitive)."""
    name = "ilike"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.ilike(str(condition_value))


class ContainsOperator(SQLAlchemyOperator):
    """Check if column contains value (case-sensitive)."""
    name = "contains"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.contains(str(condition_value))


class IContainsOperator(SQLAlchemyOperator):
    """Check if column contains value (case-insensitive)."""
    name = "icontains"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.ilike(f"%{condition_value}%")


class StartsWithOperator(SQLAlchemyOperator):
    """Check if column starts with value (case-sensitive)."""
    name = "startswith"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.startswith(str(condition_value))


class IStartsWithOperator(SQLAlchemyOperator):
    """Check if column starts with value (case-insensitive)."""
    name = "istartswith"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.ilike(f"{condition_value}%")


class EndsWithOperator(SQLAlchemyOperator):
    """Check if column ends with value (case-sensitive)."""
    name = "endswith"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.endswith(str(condition_value))


class IEndsWithOperator(SQLAlchemyOperator):
    """Check if column ends with value (case-insensitive)."""
    name = "iendswith"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.ilike(f"%{condition_value}")


class RegexOperator(SQLAlchemyOperator):
    """Regular expression match (PostgreSQL ~ operator)."""
    name = "regex"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.regexp_match(str(condition_value))


class IRegexOperator(SQLAlchemyOperator):
    """Regular expression match case-insensitive (PostgreSQL ~* operator)."""
    name = "iregex"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        # SQLAlchemy 2.0 uses regexp_match with flags
        return column.regexp_match(str(condition_value), flags="i")
