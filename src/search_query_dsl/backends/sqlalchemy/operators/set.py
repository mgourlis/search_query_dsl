"""
Set/list operators for SQLAlchemy backend.

Operators: in, not_in, between
"""

from typing import Any, Optional

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator
from search_query_dsl.core.utils import cast_value, _parse_list_value
from sqlalchemy import func, distinct, inspect, true

class AllOperator(SQLAlchemyOperator):
    """
    Check if related collection contains ALL the specified values.
    Uses INTERSECT logic via GROUP BY + HAVING COUNT.
    """
    name = "all"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        stmt = kwargs.get("stmt")
        model = kwargs.get("model")
        
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        elif not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        if not stmt or not model:
            # Fallback for simple array column (Postgres) if no context
            casted_values = [cast_value(v, value_type) for v in condition_value]
            return column.contains(casted_values)

        if not condition_value:
             return true()

        # Deduplicate and cast
        casted_values = list({cast_value(v, value_type) for v in condition_value})
        expected_count = len(casted_values)

        # Get PK
        mapper = inspect(model)
        pk_columns = mapper.primary_key
        if pk_columns:
            pk_col = pk_columns[0]
        else:
            pk_col = getattr(model, "id")

        # Create subquery based on current statement
        # We want to find PKs that have ALL the values in the related field `column`
        # column is likely an attribute of a joined table
        subquery = (
            stmt.where(column.in_(casted_values))
            .group_by(pk_col)
            .having(func.count(distinct(column)) == expected_count)
        )
        
        # Return expression: PK IN (subquery)
        return pk_col.in_(subquery)


class InOperator(SQLAlchemyOperator):
    """Check if column value is in a list of values."""
    name = "in"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        elif not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        casted_values = [cast_value(v, value_type) for v in condition_value]
        return column.in_(casted_values)


class NotInOperator(SQLAlchemyOperator):
    """Check if column value is not in a list of values."""
    name = "not_in"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        elif not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        casted_values = [cast_value(v, value_type) for v in condition_value]
        return column.not_in(casted_values)


class BetweenOperator(SQLAlchemyOperator):
    """Check if column value is between two values (inclusive)."""
    name = "between"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("'between' operator requires a list of exactly 2 values [min, max]")
        
        min_val = cast_value(condition_value[0], value_type)
        max_val = cast_value(condition_value[1], value_type)
        
        return column.between(min_val, max_val)


class NotBetweenOperator(SQLAlchemyOperator):
    """Check if column value is NOT between two values (inclusive)."""
    name = "not_between"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("'not_between' operator requires a list of exactly 2 values [min, max]")
        
        min_val = cast_value(condition_value[0], value_type)
        max_val = cast_value(condition_value[1], value_type)
        
        return ~column.between(min_val, max_val)

