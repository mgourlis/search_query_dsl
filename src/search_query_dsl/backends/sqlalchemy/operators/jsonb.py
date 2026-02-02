"""
JSONB operators for SQLAlchemy backend.

Operators: jsonb_contains, jsonb_contained_by, jsonb_has_key, jsonb_has_any_keys, jsonb_has_all_keys
"""

import json
from typing import Any, Optional

from sqlalchemy import func, literal_column

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator
from search_query_dsl.core.utils import _parse_list_value


class JsonbContainsOperator(SQLAlchemyOperator):
    """Check if JSONB column contains the given value (@> operator)."""
    name = "jsonb_contains"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        
        if isinstance(condition_value, (dict, list)):
            json_val = json.dumps(condition_value)
        else:
            json_val = str(condition_value)
        
        # Use literal_column with explicit ::jsonb cast to avoid asyncpg double-escaping
        escaped_json = json_val.replace("'", "''")  # Escape single quotes
        return column.op('@>')(literal_column(f"'{escaped_json}'::jsonb"))


class JsonbContainedByOperator(SQLAlchemyOperator):
    """Check if JSONB column is contained by the given value (<@ operator)."""
    name = "jsonb_contained_by"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        
        if isinstance(condition_value, (dict, list)):
            json_val = json.dumps(condition_value)
        else:
            json_val = str(condition_value)
        
        escaped_json = json_val.replace("'", "''")
        return column.op('<@')(literal_column(f"'{escaped_json}'::jsonb"))


class JsonbHasKeyOperator(SQLAlchemyOperator):
    """Check if JSONB column has the given key (? operator)."""
    name = "jsonb_has_key"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        return column.has_key(str(condition_value))


class JsonbHasAnyKeysOperator(SQLAlchemyOperator):
    """Check if JSONB column has any of the given keys (?| operator)."""
    name = "jsonb_has_any_keys"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        elif not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        keys = [str(k) for k in condition_value]
        
        # Use literal_column with proper array syntax
        keys_str = ", ".join(f"'{k}'" for k in keys)
        return column.op('?|')(literal_column(f"ARRAY[{keys_str}]"))


class JsonbHasAllKeysOperator(SQLAlchemyOperator):
    """Check if JSONB column has all of the given keys (?& operator)."""
    name = "jsonb_has_all_keys"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        
        # Parse string values into list
        if isinstance(condition_value, str):
            condition_value = _parse_list_value(condition_value)
        elif not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        keys = [str(k) for k in condition_value]
        
        # Use literal_column with proper array syntax
        keys_str = ", ".join(f"'{k}'" for k in keys)
        return column.op('?&')(literal_column(f"ARRAY[{keys_str}]"))


class JsonbPathExistsOperator(SQLAlchemyOperator):
    """Check if JSONB path exists."""
    name = "jsonb_path_exists"
    
    def apply(self, column, value: Any, value_type: Optional[str] = None, **kwargs) -> Any:
        """Check if a JSON path exists in the JSONB column."""
        return func.jsonb_path_exists(column, value)
