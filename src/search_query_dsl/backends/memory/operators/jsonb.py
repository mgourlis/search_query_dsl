"""
JSONB operators for memory backend.

Operators: jsonb_contains, jsonb_contained_by, jsonb_has_key, jsonb_has_any_keys, jsonb_has_all_keys
"""

from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator


class JsonbContainsOperator(MemoryOperator):
    """Check if JSONB field contains the given value."""
    name = "jsonb_contains"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        if isinstance(condition_value, dict) and isinstance(field_value, dict):
            return self._dict_contains(field_value, condition_value)
        elif isinstance(condition_value, list) and isinstance(field_value, list):
            return all(item in field_value for item in condition_value)
        else:
            return condition_value in field_value if hasattr(field_value, '__contains__') else False
    
    def _dict_contains(self, container: dict, subset: dict) -> bool:
        """Check if container dict contains all key-value pairs from subset."""
        for key, value in subset.items():
            if key not in container:
                return False
            if isinstance(value, dict) and isinstance(container[key], dict):
                if not self._dict_contains(container[key], value):
                    return False
            elif container[key] != value:
                return False
        return True


class JsonbContainedByOperator(MemoryOperator):
    """Check if JSONB field is contained by the given value."""
    name = "jsonb_contained_by"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        if isinstance(condition_value, dict) and isinstance(field_value, dict):
            return self._dict_contains(condition_value, field_value)
        elif isinstance(condition_value, list) and isinstance(field_value, list):
            return all(item in condition_value for item in field_value)
        else:
            return False
    
    def _dict_contains(self, container: dict, subset: dict) -> bool:
        """Check if container dict contains all key-value pairs from subset."""
        for key, value in subset.items():
            if key not in container:
                return False
            if isinstance(value, dict) and isinstance(container[key], dict):
                if not self._dict_contains(container[key], value):
                    return False
            elif container[key] != value:
                return False
        return True


class JsonbHasKeyOperator(MemoryOperator):
    """Check if JSONB field has the given key."""
    name = "jsonb_has_key"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None or not isinstance(field_value, dict):
            return False
        return str(condition_value) in field_value


class JsonbHasAnyKeysOperator(MemoryOperator):
    """Check if JSONB field has any of the given keys."""
    name = "jsonb_has_any_keys"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None or not isinstance(field_value, dict):
            return False
        
        if not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        return any(str(k) in field_value for k in condition_value)


class JsonbHasAllKeysOperator(MemoryOperator):
    """Check if JSONB field has all of the given keys."""
    name = "jsonb_has_all_keys"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None or not isinstance(field_value, dict):
            return False
        
        if not isinstance(condition_value, (list, tuple)):
            condition_value = [condition_value]
        
        return all(str(k) in field_value for k in condition_value)


class JsonbPathExistsOperator(MemoryOperator):
    """Check if JSONB path exists (Memory backend)."""
    name = "jsonb_path_exists"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        """Check if JSON path exists using simple key path notation."""
        if field_value is None:
            return False
        
        # Simple implementation: check if path (dot-notation) exists
        # For advanced JSONPath, would need jsonpath-ng library
        path_parts = condition_value.strip('$.').split('.')
        current = field_value
        
        for part in path_parts:
            if isinstance(current, dict):
                if part not in current:
                    return False
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if idx >= len(current):
                    return False
                current = current[idx]
            else:
                return False
        
        return True

