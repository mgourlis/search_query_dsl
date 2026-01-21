"""
Memory backend for in-memory filtering of Python objects.
"""

from typing import Any, Dict, Iterable, List, Optional, Union

from search_query_dsl.core.models import SearchQuery, SearchCondition, SearchQueryGroup
from search_query_dsl.core.exceptions import OperatorNotFoundError, FieldValidationError
from search_query_dsl.core.validator import validate_search_query
from search_query_dsl.backends.memory.resolver import resolve_field, has_field
from search_query_dsl.backends.memory.operators import REGISTRY
from search_query_dsl.backends.memory.base import MemoryOperator


class ReverseCompare:
    """Wrapper to reverse comparison for descending order sorting."""
    def __init__(self, value):
        self.value = value
    
    def __lt__(self, other):
        return self.value > other.value
    
    def __le__(self, other):
        return self.value >= other.value
    
    def __gt__(self, other):
        return self.value < other.value
    
    def __ge__(self, other):
        return self.value <= other.value
    
    def __eq__(self, other):
        return self.value == other.value
    
    def __ne__(self, other):
        return self.value != other.value


class MemoryBackend:
    """
    In-memory search backend.
    
    Filters Python objects (dicts, dataclasses, objects) based on SearchQuery conditions.
    
    Example:
        backend = MemoryBackend()
        results = await backend.search(query, items)
    """
    
    def __init__(
        self,
        registry: Optional[Dict[str, MemoryOperator]] = None,
        strict_fields: bool = False,
    ):
        """
        Initialize memory backend.
        
        Args:
            registry: Custom operator registry. Defaults to built-in REGISTRY.
            strict_fields: If True, raise FieldValidationError for missing fields.
                          If False (default), missing fields resolve to None.
        """
        self.registry = registry or REGISTRY
        self.strict_fields = strict_fields
    
    async def search(
        self,
        query: Optional[SearchQuery],
        items: Union[Any, Iterable[Any]],
    ) -> List[Any]:
        """
        Filter items matching the query.
        
        Args:
            query: SearchQuery to apply. If None or empty, returns all items.
            items: Iterable of Python objects to filter, or a single object.
        
        Returns:
            List of items matching the query.
        """
        # Handle single item input (e.g. a dict or custom object)
        if isinstance(items, (dict, str, bytes)) or not hasattr(items, '__iter__'):
            items_list = [items]
        else:
            items_list = list(items)
        
        # Validate query before processing
        validate_search_query(query, operators=set(self.registry.keys()))
        
        # Apply filters if there are any conditions
        if query.groups:
            results = [item for item in items_list if self._item_matches(query, item)]
        else:
            results = items_list
        
        # Apply ordering
        if query.order_by:
            def make_sort_key(item):
                """Create a sort key tuple for the item based on order_by fields."""
                keys = []
                for field_spec in query.order_by:
                    is_desc = field_spec.startswith('-')
                    field_name = field_spec[1:] if is_desc else field_spec
                    value = resolve_field(item, field_name)
                    
                    # Handle None values: put them at the end
                    # For ascending: (False, value) - None becomes (True, None) which sorts last
                    # For descending: we negate with a wrapper class
                    if value is None:
                        keys.append((True, value))
                    elif is_desc:
                        # Reverse comparison for descending
                        keys.append((False, ReverseCompare(value)))
                    else:
                        keys.append((False, value))
                return tuple(keys)
            
            results = sorted(results, key=make_sort_key)
        
        # Apply offset and limit
        if query.offset is not None:
            results = results[query.offset:]
        if query.limit is not None:
            results = results[:query.limit]
        
        return results
    
    def _item_matches(self, query: SearchQuery, item: Any) -> bool:
        """Check if item matches all groups in query."""
        for group in query.groups:
            if not self._evaluate_group(group, item):
                return False
        return True
    
    def _evaluate_group(self, group: SearchQueryGroup, item: Any) -> bool:
        """Evaluate a group of conditions against an item."""
        results: List[bool] = []
        
        for condition in group.conditions:
            if isinstance(condition, SearchQueryGroup):
                results.append(self._evaluate_group(condition, item))
            else:
                results.append(self._evaluate_condition(condition, item))
        
        if not results:
            return True
        
        if group.group_operator == "and":
            return all(results)
        elif group.group_operator == "or":
            return any(results)
        elif group.group_operator == "not":
            return not all(results)
        
        # Default to AND
        return all(results)
    
    def _evaluate_condition(self, condition: SearchCondition, item: Any) -> bool:
        """Evaluate a single condition against an item."""
        # Validate field path exists if strict mode is enabled
        if self.strict_fields and not has_field(item, condition.field):
            # Find the invalid part and get available fields at that level
            invalid_field, parent_obj = self._find_invalid_field(item, condition.field)
            available = self._get_available_fields(parent_obj) if parent_obj is not None else []
            parent_type = type(parent_obj).__name__ if parent_obj is not None else type(item).__name__
            raise FieldValidationError(
                invalid_field,
                parent_type,
                available,
                condition.field,
            )
        
        # Resolve field value
        field_value = resolve_field(item, condition.field)
        
        # Get operator
        operator = self.registry.get(condition.operator)
        if operator is None:
            raise OperatorNotFoundError(condition.operator, list(self.registry.keys()))
        
        # Try direct evaluation first
        try:
            if operator.evaluate(field_value, condition.value, condition.value_type):
                return True
        except (TypeError, ValueError):
            # Ignore type errors during direct comparison (e.g. list vs string)
            pass
            
        # If field_value is a list/tuple, try matching ANY item in the list
        # This supports implicit list traversal logic (listAttr.name = "X")
        if isinstance(field_value, (list, tuple)):
            for val in field_value:
                try:
                    if operator.evaluate(val, condition.value, condition.value_type):
                        return True
                except (TypeError, ValueError):
                    continue
        
        return False
    
    def _find_invalid_field(self, item: Any, field_path: str) -> tuple:
        """
        Find the first invalid field in a path and return it with its parent object.
        
        Returns:
            (invalid_field_name, parent_object) - parent is where the field should exist
        """
        parts = field_path.split('.')
        current = item
        
        for part in parts:
            if current is None:
                return part, None
            
            if isinstance(current, dict):
                if part not in current:
                    return part, current
                current = current.get(part)
            elif isinstance(current, (list, tuple)):
                try:
                    idx = int(part)
                    if idx < 0 or idx >= len(current):
                        return part, current
                    current = current[idx]
                except ValueError:
                    return part, current
            elif hasattr(current, part):
                current = getattr(current, part)
            else:
                return part, current
        
        return parts[-1], current
    
    def _get_available_fields(self, item: Any) -> List[str]:
        """Get available field names from an item for error messages."""
        if isinstance(item, (list, tuple)):
            # If item is a list, suggest fields from the first item
            # This helps with implicit list traversal errors
            if item:
                return self._get_available_fields(item[0])
            return []
            
        if isinstance(item, dict):
            return list(item.keys())
        elif hasattr(item, '__dict__'):
            return list(vars(item).keys())
        elif hasattr(item, '__slots__'):
            return list(item.__slots__)
        return []



