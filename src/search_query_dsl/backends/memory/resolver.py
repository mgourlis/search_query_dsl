"""
Field path resolver for in-memory object traversal.

This module provides utilities for resolving dot-notation field paths
on Python objects (dicts, lists, objects).

This is a lightweight utility primarily used internally by the InMemoryEvaluator,
but can also be used standalone for field resolution.
"""

from typing import Any, List


def resolve_field(obj: Any, field_path: str) -> Any:
    """
    Resolve a dot-notation field path on an object.
    
    Supports:
    - Dict keys: "status", "address.city"
    - Object attributes: "user.name"
    - List indices: "items.0.name"
    - Implicit list traversal: "items.name" -> ["Item 1", "Item 2"]
    
    Args:
        obj: The object to resolve the field from
        field_path: Dot-notation path (e.g., "address.city")
    
    Returns:
        The resolved value. If implicit list traversal occurred, returns a list of values.
        Returns None if path not found.
    """
    if obj is None:
        return None
    
    parts = field_path.split('.')
    return _resolve_recursive(obj, parts)


def _resolve_recursive(current: Any, parts: List[str]) -> Any:
    """Recursively resolve path, handling implicit list traversal."""
    if not parts:
        return current
    
    part = parts[0]
    remaining = parts[1:]
    
    if current is None:
        return None
    
    # Check for implicit list traversal
    # If current is a list AND part is NOT an integer index
    # Then treat as map: apply to all items
    if isinstance(current, (list, tuple)) and not (part.isdigit() and _is_valid_index(current, part)):
        results = []
        for item in current:
            val = _resolve_recursive(item, parts)
            if val is not None:
                if isinstance(val, list) and not remaining: 
                   # Flatten if we achieved a list of results from a sub-traversal
                   # But be careful not to flatten if the value itself IS a list
                   # actually, standard behavior for "items.tags" where tags is list -> [[tag1], [tag2]]
                   results.append(val)
                else:
                    results.append(val)
        return results if results else []
        
    # Normal resolution
    try:
        current = _get_field_value(current, part)
    except Exception:
        return None
        
    return _resolve_recursive(current, remaining)


def _is_valid_index(obj: List, key: str) -> bool:
    try:
        idx = int(key)
        return 0 <= idx < len(obj)
    except ValueError:
        return False


def _get_field_value(obj: Any, key: str) -> Any:
    """
    Get a single field value from an object.
    
    Args:
        obj: The object to get the value from
        key: The key/attribute/index to access
    
    Returns:
        The value, or None if not found
    """
    # Try dict access
    if isinstance(obj, dict):
        return obj.get(key)
    
    # Try list index
    if isinstance(obj, (list, tuple)):
        try:
            idx = int(key)
            return obj[idx] if 0 <= idx < len(obj) else None
        except ValueError:
            return None
    
    # Try object attribute
    if hasattr(obj, key):
        return getattr(obj, key)
    
    return None





def has_field(obj: Any, field_path: str) -> bool:
    """
    Check if an object has a field at the given path.
    
    Args:
        obj: The object to check
        field_path: Dot-notation path
    
    Returns:
        True if the path exists, even if the value is None
    """
    if obj is None:
        return False
    
    parts = field_path.split('.')
    return _has_field_recursive(obj, parts)


def _has_field_recursive(current: Any, parts: List[str]) -> bool:
    """Recursively check if field exists, handling implicit list traversal."""
    if not parts:
        return True
    
    part = parts[0]
    remaining = parts[1:]
    
    if current is None:
        return False
    
    # Check for implicit list traversal
    # If current is a list AND part is NOT an integer index
    if isinstance(current, (list, tuple)) and not (part.isdigit() and _is_valid_index(current, part)):
        # Return True if ANY item has the field path
        # (This matches strict_fields logic: as long as the path is valid for *some* item, it's valid)
        for item in current:
            if _has_field_recursive(item, parts):
                return True
        return False
        
    # Normal resolution check
    if isinstance(current, dict):
        if part not in current:
            return False
        current = current[part]
    elif isinstance(current, (list, tuple)):
        # We already handled implicit above, so this must be an index access
        # but _is_valid_index check above logic implies we are here only if it IS an index?
        # Actually logic above: if list and NOT index -> implicit. 
        # So here it IS an index (or list is empty/invalid index structure, but that's handled by try/except).
        try:
            idx = int(part)
            if idx < 0 or idx >= len(current):
                return False
            current = current[idx]
        except ValueError:
            return False
    elif hasattr(current, part):
        current = getattr(current, part)
    else:
        return False
    
    return _has_field_recursive(current, remaining)
