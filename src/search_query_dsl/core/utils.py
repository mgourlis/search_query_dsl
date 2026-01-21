"""
Shared utility functions for Search Query DSL.
"""

import json
import datetime
import uuid as uuid_module
from typing import Any, Optional


def _geojson_to_str(geojson: Any) -> str:
    """Convert GeoJSON dict to string if needed for ST_GeomFromGeoJSON."""
    if isinstance(geojson, dict):
        return json.dumps(geojson)
    return geojson


def _parse_list_value(value: Any) -> list:
    """
    Parse a value into a list.
    
    Supports:
    - Python collections (list, tuple, set)
    - Comma-separated strings: "val1, val2"
    - Bracketed strings: "[val1, val2]" or "['val1', 'val2']"
    """
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str):
        content = value.strip()
        if content.startswith('[') and content.endswith(']'):
            content = content[1:-1].strip()
        if not content:
            return []
        
        # Split by comma and strip whitespace and quotes
        return [v.strip().strip("'").strip('"') for v in content.split(",")]
    return [value]


def cast_value(value: Any, value_type: Optional[str] = None) -> Any:
    """
    Cast value to appropriate Python type.
    
    If value is a list, casts each item recursively.
    If value_type is None and value is a string, attempts to infer the type.
    
    Supports: string, text, integer, int, smallinteger, biginteger, float, double,
    decimal, numeric, boolean, date, datetime, time, interval, uuid, json, largebinary
    """
    # Handle lists recursively
    if isinstance(value, list):
        return [cast_value(item, value_type) for item in value]
    
    # Non-strings pass through unless explicit type given
    if not isinstance(value, str) and value_type is None:
        return value
    
    if value_type is not None:
        try:
            if value_type in ("string", "text"):
                return str(value)
            elif value_type in ("integer", "int", "smallinteger", "biginteger"):
                return int(value)
            elif value_type in ("float", "double", "decimal", "numeric"):
                return float(value)
            elif value_type == "boolean":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes")
                return bool(value)
            elif value_type == "date":
                if isinstance(value, datetime.date):
                    return value
                return datetime.datetime.fromisoformat(value).date()
            elif value_type == "datetime":
                if isinstance(value, datetime.datetime):
                    return value
                result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                if result.tzinfo is not None:
                    result = result.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                return result
            elif value_type == "time":
                if isinstance(value, datetime.time):
                    return value
                return datetime.time.fromisoformat(value)
            elif value_type == "interval":
                if isinstance(value, datetime.timedelta):
                    return value
                # Parse interval string like "1 day", "2 hours", "30 minutes", "1:30:00"
                if isinstance(value, str):
                    return _parse_interval(value)
                return datetime.timedelta(seconds=float(value))
            elif value_type == "uuid":
                if isinstance(value, uuid_module.UUID):
                    return value
                return uuid_module.UUID(value)
            elif value_type == "json":
                if isinstance(value, (dict, list)):
                    return value
                return json.loads(value)
            elif value_type == "largebinary":
                if isinstance(value, bytes):
                    return value
                if isinstance(value, str):
                    # Assume base64 encoded
                    import base64
                    return base64.b64decode(value)
                return bytes(value)
            else:
                return value
        except (ValueError, json.JSONDecodeError, TypeError):
            return value
    
    # Auto-inference for strings when value_type is None
    if isinstance(value, str):
        if value.strip() == "":
            return value
        
        # Try datetime
        try:
            result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
            if result.tzinfo is not None:
                result = result.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return result
        except ValueError:
            pass
        
        # Try date
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass
        
        # Try UUID
        try:
            return uuid_module.UUID(value)
        except ValueError:
            pass
        
        # Try JSON
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    
    return value


def _parse_interval(value: str) -> datetime.timedelta:
    """
    Parse an interval string into a timedelta.
    
    Supports formats like:
    - "1:30:00" (HH:MM:SS)
    - "1 day", "2 days"
    - "3 hours", "30 minutes", "45 seconds"
    - "1 day 2 hours 30 minutes"
    """
    import re
    
    # Try HH:MM:SS format
    time_match = re.match(r'^(\d+):(\d+):(\d+)$', value.strip())
    if time_match:
        hours, minutes, seconds = map(int, time_match.groups())
        return datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    
    # Try natural language format
    days = hours = minutes = seconds = 0
    
    day_match = re.search(r'(\d+)\s*days?', value, re.IGNORECASE)
    if day_match:
        days = int(day_match.group(1))
    
    hour_match = re.search(r'(\d+)\s*hours?', value, re.IGNORECASE)
    if hour_match:
        hours = int(hour_match.group(1))
    
    minute_match = re.search(r'(\d+)\s*minutes?', value, re.IGNORECASE)
    if minute_match:
        minutes = int(minute_match.group(1))
    
    second_match = re.search(r'(\d+)\s*seconds?', value, re.IGNORECASE)
    if second_match:
        seconds = int(second_match.group(1))
    
    if days or hours or minutes or seconds:
        return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    
    # Fallback: try to parse as seconds
    try:
        return datetime.timedelta(seconds=float(value))
    except ValueError:
        raise ValueError(f"Cannot parse interval: {value}")

