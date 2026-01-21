"""
Operator definitions and registry.

Defines all supported operators for the Search Query DSL.
"""

from enum import Enum
from typing import Set


class Operator(str, Enum):
    """All supported operators."""
    
    # Standard comparison
    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    
    # Set operations
    IN = "in"
    NOT_IN = "not_in"
    ALL = "all"
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"
    
    # String operations
    LIKE = "like"
    NOT_LIKE = "not_like"
    ILIKE = "ilike"  # Case-insensitive like
    CONTAINS = "contains"
    ICONTAINS = "icontains"
    STARTSWITH = "startswith"
    ISTARTSWITH = "istartswith"
    ENDSWITH = "endswith"
    IENDSWITH = "iendswith"
    REGEX = "regex"
    IREGEX = "iregex"
    
    # Null checks
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"
    
    # JSONB operations
    JSONB_CONTAINS = "jsonb_contains"
    JSONB_CONTAINED_BY = "jsonb_contained_by"
    JSONB_HAS_KEY = "jsonb_has_key"
    JSONB_HAS_ANY = "jsonb_has_any"
    JSONB_HAS_ALL = "jsonb_has_all"
    JSONB_PATH_EXISTS = "jsonb_path_exists"
    
    # Geometry operations (PostGIS)
    INTERSECTS = "intersects"
    WITHIN = "within"
    CONTAINS_GEOM = "contains_geom"  # Renamed to avoid conflict
    TOUCHES = "touches"
    CROSSES = "crosses"
    OVERLAPS = "overlaps"
    DISJOINT = "disjoint"
    GEOM_EQUALS = "geom_equals"
    DISTANCE_LT = "distance_lt"
    DWITHIN = "dwithin"
    BBOX_INTERSECTS = "bbox_intersects"
    
    # Full-text search
    FTS = "fts"
    FTS_PHRASE = "fts_phrase"


# Operators that don't require a value
NULL_OPERATORS: Set[str] = {
    Operator.IS_NULL.value,
    Operator.IS_NOT_NULL.value,
    Operator.IS_EMPTY.value,
    Operator.IS_NOT_EMPTY.value,
}

# Operators that expect a list value
LIST_OPERATORS: Set[str] = {
    Operator.IN.value,
    Operator.NOT_IN.value,
    Operator.ALL.value,
    Operator.BETWEEN.value,
    Operator.NOT_BETWEEN.value,
    Operator.JSONB_HAS_ANY.value,
    Operator.JSONB_HAS_ALL.value,
}

# Geometry operators
GEOMETRY_OPERATORS: Set[str] = {
    Operator.INTERSECTS.value,
    Operator.WITHIN.value,
    Operator.CONTAINS_GEOM.value,
    Operator.TOUCHES.value,
    Operator.CROSSES.value,
    Operator.OVERLAPS.value,
    Operator.DISJOINT.value,
    Operator.GEOM_EQUALS.value,
    Operator.DISTANCE_LT.value,
    Operator.DWITHIN.value,
    Operator.BBOX_INTERSECTS.value,
}

# All valid operator strings
OPERATORS: Set[str] = {op.value for op in Operator}


def is_valid_operator(operator: str) -> bool:
    """Check if an operator string is valid."""
    return operator in OPERATORS


def requires_value(operator: str) -> bool:
    """Check if an operator requires a value."""
    return operator not in NULL_OPERATORS


def requires_list(operator: str) -> bool:
    """Check if an operator requires a list value."""
    return operator in LIST_OPERATORS


def is_geometry_operator(operator: str) -> bool:
    """Check if an operator is a geometry operator."""
    return operator in GEOMETRY_OPERATORS
