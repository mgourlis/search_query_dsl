"""
SQLAlchemy backend operator registry.

Imports all operator classes and registers them in REGISTRY.
"""

from typing import Dict, Optional

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator

# Import all operators
from search_query_dsl.backends.sqlalchemy.operators.standard import (
    EqualOperator,
    NotEqualOperator,
    GreaterThanOperator,
    LessThanOperator,
    GreaterThanOrEqualOperator,
    LessThanOrEqualOperator,
    IsEmptyOperator,
    IsNotEmptyOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.string import (
    LikeOperator,
    NotLikeOperator,
    ILikeOperator,
    ContainsOperator,
    IContainsOperator,
    StartsWithOperator,
    IStartsWithOperator,
    EndsWithOperator,
    IEndsWithOperator,
    RegexOperator,
    IRegexOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.set import (
    InOperator,
    NotInOperator,
    BetweenOperator,
    NotBetweenOperator,
    AllOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.null import (
    IsNullOperator,
    IsNotNullOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.jsonb import (
    JsonbContainsOperator,
    JsonbContainedByOperator,
    JsonbHasKeyOperator,
    JsonbHasAnyKeysOperator,
    JsonbHasAllKeysOperator,
    JsonbPathExistsOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.geometry import (
    DisjointOperator,
    BboxIntersectsOperator,
    IntersectsOperator,
    DWithinOperator,
    WithinOperator,
    ContainsGeomOperator,
    OverlapsGeomOperator,
    GeomEqualsOperator,
    TouchesOperator,
    CrossesOperator,
    DistanceLessThanOperator,
)
from search_query_dsl.backends.sqlalchemy.operators.fts import FtsOperator, FtsPhraseOperator

# Operator registry: maps operator name to operator instance
REGISTRY: Dict[str, SQLAlchemyOperator] = {}

def _register(op: SQLAlchemyOperator, alias: Optional[str] = None) -> SQLAlchemyOperator:
    """Register an operator instance."""
    key = alias if alias else op.name
    REGISTRY[key] = op
    return op


# Register all operators
# Standard
_register(EqualOperator())
_register(NotEqualOperator())
_register(GreaterThanOperator())
_register(LessThanOperator())
_register(GreaterThanOrEqualOperator())
_register(LessThanOrEqualOperator())
_register(InOperator())
_register(AllOperator())
_register(NotInOperator())
_register(IsNotEmptyOperator())
_register(IsEmptyOperator())
_register(LikeOperator())
_register(NotLikeOperator())
_register(IsNullOperator())
_register(IsNotNullOperator())
_register(BetweenOperator())
_register(NotBetweenOperator())

# Geospatial
_register(IntersectsOperator())
_register(WithinOperator())
_register(ContainsGeomOperator())  # name="contains_geom"
_register(DistanceLessThanOperator())
_register(DWithinOperator())
_register(GeomEqualsOperator())
_register(TouchesOperator())
_register(CrossesOperator())
_register(OverlapsGeomOperator())
_register(DisjointOperator())
_register(BboxIntersectsOperator())

# Full-text search
_register(FtsOperator())
_register(FtsPhraseOperator())

# Other String Ops
_register(ILikeOperator())
_register(ContainsOperator())  # name="contains"
_register(IContainsOperator())
_register(StartsWithOperator())
_register(IStartsWithOperator())
_register(EndsWithOperator())
_register(IEndsWithOperator())
_register(RegexOperator())
_register(IRegexOperator())

# JSONB (keep them)
_register(JsonbContainsOperator())
_register(JsonbContainedByOperator())
_register(JsonbHasKeyOperator())
_register(JsonbHasAnyKeysOperator())
_register(JsonbHasAllKeysOperator())
_register(JsonbPathExistsOperator())


# Export all for convenience
__all__ = [
    "REGISTRY",
    # Standard
    "EqualOperator",
    "NotEqualOperator",
    "GreaterThanOperator",
    "LessThanOperator",
    "GreaterThanOrEqualOperator",
    "LessThanOrEqualOperator",
    "IsEmptyOperator",
    "IsNotEmptyOperator",
    # String
    "LikeOperator",
    "NotLikeOperator",
    "ILikeOperator",
    "ContainsOperator",
    "IContainsOperator",
    "StartsWithOperator",
    "IStartsWithOperator",
    "EndsWithOperator",
    "IEndsWithOperator",
    "RegexOperator",
    "IRegexOperator",
    # Set
    "InOperator",
    "NotInOperator",
    "BetweenOperator",
    "NotBetweenOperator",
    "AllOperator",
    # Null
    "IsNullOperator",
    "IsNotNullOperator",
    # JSONB
    "JsonbContainsOperator",
    "JsonbContainedByOperator",
    "JsonbHasKeyOperator",
    "JsonbHasAnyKeysOperator",
    "JsonbHasAllKeysOperator",
    # Geometry
    "IntersectsOperator",
    "DWithinOperator",
    "WithinOperator",
    "ContainsGeomOperator",
    "OverlapsGeomOperator",
    "GeomEqualsOperator",
    "TouchesOperator",
    "CrossesOperator",
    "DistanceLessThanOperator",
]
