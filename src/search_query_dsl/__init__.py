"""
Search Query DSL - A JSON-based query language for database filtering.

Usage:
    from search_query_dsl import search, SearchQuery, SearchQueryBuilder
    
    # Memory search
    results = await search(query, items)
    
    # SQLAlchemy search
    results = await search(query, session, User)
"""

from search_query_dsl.core.models import (
    SearchQuery,
    SearchQueryGroup,
    SearchCondition,
)
from search_query_dsl.core.builder import SearchQueryBuilder
from search_query_dsl.core.validator import SearchQueryValidator
from search_query_dsl.core.operators import Operator, OPERATORS
from search_query_dsl.core.exceptions import (
    SearchQueryError,
    ValidationError,
    FieldValidationError,
    OperatorNotFoundError,
)
from search_query_dsl.core.hooks import HookResult, ResolutionContext
from search_query_dsl.api import search, get_supported_operators

__version__ = "0.1.0"

__all__ = [
    # API
    "search",
    "get_supported_operators",
    # Modelss
    "SearchQuery",
    "SearchQueryGroup", 
    "SearchCondition",
    # Builder
    "SearchQueryBuilder",
    # Validator
    "SearchQueryValidator",
    "SearchQueryValidator",
    # Operators
    "Operator",
    "OPERATORS",
    # Hooks
    "HookResult",
    "ResolutionContext",
    # Exceptions
    "SearchQueryError",
    "ValidationError",
    "FieldValidationError",
    "OperatorNotFoundError",
]

