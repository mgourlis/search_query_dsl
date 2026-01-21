"""Core module - models, operators, validation, serialization, hooks."""

from search_query_dsl.core.models import (
    SearchQuery,
    SearchQueryGroup,
    SearchCondition,
)
from search_query_dsl.core.operators import Operator, OPERATORS
from search_query_dsl.core.builder import SearchQueryBuilder
from search_query_dsl.core.validator import SearchQueryValidator
from search_query_dsl.core.utils import cast_value, _parse_list_value
from search_query_dsl.core.hooks import HookResult, ResolutionContext
from search_query_dsl.core.exceptions import (
    SearchQueryError,
    ValidationError,
    FieldValidationError,
    OperatorNotFoundError,
)

__all__ = [
    # Models
    "SearchQuery",
    "SearchQueryGroup",
    "SearchCondition",
    # Operators
    "Operator",
    "OPERATORS",
    # Builder & Validator
    "SearchQueryBuilder",
    "SearchQueryValidator",
    # Utilities
    "cast_value",
    "_parse_list_value",
    # Hooks
    "HookResult",
    "ResolutionContext",
    # Exceptions
    "SearchQueryError",
    "ValidationError",
    "FieldValidationError",
    "RelationshipTraversalError",
    "FieldNotQueryableError",
    "OperatorNotFoundError",
]
