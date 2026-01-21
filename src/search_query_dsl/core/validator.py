"""
Query structure validation.
"""

from typing import Optional, Set

from search_query_dsl.core.models import (
    SearchQuery,
    SearchQueryGroup,
    SearchCondition,
)
from search_query_dsl.core.operators import OPERATORS, NULL_OPERATORS
from search_query_dsl.core.exceptions import ValidationError, OperatorNotFoundError


class SearchQueryValidator:
    """
    Validates SearchQuery structure and operators.
    
    Usage:
        validator = SearchQueryValidator()
        validator.validate(query)  # Raises ValidationError if invalid
    """
    
    VALID_GROUP_OPERATORS = {"and", "or", "not"}
    MAX_DEPTH = 10  # Prevent infinite recursion
    
    def __init__(self, operators: Optional[Set[str]] = None):
        """
        Initialize validator.
        
        Args:
            operators: Custom set of valid operators. Defaults to all OPERATORS.
        """
        self.valid_operators = operators or OPERATORS
    
    def validate(self, query: SearchQuery) -> None:
        """
        Validate the entire SearchQuery.
        
        Args:
            query: The SearchQuery to validate
        
        Raises:
            ValidationError: If the query structure is invalid
            OperatorNotFoundError: If an unknown operator is used
        """
        if query.limit is not None and query.limit < 1:
            raise ValidationError("limit must be >= 1", "limit")
        
        if query.offset is not None and query.offset < 0:
            raise ValidationError("offset must be >= 0", "offset")
        
        for i, group in enumerate(query.groups):
            self._validate_group(group, path=f"groups[{i}]", depth=0)
    
    def _validate_group(
        self,
        group: SearchQueryGroup,
        path: str,
        depth: int,
    ) -> None:
        """Validate a single group recursively."""
        if depth > self.MAX_DEPTH:
            raise ValidationError(
                f"Maximum nesting depth ({self.MAX_DEPTH}) exceeded",
                path,
            )
        
        if group.group_operator not in self.VALID_GROUP_OPERATORS:
            raise ValidationError(
                f"Invalid group_operator: '{group.group_operator}'. "
                f"Must be one of: {', '.join(self.VALID_GROUP_OPERATORS)}",
                f"{path}.group_operator",
            )
        
        if not group.conditions:
            raise ValidationError(
                "Group must contain at least one condition",
                f"{path}.conditions",
            )
        
        for i, item in enumerate(group.conditions):
            item_path = f"{path}.conditions[{i}]"
            
            if isinstance(item, SearchQueryGroup):
                self._validate_group(item, item_path, depth + 1)
            elif isinstance(item, SearchCondition):
                self._validate_condition(item, item_path)
            elif isinstance(item, dict):
                # Handle dict representation
                if "conditions" in item:
                    nested = SearchQueryGroup(
                        conditions=item.get("conditions", []),
                        group_operator=item.get("group_operator", "and"),
                    )
                    self._validate_group(nested, item_path, depth + 1)
                else:
                    self._validate_condition_dict(item, item_path)
            else:
                raise ValidationError(
                    f"Invalid condition type: {type(item).__name__}",
                    item_path,
                )
    
    def _validate_condition(self, condition: SearchCondition, path: str) -> None:
        """Validate a single SearchCondition."""
        if not condition.field:
            raise ValidationError("Condition field cannot be empty", f"{path}.field")
        
        if condition.operator not in self.valid_operators:
            raise OperatorNotFoundError(condition.operator, list(self.valid_operators))
        
        # Check if value is required
        if condition.operator not in NULL_OPERATORS and condition.value is None:
            raise ValidationError(
                f"Operator '{condition.operator}' requires a value",
                f"{path}.value",
            )
    
    def _validate_condition_dict(self, condition: dict, path: str) -> None:
        """Validate a condition dict."""
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")
        
        if not field:
            raise ValidationError("Condition field cannot be empty", f"{path}.field")
        
        if not operator:
            raise ValidationError("Condition operator cannot be empty", f"{path}.operator")
        
        if operator not in self.valid_operators:
            raise OperatorNotFoundError(operator, list(self.valid_operators))
        
        if operator not in NULL_OPERATORS and value is None:
            raise ValidationError(
                f"Operator '{operator}' requires a value",
                f"{path}.value",
            )

def validate_search_query(query: SearchQuery, operators: Optional[Set[str]] = None) -> None:
    """
    Validate a SearchQuery.
    
    Args:
        query: The SearchQuery to validate (must not be None)
        operators: Optional set of allowed operators
    
    Raises:
        ValueError: If query is None
    """
    if query is None:
        raise ValueError("SearchQuery cannot be None")
    
    validator = SearchQueryValidator(operators=operators)
    validator.validate(query)
