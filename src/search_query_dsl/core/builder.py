"""
Fluent query builder for constructing SearchQuery objects.
"""

from typing import Any, Optional, List

from search_query_dsl.core.models import (
    SearchQuery,
    SearchQueryGroup,
    SearchCondition,
)


class SearchQueryBuilder:
    """
    Fluent builder for constructing SearchQuery objects with nested group support.
    
    Example for (A AND B) OR (C AND D):
        query = (
            SearchQueryBuilder()
            .add_group("or")
            .add_nested_group("and")
                .add_condition("field1", "=", "A")
                .add_condition("field2", "=", "B")
            .end_nested_group()
            .add_nested_group("and")
                .add_condition("field3", "=", "C")
                .add_condition("field4", "=", "D")
            .end_nested_group()
            .build()
        )
    """
    
    def __init__(self):
        self._groups: List[SearchQueryGroup] = []
        self._group_stack: List[SearchQueryGroup] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._order_by: Optional[List[str]] = None
    
    def add_group(self, group_operator: str = "and") -> "SearchQueryBuilder":
        """
        Start a new top-level group.
        
        Args:
            group_operator: Operator for the group ('and', 'or', 'not')
        
        Returns:
            Self for method chaining
        """
        group = SearchQueryGroup(conditions=[], group_operator=group_operator)
        self._groups.append(group)
        self._group_stack = [group]
        return self
    
    def add_nested_group(self, group_operator: str = "and") -> "SearchQueryBuilder":
        """
        Add a nested group to the current group's conditions.
        
        Args:
            group_operator: Operator for the nested group
        
        Returns:
            Self for method chaining
        """
        if not self._group_stack:
            self.add_group("and")
        
        nested = SearchQueryGroup(conditions=[], group_operator=group_operator)
        self._group_stack[-1].conditions.append(nested)
        self._group_stack.append(nested)
        return self
    
    def end_nested_group(self) -> "SearchQueryBuilder":
        """
        End the current nested group and return to the parent.
        
        Returns:
            Self for method chaining
        """
        if len(self._group_stack) > 1:
            self._group_stack.pop()
        return self
    
    def add_condition(
        self,
        field: str,
        operator: str,
        value: Any = None,
        value_type: Optional[str] = None,
    ) -> "SearchQueryBuilder":
        """
        Add a condition to the current group.
        
        If no group exists, creates a default group with 'and' operator.
        
        Args:
            field: Field name or path
            operator: Comparison operator
            value: Value to compare
            value_type: Optional type hint
        
        Returns:
            Self for method chaining
        """
        if not self._group_stack:
            self.add_group("and")
        
        condition = SearchCondition(
            field=field,
            operator=operator,
            value=value,
            value_type=value_type,
        )
        self._group_stack[-1].conditions.append(condition)
        return self
    
    def limit(self, limit: int) -> "SearchQueryBuilder":
        """Set maximum number of results."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> "SearchQueryBuilder":
        """Set number of results to skip."""
        self._offset = offset
        return self
    
    def order_by(self, *fields: str) -> "SearchQueryBuilder":
        """
        Set ordering fields.
        
        Prefix with '-' for descending order.
        
        Example:
            .order_by("name", "-created")  # ASC name, DESC created
        """
        self._order_by = list(fields)
        return self
    
    def build(self) -> SearchQuery:
        """
        Build and return the SearchQuery object.
        
        Returns:
            The constructed SearchQuery
        """
        return SearchQuery(
            groups=self._groups,
            limit=self._limit,
            offset=self._offset,
            order_by=self._order_by,
        )
    
    def reset(self) -> "SearchQueryBuilder":
        """Reset the builder to initial state."""
        self._groups = []
        self._group_stack = []
        self._limit = None
        self._offset = None
        self._order_by = None
        return self
