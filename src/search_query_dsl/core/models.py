"""
Core data models for Search Query DSL.

These models are framework-agnostic and have zero external dependencies.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


@dataclass
class SearchCondition:
    """
    A single search condition (leaf node).
    
    Attributes:
        field: Field name or dot-notation path (e.g., "name", "element_type.label")
        operator: Comparison operator (e.g., "=", "like", "in", "intersects")
        value: Value to compare against (optional for is_null, is_not_null)
        value_type: Optional type hint for casting (e.g., "integer", "datetime", "geometry")
    
    Example:
        SearchCondition(field="status", operator="=", value="active")
        SearchCondition(field="created", operator=">", value="2024-01-01", value_type="datetime")
    """
    field: str
    operator: str
    value: Optional[Any] = None
    value_type: Optional[str] = None
    
    def __post_init__(self):
        if not self.field:
            raise ValueError("SearchCondition.field cannot be empty")
        if not self.operator:
            raise ValueError("SearchCondition.operator cannot be empty")
            
    def to_dict(self) -> dict:
        result = {
            "field": self.field,
            "operator": self.operator,
        }
        if self.value is not None:
            result["value"] = self.value
        if self.value_type is not None:
            result["value_type"] = self.value_type
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "SearchCondition":
        return cls(
            field=data["field"],
            operator=data["operator"],
            value=data.get("value"),
            value_type=data.get("value_type"),
        )


@dataclass
class SearchQueryGroup:
    """
    A group of conditions combined with a logical operator.
    
    Supports nesting for complex boolean logic.
    
    Attributes:
        conditions: List of SearchCondition or nested SearchQueryGroup
        group_operator: Logical operator - "and", "or", "not"
    
    Example for (A AND B) OR (C AND D):
        SearchQueryGroup(
            group_operator="or",
            conditions=[
                SearchQueryGroup(group_operator="and", conditions=[A, B]),
                SearchQueryGroup(group_operator="and", conditions=[C, D]),
            ]
        )
    """
    conditions: List[Union["SearchCondition", "SearchQueryGroup"]] = field(default_factory=list)
    group_operator: str = "and"
    
    def __post_init__(self):
        if self.group_operator not in ("and", "or", "not"):
            raise ValueError(f"Invalid group_operator: {self.group_operator}. Must be 'and', 'or', or 'not'")

    def to_dict(self) -> dict:
        return {
            "group_operator": self.group_operator,
            "conditions": [c.to_dict() for c in self.conditions],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SearchQueryGroup":
        conditions = []
        for item in data.get("conditions", []):
            if "conditions" in item and "field" not in item:
                conditions.append(SearchQueryGroup.from_dict(item))
            else:
                conditions.append(SearchCondition.from_dict(item))
                
        return cls(
            conditions=conditions,
            group_operator=data.get("group_operator", "and"),
        )


@dataclass
class SearchQuery:
    """
    Top-level search query container.
    
    Multiple groups are combined with AND by default.
    Use nested groups with "or" operator for OR logic.
    
    Attributes:
        groups: List of SearchQueryGroup (combined with AND)
        limit: Maximum number of results (optional)
        offset: Skip first N results (optional)
        order_by: List of field names to order by (prefix with - for DESC)
    
    Example:
        query = SearchQuery(
            groups=[
                SearchQueryGroup(conditions=[...])
            ],
            limit=100,
            offset=0,
            order_by=["name", "-created"]
        )
    """
    groups: List[SearchQueryGroup] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[List[str]] = None
    
    def is_empty(self) -> bool:
        """Check if the query has no conditions."""
        return not self.groups or all(not g.conditions for g in self.groups)
    
    def merge(self, other: "SearchQuery") -> "SearchQuery":
        """
        Merge another SearchQuery into this one using AND logic.
        
        Both queries' groups are combined. Since groups are ANDed together,
        this effectively creates: (self conditions) AND (other conditions).
        
        Pagination (limit/offset) from self takes precedence.
        Order by from self takes precedence if set.
        
        Use case: Merging user search queries with authorization filters.
        
        Args:
            other: Another SearchQuery to merge
            
        Returns:
            New SearchQuery with combined conditions
            
        Example:
            user_query = SearchQuery(groups=[...user filters...])
            auth_query = SearchQuery(groups=[...authorization conditions...])
            combined = user_query.merge(auth_query)
            # Result: user filters AND authorization conditions
        """
        return SearchQuery(
            groups=self.groups + other.groups,
            limit=self.limit if self.limit is not None else other.limit,
            offset=self.offset if self.offset is not None else other.offset,
            order_by=self.order_by if self.order_by is not None else other.order_by,
        )

    def to_dict(self) -> dict:
        result = {
            "groups": [g.to_dict() for g in self.groups],
        }
        if self.limit is not None:
            result["limit"] = self.limit
        if self.offset is not None:
            result["offset"] = self.offset
        if self.order_by is not None:
            result["order_by"] = self.order_by
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "SearchQuery":
        if not data:
            return cls()
            
        return cls(
            groups=[SearchQueryGroup.from_dict(g) for g in data.get("groups", [])],
            limit=data.get("limit"),
            offset=data.get("offset"),
            order_by=data.get("order_by"),
        )

