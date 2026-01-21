"""Tests for core models."""

import pytest
from search_query_dsl.core.models import (
    SearchQuery,
    SearchQueryGroup,
    SearchCondition,
)


class TestSearchCondition:
    """Tests for SearchCondition."""
    
    def test_create_simple_condition(self):
        condition = SearchCondition(field="name", operator="=", value="test")
        assert condition.field == "name"
        assert condition.operator == "="
        assert condition.value == "test"
        assert condition.value_type is None
    
    def test_create_condition_with_type(self):
        condition = SearchCondition(
            field="age",
            operator=">",
            value="25",
            value_type="integer",
        )
        assert condition.value_type == "integer"
    
    def test_empty_field_raises_error(self):
        with pytest.raises(ValueError, match="field cannot be empty"):
            SearchCondition(field="", operator="=", value="test")
    
    def test_empty_operator_raises_error(self):
        with pytest.raises(ValueError, match="operator cannot be empty"):
            SearchCondition(field="name", operator="", value="test")

    def test_serialization(self):
        condition = SearchCondition(field="name", operator="=", value="test", value_type="string")
        data = condition.to_dict()
        assert data == {
            "field": "name",
            "operator": "=",
            "value": "test",
            "value_type": "string",
        }
        
        recreated = SearchCondition.from_dict(data)
        assert recreated.field == "name"
        assert recreated.operator == "="
        assert recreated.value == "test"



class TestSearchQueryGroup:
    """Tests for SearchQueryGroup."""
    
    def test_create_and_group(self):
        group = SearchQueryGroup(
            conditions=[
                SearchCondition("a", "=", 1),
                SearchCondition("b", "=", 2),
            ],
            group_operator="and",
        )
        assert group.group_operator == "and"
        assert len(group.conditions) == 2
    
    def test_create_or_group(self):
        group = SearchQueryGroup(
            conditions=[SearchCondition("a", "=", 1)],
            group_operator="or",
        )
        assert group.group_operator == "or"
    
    def test_invalid_operator_raises_error(self):
        with pytest.raises(ValueError, match="Invalid group_operator"):
            SearchQueryGroup(conditions=[], group_operator="xor")
    
    def test_nested_groups(self):
        inner = SearchQueryGroup(
            conditions=[SearchCondition("a", "=", 1)],
            group_operator="and",
        )
        outer = SearchQueryGroup(
            conditions=[inner, SearchCondition("b", "=", 2)],
            group_operator="or",
        )
        assert len(outer.conditions) == 2
        assert isinstance(outer.conditions[0], SearchQueryGroup)

    def test_serialization(self):
        group = SearchQueryGroup(
            conditions=[SearchCondition("a", "=", 1)],
            group_operator="or",
        )
        data = group.to_dict()
        assert data["group_operator"] == "or"
        assert len(data["conditions"]) == 1
        assert data["conditions"][0]["field"] == "a"
        
        recreated = SearchQueryGroup.from_dict(data)
        assert recreated.group_operator == "or"
        assert len(recreated.conditions) == 1
        assert isinstance(recreated.conditions[0], SearchCondition)


class TestSearchQuery:
    """Tests for SearchQuery."""
    
    def test_create_empty_query(self):
        query = SearchQuery()
        assert query.groups == []
        assert query.is_empty()
    
    def test_create_query_with_groups(self):
        group = SearchQueryGroup(
            conditions=[SearchCondition("name", "=", "test")],
        )
        query = SearchQuery(groups=[group])
        assert not query.is_empty()
        assert len(query.groups) == 1
    
    def test_pagination(self):
        query = SearchQuery(limit=100, offset=50)
        assert query.limit == 100
        assert query.offset == 50
    
    def test_ordering(self):
        query = SearchQuery(order_by=["name", "-created"])
        assert query.order_by == ["name", "-created"]

    def test_serialization(self):
        query = SearchQuery(
            groups=[
                SearchQueryGroup(conditions=[SearchCondition("a", "=", 1)])
            ],
            limit=10,
            offset=0,
            order_by=["name"]
        )
        data = query.to_dict()
        assert data["limit"] == 10
        assert data["order_by"] == ["name"]
        assert len(data["groups"]) == 1
        
        recreated = SearchQuery.from_dict(data)
        assert recreated.limit == 10
        assert recreated.order_by == ["name"]
        assert len(recreated.groups) == 1
