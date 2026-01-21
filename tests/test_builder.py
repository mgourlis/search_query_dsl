"""Tests for SearchQueryBuilder."""

import pytest
from search_query_dsl.core.builder import SearchQueryBuilder
from search_query_dsl.core.models import SearchCondition, SearchQueryGroup


class TestSearchQueryBuilder:
    """Tests for SearchQueryBuilder."""
    
    def test_simple_condition(self):
        query = (
            SearchQueryBuilder()
            .add_condition("name", "=", "test")
            .build()
        )
        assert len(query.groups) == 1
        assert len(query.groups[0].conditions) == 1
        assert query.groups[0].conditions[0].field == "name"
    
    def test_multiple_conditions(self):
        query = (
            SearchQueryBuilder()
            .add_condition("a", "=", 1)
            .add_condition("b", "=", 2)
            .build()
        )
        assert len(query.groups[0].conditions) == 2
    
    def test_explicit_group(self):
        query = (
            SearchQueryBuilder()
            .add_group("or")
            .add_condition("a", "=", 1)
            .add_condition("b", "=", 2)
            .build()
        )
        assert query.groups[0].group_operator == "or"
    
    def test_nested_groups(self):
        query = (
            SearchQueryBuilder()
            .add_group("or")
            .add_nested_group("and")
                .add_condition("a", "=", 1)
                .add_condition("b", "=", 2)
            .end_nested_group()
            .add_nested_group("and")
                .add_condition("c", "=", 3)
            .end_nested_group()
            .build()
        )
        # (a=1 AND b=2) OR (c=3)
        assert query.groups[0].group_operator == "or"
        assert len(query.groups[0].conditions) == 2
        assert isinstance(query.groups[0].conditions[0], SearchQueryGroup)
    
    def test_pagination(self):
        query = (
            SearchQueryBuilder()
            .add_condition("a", "=", 1)
            .limit(100)
            .offset(50)
            .build()
        )
        assert query.limit == 100
        assert query.offset == 50
    
    def test_order_by(self):
        query = (
            SearchQueryBuilder()
            .add_condition("a", "=", 1)
            .order_by("name", "-created")
            .build()
        )
        assert query.order_by == ["name", "-created"]
    
    def test_method_chaining(self):
        builder = SearchQueryBuilder()
        result = builder.add_group("and")
        assert result is builder
        
        result = builder.add_condition("a", "=", 1)
        assert result is builder
    
    def test_reset(self):
        query = (
            SearchQueryBuilder()
            .add_condition("a", "=", 1)
            .limit(100)
            .reset()
            .add_condition("b", "=", 2)
            .build()
        )
        assert query.groups[0].conditions[0].field == "b"
        assert query.limit is None
