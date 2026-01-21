"""Tests for in-memory evaluator."""

import pytest
from search_query_dsl.core.builder import SearchQueryBuilder
from search_query_dsl.backends.memory import MemoryBackend


class TestInMemoryEvaluator:
    """Tests for MemoryBackend (async)."""
    
    @pytest.fixture
    def evaluator(self):
        return MemoryBackend()
    
    # =======================
    # Standard Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_equal(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "=", "active").build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert not await evaluator.search(query, [{"status": "inactive"}])
    
    @pytest.mark.asyncio
    async def test_not_equal(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "!=", "deleted").build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])
    
    @pytest.mark.asyncio
    async def test_greater_than(self, evaluator):
        query = SearchQueryBuilder().add_condition("age", ">", 18).build()
        
        assert await evaluator.search(query, [{"age": 25}])
        assert not await evaluator.search(query, [{"age": 15}])
    
    @pytest.mark.asyncio
    async def test_less_than(self, evaluator):
        query = SearchQueryBuilder().add_condition("price", "<", 100).build()
        
        assert await evaluator.search(query, [{"price": 50}])
        assert not await evaluator.search(query, [{"price": 150}])
    
    @pytest.mark.asyncio
    async def test_greater_than_or_equal(self, evaluator):
        query = SearchQueryBuilder().add_condition("age", ">=", 18).build()
        
        assert await evaluator.search(query, [{"age": 18}])
        assert await evaluator.search(query, [{"age": 25}])
        assert not await evaluator.search(query, [{"age": 15}])
    
    @pytest.mark.asyncio
    async def test_less_than_or_equal(self, evaluator):
        query = SearchQueryBuilder().add_condition("price", "<=", 100).build()
        
        assert await evaluator.search(query, [{"price": 100}])
        assert await evaluator.search(query, [{"price": 50}])
        assert not await evaluator.search(query, [{"price": 150}])
    
    # =======================
    # Set Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_in_operator(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "in", ["active", "pending"]).build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert await evaluator.search(query, [{"status": "pending"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])

    @pytest.mark.asyncio
    async def test_in_operator_with_string_list(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "in", "active,pending").build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert await evaluator.search(query, [{"status": "pending"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])

    @pytest.mark.asyncio
    async def test_in_operator_with_bracketed_string(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "in", "[active, pending]").build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert await evaluator.search(query, [{"status": "pending"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])

    @pytest.mark.asyncio
    async def test_in_operator_with_quoted_bracketed_string(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "in", "['active', 'pending']").build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert await evaluator.search(query, [{"status": "pending"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])
    
    @pytest.mark.asyncio
    async def test_not_in_operator(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "not_in", ["deleted", "archived"]).build()
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])
    
    @pytest.mark.asyncio
    async def test_between(self, evaluator):
        query = SearchQueryBuilder().add_condition("age", "between", [18, 65]).build()
        
        assert await evaluator.search(query, [{"age": 30}])
        assert not await evaluator.search(query, [{"age": 10}])
        assert not await evaluator.search(query, [{"age": 70}])
    
    @pytest.mark.asyncio
    async def test_all_operator(self, evaluator):
        query = SearchQueryBuilder().add_condition("tags", "all", ["python", "django"]).build()
        
        assert await evaluator.search(query, [{"tags": ["python", "django", "fastapi"]}])
        assert not await evaluator.search(query, [{"tags": ["python", "flask"]}])
    
    # =======================
    # String Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_like(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "like", "%test%").build()
        
        assert await evaluator.search(query, [{"name": "my test item"}])
        assert not await evaluator.search(query, [{"name": "something else"}])
    
    @pytest.mark.asyncio
    async def test_ilike(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "ilike", "%TEST%").build()
        
        assert await evaluator.search(query, [{"name": "My Test Item"}])
    
    @pytest.mark.asyncio
    async def test_contains(self, evaluator):
        query = SearchQueryBuilder().add_condition("description", "contains", "important").build()
        
        assert await evaluator.search(query, [{"description": "This is important info"}])
        assert not await evaluator.search(query, [{"description": "Nothing here"}])
    
    @pytest.mark.asyncio
    async def test_icontains(self, evaluator):
        query = SearchQueryBuilder().add_condition("description", "icontains", "IMPORTANT").build()
        
        assert await evaluator.search(query, [{"description": "This is important info"}])
    
    @pytest.mark.asyncio
    async def test_startswith(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "startswith", "Hello").build()
        
        assert await evaluator.search(query, [{"name": "Hello World"}])
        assert not await evaluator.search(query, [{"name": "World Hello"}])
    
    @pytest.mark.asyncio
    async def test_endswith(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "endswith", "World").build()
        
        assert await evaluator.search(query, [{"name": "Hello World"}])
        assert not await evaluator.search(query, [{"name": "World Hello"}])
    
    @pytest.mark.asyncio
    async def test_regex(self, evaluator):
        query = SearchQueryBuilder().add_condition("email", "regex", r"^[a-z]+@example\.com$").build()
        
        assert await evaluator.search(query, [{"email": "test@example.com"}])
        assert not await evaluator.search(query, [{"email": "Test@example.com"}])
    
    @pytest.mark.asyncio
    async def test_iregex(self, evaluator):
        query = SearchQueryBuilder().add_condition("email", "iregex", r"^[a-z]+@example\.com$").build()
        
        assert await evaluator.search(query, [{"email": "Test@example.com"}])
    
    # =======================
    # Null Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_is_null(self, evaluator):
        query = SearchQueryBuilder().add_condition("value", "is_null", None).build()
        
        assert await evaluator.search(query, [{"value": None}])
        assert not await evaluator.search(query, [{"value": 123}])
    
    @pytest.mark.asyncio
    async def test_is_not_null(self, evaluator):
        query = SearchQueryBuilder().add_condition("value", "is_not_null", None).build()
        
        assert await evaluator.search(query, [{"value": 123}])
        assert not await evaluator.search(query, [{"value": None}])
    
    @pytest.mark.asyncio
    async def test_is_empty(self, evaluator):
        query = SearchQueryBuilder().add_condition("items", "is_empty", None).build()
        
        assert await evaluator.search(query, [{"items": []}])
        assert await evaluator.search(query, [{"items": None}])
        assert not await evaluator.search(query, [{"items": [1, 2]}])
    
    @pytest.mark.asyncio
    async def test_is_not_empty(self, evaluator):
        query = SearchQueryBuilder().add_condition("items", "is_not_empty", None).build()
        
        assert await evaluator.search(query, [{"items": [1, 2]}])
        assert not await evaluator.search(query, [{"items": []}])
    
    # =======================
    # JSONB Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_jsonb_contains(self, evaluator):
        query = SearchQueryBuilder().add_condition("data", "jsonb_contains", {"status": "active"}).build()
        
        assert await evaluator.search(query, [{"data": {"status": "active", "name": "test"}}])
        assert not await evaluator.search(query, [{"data": {"status": "inactive"}}])
    
    @pytest.mark.asyncio
    async def test_jsonb_has_key(self, evaluator):
        query = SearchQueryBuilder().add_condition("data", "jsonb_has_key", "email").build()
        
        assert await evaluator.search(query, [{"data": {"email": "test@example.com"}}])
        assert not await evaluator.search(query, [{"data": {"name": "test"}}])
    
    @pytest.mark.asyncio
    async def test_jsonb_has_any(self, evaluator):
        query = SearchQueryBuilder().add_condition("data", "jsonb_has_any_keys", ["email", "phone"]).build()
        
        assert await evaluator.search(query, [{"data": {"email": "test@example.com"}}])
        assert await evaluator.search(query, [{"data": {"phone": "123"}}])
        assert not await evaluator.search(query, [{"data": {"name": "test"}}])
    
    @pytest.mark.asyncio
    async def test_jsonb_has_all(self, evaluator):
        query = SearchQueryBuilder().add_condition("data", "jsonb_has_all_keys", ["email", "phone"]).build()
        
        assert await evaluator.search(query, [{"data": {"email": "test@example.com", "phone": "123"}}])
        assert not await evaluator.search(query, [{"data": {"email": "test@example.com"}}])
    
    # =======================
    # Group Operators
    # =======================
    
    @pytest.mark.asyncio
    async def test_and_group(self, evaluator):
        query = (
            SearchQueryBuilder()
            .add_group("and")
            .add_condition("status", "=", "active")
            .add_condition("age", ">", 18)
            .build()
        )
        
        assert await evaluator.search(query, [{"status": "active", "age": 25}])
        assert not await evaluator.search(query, [{"status": "active", "age": 10}])
        assert not await evaluator.search(query, [{"status": "inactive", "age": 25}])
    
    @pytest.mark.asyncio
    async def test_or_group(self, evaluator):
        query = (
            SearchQueryBuilder()
            .add_group("or")
            .add_condition("status", "=", "active")
            .add_condition("status", "=", "pending")
            .build()
        )
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert await evaluator.search(query, [{"status": "pending"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])
    
    @pytest.mark.asyncio
    async def test_not_group(self, evaluator):
        query = (
            SearchQueryBuilder()
            .add_group("not")
            .add_condition("status", "=", "deleted")
            .build()
        )
        
        assert await evaluator.search(query, [{"status": "active"}])
        assert not await evaluator.search(query, [{"status": "deleted"}])
    
    # =======================
    # Field Path Resolution
    # =======================
    
    @pytest.mark.asyncio
    async def test_nested_path(self, evaluator):
        query = SearchQueryBuilder().add_condition("address.city", "=", "Athens").build()
        
        assert await evaluator.search(query, [{"address": {"city": "Athens"}}])
        assert not await evaluator.search(query, [{"address": {"city": "London"}}])
    
    @pytest.mark.asyncio
    async def test_deeply_nested_path(self, evaluator):
        query = SearchQueryBuilder().add_condition("user.profile.settings.theme", "=", "dark").build()
        
        assert await evaluator.search(query, [{"user": {"profile": {"settings": {"theme": "dark"}}}}])
        assert not await evaluator.search(query, [{"user": {"profile": {"settings": {"theme": "light"}}}}])
    
    @pytest.mark.asyncio
    async def test_list_index_path(self, evaluator):
        query = SearchQueryBuilder().add_condition("items.0.name", "=", "first").build()
        
        assert await evaluator.search(query, [{"items": [{"name": "first"}, {"name": "second"}]}])
        assert not await evaluator.search(query, [{"items": [{"name": "other"}]}])
    
    # =======================
    # Edge Cases
    # =======================
    
    @pytest.mark.asyncio
    async def test_empty_query_matches_all(self, evaluator):
        query = SearchQueryBuilder().build()
        
        assert await evaluator.search(query, [{"anything": "value"}])
    
    @pytest.mark.asyncio
    async def test_none_query_raises_error(self, evaluator):
        """None queries are invalid and should raise ValueError."""
        with pytest.raises(ValueError, match="SearchQuery cannot be None"):
            await evaluator.search(None, [{"anything": "value"}])
    
    @pytest.mark.asyncio
    async def test_missing_field_returns_none(self, evaluator):
        query = SearchQueryBuilder().add_condition("missing", "is_null", None).build()
        
        assert await evaluator.search(query, [{"other": "value"}])
    
    # =======================
    # Filter Method
    # =======================
    
    @pytest.mark.asyncio
    async def test_filter_list(self, evaluator):
        query = SearchQueryBuilder().add_condition("status", "=", "active").build()
        
        items = [
            {"status": "active", "name": "A"},
            {"status": "inactive", "name": "B"},
            {"status": "active", "name": "C"},
        ]
        
        result = await evaluator.search(query, items)
        assert len(result) == 2
        assert result[0]["name"] == "A"
        assert result[1]["name"] == "C"
    
    @pytest.mark.asyncio
    async def test_filter_with_nested_conditions(self, evaluator):
        query = (
            SearchQueryBuilder()
            .add_group("and")
            .add_condition("status", "=", "active")
            .add_condition("priority", ">", 5)
            .build()
        )
        
        items = [
            {"status": "active", "priority": 10},
            {"status": "active", "priority": 3},
            {"status": "inactive", "priority": 10},
        ]
        
        result = await evaluator.search(query, items)
        assert len(result) == 1
        assert result[0]["priority"] == 10

    # =======================
    # Single Item Search
    # =======================

    @pytest.mark.asyncio
    async def test_single_dict_search(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "=", "test").build()
        
        # Passing a single dict directly (not wrapped in list)
        item = {"name": "test"}
        results = await evaluator.search(query, item)
        
        assert len(results) == 1
        assert results[0] == item

    @pytest.mark.asyncio
    async def test_single_object_search(self, evaluator):
        class Item:
            def __init__(self, name):
                self.name = name
                
        query = SearchQueryBuilder().add_condition("name", "=", "test").build()
        
        item = Item("test")
        results = await evaluator.search(query, item)
        
        assert len(results) == 1
        assert results[0] == item

    @pytest.mark.asyncio
    async def test_list_of_dicts_search(self, evaluator):
        query = SearchQueryBuilder().add_condition("name", "=", "test").build()
        
        items = [{"name": "test"}, {"name": "other"}]
        results = await evaluator.search(query, items)
        
        assert len(results) == 1
        assert results[0]["name"] == "test"
