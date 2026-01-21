"""Tests for query pagination and ordering (limit, offset, order_by)."""

import pytest
from search_query_dsl.core.builder import SearchQueryBuilder
from search_query_dsl.backends.memory import MemoryBackend


@pytest.mark.asyncio
async def test_limit_basic():
    """Test basic limit functionality."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().add_condition("status", "=", "active").limit(2).build()
    
    items = [
        {"id": 1, "status": "active"},
        {"id": 2, "status": "active"},
        {"id": 3, "status": "active"},
        {"id": 4, "status": "active"},
    ]
    
    results = await backend.search(query, items)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_offset_basic():
    """Test basic offset functionality."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().add_condition("status", "=", "active").offset(2).build()
    
    items = [
        {"id": 1, "status": "active"},
        {"id": 2, "status": "active"},
        {"id": 3, "status": "active"},
        {"id": 4, "status": "active"},
    ]
    
    results = await backend.search(query, items)
    assert len(results) == 2
    assert results[0]["id"] == 3
    assert results[1]["id"] == 4


@pytest.mark.asyncio
async def test_limit_and_offset():
    """Test limit and offset combined."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().add_condition("status", "=", "active").limit(2).offset(1).build()
    
    items = [
        {"id": 1, "status": "active"},
        {"id": 2, "status": "active"},
        {"id": 3, "status": "active"},
        {"id": 4, "status": "active"},
    ]
    
    results = await backend.search(query, items)
    assert len(results) == 2
    assert results[0]["id"] == 2
    assert results[1]["id"] == 3


@pytest.mark.asyncio
async def test_order_by_ascending():
    """Test ordering by single field ascending."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().order_by("name").build()
    
    items = [
        {"name": "Charlie"},
        {"name": "Alice"},
        {"name": "Bob"},
    ]
    
    results = await backend.search(query, items)
    assert results[0]["name"] == "Alice"
    assert results[1]["name"] == "Bob"
    assert results[2]["name"] == "Charlie"


@pytest.mark.asyncio
async def test_order_by_descending():
    """Test ordering by single field descending."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().order_by("-name").build()
    
    items = [
        {"name": "Charlie"},
        {"name": "Alice"},
        {"name": "Bob"},
    ]
    
    results = await backend.search(query, items)
    assert results[0]["name"] == "Charlie"
    assert results[1]["name"] == "Bob"
    assert results[2]["name"] == "Alice"


@pytest.mark.asyncio
async def test_order_by_multiple_fields():
    """Test ordering by multiple fields."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().order_by("status", "-priority").build()
    
    items = [
        {"status": "active", "priority": 1},
        {"status": "pending", "priority": 3},
        {"status": "active", "priority": 5},
        {"status": "pending", "priority": 2},
    ]
    
    results = await backend.search(query, items)
    # First by status (asc): active, active, pending, pending
    # Then by priority (desc) within each status group
    assert results[0] == {"status": "active", "priority": 5}
    assert results[1] == {"status": "active", "priority": 1}
    assert results[2] == {"status": "pending", "priority": 3}
    assert results[3] == {"status": "pending", "priority": 2}


@pytest.mark.asyncio
async def test_order_by_with_none_values():
    """Test ordering handles None values correctly."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().order_by("priority").build()
    
    items = [
        {"name": "A", "priority": 3},
        {"name": "B", "priority": None},
        {"name": "C", "priority": 1},
        {"name": "D", "priority": None},
    ]
    
    results = await backend.search(query, items)
    # None values should sort to the end
    assert results[0]["priority"] == 1
    assert results[1]["priority"] == 3
    assert results[2]["priority"] is None
    assert results[3]["priority"] is None


@pytest.mark.asyncio
async def test_order_by_numeric_desc():
    """Test ordering numeric fields in descending order."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().order_by("-age").build()
    
    items = [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 35},
        {"name": "Charlie", "age": 20},
    ]
    
    results = await backend.search(query, items)
    assert results[0]["age"] == 35
    assert results[1]["age"] == 25
    assert results[2]["age"] == 20


@pytest.mark.asyncio
async def test_full_pagination_with_ordering():
    """Test limit, offset, and order_by working together."""
    backend = MemoryBackend()
    query = (
        SearchQueryBuilder()
        .add_condition("status", "=", "active")
        .order_by("-priority")
        .limit(2)
        .offset(1)
        .build()
    )
    
    items = [
        {"id": 1, "status": "active", "priority": 1},
        {"id": 2, "status": "active", "priority": 5},
        {"id": 3, "status": "active", "priority": 3},
        {"id": 4, "status": "inactive", "priority": 10},
        {"id": 5, "status": "active", "priority": 2},
    ]
    
    results = await backend.search(query, items)
    # After filtering: 1,2,3,5 (active only)
    # After ordering by -priority: 2(5), 3(3), 5(2), 1(1)
    # After offset(1) and limit(2): 3(3), 5(2)
    assert len(results) == 2
    assert results[0]["id"] == 3
    assert results[1]["id"] == 5


@pytest.mark.asyncio
async def test_limit_larger_than_results():
    """Test limit larger than available results."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().limit(10).build()
    
    items = [{"id": 1}, {"id": 2}]
    
    results = await backend.search(query, items)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_offset_larger_than_results():
    """Test offset larger than available results."""
    backend = MemoryBackend()
    query = SearchQueryBuilder().offset(10).build()
    
    items = [{"id": 1}, {"id": 2}]
    
    results = await backend.search(query, items)
    assert len(results) == 0
