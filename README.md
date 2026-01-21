# Search Query DSL

A **Domain-Specific Language (DSL)** for expressing complex database queries as JSON with support for:

- ✅ **Unified API**: Single `search()` function for both Memory and SQLAlchemy backends.
- ✅ **Streaming Support**: Memory-efficient `search_stream()` for large result sets.
- ✅ **Nested Logic**: Complex boolean expressions (AND, OR, NOT).
- ✅ **Relationship Traversal**: Automatic, robust JOINs with alias handling.
- ✅ **Pagination & Ordering**: Full support for `limit`, `offset`, and multi-field `order_by`.
- ✅ **Query Validation**: Backend-aware validation ensures only supported operators are used.
- ✅ **JSONB & Geospatial**: Advanced field queries and PostGIS support.
- ✅ **Full-Text Search**: PostgreSQL tsvector and simple token-based search.
- ✅ **Async Hooks**: Custom traversal/join logic with async side-effect support.
- ✅ **Smart Resolvers**: Implicit list traversal and fuzzy matching for error suggestions.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Usage Examples](#usage-examples)
  - [Basic Queries](#basic-queries)
  - [Relationship Traversal](#relationship-traversal)
  - [Complex Nested Logic](#complex-nested-logic)
  - [Geospatial Queries](#geospatial-queries)
  - [Full-Text Search](#full-text-search)
  - [Pagination & Ordering](#pagination--ordering)
  - [Streaming Results](#streaming-results)
- [JSON Structure](#json-structure)
- [Supported Operators](#supported-operators)
- [Performance Tips](#performance-tips)
- [Integration](#integration)
- [License](#license)

## Installation

```bash
# Core only
pip install search-query-dsl

# With SQLAlchemy & PostGIS
pip install search-query-dsl[sqlalchemy,geoalchemy]

# With FastAPI support
pip install search-query-dsl[fastapi]

# Everything
pip install search-query-dsl[all]
```

## Quick Start

### Unified Search API

The `search()` function automatically detects the backend based on your source type.

```python
from search_query_dsl.api import search

# 1. Define Query (Dictionary or Object)
query = {
    "groups": [{
        "group_operator": "and",
        "conditions": [
            {"field": "status", "operator": "=", "value": "active"},
            {"field": "priority", "operator": ">", "value": 5}
        ]
    }],
    "limit": 10,
    "order_by": ["-created_at"]
}

# 2. Search In-Memory (Source is List/Iterable)
items = [{"status": "active", "priority": 10}, {"status": "inactive"}]
results = await search(query, items)

# 3. Search SQLAlchemy (Source is AsyncSession)
async with session:
    results = await search(query, session, model=User)
```

## Core Concepts

### Query Builder Pattern

Use the builder for a more Pythonic API:

```python
from search_query_dsl.core.builder import SearchQueryBuilder

query = (
    SearchQueryBuilder()
    .add_condition("status", "=", "active")
    .add_condition("priority", ">=", 5)
    .order_by("-created_at")
    .limit(20)
    .build()
)
```

### Backend Auto-Detection

The library automatically chooses the right backend:

- **MemoryBackend**: For lists, iterables, or single objects
- **SQLAlchemyBackend**: For AsyncSession instances

### Query Validation

Queries are validated before execution:

```python
from search_query_dsl.core.validator import validate_search_query

# Validation checks for:
# - Valid operator names
# - Required values for operators
# - Valid limit/offset values
# - Non-empty condition groups
validate_search_query(query, operators={"=", ">", "in"})
```

## Usage Examples

### Basic Queries

```python
# Simple equality
query = {
    "groups": [{
        "conditions": [{"field": "name", "operator": "=", "value": "Alice"}]
    }]
}

# Range query
query = {
    "groups": [{
        "conditions": [
            {"field": "age", "operator": ">=", "value": 18},
            {"field": "age", "operator": "<=", "value": 65}
        ]
    }]
}

# IN operator
query = {
    "groups": [{
        "conditions": [
            {"field": "status", "operator": "in", "value": ["active", "pending"]}
        ]
    }]
}
```

### Relationship Traversal

Automatic JOINs are handled for you in SQLAlchemy:

```python
# Query: "Find users whose profile city is 'New York'"
query = {
    "groups": [{
        "conditions": [
            {"field": "profile.address.city", "operator": "=", "value": "New York"}
        ]
    }]
}
results = await search(query, session, model=User)
```

**Features:**
- Detects self-referential relationships (e.g. `parent.name`)
- Reuses aliases if a table is already joined
- Validates leaf nodes are valid SQL columns

### Complex Nested Logic

Build complex OR/AND/NOT combinations:

```python
# (status = 'active' AND priority > 5) OR (urgent = true)
query = {
    "groups": [{
        "group_operator": "or",
        "conditions": [
            {
                "group_operator": "and",
                "conditions": [
                    {"field": "status", "operator": "=", "value": "active"},
                    {"field": "priority", "operator": ">", "value": 5}
                ]
            },
            {"field": "urgent", "operator": "=", "value": True}
        ]
    }]
}
```

### Geospatial Queries

```python
# Find points within a polygon
query = {
    "groups": [{
        "conditions": [{
            "field": "location",
            "operator": "within",
            "value": {
                "type": "Polygon",
                "coordinates": [[
                    [-122.4, 37.8],
                    [-122.4, 37.7],
                    [-122.3, 37.7],
                    [-122.3, 37.8],
                    [-122.4, 37.8]
                ]]
            }
        }]
    }]
}

# Fast bounding box query (uses spatial index)
query = {
    "groups": [{
        "conditions": [{
            "field": "location",
            "operator": "bbox_intersects",
            "value": [-122.5, 37.7, -122.3, 37.9]  # [minX, minY, maxX, maxY]
        }]
    }]
}

# Distance query
query = {
    "groups": [{
        "conditions": [{
            "field": "location",
            "operator": "dwithin",
            "value": [
                {"type": "Point", "coordinates": [-122.4, 37.8]},
                1000  # meters
            ]
        }]
    }]
}
```

### Full-Text Search

```python
# PostgreSQL full-text search (SQLAlchemy backend)
query = {
    "groups": [{
        "conditions": [{
            "field": "description",
            "operator": "fts",
            "value": "python database"
        }]
    }]
}

# Phrase search
query = {
    "groups": [{
        "conditions": [{
            "field": "content",
            "operator": "fts_phrase",
            "value": "machine learning"
        }]
    }]
}
```

### Pagination & Ordering

```python
from search_query_dsl.core.builder import SearchQueryBuilder

query = (
    SearchQueryBuilder()
    .add_condition("status", "=", "active")
    .order_by("-created_at", "name")  # DESC created, ASC name
    .limit(20)
    .offset(40)
    .build()
)
```

**Note**: Prefix field names with `-` for descending order.

### Streaming Results

For large result sets, use `search_stream()` to process results one at a time without loading everything into memory:

```python
from search_query_dsl.api import search_stream

# Stream from SQLAlchemy (uses server-side streaming)
async with async_session() as session:
    async for user in search_stream(query, session, model=User):
        await process(user)  # Process one at a time

# Stream from in-memory collection
items = [{"status": "active", "priority": 10}, {"status": "inactive"}]
async for item in search_stream(query, items):
    await process(item)
```

**Benefits:**
- **Memory Efficient**: Doesn't load all results into memory at once.
- **Server-Side Streaming**: SQLAlchemy backend uses `stream_scalars()` for true database-level streaming.
- **Same Query Format**: Uses the exact same query structure as `search()`.

### In-Memory List Traversal

The memory backend supports implicit traversal for lists:

```python
data = {
    "users": [
        {"name": "Alice", "role": "admin"},
        {"name": "Bob", "role": "user"}
    ]
}

# Query: "users.name"
# Matches if ANY user in the list has name "Alice"
query = {
    "groups": [{
        "conditions": [
            {"field": "users.name", "operator": "=", "value": "Alice"}
        ]
    }]
}
```

### Custom Logic with Async Hooks

Customize traversal for dynamic tables or polymorphic relationships:

```python
from search_query_dsl.backends.sqlalchemy import SQLAlchemyResolutionContext, HookResult

async def my_custom_hook(ctx: SQLAlchemyResolutionContext):
    if ctx.current_attr == "dynamic_field":
         # Perform async lookups (e.g. Redis/Cache)
         cached_info = await get_schema_info()
         
         # Return resolution result
         return HookResult(...)

# Pass hooks to search function
results = await search(query, session, model=MyModel, hooks=[my_custom_hook])
```

## JSON Structure

A `SearchQuery` is composed of nested `groups` of `conditions`.

```json
{
  "groups": [
    {
      "group_operator": "or",
      "conditions": [
        {
          "field": "created_at",
          "operator": ">",
          "value": "2024-01-01"
        },
        {
          "group_operator": "and",
          "conditions": [
             {"field": "status", "operator": "=", "value": "pending"},
             {"field": "urgent", "operator": "=", "value": true}
          ]
        }
      ]
    }
  ],
  "limit": 10,
  "offset": 0,
  "order_by": ["-created_at"]
}
```

## Supported Operators

| Type | Operators |
|------|-----------|
| **Comparison** | `=`, `!=`, `>`, `<`, `>=`, `<=` |
| **Set** | `in`, `not_in`, `all`, `between`, `not_between` |
| **String** | `like`, `not_like`, `ilike`, `contains`, `icontains`, `startswith`, `istartswith`, `endswith`, `iendswith`, `regex`, `iregex` |
| **Null/Empty** | `is_null`, `is_not_null`, `is_empty`, `is_not_empty` |
| **JSONB** | `jsonb_contains`, `jsonb_contained_by`, `jsonb_has_key`, `jsonb_has_any_keys`, `jsonb_has_all_keys`, `jsonb_path_exists` |
| **Geometry** | `intersects`, `within`, `contains_geom`, `touches`, `crosses`, `overlaps`, `disjoint`, `geom_equals`, `distance_lt`, `dwithin`, `bbox_intersects` |
| **Full-Text Search** | `fts`, `fts_phrase` |

## Performance Tips

### SQLAlchemy Backend

1. **Use Spatial Indexes**: For geometry queries, ensure your geometry columns have spatial indexes:
   ```sql
   CREATE INDEX idx_location ON places USING GIST(location);
   ```

2. **Bounding Box First**: Use `bbox_intersects` before expensive operations like `within`:
   ```python
   # Fast spatial index query
   {"field": "location", "operator": "bbox_intersects", "value": [minX, minY, maxX, maxY]}
   ```

3. **FTS Indexes**: For full-text search, create tsvector columns with indexes:
   ```sql
   ALTER TABLE documents ADD COLUMN search_vector tsvector;
   CREATE INDEX idx_search ON documents USING GIN(search_vector);
   ```

4. **Limit Early**: Apply `limit` and `offset` to reduce result set size.

5. **Index Foreign Keys**: Ensure relationship fields are indexed for efficient JOINs.

### Memory Backend

1. **Pre-filter**: Reduce dataset size before passing to `search()`.

2. **Simple Operators**: Use simpler operators (`=`, `in`) instead of complex ones (`regex`, `fts`) when possible.

3. **Avoid Deep Nesting**: Minimize nested groups for better performance.

## Integration

### FastAPI

Simplify endpoint integration with the provided helper:

```python
from fastapi import Body
from search_query_dsl.contrib.fastapi import SearchQuerySchema
from search_query_dsl import search, SearchQuery

@app.post("/search")
async def search_items(query: SearchQuerySchema = Body(...)):
    # Convert Pydantic model to SearchQuery
    search_query = SearchQuery.from_dict(query.model_dump())
    return await search(search_query, session, model=Item)
```

#### Streaming with FastAPI

Use `StreamingResponse` for memory-efficient large result sets:

```python
from fastapi import Body
from fastapi.responses import StreamingResponse
from search_query_dsl.contrib.fastapi import SearchQuerySchema
from search_query_dsl import search_stream, SearchQuery
import json

@app.post("/search/stream")
async def stream_search(query: SearchQuerySchema = Body(...)):
    search_query = SearchQuery.from_dict(query.model_dump())
    
    async def generate():
        async with async_session() as session:
            async for item in search_stream(search_query, session, model=Item):
                yield json.dumps(item.to_dict()) + "\n"
    
    return StreamingResponse(generate(), media_type="application/x-ndjson")
```

### Django

Use the DRF integration for automatic serialization and validation:

```python
from rest_framework import viewsets
from search_query_dsl.contrib.django import SearchQueryMixin, SearchQuerySerializer
from search_query_dsl import search, SearchQuery

class ItemViewSet(SearchQueryMixin, viewsets.ModelViewSet):
    search_model = Item  # Your SQLAlchemy model
    
    async def list(self, request):
        # Automatically parses and validates from request.data
        query = self.get_search_query(request)
        
        # Execute search
        async with async_session() as session:
            results = await self.execute_search(query, session=session)
            return Response({"results": results})

# Or use the serializer directly
class ManualSearchView(APIView):
    async def post(self, request):
        serializer = SearchQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        query = SearchQuery.from_dict(serializer.validated_data)
        async with async_session() as session:
            results = await search(query, session, model=Item)
            return Response({"results": results})
```

#### Streaming with Django

Use `StreamingHttpResponse` for large result sets:

```python
from django.http import StreamingHttpResponse
from search_query_dsl import search_stream, SearchQuery
import json

class StreamSearchView(APIView):
    async def post(self, request):
        serializer = SearchQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        query = SearchQuery.from_dict(serializer.validated_data)
        
        async def generate():
            async with async_session() as session:
                async for item in search_stream(query, session, model=Item):
                    yield json.dumps(item.to_dict()) + "\n"
        
        return StreamingHttpResponse(
            generate(),
            content_type="application/x-ndjson"
        )
```

## License

MIT
