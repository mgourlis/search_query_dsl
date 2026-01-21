"""
Unified API for Search Query DSL.

Provides a single search() function that auto-detects the backend from the source type.
"""

from typing import Any, Dict, Iterable, List, Optional, Type, Union, overload, TYPE_CHECKING

from search_query_dsl.core.models import SearchQuery


if TYPE_CHECKING:
    from sqlalchemy.sql import Select
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import DeclarativeBase


# Type overloads for IDE support
@overload
async def search(
    query: Union[SearchQuery, Dict, None],
    source: Iterable[Any],
) -> List[Any]:
    """Search in-memory collection."""
    ...


@overload
async def search(
    query: Union[SearchQuery, Dict, None],
    source: "AsyncSession",
    model: Type["DeclarativeBase"],
    stmt: Optional["Select"] = None,
    **options: Any,
) -> List[Any]:
    """Search SQLAlchemy database."""
    ...


async def search(
    query: Union[SearchQuery, Dict, None],
    source: Any,
    model: Optional[Type["DeclarativeBase"]] = None,
    stmt: "Select" = None,
    **options: Any,
) -> List[Any]:
    """
    Universal search function. Automatically selects backend based on source type.
    
    Args:
        query: SearchQuery object, dict, or None
        source: Data source:
            - List/Iterable: Uses Memory backend
            - AsyncSession: Uses SQLAlchemy backend
        model: SQLAlchemy model class (required for SQLAlchemy)
        stmt: Optional base SQLAlchemy statement
        **options: Backend-specific options (e.g., hooks for SQLAlchemy)
    
    Returns:
        List of matching results
    
    Examples:
        # Memory backend - filter a list
        items = [{"status": "active"}, {"status": "inactive"}]
        results = await search(query, items)
        
        # SQLAlchemy backend - query database
        async with async_session() as session:
            users = await search(query, session, User)
        
        # SQLAlchemy with custom statement
        stmt = select(User).join(Profile)
        users = await search(query, session, User, stmt=stmt)
        
        # SQLAlchemy with hooks
        users = await search(query, session, User, hooks=[my_join_hook])
    """
    # Convert dict to SearchQuery
    if isinstance(query, dict):
        query = SearchQuery.from_dict(query)
    
    # Detect backend by source type
    if _is_async_session(source):
        from search_query_dsl.backends.sqlalchemy import SQLAlchemyBackend
        
        if model is None:
            raise ValueError("model is required for SQLAlchemy backend")
        
        return await SQLAlchemyBackend(**options).search(query, source, model, stmt)
    
    else:
        # Default: treat as iterable (memory backend)
        from search_query_dsl.backends.memory import MemoryBackend
        
        return await MemoryBackend(**options).search(query, source)


def _is_async_session(obj: Any) -> bool:
    """Check if obj is a SQLAlchemy AsyncSession."""
    try:
        from sqlalchemy.ext.asyncio import AsyncSession
        return isinstance(obj, AsyncSession)
    except ImportError:
        return False

def get_supported_operators(backend: str = "all") -> dict:
    """
    Get list of supported operators for specified backend(s).
    
    Args:
        backend: Which backend to query - "memory", "sqlalchemy", or "all" (default)
    
    Returns:
        Dictionary mapping backend name to list of operator names
        
    Examples:
        >>> get_supported_operators("memory")
        {"memory": ["=", "!=", ">", "<", "in", "like", ...]}
        
        >>> get_supported_operators("all")
        {
            "memory": ["=", "!=", ...],
            "sqlalchemy": ["=", "!=", ...]
        }
    """
    result = {}
    
    if backend in ("memory", "all"):
        from search_query_dsl.backends.memory.operators import REGISTRY as MEMORY_REGISTRY
        result["memory"] = sorted(MEMORY_REGISTRY.keys())
    
    if backend in ("sqlalchemy", "all"):
        from search_query_dsl.backends.sqlalchemy.operators import REGISTRY as SQL_REGISTRY
        result["sqlalchemy"] = sorted(SQL_REGISTRY.keys())
    
    if backend not in ("memory", "sqlalchemy", "all"):
        raise ValueError(f"Invalid backend: {backend}. Must be 'memory', 'sqlalchemy', or 'all'")
    
    return result


__all__ = ["search", "get_supported_operators"]





