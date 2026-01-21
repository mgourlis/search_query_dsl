"""
Full-text search operators for SQLAlchemy (PostgreSQL).
"""

from typing import Any, Optional, TYPE_CHECKING

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute


class FtsOperator(SQLAlchemyOperator):
    """Full-text search using PostgreSQL tsvector."""
    name = "fts"
    
    def apply(self, column: "InstrumentedAttribute", value: Any, value_type: Optional[str] = None, **kwargs) -> Any:
        """
        Perform full-text search.
        
        Assumes column is a tsvector or will be converted via to_tsvector.
        Value is the search query string.
        """
        from sqlalchemy import func, cast, Text
        
        # Check if column is already tsvector, otherwise convert
        # This is a simplified approach - in production you'd want tsvector columns
        ts_column = func.to_tsvector('english', cast(column, Text))
        ts_query = func.to_tsquery('english', value)
        
        return ts_column.op("@@")(ts_query)


class FtsPhraseOperator(SQLAlchemyOperator):
    """Full-text phrase search using PostgreSQL."""
    name = "fts_phrase"
    
    def apply(self, column: "InstrumentedAttribute", value: Any, value_type: Optional[str] = None, **kwargs) -> Any:
        """
        Perform full-text phrase search.
        
        Uses phraseto_tsquery for exact phrase matching.
        """
        from sqlalchemy import func, cast, Text
        
        ts_column = func.to_tsvector('english', cast(column, Text))
        ts_query = func.phraseto_tsquery('english', value)
        
        return ts_column.op("@@")(ts_query)

