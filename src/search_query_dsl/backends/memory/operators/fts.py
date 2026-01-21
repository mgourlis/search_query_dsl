"""
Full-text search operators for Memory backend (simple text-based implementation).
"""

from typing import Any, Optional
import re

from search_query_dsl.backends.memory.base import MemoryOperator


class FtsOperator(MemoryOperator):
    """Simple full-text search for memory backend."""
    name = "fts"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        """
        Perform simple text-based search.
        
        Tokenizes the search query and checks if all tokens appear in the field value.
        """
        if field_value is None:
            return False
        
        # Convert to lowercase for case-insensitive search
        text = str(field_value).lower()
        query = str(condition_value).lower()
        
        # Simple tokenization: split by whitespace and filter
        query_tokens = [token.strip() for token in re.split(r'\s+', query) if token.strip()]
        
        # Check if all tokens appear in the text
        return all(token in text for token in query_tokens)


class FtsPhraseOperator(MemoryOperator):
    """Simple phrase search for memory backend."""
    name = "fts_phrase"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        """
        Perform simple phrase search (exact substring match).
        """
        if field_value is None:
            return False
        
        # Convert to lowercase for case-insensitive search
        text = str(field_value).lower()
        phrase = str(condition_value).lower()
        
        # Check for exact phrase match
        return phrase in text
