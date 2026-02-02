"""
SQLAlchemy-specific resolution context for field resolution hooks.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type, TYPE_CHECKING

from search_query_dsl.core.hooks import ResolutionContext

if TYPE_CHECKING:
    from sqlalchemy.sql import Select
    from sqlalchemy.orm import DeclarativeBase


@dataclass
class SQLAlchemyResolutionContext(ResolutionContext):
    """
    SQLAlchemy-specific resolution context.
    
    Extends base context with statement, model, and JOIN capabilities.
    Hooks can check `isinstance(ctx, SQLAlchemyResolutionContext)` 
    to access these features.
    
    Attributes:
        stmt: Current SQLAlchemy Select statement
        model: Root SQLAlchemy model class
        current_model: Model being traversed (follows relationships)
        alias_cache: Cache of relationship -> alias mappings to avoid duplicate joins
    """
    stmt: Optional["Select"] = None
    model: Optional[Type["DeclarativeBase"]] = None
    current_model: Optional[Type] = None
    alias_cache: Dict[str, Any] = field(default_factory=dict)
    
    def get_column(self, name: str) -> Any:
        """
        Get a column from the current model.
        
        Args:
            name: Column name
        
        Returns:
            SQLAlchemy column attribute
        
        Raises:
            AttributeError: If column doesn't exist on current model
        """
        if self.current_model is None:
            raise ValueError("current_model is not set")
        return getattr(self.current_model, name)
    
    @classmethod
    def create(
        cls,
        field_path: str,
        value: Any,
        stmt: "Select",
        model: Type["DeclarativeBase"],
        value_type: Optional[str] = None,
        alias_cache: Optional[Dict[str, Any]] = None,
    ) -> "SQLAlchemyResolutionContext":
        """
        Create SQLAlchemy resolution context from field path.
        
        Args:
            field_path: Dot-notation field path
            value: Condition value
            stmt: Current SQLAlchemy statement
            model: Root model class
            value_type: Optional type hint
            alias_cache: Optional existing alias cache
        """
        parts = field_path.split('.')
        return cls(
            field_path=field_path,
            parts=parts,
            current_part=parts[0] if parts else "",
            current_index=0,
            value=value,
            value_type=value_type,
            stmt=stmt,
            model=model,
            current_model=model,
            alias_cache=alias_cache or {},
        )
