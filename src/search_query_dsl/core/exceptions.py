"""
Exception classes for Search Query DSL.

Includes FieldValidationError with fuzzy-matched suggestions.
"""

from difflib import get_close_matches
from typing import Any, Dict, List, Optional


class SearchQueryError(Exception):
    """Base exception for all Search Query DSL errors."""
    pass


class ValidationError(SearchQueryError):
    """Query structure validation failed."""
    
    def __init__(self, message: str, path: Optional[str] = None):
        self.message = message
        self.path = path  # e.g., "groups[0].conditions[2]"
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "VALIDATION_ERROR",
            "message": self.message,
            "path": self.path,
        }


class OperatorNotFoundError(SearchQueryError):
    """Unknown operator specified."""
    
    def __init__(self, operator: str, valid_operators: List[str]):
        self.operator = operator
        self.valid_operators = valid_operators
        self.suggestions = get_close_matches(operator, valid_operators, n=3, cutoff=0.6)
        
        message = f"Unknown operator: '{operator}'."
        if self.suggestions:
            message += f" Did you mean: {', '.join(self.suggestions)}?"
        message += f" Valid operators: {', '.join(sorted(valid_operators)[:10])}..."
        
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "OPERATOR_NOT_FOUND",
            "operator": self.operator,
            "suggestions": self.suggestions,
            "valid_operators": sorted(self.valid_operators),
        }


class FieldValidationError(SearchQueryError):
    """
    Invalid field path with helpful suggestions.
    
    Uses fuzzy matching to suggest similar valid field names.
    
    Example error message:
        Invalid field 'elment_type' on model 'ElementModel'.
        Did you mean one of these?
          • element_type
          • element_types
        
        Available fields: id, name, label, element_type, ...
    """
    
    def __init__(
        self,
        invalid_field: str,
        model_name: str,
        available_fields: List[str],
        full_path: Optional[str] = None,
        cutoff: float = 0.6,
    ):
        self.invalid_field = invalid_field
        self.model_name = model_name
        self.available_fields = available_fields
        self.full_path = full_path or invalid_field
        
        # Find similar field names
        self.suggestions = get_close_matches(
            invalid_field.lower(),
            [f.lower() for f in available_fields],
            n=3,
            cutoff=cutoff,
        )
        # Map back to original case
        self.suggestions = [
            f for f in available_fields
            if f.lower() in self.suggestions
        ]
        
        message = self._build_message()
        super().__init__(message)
    
    def _build_message(self) -> str:
        lines = [
            f"Invalid field '{self.invalid_field}' on model '{self.model_name}'.",
        ]
        
        if self.suggestions:
            lines.append("Did you mean one of these?")
            for suggestion in self.suggestions[:3]:
                lines.append(f"  • {suggestion}")
        
        lines.append("")
        fields_preview = ", ".join(sorted(self.available_fields)[:15])
        lines.append(f"Available fields: {fields_preview}")
        
        if len(self.available_fields) > 15:
            lines.append(f"  ... and {len(self.available_fields) - 15} more")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """JSON-serializable representation for API responses."""
        return {
            "error": "INVALID_FIELD",
            "invalid_field": self.invalid_field,
            "full_path": self.full_path,
            "model": self.model_name,
            "suggestions": self.suggestions[:3],
            "available_fields": sorted(self.available_fields),
        }


class RelationshipTraversalError(ValidationError):
    """
    Error when trying to traverse a field that is not a relationship.
    
    Happens when a query path like "name.something" is used, but "name"
    is a column (e.g. String), not a relationship to another model.
    """
    
    def __init__(
        self,
        field: str,
        model_name: str,
        full_path: Optional[str] = None,
    ):
        self.field = field
        self.model_name = model_name
        
        message = (
            f"Cannot traverse field '{field}' on model '{model_name}' "
            "because it is not a relationship."
        )
        super().__init__(message, path=full_path)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "RELATIONSHIP_TRAVERSAL_ERROR",
            "message": self.message,
            "field": self.field,
            "model": self.model_name,
            "path": self.path,
        }


class FieldNotQueryableError(ValidationError):
    """
    Error when trying to query a field that exists but is not a valid SQL column/attribute.
    
    Happens when trying to filter by a method, property (non-hybrid), or other 
    non-mapped attribute on a SQLAlchemy model.
    """
    
    def __init__(
        self,
        field: str,
        model_name: str,
        available_fields: List[str],
        full_path: Optional[str] = None,
    ):
        self.field = field
        self.model_name = model_name
        self.available_fields = available_fields
        
        message = (
            f"Field '{field}' on model '{model_name}' cannot be used in a query "
            "because it is not a mapped SQL column or relationship.\n"
            f"Available queryable fields: {', '.join(sorted(available_fields)[:15])}"
        )
        if len(available_fields) > 15:
            message += f" ... ({len(available_fields) - 15} more)"
            
        super().__init__(message, path=full_path)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "FIELD_NOT_QUERYABLE",
            "message": self.message,
            "field": self.field,
            "model": self.model_name,
            "available_fields": sorted(self.available_fields),
            "path": self.path,
        }
