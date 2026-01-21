"""
FastAPI/Pydantic integration for SearchQuery.

Provides Pydantic models for API request/response validation.
"""

try:
    from pydantic import BaseModel, Field
    from typing import Any, List, Optional, Union
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    
    class SearchConditionSchema(BaseModel):
        """Pydantic schema for SearchCondition."""
        field: str = Field(..., description="Field name or dot-notation path")
        operator: str = Field(..., description="Comparison operator (=, !=, like, etc.)")
        value: Optional[Any] = Field(None, description="Value to compare")
        value_type: Optional[str] = Field(None, description="Type hint for casting")
        
        class Config:
            json_schema_extra = {
                "example": {
                    "field": "name",
                    "operator": "like",
                    "value": "%test%"
                }
            }
    
    
    class SearchQueryGroupSchema(BaseModel):
        """Pydantic schema for SearchQueryGroup."""
        group_operator: str = Field("and", description="Logical operator (and, or, not)")
        conditions: List[Union["SearchConditionSchema", "SearchQueryGroupSchema"]] = Field(
            ..., 
            description="List of conditions or nested groups"
        )
        
        class Config:
            json_schema_extra = {
                "example": {
                    "group_operator": "and",
                    "conditions": [
                        {"field": "status", "operator": "=", "value": "active"},
                        {"field": "priority", "operator": ">", "value": 5}
                    ]
                }
            }
    
    
    class SearchQuerySchema(BaseModel):
        """Pydantic schema for SearchQuery."""
        groups: List[SearchQueryGroupSchema] = Field(
            default_factory=list,
            description="List of condition groups (combined with AND)"
        )
        limit: Optional[int] = Field(None, ge=1, le=10000, description="Max results")
        offset: Optional[int] = Field(None, ge=0, description="Skip N results")
        order_by: Optional[List[str]] = Field(None, description="Ordering (prefix - for DESC)")
        
        class Config:
            json_schema_extra = {
                "example": {
                    "groups": [{
                        "group_operator": "and",
                        "conditions": [
                            {"field": "name", "operator": "like", "value": "%test%"}
                        ]
                    }],
                    "limit": 100
                }
            }
    
    # Enable forward references for recursive types
    SearchQueryGroupSchema.model_rebuild()
    
    
    def to_search_query(schema: SearchQuerySchema):
        """Convert Pydantic schema to SearchQuery."""
        from search_query_dsl.core.models import SearchQuery
        
        # Use simple dict conversion
        # model_dump() is v2, dict() is v1. We try both for compatibility.
        if hasattr(schema, "model_dump"):
            data = schema.model_dump()
        else:
            data = schema.dict()
            
        return SearchQuery.from_dict(data)
    
    
    __all__ = [
        "SearchConditionSchema",
        "SearchQueryGroupSchema", 
        "SearchQuerySchema",
        "to_search_query",
    ]

else:
    # Pydantic not available
    __all__ = []
