"""
Django REST Framework integration for Search Query DSL.

Provides serializers and view mixins for easy integration with DRF.
"""

from typing import Optional, List, Dict, Any

try:
    from rest_framework import serializers
    from rest_framework.request import Request
    from rest_framework.response import Response
except ImportError:
    raise ImportError(
        "Django REST Framework is required for django contrib. "
        "Install with: pip install djangorestframework"
    )


class SearchConditionSerializer(serializers.Serializer):
    """Serializer for a single search condition."""
    field = serializers.CharField(required=True)
    operator = serializers.CharField(required=True)
    value = serializers.JSONField(required=False, allow_null=True)


class SearchQueryGroupSerializer(serializers.Serializer):
    """Serializer for a search query group (supports nesting)."""
    group_operator = serializers.ChoiceField(
        choices=["and", "or", "not"],
        default="and",
        required=False
    )
    conditions = serializers.ListField(
        child=serializers.DictField(),  # Can be condition or nested group
        required=False,
        default=list
    )


class SearchQuerySerializer(serializers.Serializer):
    """
    Django REST Framework serializer for SearchQuery.
    
    Usage in DRF views:
        ```python
        from search_query_dsl.contrib.django import SearchQuerySerializer
        from search_query_dsl import search, SearchQuery
        
        class MyViewSet(viewsets.ModelViewSet):
            def list(self, request):
                serializer = SearchQuerySerializer(data=request.data)
                serializer.is_valid(raise_exception=True)
                
                query = SearchQuery.from_dict(serializer.validated_data)
                results = await search(query, session, model=MyModel)
                return Response(results)
        ```
    """
    groups = SearchQueryGroupSerializer(many=True, required=False, default=list)
    limit = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=1,
        help_text="Maximum number of results to return"
    )
    offset = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text="Number of results to skip"
    )
    order_by = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_null=True,
        help_text="List of fields to order by (prefix with - for DESC)"
    )

    def validate_groups(self, value):
        """Recursively validate nested groups and conditions."""
        def validate_item(item):
            if "field" in item:
                # It's a condition
                condition_serializer = SearchConditionSerializer(data=item)
                condition_serializer.is_valid(raise_exception=True)
                return condition_serializer.validated_data
            else:
                # It's a nested group
                group_serializer = SearchQueryGroupSerializer(data=item)
                group_serializer.is_valid(raise_exception=True)
                validated = group_serializer.validated_data
                validated["conditions"] = [
                    validate_item(cond) for cond in validated.get("conditions", [])
                ]
                return validated
        
        return [validate_item(group) for group in value]


class SearchQueryMixin:
    """
    Mixin for Django REST Framework views to add search query support.
    
    Usage:
        ```python
        from rest_framework import viewsets
        from search_query_dsl.contrib.django import SearchQueryMixin
        
        class MyViewSet(SearchQueryMixin, viewsets.ModelViewSet):
            search_model = MyModel  # Set the model to search against
            
            async def list(self, request):
                # get_search_query() handles parsing and validation
                query = self.get_search_query(request)
                results = await self.execute_search(query)
                return Response(results)
        ```
    """
    search_model = None
    search_serializer_class = SearchQuerySerializer

    def get_search_query(self, request: Request):
        """
        Parse and validate search query from request data.
        
        Returns:
            SearchQuery instance
        """
        from search_query_dsl import SearchQuery
        
        serializer = self.search_serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        return SearchQuery.from_dict(serializer.validated_data)

    async def execute_search(self, query, session=None, model=None):
        """
        Execute search query against the database.
        
        Args:
            query: SearchQuery instance
            session: Optional AsyncSession (if None, must be provided by subclass)
            model: Optional SQLAlchemy model (defaults to self.search_model)
        
        Returns:
            List of matching results
        """
        from search_query_dsl import search
        
        if model is None:
            model = self.search_model
        
        if model is None:
            raise ValueError("search_model must be set or model must be provided")
        
        if session is None:
            session = await self.get_db_session()
        
        return await search(query, session, model=model)

    async def get_db_session(self):
        """
        Get database session. Override this in your view.
        
        Example:
            ```python
            async def get_db_session(self):
                from myapp.database import async_session
                async with async_session() as session:
                    return session
            ```
        """
        raise NotImplementedError(
            "get_db_session() must be implemented to use execute_search() "
            "without explicitly passing session"
        )
