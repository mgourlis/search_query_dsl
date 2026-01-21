"""
SQLAlchemy backend for database querying.

Supports statement modification and direct execution via AsyncSession.
"""

from typing import Any, AsyncGenerator, Dict, List, Optional, Type, Callable, Union, TYPE_CHECKING

from sqlalchemy import and_, or_, not_, select
from sqlalchemy.orm import RelationshipProperty, aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.inspection import inspect

from search_query_dsl.core.models import SearchQuery, SearchCondition, SearchQueryGroup
from search_query_dsl.core.exceptions import OperatorNotFoundError, FieldValidationError, RelationshipTraversalError, FieldNotQueryableError
from search_query_dsl.core.validator import validate_search_query
from search_query_dsl.backends.sqlalchemy.operators import REGISTRY
from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator
from search_query_dsl.backends.sqlalchemy.context import SQLAlchemyResolutionContext
from search_query_dsl.core.hooks import HookResult

if TYPE_CHECKING:
    from sqlalchemy.sql import Select
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import DeclarativeBase


# Type alias for join hooks
JoinHook = Callable[[SQLAlchemyResolutionContext], HookResult]


class SQLAlchemyBackend:
    """
    SQLAlchemy search backend with query execution support.
    
    Provides both statement modification (apply) and direct execution (search).
    
    Example:
        backend = SQLAlchemyBackend()
        
        # Apply to statement (for further modification)
        stmt = await backend.apply(query, stmt, User)
        
        # Or execute directly
        results = await backend.search(query, session, User)
    """
    
    def __init__(
        self,
        registry: Optional[Dict[str, SQLAlchemyOperator]] = None,
        hooks: Optional[List[JoinHook]] = None,
    ):
        """
        Initialize SQLAlchemy backend.
        
        Args:
            registry: Custom operator registry. Defaults to built-in REGISTRY.
            hooks: List of join hooks for custom field resolution.
        """
        self.registry = registry or REGISTRY
        self.hooks = hooks or []
    
    async def search(
        self,
        query: Optional[SearchQuery],
        session: "AsyncSession",
        model: Type["DeclarativeBase"],
        stmt: Optional["Select"] = None,
    ) -> List[Any]:
        """
        Apply query and execute against session.
        
        Args:
            query: SearchQuery to apply
            session: SQLAlchemy AsyncSession
            model: SQLAlchemy model class
            stmt: Optional base statement (defaults to select(model))
        
        Returns:
            List of matching model instances
        """
        if stmt is None:
            stmt = select(model)
        
        stmt = await self.apply(query, stmt, model)
        result = await session.execute(stmt)
        return list(result.scalars().all())
    
    async def stream(
        self,
        query: Optional[SearchQuery],
        session: "AsyncSession",
        model: Type["DeclarativeBase"],
        stmt: Optional["Select"] = None,
    ) -> AsyncGenerator[Any, None]:
        """
        Apply query and stream results from session.
        
        Uses server-side streaming to avoid loading all results into memory.
        Ideal for processing large result sets.
        
        Args:
            query: SearchQuery to apply
            session: SQLAlchemy AsyncSession
            model: SQLAlchemy model class
            stmt: Optional base statement (defaults to select(model))
        
        Yields:
            Individual matching model instances
            
        Example:
            async for user in backend.stream(query, session, User):
                process(user)
        """
        if stmt is None:
            stmt = select(model)
        
        stmt = await self.apply(query, stmt, model)
        result = await session.stream_scalars(stmt)
        async for row in result:
            yield row
    
    async def apply(
        self,
        query: Optional[SearchQuery],
        stmt: "Select",
        model: Type["DeclarativeBase"],
    ) -> "Select":
        """
        Apply query to statement without execution.
        
        Use this when you need the statement for further modification.
        
        Args:
            query: SearchQuery to apply
            stmt: SQLAlchemy Select statement
            model: SQLAlchemy model class
        
        Returns:
            Modified statement with WHERE clauses applied
        """ 
        # Validate query before processing
        validate_search_query(query, operators=set(self.registry.keys()))
        
        if not query.groups:
            return stmt
        
        alias_cache: Dict[str, Any] = {}
        
        for group in query.groups:
            clause, stmt = await self._build_group(group, model, alias_cache, stmt)
            if clause is not None:
                stmt = stmt.where(clause)
        
        if query.limit is not None:
            stmt = stmt.limit(query.limit)
        if query.offset is not None:
            stmt = stmt.offset(query.offset)
        
        # Apply ordering
        if query.order_by:
            for field_spec in query.order_by:
                if field_spec.startswith('-'):
                    # Descending order
                    field_name = field_spec[1:]
                    column = getattr(model, field_name)
                    stmt = stmt.order_by(column.desc())
                else:
                    # Ascending order
                    column = getattr(model, field_spec)
                    stmt = stmt.order_by(column.asc())
        
        return stmt
    
    async def _build_group(
        self,
        group: SearchQueryGroup,
        model: Type["DeclarativeBase"],
        alias_cache: Dict[str, Any],
        stmt: "Select",
    ) -> tuple:
        """Build SQLAlchemy clause from group."""
        clauses = []
        
        for condition in group.conditions:
            if isinstance(condition, SearchQueryGroup):
                clause, stmt = await self._build_group(condition, model, alias_cache, stmt)
            else:
                clause, stmt = await self._build_condition(condition, model, alias_cache, stmt)
            
            if clause is not None:
                clauses.append(clause)
        
        if not clauses:
            return None, stmt
        
        if group.group_operator == "and":
            result = and_(*clauses) if len(clauses) > 1 else clauses[0]
        elif group.group_operator == "or":
            result = or_(*clauses) if len(clauses) > 1 else clauses[0]
        elif group.group_operator == "not":
            inner = and_(*clauses) if len(clauses) > 1 else clauses[0]
            result = not_(inner)
        else:
            result = and_(*clauses) if len(clauses) > 1 else clauses[0]
        
        return result, stmt
    
    async def _build_condition(
        self,
        condition: SearchCondition,
        model: Type["DeclarativeBase"],
        alias_cache: Dict[str, Any],
        stmt: "Select",
    ) -> tuple:
        """Build SQLAlchemy clause from condition."""
        # Get operator to check capabilities
        operator = self.registry.get(condition.operator)
        if operator is None:
            raise OperatorNotFoundError(condition.operator, list(self.registry.keys()))

        # Resolve field to column (may involve JOINs)
        column, stmt = await self._resolve_field(
            condition.field,
            model,
            alias_cache,
            stmt,
            condition.value,
            condition.value_type,
            allow_relationship=getattr(operator, "supports_relationship", False),
        )
        
        clause = operator.apply(
            column, 
            condition.value, 
            condition.value_type,
            model=model,
            stmt=stmt
        )
        return clause, stmt
    
    async def _resolve_field(
        self,
        field_path: str,
        model: Type["DeclarativeBase"],
        alias_cache: Dict[str, Any],
        stmt: "Select",
        value: Any,
        value_type: Optional[str] = None,
        allow_relationship: bool = False,
    ) -> tuple:
        """
        Resolve a field path with robust join logic and aliasing.
        """
        from search_query_dsl.backends.sqlalchemy.utils import extract_tables_from_statement
        
        parts = field_path.split('.')
        current_model = model
        base_model = model
        path_so_far = []
        
        for idx, part in enumerate(parts):
            path_so_far.append(part)
            current_path = '.'.join(path_so_far)
            remaining_attrs = parts[idx+1:]
            
            # 1. Check hooks for current attribute
            hook_ctx = SQLAlchemyResolutionContext.create(
                field_path=field_path,
                value=value,
                stmt=stmt,
                model=model,
                value_type=value_type,
                alias_cache=alias_cache,
            )
            # Update context with traversal state
            hook_ctx.current_model = current_model
            hook_ctx.current_attr = part
            hook_ctx.remaining_attrs = remaining_attrs
            
            hook_handled = False
            for hook in self.hooks:
                result = hook(hook_ctx)
                if inspect(result).is_awaitable:
                    result = await result
                
                if result and result.handled:
                    stmt = result.new_statement if result.new_statement is not None else stmt
                    if result.resolved_field is not None:
                        # Hook resolved the final field (or intermediate step)
                        # If it returned a resolved_field, we assume matches the *rest* of the path
                        # commonly hooks resolve the final leaf or a specific relationship step
                        if not remaining_attrs:
                             return result.resolved_field, stmt
                        
                        # If hook returned a new model but we have remaining attrs, continue traversal from there
                        if result.new_model:
                             current_model = result.new_model
                             hook_handled = True
                             break
                        else:
                             # Hook resolved field but there are remaining attrs?
                             # This assumes the hook handled the rest of the chain or returned a complex object
                             return result.resolved_field, stmt
                    
                    if result.new_model:
                        current_model = result.new_model
                        hook_handled = True
                        break
            
            if hook_handled:
                continue

            # 2. Handle final attribute (Leaf Node)
            if idx == len(parts) - 1:
                if hasattr(current_model, part):
                    attr = getattr(current_model, part)
                    # Ensure it's a valid SQL expression/attribute
                    # We accept InstrumentedAttribute (standard columns/relationships)
                    # We could also check for hybrid_property if needed, but usually they resolve to SQL expressions usage
                    if isinstance(attr, InstrumentedAttribute):
                        return attr, stmt
                    
                    # If not InstrumentedAttribute, it might be a property or method not suitable for SQL
                    # But hybrid_properties might be useful. 
                    # For now, strict check matches user suggestion.
                    # Fallback: check if it's in mapper columns/descriptors to be safe?
                    # The user specifically asked about InstrumentedAttribute.
                    
                    from sqlalchemy.orm import RelationshipProperty
                    
                    # Let's allow it if it is InstrumentedAttribute OR if it's a hybrid property (check later if needed)
                    # For now, if the user requested InstrumentedAttribute logic:
                    if isinstance(attr, InstrumentedAttribute):
                        # If it is a relationship, check if allowed
                        if hasattr(attr, "property") and isinstance(attr.property, RelationshipProperty):
                            if not allow_relationship:
                                mapper = inspect(current_model)
                                available = [c.key for c in mapper.columns]
                                raise FieldNotQueryableError(part, current_model.__name__, available, field_path)
                                
                        return attr, stmt

                    # If valid column but not InstrumentedAttribute (e.g. synonym?), inspect would show it.
                    # Let's check mapper.
                    mapper = inspect(current_model)
                    if part in mapper.all_orm_descriptors:
                         return attr, stmt
                         
                    # If we are here, it's likely a non-SQL attribute (like a method)
                    # raise error
                    model_name = current_model.__name__
                    available = [c.key for c in mapper.columns]
                    raise FieldNotQueryableError(part, model_name, available, field_path)

                else:
                     # Error handling for missing field
                     model_name = current_model.__name__ if hasattr(current_model, '__name__') else str(type(current_model))
                     try:
                         mapper = inspect(current_model)
                         available = [c.key for c in mapper.columns] + list(mapper.relationships.keys())
                     except Exception:
                         available = []
                     raise FieldValidationError(part, model_name, available, field_path)

            # 3. Handle Relationship Traversal
            mapper = inspect(current_model)
            if part in mapper.relationships:
                rel = mapper.relationships[part]
                related_model = rel.mapper.class_
                
                # Check alias cache
                if current_path in alias_cache:
                    current_model = alias_cache[current_path]
                else:
                    # Robust aliasing logic
                    tables = extract_tables_from_statement(stmt)
                    table_names = [t.name for t in tables]
                    
                    is_self_referential = related_model.__table__.name == current_model.__table__.name
                    target_table_already_joined = related_model.__table__.name in table_names
                    
                    # Aliasing strategy:
                    # - Always alias if self-referential
                    # - Always alias if table already in query (to avoid ambiguity)
                    # - Otherwise join without alias (first occurrence)
                    if is_self_referential or target_table_already_joined:
                        aliased_model = aliased(related_model)
                        stmt = stmt.join(aliased_model, getattr(current_model, part))
                        current_model = aliased_model
                    else:
                        stmt = stmt.join(getattr(current_model, part))
                        current_model = related_model
                    
                    alias_cache[current_path] = current_model
            else:
                 # Attribute is not a relationship and not the last part -> Error
                 # (Unless handled by a hook, which we checked)
                 model_name = current_model.__name__
                 raise RelationshipTraversalError(part, model_name, field_path)
        
        # Should be unreachable if logic is correct
        return getattr(current_model, parts[-1]), stmt
