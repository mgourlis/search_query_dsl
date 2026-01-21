"""
Utilities for SQLAlchemy backend.
"""

from typing import List, Set
from sqlalchemy.sql import Select
from sqlalchemy.schema import Table
from sqlalchemy.sql.selectable import Join


def extract_tables_from_statement(stmt: Select) -> List[Table]:
    """
    Extract all tables present in the FROM clause of a statement (including joins).
    
    This is useful for hooks to check if a table is already joined.
    """
    tables: Set[Table] = set()
    
    # Visit all elements in the FROM clause
    for from_obj in stmt.froms:
        _extract_tables_recursive(from_obj, tables)
            
    return list(tables)


def _extract_tables_recursive(from_obj, tables: Set[Table]):
    """Recursively extract tables from a FROM object (Table or Join)."""
    if isinstance(from_obj, Join):
        _extract_tables_recursive(from_obj.left, tables)
        _extract_tables_recursive(from_obj.right, tables)
    elif hasattr(from_obj, 'element'): # AliasedClass
        if hasattr(from_obj.element, '__table__'):
             tables.add(from_obj.element.__table__)
        elif hasattr(from_obj, '__table__'):
             tables.add(from_obj.__table__)
    elif hasattr(from_obj, '__table__'): # Declarative Model
        tables.add(from_obj.__table__)
    elif isinstance(from_obj, Table):
        tables.add(from_obj)
    # Handle other selectables if necessary
