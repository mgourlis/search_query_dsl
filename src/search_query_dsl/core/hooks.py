"""
Resolution hooks for field path resolution.

Provides base context and result types for backend-specific hooks.
"""

from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar, List

T = TypeVar('T')


@dataclass
class HookResult(Generic[T]):
    """
    Result from a resolution hook.
    
    Attributes:
        value: The resolved value (column, expression, etc.)
        handled: If True, skip default resolution. If False, continue with default.
    """
    value: T
    handled: bool = True
    
    @classmethod
    def skip(cls) -> "HookResult[None]":
        """Return to let default resolution handle it."""
        return cls(value=None, handled=False)


@dataclass
class ResolutionContext:
    """
    Base context for field resolution hooks.
    
    Backends extend this with backend-specific capabilities.
    Hooks receive this context and can return HookResult to override
    the default field resolution behavior.
    
    Attributes:
        field_path: Full dot-notation path (e.g., "element_type.label")
        parts: Split path parts (e.g., ["element_type", "label"])
        current_part: The part currently being resolved
        current_index: Index of current_part in parts
        value: The condition value being compared
        value_type: Optional type hint for value casting
    """
    field_path: str
    parts: List[str]
    current_part: str
    current_index: int
    value: Any
    value_type: Optional[str]
    
    @property
    def remaining_parts(self) -> List[str]:
        """Parts after current_part."""
        return self.parts[self.current_index + 1:]
    
    @property
    def is_last_part(self) -> bool:
        """True if current_part is the final part of the path."""
        return self.current_index == len(self.parts) - 1
    
    @classmethod
    def from_field(
        cls,
        field_path: str,
        value: Any,
        value_type: Optional[str] = None,
    ) -> "ResolutionContext":
        """Create context from a field path."""
        parts = field_path.split('.')
        return cls(
            field_path=field_path,
            parts=parts,
            current_part=parts[0] if parts else "",
            current_index=0,
            value=value,
            value_type=value_type,
        )
