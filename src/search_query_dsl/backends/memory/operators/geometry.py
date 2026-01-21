"""
Geometry operators for memory backend.

Uses Shapely for geometry operations. If Shapely is not installed,
these operators will raise NotImplementedError.

Operators: intersects, dwithin, within, contains_geom, overlaps_geom
"""

from typing import Any, Optional

from search_query_dsl.backends.memory.base import MemoryOperator


def _to_shapely_geom(value: Any):
    """Convert value to Shapely geometry."""
    try:
        from shapely.geometry import shape
    except ImportError:
        raise NotImplementedError(
            "Geometry operations require shapely. Install with: pip install shapely"
        )
    
    if value is None:
        return None
    
    # Already a Shapely geometry
    if hasattr(value, 'is_valid'):
        return value
    
    # GeoJSON dict
    if isinstance(value, dict):
        return shape(value)
    
    raise ValueError(f"Cannot convert {type(value)} to geometry")


class IntersectsOperator(MemoryOperator):
    """Check if geometries intersect."""
    name = "intersects"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.intersects(query_geom)


class DWithinOperator(MemoryOperator):
    """Check if geometries are within distance (in same units as coordinates)."""
    name = "dwithin"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("dwithin requires [GeoJSON, distance]")
        
        geom, distance = condition_value
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(geom)
        
        return field_geom.distance(query_geom) <= distance


class WithinOperator(MemoryOperator):
    """Check if field geometry is within query geometry."""
    name = "within"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.within(query_geom)


class ContainsGeomOperator(MemoryOperator):
    """Check if field geometry contains query geometry."""
    name = "contains_geom"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.contains(query_geom)


class OverlapsGeomOperator(MemoryOperator):
    """Check if geometries overlap."""
    name = "overlaps"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.overlaps(query_geom)


class GeomEqualsOperator(MemoryOperator):
    """Check if geometries are equal."""
    name = "geom_equals"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.equals(query_geom)


class TouchesOperator(MemoryOperator):
    """Check if geometries touch."""
    name = "touches"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.touches(query_geom)


class CrossesOperator(MemoryOperator):
    """Check if geometries cross."""
    name = "crosses"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(condition_value)
        
        return field_geom.crosses(query_geom)


class DistanceLessThanOperator(MemoryOperator):
    """Check if distance < X."""
    name = "distance_lt"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        if field_value is None:
            return False
        
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("distance_lt requires [GeoJSON, distance]")
        
        geom, distance = condition_value
        
        field_geom = _to_shapely_geom(field_value)
        query_geom = _to_shapely_geom(geom)
        
        return field_geom.distance(query_geom) < distance


class DisjointOperator(MemoryOperator):
    """Check if geometries are disjoint (Memory backend)."""
    name = "disjoint"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        """Check if geometries are disjoint using shapely."""
        try:
            from shapely import geometry as geom
            from shapely.geometry import shape
        except ImportError:
            raise ImportError("shapely is required for geometry operations in memory backend")
        
        if field_value is None:
            return False
        
        geom1 = shape(field_value) if isinstance(field_value, dict) else field_value
        geom2 = shape(condition_value) if isinstance(condition_value, dict) else condition_value
        
        return geom1.disjoint(geom2)


class BboxIntersectsOperator(MemoryOperator):
    """Check if bounding boxes intersect (Memory backend)."""
    name = "bbox_intersects"
    
    def evaluate(self, field_value: Any, condition_value: Any, value_type: Optional[str] = None) -> bool:
        """Check if bounding boxes overlap using [minX, minY, maxX, maxY] array."""
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 4:
            raise ValueError("bbox_intersects requires [minX, minY, maxX, maxY]")
        
        try:
            from shapely.geometry import shape
        except ImportError:
            raise ImportError("shapely is required for geometry operations in memory backend")
        
        if field_value is None:
            return False
        
        geom1 = shape(field_value) if isinstance(field_value, dict) else field_value
        
        # Get bounding box of geom1
        bbox1 = geom1.bounds  # (minx, miny, maxx, maxy)
        bbox2 = condition_value  # Already [minx, miny, maxx, maxy]
        
        # Check if bounding boxes intersect
        return not (bbox1[2] < bbox2[0] or  # geom1 max_x < bbox2 min_x
                   bbox1[0] > bbox2[2] or  # geom1 min_x > bbox2 max_x
                   bbox1[3] < bbox2[1] or  # geom1 max_y < bbox2 min_y
                   bbox1[1] > bbox2[3])    # geom1 min_y > bbox2 max_y


