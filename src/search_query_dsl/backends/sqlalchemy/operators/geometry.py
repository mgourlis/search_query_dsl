"""
PostGIS geometry operators for SQLAlchemy backend.

These operators are ONLY available in the SQLAlchemy backend.

Operators: intersects, dwithin, within, contains_geom, overlaps_geom
"""

import json
from typing import Any, Optional

from sqlalchemy import func

from search_query_dsl.backends.sqlalchemy.base import SQLAlchemyOperator


def _geojson_to_str(value: Any) -> str:
    """Convert value to GeoJSON string."""
    if isinstance(value, str):
        return value
    return json.dumps(value)


class IntersectsOperator(SQLAlchemyOperator):
    """PostGIS ST_Intersects - check if geometries intersect."""
    name = "intersects"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Intersects(func.ST_Transform(column, 4326), geom)


class DWithinOperator(SQLAlchemyOperator):
    """PostGIS ST_DWithin - check if geometries are within distance."""
    name = "dwithin"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("dwithin requires [GeoJSON, distance_meters]")
        
        geom, distance = condition_value
        query_geom = func.ST_Transform(
            func.ST_SetSRID(func.ST_GeomFromGeoJSON(_geojson_to_str(geom)), 4326),
            3857
        )
        return func.ST_DWithin(func.ST_Transform(column, 3857), query_geom, distance)


class WithinOperator(SQLAlchemyOperator):
    """PostGIS ST_Within - check if first geometry is within second."""
    name = "within"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Within(func.ST_Transform(column, 4326), geom)


class ContainsGeomOperator(SQLAlchemyOperator):
    """PostGIS ST_Contains - check if first geometry contains second."""
    name = "contains_geom"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Contains(func.ST_Transform(column, 4326), geom)


class OverlapsGeomOperator(SQLAlchemyOperator):
    """PostGIS ST_Overlaps - check if geometries overlap."""
    name = "overlaps"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Overlaps(func.ST_Transform(column, 4326), geom)


class GeomEqualsOperator(SQLAlchemyOperator):
    """PostGIS ST_Equals - check if geometries are equal."""
    name = "geom_equals"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Equals(func.ST_Transform(column, 4326), geom)


class TouchesOperator(SQLAlchemyOperator):
    """PostGIS ST_Touches - check if geometries touch."""
    name = "touches"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Touches(func.ST_Transform(column, 4326), geom)


class CrossesOperator(SQLAlchemyOperator):
    """PostGIS ST_Crosses - check if geometries cross."""
    name = "crosses"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Crosses(func.ST_Transform(column, 4326), geom)


class DistanceLessThanOperator(SQLAlchemyOperator):
    """PostGIS ST_Distance < X."""
    name = "distance_lt"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 2:
            raise ValueError("distance_lt requires [GeoJSON, distance_meters]")
        
        geom_val, distance = condition_value
        geom = func.ST_Transform(
            func.ST_SetSRID(func.ST_GeomFromGeoJSON(_geojson_to_str(geom_val)), 4326),
            3857
        )
        return func.ST_Distance(func.ST_Transform(column, 3857), geom) < distance



class DisjointOperator(SQLAlchemyOperator):
    """PostGIS ST_Disjoint - check if geometries are disjoint."""
    name = "disjoint"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        geom = func.ST_SetSRID(
            func.ST_GeomFromGeoJSON(_geojson_to_str(condition_value)),
            4326
        )
        return func.ST_Disjoint(func.ST_Transform(column, 4326), geom)


class BboxIntersectsOperator(SQLAlchemyOperator):
    """Bounding box intersection operator (fast spatial index)."""
    name = "bbox_intersects"
    
    def apply(self, column, condition_value: Any, value_type: Optional[str] = None, **kwargs):
        """Check bbox intersection using [minX, minY, maxX, maxY] array."""
        if not isinstance(condition_value, (list, tuple)) or len(condition_value) != 4:
            raise ValueError("bbox_intersects requires [minX, minY, maxX, maxY]")
        
        minx, miny, maxx, maxy = condition_value
        # Create a box geometry from bounds
        bbox_geom = func.ST_MakeEnvelope(minx, miny, maxx, maxy, 4326)
        # Use &&& operator for bbox intersection (uses spatial index)
        return column.op("&&&")(bbox_geom)


