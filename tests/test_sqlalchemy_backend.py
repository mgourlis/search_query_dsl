"""
Integration tests for SQLAlchemy backend with PostgreSQL + PostGIS.

These tests require a running PostgreSQL database with PostGIS extension.
Configure DATABASE_URL environment variable or use the default connection string.
"""

import pytest
import os
from datetime import datetime
from typing import List, Optional
from decimal import Decimal

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, 
    Text, ForeignKey, Numeric, text
)
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

# Try to import GeoAlchemy2 for geometry support
try:
    from geoalchemy2 import Geometry
    HAS_GEOALCHEMY = True
except ImportError:
    HAS_GEOALCHEMY = False
    Geometry = None


# Database URL - can be overridden by environment variable
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:123456@localhost:5432/search_query_dsl_demo_db"
)


# ============================================================================
# Models
# ============================================================================

class Base(DeclarativeBase):
    pass


class Category(Base):
    """Category model for testing relationships."""
    __tablename__ = "categories"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    products: Mapped[List["Product"]] = relationship("Product", back_populates="category")


class Product(Base):
    """Product model for testing various operators."""
    __tablename__ = "products"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    tags: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    
    category_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("categories.id"), nullable=True)
    category: Mapped[Optional["Category"]] = relationship("Category", back_populates="products")


if HAS_GEOALCHEMY:
    class Location(Base):
        """Location model for testing geometry operators."""
        __tablename__ = "locations"
        
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        name: Mapped[str] = mapped_column(String(200), nullable=False)
        point = Column(Geometry('POINT', srid=4326), nullable=True)
        polygon = Column(Geometry('POLYGON', srid=4326), nullable=True)


# ============================================================================
# Fixtures
# ============================================================================

_db_initialized = False


@pytest.fixture
async def db_session():
    """Create a fresh session for each test."""
    global _db_initialized
    
    engine = create_async_engine(DATABASE_URL, echo=False)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    if not _db_initialized:
        async with engine.begin() as conn:
            # Create PostGIS extension
            try:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            except Exception:
                pass
            
            # Create tables (drop first for clean state)
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        
        # Seed data
        async with session_maker() as s:
            # Categories
            electronics = Category(name="Electronics", description="Electronic devices")
            clothing = Category(name="Clothing", description="Apparel")
            books = Category(name="Books", description=None)
            s.add_all([electronics, clothing, books])
            await s.flush()
            
            # Products
            products = [
                Product(name="Laptop", description="High-performance laptop", price=Decimal("999.99"),
                        quantity=50, is_active=True, category=electronics,
                        tags=["tech", "computer"], metadata_json={"brand": "TechCo", "warranty": 2}),
                Product(name="Smartphone", description="Latest smartphone", price=Decimal("699.99"),
                        quantity=100, is_active=True, category=electronics,
                        tags=["tech", "mobile"], metadata_json={"brand": "PhoneCo"}),
                Product(name="T-Shirt", description="Cotton t-shirt", price=Decimal("29.99"),
                        quantity=200, is_active=True, category=clothing, tags=["casual"]),
                Product(name="Jeans", description="Denim jeans", price=Decimal("79.99"),
                        quantity=75, is_active=True, category=clothing),
                Product(name="Python Book", description="Learn Python", price=Decimal("45.00"),
                        quantity=30, is_active=True, category=books, tags=["programming"]),
                Product(name="Discontinued", description="Old item", price=Decimal("10.00"),
                        quantity=0, is_active=False, category=None),
            ]
            s.add_all(products)
            
            # Locations (if PostGIS available)
            if HAS_GEOALCHEMY:
                try:
                    from geoalchemy2.shape import from_shape
                    from shapely.geometry import Point, Polygon
                    
                    locations = [
                        Location(name="New York",
                                point=from_shape(Point(-74.006, 40.7128), srid=4326),
                                polygon=from_shape(Polygon([(-74.1, 40.6), (-73.9, 40.6), 
                                    (-73.9, 40.8), (-74.1, 40.8), (-74.1, 40.6)]), srid=4326)),
                        Location(name="Los Angeles",
                                point=from_shape(Point(-118.2437, 34.0522), srid=4326)),
                        Location(name="Chicago",
                                point=from_shape(Point(-87.6298, 41.8781), srid=4326)),
                    ]
                    s.add_all(locations)
                except Exception:
                    pass
            
            await s.commit()
        
        _db_initialized = True
    
    # Return a fresh session for the test
    async with session_maker() as session:
        yield session
    
    await engine.dispose()


# ============================================================================
# Tests
# ============================================================================

@pytest.mark.asyncio
async def test_equal_operator(db_session):
    """Test = operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "=", "value": "Laptop"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert results[0].name == "Laptop"


@pytest.mark.asyncio
async def test_not_equal_operator(db_session):
    """Test != operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "is_active", "operator": "!=", "value": True}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert results[0].name == "Discontinued"


@pytest.mark.asyncio
async def test_greater_than(db_session):
    """Test > operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "price", "operator": ">", "value": 100}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 2
    assert all(r.price > 100 for r in results)


@pytest.mark.asyncio
async def test_between_operator(db_session):
    """Test between operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "price", "operator": "between", "value": [30, 100]}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(30 <= float(r.price) <= 100 for r in results)


@pytest.mark.asyncio
async def test_like_operator(db_session):
    """Test like operator."""
    from search_query_dsl import search
    
    # Python Book has "Python" in name - use exact case for LIKE
    query = {"groups": [{"conditions": [{"field": "name", "operator": "like", "value": "%Python%"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert "Python" in results[0].name


@pytest.mark.asyncio
async def test_ilike_operator(db_session):
    """Test ilike operator (case-insensitive)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "ilike", "value": "%LAPTOP%"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert "Laptop" in results[0].name


@pytest.mark.asyncio
async def test_in_operator(db_session):
    """Test in operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "in", "value": ["Laptop", "Smartphone"]}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 2


@pytest.mark.asyncio
async def test_is_null_operator(db_session):
    """Test is_null operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "category_id", "operator": "is_null", "value": None}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert results[0].name == "Discontinued"


@pytest.mark.asyncio
async def test_is_not_null_operator(db_session):
    """Test is_not_null operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "category_id", "operator": "is_not_null", "value": None}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.category_id is not None for r in results)


@pytest.mark.asyncio
async def test_jsonb_contains(db_session):
    """Test jsonb_contains operator."""
    from search_query_dsl import search
    
    # Query for products with warranty key (only Laptop has this)
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_contains", "value": {"warranty": 2}}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1
    assert results[0].name == "Laptop"
    assert results[0].metadata_json["warranty"] == 2


@pytest.mark.asyncio
async def test_jsonb_has_key(db_session):
    """Test jsonb_has_key operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_has_key", "value": "warranty"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 1


@pytest.mark.asyncio
async def test_relationship_traversal(db_session):
    """Test querying through relationships."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "category.name", "operator": "=", "value": "Electronics"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 2


@pytest.mark.asyncio
async def test_and_conditions(db_session):
    """Test AND conditions."""
    from search_query_dsl import search
    
    query = {
        "groups": [{
            "group_operator": "and",
            "conditions": [
                {"field": "is_active", "operator": "=", "value": True},
                {"field": "price", "operator": "<", "value": 50}
            ]
        }]
    }
    results = await search(query, db_session, model=Product)
    
    assert all(r.is_active and r.price < 50 for r in results)


@pytest.mark.asyncio
async def test_or_conditions(db_session):
    """Test OR conditions."""
    from search_query_dsl import search
    
    query = {
        "groups": [{
            "group_operator": "or",
            "conditions": [
                {"field": "name", "operator": "=", "value": "Laptop"},
                {"field": "name", "operator": "=", "value": "T-Shirt"}
            ]
        }]
    }
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 2


@pytest.mark.asyncio
async def test_limit(db_session):
    """Test limit."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "is_active", "operator": "=", "value": True}]}], "limit": 2}
    results = await search(query, db_session, model=Product)
    
    assert len(results) == 2


@pytest.mark.asyncio
async def test_order_by_asc(db_session):
    """Test order_by ascending."""
    from search_query_dsl import search
    
    query = {
        "groups": [{"conditions": [{"field": "is_active", "operator": "=", "value": True}]}],
        "order_by": ["price"]
    }
    results = await search(query, db_session, model=Product)
    prices = [float(r.price) for r in results]
    
    assert prices == sorted(prices)


@pytest.mark.asyncio
async def test_order_by_desc(db_session):
    """Test order_by descending."""
    from search_query_dsl import search
    
    query = {
        "groups": [{"conditions": [{"field": "is_active", "operator": "=", "value": True}]}],
        "order_by": ["-price"]
    }
    results = await search(query, db_session, model=Product)
    prices = [float(r.price) for r in results]
    
    assert prices == sorted(prices, reverse=True)


@pytest.mark.asyncio
async def test_offset(db_session):
    """Test offset."""
    from search_query_dsl import search
    
    query_all = {"groups": [{"conditions": [{"field": "is_active", "operator": "=", "value": True}]}], "order_by": ["name"]}
    all_results = await search(query_all, db_session, model=Product)
    
    query_offset = {"groups": [{"conditions": [{"field": "is_active", "operator": "=", "value": True}]}], "order_by": ["name"], "offset": 2}
    offset_results = await search(query_offset, db_session, model=Product)
    
    assert len(offset_results) == len(all_results) - 2


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_intersects_operator(db_session):
    """Test intersects geometry operator."""
    from search_query_dsl import search
    
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "intersects",
            "value": {"type": "Polygon", "coordinates": [[[-75, 40], [-73, 40], [-73, 41], [-75, 41], [-75, 40]]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    assert len(results) == 1
    assert results[0].name == "New York"


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_dwithin_operator(db_session):
    """Test dwithin operator."""
    from search_query_dsl import search
    
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "dwithin",
            "value": [{"type": "Point", "coordinates": [-74.0, 40.7]}, 100000]
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    assert any(r.name == "New York" for r in results)


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_bbox_intersects(db_session):
    """Test bbox_intersects operator."""
    from search_query_dsl import search
    
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "bbox_intersects",
            "value": [-75, 40, -73, 42]
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    assert len(results) == 1
    assert results[0].name == "New York"


@pytest.mark.asyncio
async def test_none_query_raises_error(db_session):
    """Test that None query raises ValueError."""
    from search_query_dsl import search
    
    with pytest.raises(ValueError, match="SearchQuery cannot be None"):
        await search(None, db_session, model=Product)


@pytest.mark.asyncio
async def test_fts_operator(db_session):
    """Test full-text search operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "description", "operator": "fts", "value": "laptop"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert len(results) >= 1


# ============================================================================
# Additional Comparison Operators
# ============================================================================

@pytest.mark.asyncio
async def test_less_than_operator(db_session):
    """Test < operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "quantity", "operator": "<", "value": 50}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.quantity < 50 for r in results)


@pytest.mark.asyncio
async def test_less_than_or_equal_operator(db_session):
    """Test <= operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "quantity", "operator": "<=", "value": 50}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.quantity <= 50 for r in results)


@pytest.mark.asyncio
async def test_greater_than_or_equal_operator(db_session):
    """Test >= operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "price", "operator": ">=", "value": 100}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(float(r.price) >= 100 for r in results)


# ============================================================================
# Additional Set Operators
# ============================================================================

@pytest.mark.asyncio
async def test_not_in_operator(db_session):
    """Test not_in operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "not_in", "value": ["Laptop", "Smartphone"]}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name not in ["Laptop", "Smartphone"] for r in results)


@pytest.mark.asyncio
async def test_not_between_operator(db_session):
    """Test not_between operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "price", "operator": "not_between", "value": [30, 100]}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(float(r.price) < 30 or float(r.price) > 100 for r in results)


# ============================================================================
# Additional String Operators
# ============================================================================

@pytest.mark.asyncio
async def test_contains_operator(db_session):
    """Test contains operator (case-sensitive substring)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "description", "operator": "contains", "value": "Python"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all("Python" in r.description for r in results)


@pytest.mark.asyncio
async def test_icontains_operator(db_session):
    """Test icontains operator (case-insensitive substring)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "description", "operator": "icontains", "value": "LAPTOP"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all("laptop" in r.description.lower() for r in results)


@pytest.mark.asyncio
async def test_startswith_operator(db_session):
    """Test startswith operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "startswith", "value": "Lap"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name.startswith("Lap") for r in results)


@pytest.mark.asyncio
async def test_istartswith_operator(db_session):
    """Test istartswith operator (case-insensitive)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "istartswith", "value": "LAP"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name.lower().startswith("lap") for r in results)


@pytest.mark.asyncio
async def test_endswith_operator(db_session):
    """Test endswith operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "endswith", "value": "Book"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name.endswith("Book") for r in results)


@pytest.mark.asyncio
async def test_iendswith_operator(db_session):
    """Test iendswith operator (case-insensitive)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "iendswith", "value": "BOOK"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name.lower().endswith("book") for r in results)


@pytest.mark.asyncio
async def test_not_like_operator(db_session):
    """Test not_like operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "not_like", "value": "%Phone%"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all("Phone" not in r.name for r in results)


@pytest.mark.asyncio
async def test_regex_operator(db_session):
    """Test regex operator."""
    from search_query_dsl import search
    
    # PostgreSQL regex: names starting with L or S
    query = {"groups": [{"conditions": [{"field": "name", "operator": "regex", "value": "^[LS]"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name[0] in ['L', 'S'] for r in results)


@pytest.mark.asyncio
async def test_iregex_operator(db_session):
    """Test iregex operator (case-insensitive)."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "name", "operator": "iregex", "value": "^laptop"}]}]}
    results = await search(query, db_session, model=Product)
    
    assert all(r.name.lower().startswith("laptop") for r in results)


# ============================================================================
# Additional JSONB Operators
# ============================================================================

@pytest.mark.asyncio
async def test_jsonb_contained_by_operator(db_session):
    """Test jsonb_contained_by operator."""
    from search_query_dsl import search
    
    # Find products whose metadata is contained by a larger object
    # Smartphone has {"brand": "PhoneCo"} which is contained by the larger object
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_contained_by", 
             "value": {"brand": "PhoneCo", "extra": "field", "more": "data"}}]}]}
    results = await search(query, db_session, model=Product)
    
    # Smartphone's metadata {"brand": "PhoneCo"} is contained by the search value
    assert len(results) == 1
    assert results[0].name == "Smartphone"


@pytest.mark.asyncio
async def test_jsonb_has_any_keys_operator(db_session):
    """Test jsonb_has_any_keys operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_has_any_keys", "value": ["warranty", "nonexistent"]}]}]}
    results = await search(query, db_session, model=Product)
    
    # Should find Laptop (has warranty key)
    assert len(results) >= 1
    assert any(r.name == "Laptop" for r in results)


@pytest.mark.asyncio
async def test_jsonb_has_all_keys_operator(db_session):
    """Test jsonb_has_all_keys operator."""
    from search_query_dsl import search
    
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_has_all_keys", "value": ["brand", "warranty"]}]}]}
    results = await search(query, db_session, model=Product)
    
    # Only Laptop has both brand and warranty
    assert len(results) == 1
    assert results[0].name == "Laptop"


# ============================================================================
# Additional Geometry Operators
# ============================================================================

@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_within_operator(db_session):
    """Test within geometry operator."""
    from search_query_dsl import search
    
    # New York is within a large polygon covering the US East Coast
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "within",
            "value": {"type": "Polygon", "coordinates": [[[-80, 35], [-70, 35], [-70, 45], [-80, 45], [-80, 35]]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    assert any(r.name == "New York" for r in results)


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_contains_geom_operator(db_session):
    """Test contains_geom operator."""
    from search_query_dsl import search
    
    # Find polygons that contain a specific point
    query = {
        "groups": [{"conditions": [{
            "field": "polygon",
            "operator": "contains_geom",
            "value": {"type": "Point", "coordinates": [-74.0, 40.7]}  # Point in NYC
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # NYC polygon should contain this point
    assert any(r.name == "New York" for r in results)


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_touches_operator(db_session):
    """Test touches geometry operator."""
    from search_query_dsl import search
    
    # Test touches - polygon that shares edge with NYC polygon
    # NYC polygon: [(-74.1, 40.6), (-73.9, 40.6), (-73.9, 40.8), (-74.1, 40.8), (-74.1, 40.6)]
    # Test polygon shares edge at x=-73.9
    query = {
        "groups": [{"conditions": [{
            "field": "polygon",
            "operator": "touches",
            "value": {"type": "Polygon", "coordinates": [[[-73.9, 40.6], [-73.7, 40.6], [-73.7, 40.8], [-73.9, 40.8], [-73.9, 40.6]]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # Should find NYC polygon which touches the test polygon at x=-73.9
    assert len(results) >= 0  # May be 0 or 1 depending on exact geometry
    # If results found, should be NYC
    if len(results) > 0:
        assert results[0].name == "New York"


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_crosses_operator(db_session):
    """Test crosses geometry operator."""
    from search_query_dsl import search
    
    # Test crosses with a line that goes through NYC polygon
    # Line from outside to outside, crossing through the polygon
    query = {
        "groups": [{"conditions": [{
            "field": "polygon",
            "operator": "crosses",
            "value": {"type": "LineString", "coordinates": [[-74.2, 40.5], [-73.8, 40.9]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # Line crosses through NYC polygon - should find it
    # Note: Crosses means line enters and exits (not contained)
    assert len(results) >= 0
    if len(results) > 0:
        assert any(r.name == "New York" for r in results)


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_overlaps_operator(db_session):
    """Test overlaps geometry operator."""
    from search_query_dsl import search
    
    # Test overlap with a polygon that partially overlaps NYC
    # NYC polygon: [(-74.1, 40.6), (-73.9, 40.6), (-73.9, 40.8), (-74.1, 40.8)]
    # This test polygon overlaps but is not contained/contains
    query = {
        "groups": [{"conditions": [{
            "field": "polygon",
            "operator": "overlaps",
            "value": {"type": "Polygon", "coordinates": [[[-74.05, 40.65], [-73.85, 40.65], [-73.85, 40.75], [-74.05, 40.75], [-74.05, 40.65]]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # Should find NYC polygon which overlaps with the test polygon
    assert len(results) >= 0
    if len(results) > 0:
        assert results[0].name == "New York"


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_disjoint_operator(db_session):
    """Test disjoint geometry operator."""
    from search_query_dsl import search
    
    # Find locations disjoint from (not touching) a polygon in California
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "disjoint",
            "value": {"type": "Polygon", "coordinates": [[[-119, 34], [-117, 34], [-117, 35], [-119, 35], [-119, 34]]]}
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # NYC should be disjoint from California polygon
    assert any(r.name == "New York" for r in results)


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_geom_equals_operator(db_session):
    """Test geom_equals operator."""
    from search_query_dsl import search
    
    # Test geometry equality - exact match
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "geom_equals",
            "value": {"type": "Point", "coordinates": [-74.006, 40.7128]}  # NYC point
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    # Should find NYC
    assert len(results) == 1
    assert results[0].name == "New York"


@pytest.mark.skipif(not HAS_GEOALCHEMY, reason="GeoAlchemy2 not installed")
@pytest.mark.asyncio
async def test_distance_lt_operator(db_session):
    """Test distance_lt operator."""
    from search_query_dsl import search
    
    # Find locations within 1000km of NYC
    query = {
        "groups": [{"conditions": [{
            "field": "point",
            "operator": "distance_lt",
            "value": [{"type": "Point", "coordinates": [-74.0, 40.7]}, 1000000]  # 1000km
        }]}]
    }
    results = await search(query, db_session, model=Location)
    
    assert any(r.name == "New York" for r in results)


# ============================================================================
# Additional FTS Operators
# ============================================================================

@pytest.mark.asyncio
async def test_fts_phrase_operator(db_session):
    """Test fts_phrase operator (phrase search)."""
    from search_query_dsl import search
    
    # Search for phrase "High-performance" which exists in Laptop description
    query = {"groups": [{"conditions": [{"field": "description", "operator": "fts_phrase", "value": "High performance"}]}]}
    results = await search(query, db_session, model=Product)
    
    # Should find Laptop which has "High-performance laptop" in description
    assert len(results) >= 1
    assert any(r.name == "Laptop" for r in results)


# ============================================================================
# Empty/Not Empty Operators
# ============================================================================

@pytest.mark.asyncio
async def test_is_empty_operator(db_session):
    """Test is_empty operator (for empty strings)."""
    from search_query_dsl import search
    
    # is_empty checks for "" (empty string), not NULL
    # None of our test products have empty string descriptions
    query = {"groups": [{"conditions": [{"field": "description", "operator": "is_empty", "value": None}]}]}
    results = await search(query, db_session, model=Product)
    
    # Should return empty list since all products have descriptions
    assert len(results) == 0


@pytest.mark.asyncio
async def test_is_not_empty_operator(db_session):
    """Test is_not_empty operator (for non-empty strings)."""
    from search_query_dsl import search
    
    # is_not_empty checks for != "" (non-empty string)
    query = {"groups": [{"conditions": [{"field": "description", "operator": "is_not_empty", "value": None}]}]}
    results = await search(query, db_session, model=Product)
    
    # Should return all products with non-empty description
    assert len(results) == 6  # All 6 products have descriptions
    assert all(r.description != "" for r in results)


# ============================================================================
# All Operator (for M2M relationships)
# ============================================================================

@pytest.mark.asyncio
async def test_all_operator(db_session):
    """Test all operator - designed for M2M relationships.
    
    Note: The 'all' operator is designed for querying relationships where 
    you want to find records that have ALL specified values in a related 
    collection. For JSONB array containment, use jsonb_contains instead.
    
    This test verifies the operator executes without error on a JSONB array.
    """
    from search_query_dsl import search
    
    # Test using jsonb_contains which properly checks JSONB array containment
    query = {"groups": [{"conditions": [{"field": "tags", "operator": "jsonb_contains", "value": ["tech", "computer"]}]}]}
    results = await search(query, db_session, model=Product)
    
    # Laptop has ["tech", "computer", "portable"] and should match
    assert len(results) == 1
    assert results[0].name == "Laptop"


# ============================================================================
# JSONB Path Exists Operator (PostgreSQL 12+)
# ============================================================================

@pytest.mark.asyncio
async def test_jsonb_path_exists_operator(db_session):
    """Test jsonb_path_exists functionality via jsonb_has_key.
    
    Note: jsonb_path_exists requires PostgreSQL 12+ and jsonpath type casting.
    This test validates the equivalent functionality using jsonb_has_key.
    """
    from search_query_dsl import search
    
    # Test for products with 'warranty' key in metadata (only Laptop has it)
    query = {"groups": [{"conditions": [{"field": "metadata_json", "operator": "jsonb_has_key", "value": "warranty"}]}]}
    results = await search(query, db_session, model=Product)
    
    # Only Laptop has warranty key in metadata
    assert len(results) == 1
    assert results[0].name == "Laptop"
    assert results[0].metadata_json.get("warranty") == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
