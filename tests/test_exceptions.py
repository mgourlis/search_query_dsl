"""Tests for core exceptions."""

import pytest
from search_query_dsl.core.exceptions import (
    FieldValidationError,
    FieldNotQueryableError,
    OperatorNotFoundError,
    RelationshipTraversalError,
)


class TestFieldValidationError:
    def test_fuzzy_matching(self):
        available = ["username", "email", "created_at"]
        error = FieldValidationError("usrname", "User", available)
        
        msg = str(error)
        assert "Invalid field 'usrname' on model 'User'" in msg
        assert "Did you mean one of these?" in msg
        assert "username" in msg
        
    def test_listing_available_fields(self):
        available = ["a", "b", "c"]
        error = FieldValidationError("z", "Model", available)
        assert "Available fields: a, b, c" in str(error)

    def test_truncates_long_lists(self):
        available = [f"field_{i}" for i in range(20)]
        error = FieldValidationError("z", "Model", available)
        assert "more" in str(error)


class TestFieldNotQueryableError:
    def test_message_structure(self):
        available = ["id", "name"]
        error = FieldNotQueryableError("my_method", "User", available)
        
        msg = str(error)
        assert "Field 'my_method' on model 'User' cannot be used in a query" in msg
        assert "not a mapped SQL column" in msg
        assert "Available queryable fields: id, name" in msg

    def test_to_dict(self):
        error = FieldNotQueryableError("my_method", "User", ["id"])
        data = error.to_dict()
        assert data["error"] == "FIELD_NOT_QUERYABLE"
        assert data["field"] == "my_method"
        assert data["available_fields"] == ["id"]


class TestOperatorNotFoundError:
    def test_suggestions(self):
        valid = ["equals", "contains"]
        error = OperatorNotFoundError("equal", valid)
        assert "Did you mean: equals?" in str(error)


class TestRelationshipTraversalError:
    def test_message(self):
        error = RelationshipTraversalError("name", "User", "name.subfield")
        assert "Cannot traverse field 'name' on model 'User' because it is not a relationship" in str(error)
