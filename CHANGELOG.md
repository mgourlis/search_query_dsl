# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Core DSL models for `SearchQuery`, `SearchQueryGroup`, and `SearchCondition`.
- SQLAlchemy backend for converting DSL to SQL filters.
- Support for complex nested logical operations (AND, OR, NOT).
- Support for variety of operators: equality, comparison, set operations, string operations.
- PostGIS spatial operator support.
- JSONB operator support for PostgreSQL.
- Pre-commit hooks for ruff and mypy.
- GitHub Actions CI workflow.
