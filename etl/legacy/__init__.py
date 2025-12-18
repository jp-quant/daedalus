"""
Legacy ETL Module (Archived)
============================

This module contains the original NDJSON-based ETL pipeline implementation.
It is preserved for reference and potential rollback, but is NOT actively used.

**DO NOT IMPORT FROM THIS MODULE IN PRODUCTION CODE.**

The new ETL framework is in:
- etl/core/        - Framework base classes and executor
- etl/features/    - Feature computation library
- etl/transforms/  - Transform implementations

Legacy Components (for reference only):
- orchestrators/   - Pipeline composition for NDJSON
- parsers/         - NDJSON parsing (Coinbase, CCXT)
- processors/      - Per-record processing
- readers/         - NDJSON reading
- writers/         - Parquet writing (old style)
- job.py           - NDJSON segment job runner
"""

# No exports - this is archived code
__all__ = []
