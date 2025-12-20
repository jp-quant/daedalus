"""
Test that parquet_writer creates data with partition columns that match path values.

This validates the Directory-Aligned Partitioning approach:
- ALL partition column values EXIST in the Parquet data
- ALL partition column values MATCH the directory partition values exactly
- ALL partition values are SANITIZED for filesystem compatibility (/, \\, :, etc. -> -)
- year, month, day, hour are stored as int32 columns in data
"""
import asyncio
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow.parquet as pq

from storage.base import LocalStorage
from ingestion.writers.parquet_writer import (
    StreamingParquetWriter,
    CHANNEL_SCHEMAS,
    _get_generic_schema,
    sanitize_partition_value,
    DEFAULT_PARTITION_COLUMNS,
)


async def test_partition_columns_match():
    """Test that partition columns in data match partition path values."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create storage
        storage = LocalStorage(base_path=tmpdir)
        
        # Create writer with default partitioning (all 6 columns)
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            batch_size=1,
            flush_interval_seconds=0.1,
            segment_max_mb=1,
            # Uses default partition_by: ["exchange", "symbol", "year", "month", "day", "hour"]
        )
        
        await writer.start()
        
        # Write a test record with unsanitized values
        test_record = {
            "type": "ticker",
            "exchange": "binanceus",
            "symbol": "BTC/USD",  # Contains slash - should be sanitized
            "timestamp": 1734616800000,  # 2024-12-19 14:00:00 UTC (hour=14)
            "capture_ts": "2024-12-19T14:00:00Z",
            "data": {
                "bid": 100000.0,
                "ask": 100001.0,
                "last": 100000.5,
                "timestamp": 1734616800000,
            },
        }
        
        await writer.write(test_record)
        await asyncio.sleep(0.5)  # Wait for flush
        await writer.stop()
        
        # Find the written parquet file
        ready_path = Path(tmpdir) / "raw" / "ready" / "ccxt" / "ticker"
        parquet_files = list(ready_path.rglob("*.parquet"))
        
        if not parquet_files:
            print("ERROR: No parquet files found!")
            return False
        
        # Read the single parquet file directly
        pf = pq.ParquetFile(str(parquet_files[0]))
        table = pf.read()
        
        # Check partition column values
        print("\n=== Partition Column Validation ===")
        
        # Check symbol is sanitized
        symbol_values = table.column("symbol").to_pylist()
        expected_symbol = "BTC-USD"  # Sanitized (/ -> -)
        if symbol_values[0] != expected_symbol:
            print(f"ERROR: Symbol mismatch! Expected '{expected_symbol}', got '{symbol_values[0]}'")
            return False
        print(f"✓ Symbol: '{symbol_values[0]}' (correctly sanitized)")
        
        # Check exchange (should also be sanitized, though it has no special chars)
        exchange_values = table.column("exchange").to_pylist()
        if exchange_values[0] != "binanceus":
            print(f"ERROR: Exchange mismatch! Expected 'binanceus', got '{exchange_values[0]}'")
            return False
        print(f"✓ Exchange: '{exchange_values[0]}'")
        
        # Check partition path contains sanitized values
        parquet_path = str(parquet_files[0])
        if "symbol=BTC-USD" not in parquet_path:
            print(f"ERROR: Partition path doesn't contain 'symbol=BTC-USD': {parquet_path}")
            return False
        print(f"✓ Partition path contains 'symbol=BTC-USD'")
        
        # Check year/month/day/hour columns exist and have correct values
        year_values = table.column("year").to_pylist()
        month_values = table.column("month").to_pylist()
        day_values = table.column("day").to_pylist()
        hour_values = table.column("hour").to_pylist()
        
        if year_values[0] != 2024:
            print(f"ERROR: Year mismatch! Expected 2024, got {year_values[0]}")
            return False
        print(f"✓ Year: {year_values[0]}")
        
        if month_values[0] != 12:
            print(f"ERROR: Month mismatch! Expected 12, got {month_values[0]}")
            return False
        print(f"✓ Month: {month_values[0]}")
        
        if day_values[0] != 19:
            print(f"ERROR: Day mismatch! Expected 19, got {day_values[0]}")
            return False
        print(f"✓ Day: {day_values[0]}")
        
        if hour_values[0] != 14:
            print(f"ERROR: Hour mismatch! Expected 14, got {hour_values[0]}")
            return False
        print(f"✓ Hour: {hour_values[0]}")
        
        # Verify partition path has matching date/time values
        if "year=2024" not in parquet_path:
            print(f"ERROR: Path missing 'year=2024'")
            return False
        if "month=12" not in parquet_path:
            print(f"ERROR: Path missing 'month=12'")
            return False
        if "day=19" not in parquet_path:
            print(f"ERROR: Path missing 'day=19'")
            return False
        if "hour=14" not in parquet_path:
            print(f"ERROR: Path missing 'hour=14'")
            return False
        print(f"✓ Partition path has matching year/month/day/hour")
        
        print(f"\nParquet file: {parquet_files[0]}")
        print(f"Columns: {table.column_names}")
        
        return True


async def test_schema_has_partition_columns():
    """Test that all schemas include year/month/day/hour columns."""
    print("\n=== Schema Validation ===")
    
    all_schemas = {
        "ticker": CHANNEL_SCHEMAS["ticker"],
        "trades": CHANNEL_SCHEMAS["trades"],
        "orderbook": CHANNEL_SCHEMAS["orderbook"],
        "generic": _get_generic_schema(),
    }
    
    required_partition_cols = ["exchange", "symbol", "year", "month", "day", "hour"]
    
    for name, schema in all_schemas.items():
        field_names = [f.name for f in schema]
        
        # Check required partition columns
        missing = [col for col in required_partition_cols if col not in field_names]
        
        if missing:
            print(f"ERROR: Schema '{name}' missing partition columns: {missing}")
            return False
        
        # Check types
        year_field = schema.field("year")
        month_field = schema.field("month")
        day_field = schema.field("day")
        hour_field = schema.field("hour")
        
        for col, field in [("year", year_field), ("month", month_field), 
                           ("day", day_field), ("hour", hour_field)]:
            if str(field.type) != "int32":
                print(f"ERROR: Schema '{name}' {col} column is {field.type}, expected int32")
                return False
        
        print(f"✓ Schema '{name}' has all partition columns with correct types")
    
    return True


def test_sanitize_partition_value():
    """Test the generalized partition value sanitization."""
    print("\n=== Sanitization Validation ===")
    
    test_cases = [
        ("BTC/USD", "BTC-USD"),
        ("ETH/USDT", "ETH-USDT"),
        ("exchange:test", "exchange-test"),
        ("test\\path", "test-path"),
        ("test*value", "test-value"),
        ("test?query", "test-query"),
        ('test"quote', "test-quote"),
        ("test<angle>", "test-angle-"),
        ("test|pipe", "test-pipe"),
        ("normal_value", "normal_value"),
        ("already-sanitized", "already-sanitized"),
        (None, "unknown"),
        (12345, "12345"),
    ]
    
    all_passed = True
    for input_val, expected in test_cases:
        result = sanitize_partition_value(input_val)
        if result != expected:
            print(f"ERROR: sanitize_partition_value({repr(input_val)}) = {repr(result)}, expected {repr(expected)}")
            all_passed = False
        else:
            print(f"✓ sanitize_partition_value({repr(input_val)}) = {repr(result)}")
    
    return all_passed


def test_default_partition_columns():
    """Test that default partition columns are correctly defined."""
    print("\n=== Default Configuration Validation ===")
    
    expected_defaults = ["exchange", "symbol", "year", "month", "day", "hour"]
    
    if DEFAULT_PARTITION_COLUMNS != expected_defaults:
        print(f"ERROR: DEFAULT_PARTITION_COLUMNS = {DEFAULT_PARTITION_COLUMNS}, expected {expected_defaults}")
        return False
    
    print(f"✓ DEFAULT_PARTITION_COLUMNS = {DEFAULT_PARTITION_COLUMNS}")
    return True


async def main():
    print("=" * 70)
    print("Directory-Aligned Partitioning Validation Tests")
    print("=" * 70)
    
    all_passed = True
    
    # Test default configuration
    if not test_default_partition_columns():
        all_passed = False
    
    # Test sanitization
    if not test_sanitize_partition_value():
        all_passed = False
    
    # Test schemas
    if not await test_schema_has_partition_columns():
        all_passed = False
    
    # Test actual writing
    print("\n=== Write Test ===")
    if not await test_partition_columns_match():
        all_passed = False
    
    print("\n" + "=" * 70)
    if all_passed:
        print("ALL TESTS PASSED!")
    else:
        print("SOME TESTS FAILED!")
        sys.exit(1)
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
