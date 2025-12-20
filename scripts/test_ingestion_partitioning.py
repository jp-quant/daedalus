"""
Test script to verify ingestion partitioning is working correctly.

Directory-Aligned Partitioning Tests:
1. Default partition configuration (6 columns with hour)
2. Partition value sanitization (generalized)
3. Partition path building
4. Partition key extraction from records
5. End-to-end segment writing with partitions
"""
import sys
import tempfile
import time
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.base import LocalStorage
from ingestion.writers.parquet_writer import (
    StreamingParquetWriter,
    DEFAULT_PARTITION_COLUMNS,
    sanitize_partition_value,
    extract_datetime_components,
    CHANNEL_SCHEMAS,
)


def test_default_partition_columns():
    """Test that default partition columns include hour."""
    print("=" * 60)
    print("Test: Default Partition Columns")
    print("=" * 60)
    
    expected = ["exchange", "symbol", "year", "month", "day", "hour"]
    
    print(f"Expected: {expected}")
    print(f"Actual:   {DEFAULT_PARTITION_COLUMNS}")
    
    assert DEFAULT_PARTITION_COLUMNS == expected, f"Mismatch: {DEFAULT_PARTITION_COLUMNS}"
    print("✓ PASSED - Default partition columns include hour")
    print()


def test_sanitize_partition_value():
    """Test partition value sanitization for all prohibited characters."""
    print("=" * 60)
    print("Test: Partition Value Sanitization")
    print("=" * 60)
    
    test_cases = [
        # (input, expected)
        ("BTC/USD", "BTC-USD"),  # Forward slash
        ("BTC\\USD", "BTC-USD"),  # Backslash
        ("exchange:test", "exchange-test"),  # Colon
        ("file*name", "file-name"),  # Asterisk
        ('quote"test', "quote-test"),  # Double quote
        ("less<more", "less-more"),  # Less than
        ("more>less", "more-less"),  # Greater than
        ("pipe|test", "pipe-test"),  # Pipe
        ("multi/path\\test:value", "multi-path-test-value"),  # Multiple chars
        ("simple", "simple"),  # No special chars
        ("BTC-USD", "BTC-USD"),  # Already safe
        (2024, "2024"),  # Integer input
        (None, "unknown"),  # None input
    ]
    
    all_passed = True
    for input_val, expected in test_cases:
        result = sanitize_partition_value(input_val)
        status = "✓" if result == expected else "✗"
        print(f"  {status} sanitize({repr(input_val)}) = {repr(result)} (expected: {repr(expected)})")
        if result != expected:
            all_passed = False
    
    assert all_passed, "Some sanitization tests failed"
    print("\n✓ PASSED - All sanitization tests passed")
    print()


def test_extract_datetime_components():
    """Test datetime component extraction returns year, month, day, hour."""
    print("=" * 60)
    print("Test: DateTime Component Extraction")
    print("=" * 60)
    
    # Test with millisecond timestamp (1750341600000 = 2025-06-19 14:00:00 UTC)
    record = {"timestamp": 1750341600000}
    year, month, day, hour = extract_datetime_components(record)
    
    print(f"Input: timestamp=1750341600000 (2025-06-19 14:00:00 UTC)")
    print(f"Output: year={year}, month={month}, day={day}, hour={hour}")
    
    assert year == 2025, f"Year mismatch: {year}"
    assert month == 6, f"Month mismatch: {month}"
    assert day == 19, f"Day mismatch: {day}"
    assert hour == 14, f"Hour mismatch: {hour}"
    
    print("✓ PASSED - DateTime extraction includes hour")
    print()


def test_schema_has_partition_columns():
    """Test that all schemas include the 6 partition columns."""
    print("=" * 60)
    print("Test: Schema Partition Columns")
    print("=" * 60)
    
    partition_cols = ["exchange", "symbol", "year", "month", "day", "hour"]
    
    for channel, schema in CHANNEL_SCHEMAS.items():
        schema_fields = [f.name for f in schema]
        missing = [col for col in partition_cols if col not in schema_fields]
        
        if missing:
            print(f"✗ {channel}: missing {missing}")
            assert False, f"Schema {channel} missing partition columns: {missing}"
        else:
            print(f"✓ {channel}: has all partition columns")
    
    print("\n✓ PASSED - All schemas have partition columns including hour")
    print()


def test_partition_path_building():
    """Test that partition paths are built correctly with hour."""
    print("=" * 60)
    print("Test: Partition Path Building")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        # Test with full 6-column partitioning (default)
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            # Uses default partition_by: ["exchange", "symbol", "year", "month", "day", "hour"]
        )
        
        # Partition key tuple as returned by _get_partition_key
        # (channel, exchange, symbol, year, month, day, hour)
        partition_key = ("orderbook", "binanceus", "BTC-USD", 2025, 6, 19, 14)
        
        path = writer._build_partition_path("raw/ready/ccxt", partition_key)
        expected = "raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=6/day=19/hour=14"
        
        print(f"Partition key: {partition_key}")
        print(f"Built path:    {path.replace(chr(92), '/')}")  # Normalize for display
        print(f"Expected:      {expected}")
        
        # Normalize path separators for comparison
        path_normalized = path.replace("\\", "/")
        assert path_normalized == expected, f"Path mismatch: {path_normalized} != {expected}"
        print("✓ PASSED - Full partition path with hour")
        print()
        
        # Test with reduced partitioning (no hour)
        writer_no_hour = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            partition_by=["exchange", "symbol", "year", "month", "day"],
        )
        
        # For no-hour config: (channel, exchange, symbol, year, month, day)
        partition_key_no_hour = ("orderbook", "binanceus", "BTC-USD", 2025, 6, 19)
        path_no_hour = writer_no_hour._build_partition_path("raw/ready/ccxt", partition_key_no_hour)
        expected_no_hour = "raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD/year=2025/month=6/day=19"
        
        print(f"Without hour partition:")
        print(f"Built path:    {path_no_hour.replace(chr(92), '/')}")
        print(f"Expected:      {expected_no_hour}")
        
        path_no_hour_normalized = path_no_hour.replace("\\", "/")
        assert path_no_hour_normalized == expected_no_hour, f"Path mismatch"
        print("✓ PASSED - Partition path without hour")
        print()
        
        # Test with minimal partitioning (exchange and symbol only)
        writer_minimal = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            partition_by=["exchange", "symbol"],
        )
        
        # For minimal config: (channel, exchange, symbol)
        partition_key_minimal = ("orderbook", "binanceus", "BTC-USD")
        path_minimal = writer_minimal._build_partition_path("raw/ready/ccxt", partition_key_minimal)
        expected_minimal = "raw/ready/ccxt/orderbook/exchange=binanceus/symbol=BTC-USD"
        
        print(f"Minimal (exchange+symbol only):")
        print(f"Built path:    {path_minimal.replace(chr(92), '/')}")
        print(f"Expected:      {expected_minimal}")
        
        path_minimal_normalized = path_minimal.replace("\\", "/")
        assert path_minimal_normalized == expected_minimal, f"Path mismatch"
        print("✓ PASSED - Minimal partition path")
        print()


def test_partition_key_extraction():
    """Test that partition keys are extracted correctly with sanitization."""
    print("=" * 60)
    print("Test: Partition Key Extraction")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            # Default partition_by: ["exchange", "symbol", "year", "month", "day", "hour"]
        )
        
        # Test orderbook record with special characters
        orderbook_record = {
            "type": "orderbook",
            "exchange": "binanceus",  # Clean
            "symbol": "BTC/USDT",  # Has / that needs sanitization
            "timestamp": 1750341600000,  # 2025-06-19 14:00:00 UTC
            "bids": [[100000.0, 1.0]],
            "asks": [[100001.0, 1.0]],
        }
        
        # _get_partition_key returns a tuple: (channel, exchange, symbol, year, month, day, hour)
        partition_key = writer._get_partition_key(orderbook_record)
        
        print(f"Orderbook record: exchange={orderbook_record['exchange']}, symbol={orderbook_record['symbol']}")
        print(f"Extracted partition_key: {partition_key}")
        
        assert partition_key[0] == "orderbook", f"Channel mismatch: {partition_key[0]}"
        assert partition_key[1] == "binanceus", f"Exchange mismatch: {partition_key[1]}"
        assert partition_key[2] == "BTC-USDT", f"Symbol mismatch (expected sanitized): {partition_key[2]}"
        assert partition_key[3] == 2025, f"Year mismatch: {partition_key[3]}"
        assert partition_key[4] == 6, f"Month mismatch: {partition_key[4]}"
        assert partition_key[5] == 19, f"Day mismatch: {partition_key[5]}"
        assert partition_key[6] == 14, f"Hour mismatch: {partition_key[6]}"
        print("✓ PASSED - Orderbook partition key extraction with hour")
        print()
        
        # Test ticker record
        ticker_record = {
            "type": "ticker",
            "exchange": "coinbase",
            "symbol": "ETH/USD",
            "timestamp": 1750381200000,  # 2025-06-20 01:00:00 UTC
            "last": 3500.0,
            "bid": 3499.0,
            "ask": 3501.0,
        }
        
        partition_key = writer._get_partition_key(ticker_record)
        
        print(f"Ticker record: exchange={ticker_record['exchange']}, symbol={ticker_record['symbol']}")
        print(f"Extracted partition_key: {partition_key}")
        
        assert partition_key[0] == "ticker", f"Channel mismatch"
        assert partition_key[2] == "ETH-USD", f"Symbol mismatch"
        assert partition_key[6] == 1, f"Hour mismatch (expected 1): {partition_key[6]}"
        print("✓ PASSED - Ticker partition key extraction")
        print()


def main():
    """Run all tests."""
    print("=" * 60)
    print("Directory-Aligned Partitioning Tests")
    print("=" * 60)
    print()
    
    try:
        test_default_partition_columns()
        test_sanitize_partition_value()
        test_extract_datetime_components()
        test_schema_has_partition_columns()
        test_partition_path_building()
        test_partition_key_extraction()
        
        print()
        print("=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)
        print()
        print("Run `scripts/run_ingestion.py` to test the full pipeline")
        
    except AssertionError as e:
        print(f"\n✗ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
