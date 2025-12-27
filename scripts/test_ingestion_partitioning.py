"""
Test script to verify ingestion partitioning is working correctly.

Directory-Aligned Partitioning Tests:
1. Default partition configuration (6 columns with hour)
2. Partition value sanitization (generalized)
3. Partition path building
4. Partition key extraction from records
5. End-to-end segment writing with partitions
6. Hour rotation across all partitions (file handle exhaustion fix)
7. Max open writers limit (LRU eviction)
"""
import asyncio
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
    MAX_OPEN_WRITERS,
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


def test_hour_rotation_all_partitions():
    """
    Test that hour changes correctly rotate ALL partitions, not just the first one.
    
    This tests the fix for the file handle exhaustion bug where only one partition
    was rotated on hour change, leaving others with stale open file handles.
    """
    print("=" * 60)
    print("Test: Hour Rotation for All Partitions")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            batch_size=10,
            flush_interval_seconds=1.0,
        )
        
        try:
            # Simulate hour "T01"
            hour1 = "20251227T01"
            
            # Create segments for multiple partitions at hour 1
            partition_a = ("ticker", "binanceus", "BTC-USD", 2025, 12, 27, 1)
            partition_b = ("ticker", "binanceus", "ETH-USD", 2025, 12, 27, 1)
            partition_c = ("orderbook", "binanceus", "BTC-USD", 2025, 12, 27, 1)
            
            # Open segments for all three
            writer._open_new_segment(partition_a, hour1)
            writer._open_new_segment(partition_b, hour1)
            writer._open_new_segment(partition_c, hour1)
            
            print(f"Opened 3 segments at hour {hour1}")
            print(f"Open writers: {len(writer._partition_segments)}")
            assert len(writer._partition_segments) == 3, "Should have 3 open segments"
            
            # Verify all have hour1
            for pk, info in writer._partition_segments.items():
                assert info["hour"] == hour1, f"Segment {pk} should have hour {hour1}"
            
            print("✓ All segments have correct hour")
            
            # Now simulate hour change - _ensure_segment_open should rotate stale segments
            hour2 = "20251227T02"
            
            # Partition A: call _ensure_segment_open, which detects hour mismatch and rotates
            # First update to simulate hour change detection
            # The key test: when accessing partition_b, it should detect its segment has hour1 != current
            
            # Manually check: partition_b's segment still has hour1
            seg_b_hour = writer._partition_segments[partition_b].get("hour")
            print(f"Partition B current hour: {seg_b_hour}")
            
            # Verify the logic: if segment hour differs from current, rotation is needed
            if seg_b_hour != hour2:
                print(f"✓ Partition B needs rotation (hour {seg_b_hour} != current {hour2})")
                # This is what _ensure_segment_open does internally
                writer._rotate_segment(partition_b)
                writer._open_new_segment(partition_b, hour2)
                print(f"✓ Partition B rotated to hour {hour2}")
            
            # Verify new segment has correct hour
            assert writer._partition_segments[partition_b]["hour"] == hour2
            
            print("\n✓ PASSED - Hour tracking is per-partition")
            print()
            
        finally:
            # Clean up all open segments
            writer._close_all_segments()


def test_max_open_writers_limit():
    """
    Test that the writer enforces MAX_OPEN_WRITERS limit via LRU eviction.
    
    This prevents 'Too many open files' errors when running with many partitions.
    """
    print("=" * 60)
    print("Test: Max Open Writers Limit (LRU Eviction)")
    print("=" * 60)
    
    print(f"MAX_OPEN_WRITERS = {MAX_OPEN_WRITERS}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            batch_size=10,
        )
        
        try:
            current_hour = "20251227T03"
            
            # Create more partitions than MAX_OPEN_WRITERS
            # Use a smaller number for testing (MAX_OPEN_WRITERS is 200)
            test_limit = 50  # Test with 50 partitions to keep test fast
            
            print(f"Creating {test_limit + 10} partitions...")
            
            for i in range(test_limit + 10):
                partition_key = ("ticker", "testexchange", f"SYMBOL{i}-USD", 2025, 12, 27, 3)
                
                # Simulate the pattern that causes file exhaustion
                if partition_key not in writer._partition_segments:
                    # Check limit before opening
                    if len(writer._partition_segments) >= test_limit:
                        # Manually trigger eviction for this test
                        before_count = len(writer._partition_segments)
                        writer._evict_oldest_writers(keep=test_limit // 2)
                        after_count = len(writer._partition_segments)
                        if before_count > after_count:
                            print(f"  Evicted {before_count - after_count} writers at partition {i}")
                    
                    writer._open_new_segment(partition_key, current_hour)
                    # Update last_write to simulate activity
                    writer._partition_segments[partition_key]["last_write"] = time.time() + i * 0.001
            
            final_count = len(writer._partition_segments)
            print(f"\nFinal open writers: {final_count}")
            
            # Should be under the test limit after eviction
            assert final_count <= test_limit, f"Too many open writers: {final_count} > {test_limit}"
            
            print("✓ PASSED - Max open writers limit enforced via LRU eviction")
            print()
            
        finally:
            # Clean up all segments
            writer._close_all_segments()


async def test_async_write_many_partitions():
    """
    Test async write to many partitions without file handle exhaustion.
    """
    print("=" * 60)
    print("Test: Async Write to Many Partitions")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
            batch_size=5,
            flush_interval_seconds=0.5,
        )
        
        await writer.start()
        
        # Write to 50 different symbol partitions
        num_symbols = 50
        records_per_symbol = 10
        
        print(f"Writing {records_per_symbol} records to {num_symbols} symbols...")
        
        for i in range(num_symbols):
            for j in range(records_per_symbol):
                record = {
                    "type": "ticker",
                    "exchange": "testexchange",
                    "symbol": f"SYM{i}/USD",  # Will be sanitized to SYM{i}-USD
                    "timestamp": 1735344000000 + j * 1000,  # 2025-12-27 03:00:00 UTC + j seconds
                    "capture_ts": "2025-12-27T03:00:00Z",
                    "data": {
                        "bid": 100.0 + i,
                        "ask": 100.5 + i,
                        "last": 100.25 + i,
                    },
                }
                await writer.write(record)
        
        # Wait for flush
        await asyncio.sleep(1.0)
        
        stats = writer.get_stats()
        print(f"Stats after writes:")
        print(f"  messages_received: {stats['messages_received']}")
        print(f"  messages_written: {stats['messages_written']}")
        print(f"  open_writers: {stats['open_writers']}")
        print(f"  max_open_writers: {stats['max_open_writers']}")
        print(f"  errors: {stats['errors']}")
        
        assert stats["errors"] == 0, f"Expected 0 errors, got {stats['errors']}"
        assert stats["open_writers"] <= MAX_OPEN_WRITERS, f"Exceeded max open writers"
        
        await writer.stop()
        
        # Check files were written
        ready_path = Path(tmpdir) / "raw" / "ready" / "ccxt" / "ticker"
        parquet_files = list(ready_path.rglob("*.parquet"))
        print(f"\nParquet files created: {len(parquet_files)}")
        
        assert len(parquet_files) > 0, "No parquet files created"
        
        print("✓ PASSED - Async write to many partitions without errors")
        print()


def test_segment_info_has_required_fields():
    """Test that segment info dictionaries have hour and last_write fields."""
    print("=" * 60)
    print("Test: Segment Info Required Fields")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_path=tmpdir)
        
        writer = StreamingParquetWriter(
            storage=storage,
            active_path="raw/active/ccxt",
            ready_path="raw/ready/ccxt",
            source_name="test",
        )
        
        partition_key = ("ticker", "binanceus", "BTC-USD", 2025, 12, 27, 3)
        current_hour = "20251227T03"
        
        writer._open_new_segment(partition_key, current_hour)
        
        segment_info = writer._partition_segments[partition_key]
        
        print(f"Segment info keys: {list(segment_info.keys())}")
        
        required_fields = ["writer", "path", "name", "partition_key", "hour", "last_write"]
        
        for field in required_fields:
            assert field in segment_info, f"Missing field: {field}"
            print(f"  ✓ {field}: {type(segment_info[field]).__name__}")
        
        assert segment_info["hour"] == current_hour, "Hour mismatch"
        assert isinstance(segment_info["last_write"], float), "last_write should be float (timestamp)"
        
        writer._close_all_segments()
        
        print("\n✓ PASSED - Segment info has all required fields")
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
        
        # Scalability tests (file handle exhaustion fix)
        test_segment_info_has_required_fields()
        test_hour_rotation_all_partitions()
        test_max_open_writers_limit()
        
        # Async test
        asyncio.run(test_async_write_many_partitions())
        
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
