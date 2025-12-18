"""Tests for log writer."""
import pytest
import asyncio
from pathlib import Path
import sys

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.writers.log_writer import LogWriter
from ingestion.utils.time import utc_now
from storage.base import LocalStorage


@pytest.mark.asyncio
async def test_log_writer_basic(temp_dir):
    """Test basic log writer functionality."""
    storage = LocalStorage(str(temp_dir))
    writer = LogWriter(
        storage=storage,
        active_path="active/test",
        ready_path="ready/test",
        source_name="test",
        batch_size=5,
        flush_interval_seconds=1.0,
        queue_maxsize=100,
    )
    
    await writer.start()
    
    # Write some records
    for i in range(10):
        record = {
            "id": i,
            "timestamp": utc_now().isoformat(),
            "data": f"test-{i}",
        }
        await writer.write(record)
    
    # Wait for flush
    await asyncio.sleep(2)
    
    # Stop writer
    await writer.stop()
    
    # Final segment should be moved to ready/
    ready_dir = temp_dir / "ready" / "test"
    segments = sorted(ready_dir.glob("segment_*.ndjson"))
    assert len(segments) >= 1

    # Check records across all segments
    all_lines = []
    for seg in segments:
        content = seg.read_text().strip()
        if content:
            all_lines.extend(content.split("\n"))
    assert len(all_lines) == 10


@pytest.mark.asyncio
async def test_log_writer_backpressure(temp_dir):
    """Test backpressure handling."""
    storage = LocalStorage(str(temp_dir))
    writer = LogWriter(
        storage=storage,
        active_path="active/test",
        ready_path="ready/test",
        source_name="test",
        batch_size=100,
        flush_interval_seconds=10.0,
        queue_maxsize=10,  # Small queue
    )
    
    await writer.start()
    
    # Fill queue
    for i in range(10):
        await writer.write({"id": i})
    
    # Next write should raise QueueFull with block=False
    with pytest.raises(asyncio.QueueFull):
        await writer.write({"id": 11}, block=False)
    
    await writer.stop()


@pytest.mark.asyncio
async def test_log_writer_stats(temp_dir):
    """Test statistics tracking."""
    storage = LocalStorage(str(temp_dir))
    writer = LogWriter(
        storage=storage,
        active_path="active/test",
        ready_path="ready/test",
        source_name="test",
        batch_size=5,
        flush_interval_seconds=1.0,
    )
    
    await writer.start()
    
    # Write records
    for i in range(12):
        await writer.write({"id": i})
    
    await asyncio.sleep(2)
    await writer.stop()
    
    stats = writer.get_stats()
    assert stats["messages_received"] == 12
    assert stats["messages_written"] == 12
    assert stats["flushes"] >= 2  # At least 2 flushes for 12 records with batch_size=5
