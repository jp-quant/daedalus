"""Test StreamingParquetWriter compression benefits."""
import json
import io
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow.parquet as pq
from ingestion.writers.parquet_writer import StreamingParquetWriter

def test_compression_ratio():
    """Calculate actual space savings between NDJSON and Parquet."""
    ndjson_file = Path("data/raw/ready/ccxt/segment_20251213T08_00001.ndjson")
    
    if not ndjson_file.exists():
        print(f"Test file not found: {ndjson_file}")
        return
    
    # Load ALL records
    ndjson_size = 0
    records_by_type = {"ticker": [], "trades": [], "orderbook": []}
    
    with open(ndjson_file, "r") as f:
        for line in f:
            ndjson_size += len(line.encode("utf-8"))
            rec = json.loads(line)
            rtype = rec.get("type")
            if rtype in records_by_type:
                records_by_type[rtype].append(rec)
    
    print(f"NDJSON file size: {ndjson_size / 1024 / 1024:.2f} MB")
    print(f"Records: ticker={len(records_by_type['ticker'])}, "
          f"trades={len(records_by_type['trades'])}, "
          f"orderbook={len(records_by_type['orderbook'])}")
    print()
    
    # Convert to parquet and measure
    writer = StreamingParquetWriter.__new__(StreamingParquetWriter)
    total_parquet_size = 0
    
    for rtype, recs in records_by_type.items():
        if not recs:
            continue
        
        if rtype == "ticker":
            table = writer._convert_ticker_records(recs)
        elif rtype == "trades":
            table = writer._convert_trades_records(recs)
        elif rtype == "orderbook":
            table = writer._convert_orderbook_records(recs)
        
        # Write to buffer with zstd compression
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd", compression_level=3)
        parquet_size = buf.tell()
        total_parquet_size += parquet_size
        print(f"{rtype}: {len(recs):,} records -> {parquet_size / 1024:.1f} KB parquet")
    
    print()
    print(f"=" * 50)
    print(f"Total NDJSON:         {ndjson_size / 1024 / 1024:.2f} MB")
    print(f"Total Parquet (zstd): {total_parquet_size / 1024 / 1024:.2f} MB")
    print(f"Compression ratio:    {ndjson_size / total_parquet_size:.1f}x smaller")
    print(f"=" * 50)
    
    return ndjson_size, total_parquet_size

if __name__ == "__main__":
    test_compression_ratio()
