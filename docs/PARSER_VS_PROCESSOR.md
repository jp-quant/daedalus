# Parser vs Processor Architecture

## The Confusion

You're right to question this! Let me clarify the roles:

## Architecture Layers

```
Raw NDJSON
    ↓
┌─────────────────────────────────────────────┐
│ PARSERS (etl/parsers/)                      │
│ Source-specific message extraction          │
│ - coinbase_parser.py                        │
│ - databento_parser.py (future)              │
│ - ibkr_parser.py (future)                   │
└─────────────────────────────────────────────┘
    ↓
Structured Records (still source-specific schema)
    ↓
┌─────────────────────────────────────────────┐
│ PROCESSORS (etl/processors/)                │
│ Channel & source-specific transformations   │
│ - coinbase_level2_processor.py              │
│ - coinbase_trades_processor.py              │
│ - coinbase_ticker_processor.py              │
│ - databento_trades_processor.py (future)    │
└─────────────────────────────────────────────┘
    ↓
Enhanced/Normalized Records
    ↓
Parquet Files
```

## Detailed Roles

### 1. Parsers (etl/parsers/)

**Purpose**: Extract structured data from source-specific message formats

**Characteristics**:
- **Source-specific** (one per data source)
- **Protocol-aware** (understand WebSocket message structure)
- **Extraction only** (no transformation/enrichment)
- **Multi-channel** (one parser handles all channels from a source)

**Example - CoinbaseParser**:
```python
# Input: Raw NDJSON from ingestion
{
    "capture_ts": "2025-11-26T14:30:00Z",
    "data": {
        "channel": "level2",
        "events": [{
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [...]
        }]
    }
}

# Output: Extracted structured records
[{
    "channel": "level2",
    "event_type": "snapshot",
    "product_id": "BTC-USD",
    "side": "bid",
    "price_level": 50000.0,
    "new_quantity": 1.5,
    "sequence_num": 12345,
    "server_timestamp": "...",
    "capture_timestamp": "..."
}]
```

### 2. RawParser Processor (Bridge)

**Purpose**: Connect readers to source-specific parsers

**Why it exists**:
- Processors expect a standard interface
- Parsers are source-specific
- RawParser acts as an adapter

```python
# RawParser is just a thin wrapper
class RawParser(BaseProcessor):
    def __init__(self, source: str, channel: Optional[str] = None):
        if source == "coinbase":
            self.parser = CoinbaseParser()
        elif source == "databento":
            self.parser = DatabentoParser()
        # ...
```

### 3. Channel Processors (etl/processors/)

**Purpose**: Transform extracted data into analytics-ready format

**Characteristics**:
- **Source & channel specific** (etl.processors.coinbase.Level2Processor, etl.processors.databento.TradesProcessor)
- **Add derived fields** (mid-price, spread, date partitions)
- **Reconstruct state** (orderbook, VWAP)
- **Compute features** (microstructure, imbalance)
- **Normalize for querying** (consistent schema, partitioning)

**Example - Level2Processor (Coinbase)**:
```python
# Input: Parsed Coinbase level2 record
{
    "channel": "level2",
    "product_id": "BTC-USD",
    "price_level": 50000.0,
    "event_time": "2025-11-26T14:30:00Z"
}

# Output: Enhanced with derived fields
{
    "channel": "level2",
    "product_id": "BTC-USD",
    "price_level": 50000.0,
    "event_time": "2025-11-26T14:30:00Z",
    "date": "2025-11-26",          # Added for partitioning
    "year": 2025,                  # Added for partitioning
    "month": 11,
    "day": 26,
    "hour": 14,
    "mid_price": 50005.0,          # Computed from orderbook
    "spread": 10.0,                # Computed
    "spread_bps": 2.0,             # Computed
}
```

## Why Separate?

### Parsers Handle Source Differences

**Coinbase level2**:
```json
{
  "channel": "level2",
  "events": [{
    "type": "snapshot",
    "updates": [{"side": "offer", "price_level": "50000", "new_quantity": "1.5"}]
  }]
}
```

**Databento level2** (different format):
```json
{
  "ts_event": 1701234567890,
  "action": "A",  // Add
  "side": "B",    // Bid
  "price": 5000000000,  // Fixed-point
  "size": 150
}
```

**Role of Parser**: Normalize these into a common intermediate format

### Processors Handle Domain Logic

Even after parsing, Coinbase and Databento level2 data might need different processing:

**Coinbase**:
- Sequence numbers for ordering
- Snapshot vs update handling
- Product-specific conventions

**Databento**:
- Nanosecond timestamps
- Fixed-point price representation
- Symbol mapping (AAPL.XNAS → AAPL)

## File Organization

### Current Structure (Correct!)

```
etl/
├── parsers/                           # Source-specific extraction
│   ├── coinbase_parser.py            # Handles ALL Coinbase channels
│   ├── databento_parser.py (future)   # Handles ALL Databento schemas
│   └── ibkr_parser.py (future)        # Handles ALL IBKR contracts
│
├── processors/
│   ├── base_processor.py              # Abstract interface
│   ├── raw_parser.py                  # Bridge to parsers
│   │
│   ├── coinbase_level2_processor.py   # Coinbase LOB specific
│   ├── coinbase_trades_processor.py   # Coinbase trades specific
│   ├── coinbase_ticker_processor.py   # Coinbase ticker specific
│   │
│   ├── databento_trades_processor.py (future)
│   ├── databento_ohlcv_processor.py (future)
│   │
│   └── custom_vwap_processor.py (user-defined)
```

## Adding New Sources

### Step 1: Create Parser

```python
# etl/parsers/databento_parser.py
class DatabentoParser:
    """Extract structured records from Databento messages."""
    
    def parse_record(self, record: dict):
        # Handle Databento-specific format
        # Return normalized records
        pass
```

### Step 2: Create Processors (per channel/schema)

```python
# etl/processors/databento_trades_processor.py
class DatabentoTradesProcessor(BaseProcessor):
    """Process Databento trade messages."""
    
    def process_record(self, record: dict) -> dict:
        # Add derived fields
        # Normalize timestamps
        # Add partitioning columns
        pass
```

### Step 3: Wire in CoinbaseSegmentPipeline

```python
# etl/orchestrators/coinbase_segment_pipeline.py
def _get_processor_class(self, channel: str):
    # Coinbase-specific processor routing
    from etl.processors.coinbase import Level2Processor, TradesProcessor, TickerProcessor
    
    processors = {
        "level2": Level2Processor,
        "market_trades": TradesProcessor,
        }
    elif self.source == "databento":
        processors = {
            "trades": DatabentoTradesProcessor,
            "ohlcv": DatabentoOHLCVProcessor,
        }
    return processors.get(channel)
```

## Summary

| Component | Scope | Purpose | Example |
|-----------|-------|---------|---------|  
| **Parser** | Source | Extract from protocol | `CoinbaseParser` |
| **RawParser** | Bridge | Connect reader→parser | Routes to `CoinbaseParser` |
| **Processor** | Source+Channel | Transform & enrich | `etl.processors.coinbase.Level2Processor` |

## Key Insight

- **Parsers** = "What did the exchange send?"
- **Processors** = "What do I want to store?"

Different exchanges send data differently (parsers handle this).
Different data types need different features (processors handle this).

## Your Observation Was Correct!

The processors ARE Coinbase-specific, which is why we organized them:
- ❌ `etl/processors/level2_processor.py` (misleading - implies generic)
- ✅ `etl/processors/coinbase/level2_processor.py` (correct - explicit namespace)

This makes it clear that Databento would need `etl/processors/databento/level2_processor.py` with its own logic.
