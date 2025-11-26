# Import Patterns Guide

## Recommended Import Patterns

### ✅ Best Practice: Direct Import from Submodule

```python
from etl.processors.coinbase import Level2Processor, TradesProcessor, TickerProcessor
from etl.orchestrators import CoinbaseSegmentPipeline
```

**Why this is best:**
- Import path clearly shows the namespace (`coinbase`)
- Class names are clean (`Level2Processor` not `CoinbaseLevel2Processor`)
- When you see `Level2Processor()` in code, you know to look in `processors.coinbase`
- Follows Python conventions (like `from collections.abc import Mapping`)

### ✅ Alternative: Aliased Import from Main Module

```python
from etl.processors import CoinbaseLevel2Processor, CoinbaseTradesProcessor, CoinbaseTickerProcessor
from etl.orchestrators import CoinbaseSegmentPipeline
```

**When to use:**
- Quick scripts where you don't want to think about module structure
- Code where source-specific nature should be obvious at every usage site
- Backward compatibility with existing code

**Why this works:**
- `etl/processors/__init__.py` aliases: `from .coinbase import Level2Processor as CoinbaseLevel2Processor`
- Makes Coinbase-specific nature explicit in the name
- No namespace ambiguity

## Examples

### Example 1: Building a Pipeline (Recommended)

```python
from etl.readers import NDJSONReader
from etl.processors import RawParser
from etl.processors.coinbase import Level2Processor  # Direct import
from etl.writers import ParquetWriter
from etl.orchestrators import ETLPipeline

pipeline = ETLPipeline(
    reader=NDJSONReader(),
    processors=[
        RawParser(source="coinbase", channel="level2"),
        Level2Processor(compute_features=True),
    ],
    writer=ParquetWriter(),
)
```

### Example 2: Using Segment Pipeline (Recommended)

```python
from etl.orchestrators import CoinbaseSegmentPipeline

pipeline = CoinbaseSegmentPipeline(
    output_dir="data/processed",
    channel_config={
        "level2": {
            "partition_cols": ["product_id", "date"],
            "processor_options": {"compute_mid_price": True},
        },
    },
)
```

### Example 3: Quick Script (Aliased Import)

```python
from etl.processors import CoinbaseLevel2Processor  # Aliased import - explicit

processor = CoinbaseLevel2Processor(reconstruct_lob=True)
```

## What NOT to Do

### ❌ Don't Use Generic Names from Main Module

```python
# This would be confusing if we allowed it:
from etl.processors import Level2Processor  # Which source is this for???
```

**Why we prevent this:**
- `Level2Processor` without context is ambiguous
- Could be Coinbase, Databento, IBKR, etc.
- Our `__init__.py` uses explicit aliases to prevent this

## Module Structure

```
etl/processors/
├── __init__.py              # Exports with aliases
│   from .coinbase import Level2Processor as CoinbaseLevel2Processor
│   from .coinbase import TradesProcessor as CoinbaseTradesProcessor
│   ...
├── base_processor.py
├── raw_parser.py
└── coinbase/                # Coinbase namespace
    ├── __init__.py
    ├── level2_processor.py  # class Level2Processor
    ├── trades_processor.py  # class TradesProcessor
    └── ticker_processor.py  # class TickerProcessor
```

## Import Pattern Summary

| Import Style | Usage | Name in Code | Clarity |
|--------------|-------|--------------|---------|
| `from etl.processors.coinbase import Level2Processor` | ✅ Recommended | `Level2Processor()` | ⭐⭐⭐ Namespace in import |
| `from etl.processors import CoinbaseLevel2Processor` | ✅ Valid | `CoinbaseLevel2Processor()` | ⭐⭐ Explicit in name |
| `from etl.processors import Level2Processor` | ❌ Not Allowed | N/A | ❌ Ambiguous |

## When Adding New Sources

### Databento Example

1. Create `etl/processors/databento/` directory
2. Add `trades_processor.py` with `class TradesProcessor`
3. Update `etl/processors/__init__.py`:
   ```python
   from .databento import TradesProcessor as DatabentoTradesProcessor
   ```
4. Users can then:
   ```python
   from etl.processors.databento import TradesProcessor  # Recommended
   # OR
   from etl.processors import DatabentoTradesProcessor   # Also OK
   ```

## Key Principle

**The source (Coinbase, Databento, etc.) must always be clear, either in:**
1. The import path (`processors.coinbase`), OR
2. The class name (`CoinbaseLevel2Processor`)

**Never allow ambiguous names like `Level2Processor` to be imported directly from the main processors module.**
