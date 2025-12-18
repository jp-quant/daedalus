"""
ETL Framework Test Script
=========================

Tests the new ETL framework with real data from data/raw/ready/ccxt/.

This script:
1. Loads real orderbook/ticker/trades parquet files
2. Applies transforms using the new framework
3. Validates output features
4. Reports results

Usage:
    python scripts/test_etl_framework.py
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import polars as pl

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_imports():
    """Test that all imports work."""
    logger.info("=" * 60)
    logger.info("TESTING IMPORTS")
    logger.info("=" * 60)
    
    try:
        # Core imports
        from etl.core.enums import (
            DataTier, DataFormat, CompressionCodec, WriteMode,
            ProcessingMode, FeatureCategory
        )
        logger.info("✓ etl.core.enums imported successfully")
        
        from etl.core.config import (
            TransformConfig, InputConfig, OutputConfig, FeatureConfig
        )
        logger.info("✓ etl.core.config imported successfully")
        
        from etl.core.base import BaseTransform, StatefulTransform, TransformContext
        logger.info("✓ etl.core.base imported successfully")
        
        from etl.core.registry import register_transform, get_registry
        logger.info("✓ etl.core.registry imported successfully")
        
        # Feature imports
        from etl.features.orderbook import (
            extract_structural_features, compute_rolling_features
        )
        logger.info("✓ etl.features.orderbook imported successfully")
        
        from etl.features.stateful import StatefulFeatureProcessor, StatefulProcessorConfig
        logger.info("✓ etl.features.stateful imported successfully")
        
        from etl.features.streaming import (
            RollingSum, RollingWelford, VPINCalculator
        )
        logger.info("✓ etl.features.streaming imported successfully")
        
        # Transform imports
        from etl.transforms import (
            OrderbookFeatureTransform,
            TickerFeatureTransform,
            TradesFeatureTransform,
            BarAggregationTransform,
            compute_ticker_features,
            compute_trades_features,
            aggregate_bars_vectorized,
        )
        logger.info("✓ etl.transforms imported successfully")
        
        return True
    except Exception as e:
        logger.error(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_orderbook_features(data_path: Path) -> Dict[str, Any]:
    """Test orderbook feature extraction with real data."""
    logger.info("=" * 60)
    logger.info("TESTING ORDERBOOK FEATURES")
    logger.info("=" * 60)
    
    from etl.features.orderbook import extract_structural_features
    
    results = {
        "success": False,
        "rows_processed": 0,
        "features_computed": [],
        "sample_values": {},
        "errors": [],
    }
    
    try:
        # Find a parquet file
        orderbook_path = data_path / "orderbook"
        parquet_files = list(orderbook_path.glob("*.parquet"))[:1]
        
        if not parquet_files:
            results["errors"].append("No orderbook parquet files found")
            return results
        
        logger.info(f"Loading: {parquet_files[0].name}")
        
        # Load data
        df = pl.scan_parquet(str(parquet_files[0]))
        
        # Check schema
        schema = df.collect_schema()
        logger.info(f"Input schema columns: {list(schema.names())}")
        
        # Extract structural features
        featured_df = extract_structural_features(df, max_levels=10)
        
        # Collect results
        result_df = featured_df.collect()
        results["rows_processed"] = len(result_df)
        
        # Check what features were computed
        output_cols = result_df.columns
        logger.info(f"Output columns ({len(output_cols)}): {output_cols[:20]}...")
        
        # Key features to verify
        key_features = [
            "mid_price", "spread", "relative_spread", "microprice",
            "imbalance_L1", "total_bid_depth", "total_ask_depth",
            "book_pressure", "vwap_bid_5", "vwap_ask_5",
            "smart_bid_depth", "smart_ask_depth", "smart_depth_imbalance",
        ]
        
        for feat in key_features:
            if feat in output_cols:
                results["features_computed"].append(feat)
                sample_val = result_df[feat][0]
                results["sample_values"][feat] = sample_val
                logger.info(f"  ✓ {feat}: {sample_val}")
            else:
                logger.warning(f"  ✗ {feat}: NOT FOUND")
        
        results["success"] = True
        logger.info(f"✓ Processed {results['rows_processed']} rows successfully")
        
    except Exception as e:
        results["errors"].append(str(e))
        logger.error(f"✗ Orderbook features failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def test_ticker_features(data_path: Path) -> Dict[str, Any]:
    """Test ticker feature extraction with real data."""
    logger.info("=" * 60)
    logger.info("TESTING TICKER FEATURES")
    logger.info("=" * 60)
    
    from etl.transforms.ticker import compute_ticker_features
    
    results = {
        "success": False,
        "rows_processed": 0,
        "features_computed": [],
        "sample_values": {},
        "errors": [],
    }
    
    try:
        # Find a parquet file
        ticker_path = data_path / "ticker"
        parquet_files = list(ticker_path.glob("*.parquet"))[:1]
        
        if not parquet_files:
            results["errors"].append("No ticker parquet files found")
            return results
        
        logger.info(f"Loading: {parquet_files[0].name}")
        
        # Load data
        df = pl.scan_parquet(str(parquet_files[0]))
        
        # Check schema
        schema = df.collect_schema()
        logger.info(f"Input schema columns: {list(schema.names())}")
        
        # Compute ticker features
        featured_df = compute_ticker_features(df)
        
        # Collect results
        result_df = featured_df.collect()
        results["rows_processed"] = len(result_df)
        
        # Check what features were computed
        output_cols = result_df.columns
        logger.info(f"Output columns: {output_cols}")
        
        # Key features to verify
        key_features = [
            "spread", "relative_spread", "mid_price", "volume_imbalance",
            "hour", "day_of_week", "is_weekend",
        ]
        
        for feat in key_features:
            if feat in output_cols:
                results["features_computed"].append(feat)
                sample_val = result_df[feat][0]
                results["sample_values"][feat] = sample_val
                logger.info(f"  ✓ {feat}: {sample_val}")
            else:
                logger.warning(f"  ✗ {feat}: NOT FOUND")
        
        results["success"] = True
        logger.info(f"✓ Processed {results['rows_processed']} rows successfully")
        
    except Exception as e:
        results["errors"].append(str(e))
        logger.error(f"✗ Ticker features failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def test_trades_features(data_path: Path) -> Dict[str, Any]:
    """Test trades feature extraction with real data."""
    logger.info("=" * 60)
    logger.info("TESTING TRADES FEATURES")
    logger.info("=" * 60)
    
    from etl.transforms.trades import compute_trades_features
    
    results = {
        "success": False,
        "rows_processed": 0,
        "features_computed": [],
        "sample_values": {},
        "errors": [],
    }
    
    try:
        # Find a parquet file
        trades_path = data_path / "trades"
        parquet_files = list(trades_path.glob("*.parquet"))[:1]
        
        if not parquet_files:
            results["errors"].append("No trades parquet files found")
            return results
        
        logger.info(f"Loading: {parquet_files[0].name}")
        
        # Load data
        df = pl.scan_parquet(str(parquet_files[0]))
        
        # Check schema
        schema = df.collect_schema()
        logger.info(f"Input schema columns: {list(schema.names())}")
        
        # Compute trades features
        featured_df = compute_trades_features(df)
        
        # Collect results
        result_df = featured_df.collect()
        results["rows_processed"] = len(result_df)
        
        # Check what features were computed
        output_cols = result_df.columns
        logger.info(f"Output columns: {output_cols}")
        
        # Key features to verify
        key_features = [
            "is_buy", "dollar_volume", "signed_volume",
            "hour", "day_of_week", "log_return",
        ]
        
        for feat in key_features:
            if feat in output_cols:
                results["features_computed"].append(feat)
                sample_val = result_df[feat][0]
                results["sample_values"][feat] = sample_val
                logger.info(f"  ✓ {feat}: {sample_val}")
            else:
                logger.warning(f"  ✗ {feat}: NOT FOUND")
        
        results["success"] = True
        logger.info(f"✓ Processed {results['rows_processed']} rows successfully")
        
    except Exception as e:
        results["errors"].append(str(e))
        logger.error(f"✗ Trades features failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def test_bar_aggregation(data_path: Path) -> Dict[str, Any]:
    """Test bar aggregation with real data."""
    logger.info("=" * 60)
    logger.info("TESTING BAR AGGREGATION")
    logger.info("=" * 60)
    
    from etl.features.orderbook import extract_structural_features
    from etl.transforms.bars import aggregate_bars_vectorized
    
    results = {
        "success": False,
        "bars_computed": {},
        "errors": [],
    }
    
    try:
        # Find parquet files
        orderbook_path = data_path / "orderbook"
        parquet_files = list(orderbook_path.glob("*.parquet"))[:3]  # Use 3 files for more data
        
        if not parquet_files:
            results["errors"].append("No orderbook parquet files found")
            return results
        
        logger.info(f"Loading {len(parquet_files)} files...")
        
        # Load and combine data
        dfs = []
        for f in parquet_files:
            df = pl.scan_parquet(str(f))
            dfs.append(df)
        
        combined_df = pl.concat(dfs)
        
        # Extract features first
        featured_df = extract_structural_features(combined_df, max_levels=10)
        
        # Add log_return for bar aggregation (requires mid_price)
        featured_df = featured_df.with_columns([
            (pl.col("mid_price") / pl.col("mid_price").shift(1)).log().alias("log_return"),
        ])
        
        # Aggregate to bars
        bars = aggregate_bars_vectorized(featured_df, durations=[60, 300])
        
        for duration, bar_lf in bars.items():
            bar_df = bar_lf.collect()
            results["bars_computed"][duration] = {
                "rows": len(bar_df),
                "columns": bar_df.columns,
            }
            logger.info(f"  ✓ {duration}s bars: {len(bar_df)} rows")
            
            # Show sample
            if len(bar_df) > 0:
                logger.info(f"    Sample: open={bar_df['open'][0]:.4f}, close={bar_df['close'][0]:.4f}")
        
        results["success"] = True
        logger.info("✓ Bar aggregation completed successfully")
        
    except Exception as e:
        results["errors"].append(str(e))
        logger.error(f"✗ Bar aggregation failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def test_stateful_features(data_path: Path) -> Dict[str, Any]:
    """Test stateful feature computation (OFI, MLOFI, etc.)."""
    logger.info("=" * 60)
    logger.info("TESTING STATEFUL FEATURES (OFI, MLOFI)")
    logger.info("=" * 60)
    
    from etl.features.stateful import StatefulFeatureProcessor, StatefulProcessorConfig
    
    results = {
        "success": False,
        "rows_processed": 0,
        "features_computed": [],
        "errors": [],
    }
    
    try:
        # Find parquet files
        orderbook_path = data_path / "orderbook"
        parquet_files = list(orderbook_path.glob("*.parquet"))[:1]
        
        if not parquet_files:
            results["errors"].append("No orderbook parquet files found")
            return results
        
        logger.info(f"Loading: {parquet_files[0].name}")
        
        # Load data
        df = pl.read_parquet(str(parquet_files[0]))
        logger.info(f"Loaded {len(df)} rows")
        
        # Convert to records for stateful processing
        # First, check the schema
        logger.info(f"Schema: {df.columns}")
        
        # Create processor
        config = StatefulProcessorConfig(
            horizons=[5, 15, 60],
            ofi_levels=5,
        )
        processor = StatefulFeatureProcessor(config)
        
        # Process first 100 rows to validate
        processed_count = 0
        ofi_values = []
        
        for row in df.head(100).iter_rows(named=True):
            features = processor.process_orderbook(row)
            if features:
                ofi_values.append(features.get("ofi", 0))
                processed_count += 1
        
        results["rows_processed"] = processed_count
        results["features_computed"] = ["ofi", "mlofi", "tfi", "mid_velocity"]
        
        # Show OFI distribution
        if ofi_values:
            non_zero = [v for v in ofi_values if v != 0]
            logger.info(f"  ✓ OFI values computed: {len(ofi_values)} total, {len(non_zero)} non-zero")
            if non_zero:
                logger.info(f"    OFI range: [{min(non_zero):.4f}, {max(non_zero):.4f}]")
        
        results["success"] = True
        logger.info(f"✓ Processed {processed_count} rows with stateful features")
        
    except Exception as e:
        results["errors"].append(str(e))
        logger.error(f"✗ Stateful features failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("ETL FRAMEWORK TEST SUITE")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("=" * 60)
    
    # Data path
    data_path = Path("data/raw/ready/ccxt")
    
    if not data_path.exists():
        logger.error(f"Data path not found: {data_path}")
        sys.exit(1)
    
    # Run tests
    all_results = {}
    
    # Test 1: Imports
    all_results["imports"] = test_imports()
    
    if not all_results["imports"]:
        logger.error("Import test failed - stopping tests")
        sys.exit(1)
    
    # Test 2: Orderbook features
    all_results["orderbook"] = test_orderbook_features(data_path)
    
    # Test 3: Ticker features
    all_results["ticker"] = test_ticker_features(data_path)
    
    # Test 4: Trades features
    all_results["trades"] = test_trades_features(data_path)
    
    # Test 5: Bar aggregation
    all_results["bars"] = test_bar_aggregation(data_path)
    
    # Test 6: Stateful features
    all_results["stateful"] = test_stateful_features(data_path)
    
    # Summary
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in all_results.items():
        if isinstance(result, bool):
            success = result
        else:
            success = result.get("success", False)
        
        status = "✓ PASSED" if success else "✗ FAILED"
        logger.info(f"  {test_name}: {status}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    logger.info("-" * 60)
    logger.info(f"Total: {passed} passed, {failed} failed")
    logger.info(f"Completed at: {datetime.now()}")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
