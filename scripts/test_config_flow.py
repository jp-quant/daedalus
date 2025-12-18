"""
Test script to verify processor_options configuration flow.
Validates that config.yaml parameters correctly reach the processors.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config import load_config
from etl.features.stateful import StatefulProcessorConfig

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_config_flow_check() -> bool:
    """Run a config-flow sanity check. Returns True on success."""
    
    # Load config
    logger.info("Loading configuration...")
    config = load_config()
    
    # Check ETL config exists
    if not config.etl:
        logger.error("No ETL configuration found")
        return False
        
    # Check orderbook channel config
    orderbook_config = config.etl.channels.get("orderbook")
    if not orderbook_config:
        logger.error("No orderbook channel configuration found")
        return False
        
    logger.info(f"Orderbook config: {orderbook_config}")
    
    # Extract processor_options (orderbook_config is a Pydantic model)
    processor_options = orderbook_config.processor_options if hasattr(orderbook_config, 'processor_options') else {}
    logger.info(f"Processor options from config: {processor_options}")
    
    # Expected parameters
    expected_params = [
        "max_levels",
        "bands_bps",
        "horizons",
        "bar_durations",
        "enable_stateful",
        "ofi_levels",
        "ofi_decay_alpha",
        "use_dynamic_spread_regime",
        "spread_regime_window",
        "spread_tight_percentile",
        "spread_wide_percentile",
        "tight_spread_threshold",
        "kyle_lambda_window",
        "enable_vpin",
        "vpin_bucket_volume",
        "vpin_window_buckets",
    ]
    
    # Verify each parameter
    missing_params = []
    for param in expected_params:
        if param in processor_options:
            logger.info(f"✓ {param}: {processor_options[param]}")
        else:
            logger.warning(f"✗ {param}: not set (will use default)")
            missing_params.append(param)
    
    # Test StatefulProcessorConfig creation (stateful module)
    logger.info("\nTesting StatefulProcessorConfig creation...")
    try:
        stateful_params = {
            k: v for k, v in processor_options.items()
            if k in StatefulProcessorConfig.__dataclass_fields__
        }
        logger.info(f"Filtered StatefulProcessorConfig params: {stateful_params}")

        stateful_config = StatefulProcessorConfig(**stateful_params)
        logger.info("✓ StatefulProcessorConfig created successfully")
        logger.info(f"  - horizons: {stateful_config.horizons}")
        logger.info(f"  - ofi_levels: {stateful_config.ofi_levels}")
        logger.info(f"  - ofi_decay_alpha: {stateful_config.ofi_decay_alpha}")
        logger.info(f"  - spread_regime_window: {stateful_config.spread_regime_window}")
        logger.info(f"  - kyle_lambda_window: {stateful_config.kyle_lambda_window}")
        logger.info(f"  - enable_vpin: {stateful_config.enable_vpin}")
        
    except Exception as e:
        logger.error(f"✗ Failed to create StatefulProcessorConfig: {e}")
        return False
    
    # Sanity check that feature functions can be called with configured parameters
    logger.info("\nTesting orderbook feature module wiring...")
    try:
        from etl.features.orderbook import extract_structural_features, compute_rolling_features

        # Just verify signatures are callable with our configured knobs.
        max_levels = int(processor_options.get("max_levels", 20))
        bands_bps = processor_options.get("bands_bps", [5, 10, 25, 50, 100])
        horizons = processor_options.get("horizons", [5, 15, 60, 300, 900])

        # Minimal dummy schema (won't be executed here)
        import polars as pl
        dummy = pl.LazyFrame(
            {
                "timestamp": [0],
                "capture_ts": ["2025-01-01 00:00:00"],
                "bids": [[{"price": 100.0, "size": 1.0}]],
                "asks": [[{"price": 101.0, "size": 1.0}]],
            }
        ).with_columns(pl.col("capture_ts").str.to_datetime())

        lf = extract_structural_features(dummy, max_levels=max_levels, bands_bps=bands_bps)
        lf = compute_rolling_features(lf, horizons=horizons)
        _ = lf.collect()

        logger.info("✓ Orderbook feature modules executed on dummy data")
        
    except Exception as e:
        logger.error(f"✗ Feature module wiring failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    logger.info("\n" + "="*60)
    logger.info("Configuration flow test PASSED ✓")
    logger.info("="*60)
    return True

if __name__ == "__main__":
    success = run_config_flow_check()
    sys.exit(0 if success else 1)
