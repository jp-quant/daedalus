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
from etl.orchestrators.ccxt_segment_pipeline import CcxtSegmentPipeline
from etl.features.state import StateConfig

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_config_flow():
    """Test that processor_options flow from config.yaml to processors."""
    
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
        'max_levels',
        'ofi_levels', 
        'bands_bps',
        'hf_emit_interval',
        'bar_durations',
        'horizons',
        'keep_raw_arrays',
        'tight_spread_threshold'
    ]
    
    # Verify each parameter
    missing_params = []
    for param in expected_params:
        if param in processor_options:
            logger.info(f"✓ {param}: {processor_options[param]}")
        else:
            logger.warning(f"✗ {param}: not set (will use default)")
            missing_params.append(param)
    
    # Test StateConfig creation
    logger.info("\nTesting StateConfig creation...")
    try:
        # Filter parameters to match StateConfig
        state_config_params = {
            k: v for k, v in processor_options.items()
            if k in StateConfig.__annotations__
        }
        logger.info(f"Filtered StateConfig params: {state_config_params}")
        
        state_config = StateConfig(**state_config_params)
        logger.info(f"✓ StateConfig created successfully:")
        logger.info(f"  - max_levels: {state_config.max_levels}")
        logger.info(f"  - ofi_levels: {state_config.ofi_levels}")
        logger.info(f"  - hf_emit_interval: {state_config.hf_emit_interval}")
        logger.info(f"  - bar_durations: {state_config.bar_durations}")
        logger.info(f"  - horizons: {state_config.horizons}")
        logger.info(f"  - bands_bps: {state_config.bands_bps}")
        logger.info(f"  - keep_raw_arrays: {state_config.keep_raw_arrays}")
        logger.info(f"  - tight_spread_threshold: {state_config.tight_spread_threshold}")
        
    except Exception as e:
        logger.error(f"✗ Failed to create StateConfig: {e}")
        return False
    
    # Test processor initialization with config
    logger.info("\nTesting processor initialization...")
    try:
        from etl.processors.ccxt.advanced_orderbook_processor import CcxtAdvancedOrderbookProcessor
        
        processor = CcxtAdvancedOrderbookProcessor(**processor_options)
        logger.info("✓ CcxtAdvancedOrderbookProcessor initialized successfully")
        logger.info(f"  Processor config: {processor.config}")
        
        # Verify the values match what we set in config.yaml
        # Research-optimized defaults (Dec 2025)
        assert processor.config.max_levels == 20, f"Expected max_levels=20, got {processor.config.max_levels}"
        assert processor.config.ofi_levels == 10, f"Expected ofi_levels=10, got {processor.config.ofi_levels}"
        assert processor.config.ofi_decay_alpha == 0.5, f"Expected ofi_decay_alpha=0.5, got {processor.config.ofi_decay_alpha}"
        assert processor.config.hf_emit_interval == 1.0, f"Expected hf_emit_interval=1.0, got {processor.config.hf_emit_interval}"
        assert processor.config.bar_durations == [60, 300, 900, 3600], f"Expected bar_durations=[60,300,900,3600], got {processor.config.bar_durations}"
        assert processor.config.horizons == [5, 15, 60, 300, 900], f"Expected horizons=[5,15,60,300,900], got {processor.config.horizons}"
        assert processor.config.bands_bps == [5, 10, 25, 50, 100], f"Expected bands_bps with 100, got {processor.config.bands_bps}"
        assert processor.config.use_dynamic_spread_regime == True, f"Expected use_dynamic_spread_regime=True"
        assert processor.config.kyle_lambda_window == 300, f"Expected kyle_lambda_window=300"
        logger.info("✓ All configuration values verified (research-optimized)")
        
        # Log new research features
        logger.info("\n--- NEW RESEARCH FEATURES ---")
        logger.info(f"  ofi_decay_alpha: {processor.config.ofi_decay_alpha} (MLOFI decay)")
        logger.info(f"  use_dynamic_spread_regime: {processor.config.use_dynamic_spread_regime}")
        logger.info(f"  spread_regime_window: {processor.config.spread_regime_window}s")
        logger.info(f"  kyle_lambda_window: {processor.config.kyle_lambda_window}s")
        logger.info(f"  vpin_bucket_count: {processor.config.vpin_bucket_count}")
        
    except Exception as e:
        logger.error(f"✗ Failed to initialize processor: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    logger.info("\n" + "="*60)
    logger.info("Configuration flow test PASSED ✓")
    logger.info("="*60)
    return True

if __name__ == "__main__":
    success = test_config_flow()
    sys.exit(0 if success else 1)
