"""Run Daedalus ingestion pipeline."""
import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config
from ingestion.orchestrators import IngestionPipeline


logger = logging.getLogger(__name__)


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daedalus Ingestion Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=60,
        help="Print stats every N seconds (default: 60)"
    )
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources to run (e.g. 'coinbase,ccxt'). If omitted, runs all configured sources."
    )
    
    args = parser.parse_args()
    
    # Parse sources
    sources = None
    if args.sources:
        sources = [s.strip() for s in args.sources.split(",")]
    
    # Load config
    config = load_config(config_path=args.config)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format=config.log_format
    )
    
    # Create pipeline
    pipeline = IngestionPipeline(config, sources=sources)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        pipeline.request_shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start stats printer
    async def stats_printer():
        while True:
            try:
                await asyncio.sleep(args.stats_interval)
                await pipeline.print_stats()
            except asyncio.CancelledError:
                break
    
    stats_task = asyncio.create_task(stats_printer())
    
    try:
        # Start pipeline
        await pipeline.start()
        
        # Wait for shutdown signal
        await pipeline.wait_for_shutdown()
    
    finally:
        # Cleanup
        stats_task.cancel()
        try:
            await stats_task
        except asyncio.CancelledError:
            pass
        
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())
