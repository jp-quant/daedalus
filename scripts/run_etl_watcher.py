"""Continuous ETL watcher - processes segments as they become ready."""
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Change to parent directory to fix relative imports
os.chdir(Path(__file__).parent.parent)

from config import load_config
from storage.factory import (
    create_etl_storage_input,
    create_etl_storage_output,
    get_etl_input_path,
    get_etl_output_path,
    get_processing_path
)
from etl.job import ETLJob


logger = logging.getLogger(__name__)


class ContinuousETLWatcher:
    """
    Watches ready/ directory and processes segments as they appear.
    
    This allows near-real-time ETL without manual intervention.
    """
    
    def __init__(self, config_path: str = None, poll_interval: int = 30):
        """
        Initialize continuous ETL watcher.
        
        Args:
            config_path: Path to config file
            poll_interval: Seconds between directory scans
        """
        self.config = load_config(config_path)
        self.poll_interval = poll_interval
        self._shutdown_event = asyncio.Event()
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format=self.config.log_format
        )
        
        # Create storage backends (may be different in hybrid mode)
        self.storage_input = create_etl_storage_input(self.config)
        self.storage_output = create_etl_storage_output(self.config)
        
        # Create ETL job for each configured source
        self.jobs = {}
        if self.config.coinbase:
            # Get paths from config
            input_path = get_etl_input_path(self.config, "coinbase")
            output_path = get_etl_output_path(self.config, "coinbase")
            processing_path = get_processing_path(self.config, "coinbase")
            
            # Convert channel config from Pydantic to dict format
            channel_config = None
            if hasattr(self.config.etl, 'channels') and self.config.etl.channels:
                channel_config = {
                    channel_name: {
                        "partition_cols": channel_cfg.partition_cols,
                        "processor_options": channel_cfg.processor_options,
                    }
                    for channel_name, channel_cfg in self.config.etl.channels.items()
                    if channel_cfg.enabled
                }
            
            self.jobs["coinbase"] = ETLJob(
                storage_input=self.storage_input,
                storage_output=self.storage_output,
                input_path=input_path,
                output_path=output_path,
                source="coinbase",
                delete_after_processing=self.config.etl.delete_after_processing,
                processing_path=processing_path,
                channel_config=channel_config,
            )
        
        if self.config.ccxt and self.config.ccxt.exchanges:
            # Convert channel config from Pydantic to dict format
            channel_config = None
            if hasattr(self.config.etl, 'channels') and self.config.etl.channels:
                channel_config = {
                    channel_name: {
                        "partition_cols": channel_cfg.partition_cols,
                        "processor_options": channel_cfg.processor_options,
                    }
                    for channel_name, channel_cfg in self.config.etl.channels.items()
                    if channel_cfg.enabled
                }

            # Create a single job for unified CCXT source
            source_name = "ccxt"
            
            # Get paths for unified source
            input_path = get_etl_input_path(self.config, source_name)
            output_path = get_etl_output_path(self.config, source_name)
            processing_path = get_processing_path(self.config, source_name)
            
            logger.info(f"Initializing watcher for {source_name} at {input_path}")
            
            self.jobs[source_name] = ETLJob(
                storage_input=self.storage_input,
                storage_output=self.storage_output,
                input_path=input_path,
                output_path=output_path,
                source=source_name,
                delete_after_processing=self.config.etl.delete_after_processing,
                processing_path=processing_path,
                channel_config=channel_config,
            )
    
    async def run(self):
        """Main watcher loop."""
        logger.info("=" * 80)
        logger.info("Daedalus Continuous ETL Watcher Starting")
        logger.info("=" * 80)
        logger.info(f"Storage Input:  {self.storage_input.backend_type} @ {self.storage_input.base_path}")
        logger.info(f"Storage Output: {self.storage_output.backend_type} @ {self.storage_output.base_path}")
        logger.info(f"Poll interval: {self.poll_interval} seconds")
        logger.info(f"Watching sources: {list(self.jobs.keys())}")
        logger.info("=" * 80)
        
        while not self._shutdown_event.is_set():
            try:
                # Process segments for each source
                for source_name, job in self.jobs.items():
                    # List segments in input path
                    try:
                        files = self.storage_input.list_files(
                            path=job.input_path,
                            pattern="segment_*.ndjson"
                        )
                    except Exception as e:
                        logger.error(f"[{source_name}] Failed to list segments: {e}")
                        continue
                    
                    if files:
                        logger.info(
                            f"[{source_name}] Found {len(files)} segment(s) to process"
                        )
                        
                        # Process all available segments
                        success_count = 0
                        for file_info in files:
                            segment_path = file_info["path"]
                            if job.process_segment(Path(segment_path)):
                                success_count += 1
                        
                        logger.info(
                            f"[{source_name}] Processed {success_count}/{len(files)} "
                            "segments successfully"
                        )
                
                # Wait before next poll
                await asyncio.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"Error in watcher loop: {e}", exc_info=True)
                await asyncio.sleep(5)  # Brief pause on error
        
        logger.info("Continuous ETL watcher stopped")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Daedalus Continuous ETL Watcher"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between directory scans (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Create watcher
    watcher = ContinuousETLWatcher(
        config_path=args.config,
        poll_interval=args.poll_interval
    )
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        watcher._shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run watcher
    await watcher.run()


if __name__ == "__main__":
    asyncio.run(main())
