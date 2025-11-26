"""Run FluxForge ETL pipeline."""
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Change to parent directory to fix relative imports
os.chdir(Path(__file__).parent.parent)

from config import load_config
from etl.job import ETLJob


logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="FluxForge ETL Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="coinbase",
        choices=["coinbase", "databento", "ibkr"],
        help="Data source to process (default: coinbase)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="all",
        choices=["all", "date", "range"],
        help="Processing mode (default: all)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Process specific date (YYYY-MM-DD) - used with --mode=date"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD) - used with --mode=range"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD) - used with --mode=range (default: today)"
    )
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format=config.log_format
    )
    
    logger.info("=" * 80)
    logger.info("FluxForge ETL Pipeline Starting")
    logger.info("=" * 80)
    logger.info(f"Source: {args.source}")
    logger.info(f"Mode: {args.mode}")
    
    # Convert channel config from Pydantic to dict format
    channel_config = None
    if hasattr(config.etl, 'channels') and config.etl.channels:
        channel_config = {
            channel_name: {
                "partition_cols": channel_cfg.partition_cols,
                "processor_options": channel_cfg.processor_options,
            }
            for channel_name, channel_cfg in config.etl.channels.items()
            if channel_cfg.enabled
        }
    
    # Create ETL job (Coinbase-specific, no source parameter needed)
    job = ETLJob(
        input_dir=config.etl.input_dir,
        output_dir=config.etl.output_dir,
        delete_after_processing=config.etl.delete_after_processing,
        processing_dir=config.etl.processing_dir,
        channel_config=channel_config,
    )
    
    # Execute based on mode
    if args.mode == "all":
        logger.info("Processing all available log files...")
        job.process_all()
    
    elif args.mode == "date":
        if not args.date:
            logger.error("--date is required when using --mode=date")
            sys.exit(1)
        
        try:
            date = datetime.strptime(args.date, "%Y-%m-%d")
            logger.info(f"Processing date: {args.date}")
            job.process_date_range(start_date=date)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)
    
    elif args.mode == "range":
        if not args.start_date:
            logger.error("--start-date is required when using --mode=range")
            sys.exit(1)
        
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            
            if args.end_date:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            else:
                end_date = datetime.now()
            
            logger.info(f"Processing date range: {args.start_date} to {end_date.strftime('%Y-%m-%d')}")
            job.process_date_range(start_date=start_date, end_date=end_date)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)
    
    logger.info("=" * 80)
    logger.info("ETL Pipeline Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
