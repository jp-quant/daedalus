"""Run Daedalus ETL pipeline."""
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
from storage.factory import (
    create_etl_storage_input,
    create_etl_storage_output,
    get_etl_input_path,
    get_etl_output_path,
    get_processing_path
)
from etl.job import ETLJob


logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daedalus ETL Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="ccxt",
        choices=["coinbase", "ccxt"],
        help="Data source to process (default: ccxt)"
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
    logger.info("Daedalus ETL Pipeline Starting")
    logger.info("=" * 80)
    logger.info(f"Source: {args.source}")
    logger.info(f"Mode: {args.mode}")
    
    # Create storage backends
    storage_input = create_etl_storage_input(config)
    storage_output = create_etl_storage_output(config)
    
    # Get paths from config
    input_path = get_etl_input_path(config, args.source)
    output_path = get_etl_output_path(config, args.source)
    processing_path = get_processing_path(config, args.source)
    
    logger.info(f"Storage Input:  {storage_input.backend_type} @ {storage_input.base_path}")
    logger.info(f"Storage Output: {storage_output.backend_type} @ {storage_output.base_path}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Processing path: {processing_path}")
    
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
    
    # Create ETL job
    job = ETLJob(
        storage_input=storage_input,
        storage_output=storage_output,
        input_path=input_path,
        output_path=output_path,
        source=args.source,
        delete_after_processing=config.etl.delete_after_processing,
        processing_path=processing_path,
        channel_config=channel_config,
        compression=config.etl.compression,
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
