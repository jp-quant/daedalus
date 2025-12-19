"""
Migration script for schema changes in ETL output.

Use this when you've updated ETL logic and need to reprocess old data
to match the new schema.

Usage:
    # Delete old files and reprocess everything
    python scripts/migrate_schema.py --delete-old --reprocess
    
    # Archive old files and reprocess
    python scripts/migrate_schema.py --archive --reprocess
    
    # Just show what would be done (dry run)
    python scripts/migrate_schema.py --dry-run
"""
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config


def migrate_schema(
    source: str = "ccxt",
    channels: list[str] = None,
    action: str = "delete",  # delete, archive, or dry-run
    reprocess: bool = False
):
    """
    Migrate processed data to new schema.
    
    Args:
        source: Data source (coinbase, ccxt)
        channels: List of channels to migrate (None = all)
        action: What to do with old files (delete, archive, dry-run)
        reprocess: Whether to reprocess from raw data after migration
    """
    config = load_config("config/config.yaml")
    processed_dir = Path(config.etl.output_dir)
    source_dir = processed_dir / source
    
    if not source_dir.exists():
        print(f"❌ Source directory not found: {source_dir}")
        return
    
    # Get channels to migrate
    if channels is None:
        channels = [d.name for d in source_dir.iterdir() if d.is_dir()]
    
    print(f"\n{'='*60}")
    print(f"Schema Migration for {source}")
    print(f"{'='*60}")
    print(f"Action: {action}")
    print(f"Channels: {', '.join(channels)}")
    print(f"Reprocess after migration: {reprocess}")
    print(f"{'='*60}\n")
    
    # Process each channel
    for channel in channels:
        channel_dir = source_dir / channel
        
        if not channel_dir.exists():
            print(f"⚠️  Channel directory not found: {channel_dir}")
            continue
        
        # Count files and size
        parquet_files = list(channel_dir.rglob("*.parquet"))
        total_size = sum(f.stat().st_size for f in parquet_files)
        size_mb = total_size / 1024**2
        
        print(f"\n📊 {channel}/")
        print(f"   Files: {len(parquet_files)}")
        print(f"   Size: {size_mb:.1f} MB")
        
        if action == "dry-run":
            print(f"   [DRY RUN] Would {action} this directory")
            continue
        
        if action == "delete":
            if input(f"   ⚠️  Delete all files in {channel}/? (yes/no): ").lower() != "yes":
                print(f"   ⏭️  Skipped")
                continue
            
            shutil.rmtree(channel_dir)
            channel_dir.mkdir(parents=True)
            print(f"   ✅ Deleted all files")
        
        elif action == "archive":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = processed_dir.parent / f"processed_archive_{timestamp}" / source / channel
            archive_dir.parent.mkdir(parents=True, exist_ok=True)
            
            print(f"   📦 Archiving to: {archive_dir}")
            shutil.move(str(channel_dir), str(archive_dir))
            channel_dir.mkdir(parents=True)
            print(f"   ✅ Archived")
    
    print(f"\n{'='*60}")
    print("Migration complete!")
    print(f"{'='*60}\n")
    
    # Optionally reprocess
    if reprocess and action != "dry-run":
        print("\n🔄 Starting reprocessing from raw data...\n")
        
        import subprocess
        result = subprocess.run(
            [sys.executable, "scripts/run_etl.py"],
            cwd=Path(__file__).parent.parent
        )
        
        if result.returncode == 0:
            print("\n✅ Reprocessing complete!")
        else:
            print("\n❌ Reprocessing failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate processed data to new schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (see what would be done)
  python scripts/migrate_schema.py --dry-run
  
  # Delete old level2 data and reprocess
  python scripts/migrate_schema.py --delete-old --channels level2 --reprocess
  
  # Archive all old data and reprocess everything
  python scripts/migrate_schema.py --archive --reprocess
  
Schema Version History:
  v1.0.0: Initial schema (year, month, day, hour, date)
  v1.1.0: Added minute, second, microsecond fields (2025-11-26)
        """
    )
    
    parser.add_argument(
        "--source",
        default="coinbase",
        help="Data source to migrate (default: coinbase)"
    )
    
    parser.add_argument(
        "--channels",
        nargs="+",
        help="Channels to migrate (default: all channels)"
    )
    
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        "--delete-old",
        action="store_const",
        const="delete",
        dest="action",
        help="Delete old files"
    )
    action_group.add_argument(
        "--archive",
        action="store_const",
        const="archive",
        dest="action",
        help="Archive old files to processed_archive_TIMESTAMP/"
    )
    action_group.add_argument(
        "--dry-run",
        action="store_const",
        const="dry-run",
        dest="action",
        help="Show what would be done without making changes"
    )
    
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Reprocess from raw data after migration"
    )
    
    args = parser.parse_args()
    
    migrate_schema(
        source=args.source,
        channels=args.channels,
        action=args.action,
        reprocess=args.reprocess
    )
