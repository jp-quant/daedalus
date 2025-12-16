"""Health check script for Daedalus segment-based system."""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config
from storage.factory import create_ingestion_storage, get_ingestion_path


def check_health(config_path: str = None):
    """Check system health and report status."""
    config = load_config(config_path)
    
    print("=" * 80)
    print("Daedalus Health Check")
    print("=" * 80)
    
    # Create storage backend
    storage = create_ingestion_storage(config)
    print(f"Storage backend: {storage.backend_type}")
    print(f"Base path: {storage.base_path}")
    print()
    
    for source in ["coinbase"]:  # Add more sources as configured
        print(f"\n📊 Source: {source}")
        print("-" * 80)
        
        # Get paths using storage abstraction
        active_path = get_ingestion_path(config, source, state="active")
        ready_path = get_ingestion_path(config, source, state="ready")
        
        # Import processing path helper
        from storage.factory import get_processing_path
        processing_path = get_processing_path(config, source)
        
        # Check active segments
        try:
            active_files = storage.list_files(active_path, pattern="segment_*.ndjson")
            print(f"✓ Active path: {active_path}")
            print(f"  Full path: {storage.get_full_path(active_path)}")
            print(f"  Segments: {len(active_files)}")
            
            if active_files:
                for file_info in active_files:
                    size_mb = file_info["size"] / 1024 / 1024
                    age_sec = datetime.now().timestamp() - file_info["modified"]
                    age_min = int(age_sec / 60)
                    filename = file_info["path"].split("/")[-1]
                    print(f"  - {filename}: {size_mb:.2f} MB (age: {age_min} min)")
                    
                    if size_mb > config.ingestion.segment_max_mb * 0.9:
                        print(f"    ⚠️  Near rotation threshold!")
        except Exception as e:
            print(f"❌ Error checking active path: {e}")
        
        # Check ready segments
        try:
            ready_files = storage.list_files(ready_path, pattern="segment_*.ndjson")
            print(f"\n✓ Ready path: {ready_path}")
            print(f"  Full path: {storage.get_full_path(ready_path)}")
            print(f"  Segments: {len(ready_files)}")
            
            if ready_files:
                total_size_mb = sum(f["size"] for f in ready_files) / 1024 / 1024
                oldest = min(ready_files, key=lambda f: f["modified"])
                oldest_age_sec = datetime.now().timestamp() - oldest["modified"]
                oldest_age_min = int(oldest_age_sec / 60)
                oldest_name = oldest["path"].split("/")[-1]
                
                print(f"  Total size: {total_size_mb:.2f} MB")
                print(f"  Oldest: {oldest_name} ({oldest_age_min} min ago)")
                
                if len(ready_files) > 50:
                    print(f"  ⚠️  Large backlog - ETL falling behind!")
                elif len(ready_files) > 20:
                    print(f"  ⚡ Moderate backlog - consider faster ETL")
            else:
                print(f"  ✓ No backlog - ETL is caught up")
        except Exception as e:
            print(f"❌ Error checking ready path: {e}")
        
        # Check processing segments
        try:
            processing_files = storage.list_files(processing_path, pattern="segment_*.ndjson")
            print(f"\n✓ Processing path: {processing_path}")
            print(f"  Full path: {storage.get_full_path(processing_path)}")
            
            if processing_files:
                print(f"  Segments: {len(processing_files)}")
                for file_info in processing_files:
                    age_sec = datetime.now().timestamp() - file_info["modified"]
                    age_min = int(age_sec / 60)
                    filename = file_info["path"].split("/")[-1]
                    print(f"  - {filename} (age: {age_min} min)")
                    
                    if age_min > 10:
                        print(f"    ⚠️  Stuck in processing? ETL may have crashed")
            else:
                print(f"  ✓ No segments in processing")
        except Exception as e:
            print(f"❌ Error checking processing path: {e}")
    
    # Configuration check
    print(f"\n⚙️  Configuration")
    print("-" * 80)
    print(f"Segment max size: {config.ingestion.segment_max_mb} MB")
    print(f"Batch size: {config.ingestion.batch_size}")
    print(f"Flush interval: {config.ingestion.flush_interval_seconds}s")
    print(f"Delete after processing: {config.etl.delete_after_processing}")
    
    # Recommendations
    print(f"\n💡 Recommendations")
    print("-" * 80)
    
    try:
        ready_path = get_ingestion_path(config, "coinbase", state="ready")
        ready_files = storage.list_files(ready_path, pattern="segment_*.ndjson")
        ready_count = len(ready_files)
        
        if ready_count == 0:
            print("✓ System healthy - no backlog")
        elif ready_count < 10:
            print("✓ System healthy - small backlog")
        elif ready_count < 20:
            print("⚡ Consider running ETL watcher more frequently")
            print("  python scripts/run_etl_watcher.py --poll-interval 15")
        else:
            print("⚠️  Large backlog detected!")
            print("  1. Start continuous ETL watcher:")
            print("     python scripts/run_etl_watcher.py --poll-interval 10")
            print("  2. Or manually process backlog:")
            print("     python scripts/run_etl.py --source coinbase --mode all")
    except Exception as e:
        print(f"⚠️  Could not check backlog: {e}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Daedalus Health Check")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    
    args = parser.parse_args()
    check_health(args.config)
