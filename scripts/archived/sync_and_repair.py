#!/usr/bin/env python3
"""
Sync & Repair — Pull data from cloud, validate, and fix corrupted parquet files.

Usage:
    python scripts/archived/sync_and_repair.py                    # Full sync + repair
    python scripts/archived/sync_and_repair.py --repair-only      # Skip sync, just repair
    python scripts/archived/sync_and_repair.py --sync-only        # Sync only, skip repair
    python scripts/archived/sync_and_repair.py --dry-run          # Preview what would happen
    python scripts/archived/sync_and_repair.py --exchange coinbaseadvanced  # Filter by exchange

Workflow:
    1. Sync: Pull new/missing files from S3 → local (skip_existing=True)
    2. Repair: Validate all local parquets; re-download corrupted ones from S3;
       delete unrecoverable files from both local and S3.
"""
import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

# Project imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config import load_config
from storage.sync import StorageSync
from storage.factory import create_sync_source_storage, create_sync_destination_storage

logger = logging.getLogger(__name__)

# ─── Default parameters (edit these) ────────────────────────────────────────
SYNC_PATHS = [
    "raw/ready/ccxt/orderbook/",
    "raw/ready/ccxt/trades/",
    "raw/ready/ccxt/ticker/",
]

# Append exchange filter to each path (empty string = all exchanges)
EXCHANGE_SUFFIX = ""  # e.g. "exchange=coinbaseadvanced/"

# Parallelism
SYNC_MAX_WORKERS = 50
REPAIR_MAX_WORKERS = 32

# Config
CONFIG_PATH = "config/config.yaml"


def fast_validate_parquet(file_path: str) -> bool:
    """Read just the parquet footer to validate file integrity (10-100x faster than full read)."""
    try:
        pq.read_metadata(file_path)
        return True
    except Exception:
        return False


def validate_and_repair(
    file_info: dict,
    local_storage,
    s3_storage,
    sync: StorageSync,
    dry_run: bool = False,
) -> dict | None:
    """
    Validate a single parquet file and attempt repair if corrupted.

    Returns:
        None if file is OK, or a dict with status/path/error for reporting.
    """
    rel_path = str(file_info["path"])
    full_path = local_storage.get_full_path(rel_path)

    if fast_validate_parquet(full_path):
        return None

    if dry_run:
        return {"status": "would_repair", "path": rel_path}

    # Attempt re-download from S3
    if s3_storage.exists(rel_path):
        try:
            sync._transfer_file(src_path=rel_path, dst_path=rel_path, delete_after=False)
        except Exception as e:
            logger.warning(f"Re-download failed for {rel_path}: {e}")

        if fast_validate_parquet(full_path):
            return {"status": "repaired", "path": rel_path}

        # Still broken in S3 — delete from both
        try:
            s3_storage.delete(rel_path)
        except Exception:
            pass
        try:
            local_storage.delete(rel_path)
        except Exception:
            pass
        return {"status": "deleted_both", "path": rel_path, "error": "corrupted_in_s3"}

    # Not in S3, just delete local
    try:
        local_storage.delete(rel_path)
    except Exception:
        pass
    return {"status": "deleted_local", "path": rel_path, "error": "not_in_s3"}


def run_sync(
    sync: StorageSync,
    paths: list[str],
    max_workers: int,
    dry_run: bool,
):
    """Sync files from S3 → local."""
    print(f"\n{'='*80}")
    print("SYNC: S3 → Local")
    print(f"{'='*80}")

    for sync_path in paths:
        print(f"\n  Syncing: {sync_path}")
        stats = sync.sync(
            source_path=sync_path,
            dest_path=sync_path,
            pattern="**/*.parquet",
            recursive_list_files=False,
            delete_after_transfer=False,
            max_workers=max_workers,
            skip_existing=True,
            dry_run=dry_run,
        )
        print(f"  {stats}")


def run_repair(
    local_storage,
    s3_storage,
    sync: StorageSync,
    paths: list[str],
    max_workers: int,
    dry_run: bool,
):
    """Validate and repair all local parquet files."""
    print(f"\n{'='*80}")
    print("REPAIR: Validate & Fix Corrupted Files")
    print(f"{'='*80}")

    total_checked = 0
    total_repaired = 0
    total_deleted = 0

    for sync_path in paths:
        print(f"\n  Checking: {sync_path}")
        parquet_files = local_storage.list_files(sync_path, pattern="**/*.parquet")
        total = len(parquet_files)
        print(f"  Found {total} parquet files")

        if total == 0:
            continue

        repaired = []
        deleted = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    validate_and_repair, pf, local_storage, s3_storage, sync, dry_run
                ): pf
                for pf in parquet_files
            }
            for future in tqdm(as_completed(futures), total=total, desc="  Validating"):
                result = future.result()
                if result:
                    if result["status"] == "repaired":
                        repaired.append(result)
                    elif result["status"] == "would_repair":
                        repaired.append(result)
                    else:
                        deleted.append(result)

        total_checked += total
        total_repaired += len(repaired)
        total_deleted += len(deleted)

        if repaired:
            print(f"  Repaired: {len(repaired)}")
        if deleted:
            print(f"  Deleted (unrecoverable): {len(deleted)}")
            for d in deleted:
                print(f"    - {d['path']} ({d.get('error', '')})")

    print(f"\n{'='*80}")
    print(f"REPAIR SUMMARY")
    print(f"  Total files checked: {total_checked}")
    print(f"  Repaired from S3:    {total_repaired}")
    print(f"  Deleted:             {total_deleted}")
    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(description="Sync cloud data locally and repair corrupted parquet files")
    parser.add_argument("--config", default=CONFIG_PATH, help="Config file path")
    parser.add_argument("--sync-only", action="store_true", help="Only sync, skip repair")
    parser.add_argument("--repair-only", action="store_true", help="Only repair, skip sync")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without executing")
    parser.add_argument("--exchange", type=str, default="", help="Filter to a specific exchange (e.g. coinbaseadvanced)")
    parser.add_argument("--sync-workers", type=int, default=SYNC_MAX_WORKERS, help="Sync parallelism")
    parser.add_argument("--repair-workers", type=int, default=REPAIR_MAX_WORKERS, help="Repair parallelism")
    parser.add_argument("--paths", nargs="+", default=SYNC_PATHS, help="Override sync paths")
    args = parser.parse_args()

    # Setup
    config = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format=config.log_format,
    )

    s3_storage = create_sync_destination_storage(config)
    local_storage = create_sync_source_storage(config)

    # Build paths with optional exchange filter
    exchange_suffix = f"exchange={args.exchange}/" if args.exchange else EXCHANGE_SUFFIX
    paths = [p + exchange_suffix for p in args.paths]

    # Sync direction: S3 → Local
    sync = StorageSync(source=s3_storage, destination=local_storage)

    start = time.time()

    if not args.repair_only:
        run_sync(sync, paths, args.sync_workers, args.dry_run)

    if not args.sync_only:
        run_repair(local_storage, s3_storage, sync, paths, args.repair_workers, args.dry_run)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
