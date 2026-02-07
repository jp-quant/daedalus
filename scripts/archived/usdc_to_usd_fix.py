#!/usr/bin/env python3
"""
USDC → USD Migration for Coinbase Advanced — Multi-Backend.

Coinbase Advanced's WebSocket API returns product_id with USD (e.g. BTC-USD)
regardless of whether you subscribe to USDC pairs. Some data was incorrectly
partitioned under symbol=*-USDC. This script consolidates everything back to
the canonical USD symbols.

For each parquet file in coinbaseadvanced symbol=*-USDC partitions:
  1. Reads the file via StorageBackend.read_bytes()
  2. Rewrites the 'symbol' column: e.g. 'BTC-USDC' → 'BTC-USD'
  3. Writes to the correct symbol=*-USD path via StorageBackend.write_bytes()
  4. Deletes originals (batch_delete for S3 efficiency)
  5. Cleans up empty directories (local only — S3 has no real dirs)

Usage:
    # Local storage (default)
    python scripts/archived/usdc_to_usd_fix.py --backend local
    python scripts/archived/usdc_to_usd_fix.py --backend local --dry-run

    # S3 storage
    python scripts/archived/usdc_to_usd_fix.py --backend s3
    python scripts/archived/usdc_to_usd_fix.py --backend s3 --dry-run

    # Filter by channel
    python scripts/archived/usdc_to_usd_fix.py --backend s3 --channel orderbook

    # Adjust parallelism (higher for S3, network-bound)
    python scripts/archived/usdc_to_usd_fix.py --backend s3 --workers 32
"""
import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

# Project imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DaedalusConfig, load_config
from storage.base import StorageBackend, LocalStorage
from storage.factory import _create_backend_from_layer_config

logger = logging.getLogger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────
EXCHANGE = "coinbaseadvanced"
CHANNELS = ["orderbook", "trades", "ticker"]
DATA_PREFIX = "raw/ready/ccxt"  # Relative to storage root
DEFAULT_WORKERS_LOCAL = 16
DEFAULT_WORKERS_S3 = 32


# ─── Storage factory ────────────────────────────────────────────────────────

def create_storage(config: DaedalusConfig, backend: str) -> StorageBackend:
    """
    Create storage backend from config based on the chosen backend type.

    Mapping:
      local → ingestion_storage config
      s3    → sync.destination config (where cloud data lives)

    Extend this function when adding new backends.
    """
    if backend == "local":
        return _create_backend_from_layer_config(
            config.storage.ingestion_storage, "migration_local"
        )
    elif backend == "s3":
        return _create_backend_from_layer_config(
            config.storage.sync.destination, "migration_s3"
        )
    else:
        raise ValueError(
            f"Unknown backend '{backend}'. Supported: local, s3"
        )


# ─── Discovery ──────────────────────────────────────────────────────────────

def find_usdc_partitions(
    storage: StorageBackend,
    channels: list[str],
) -> list[str]:
    """
    Find all symbol=*-USDC partition prefixes for coinbaseadvanced.

    Returns relative paths like:
      raw/ready/ccxt/orderbook/exchange=coinbaseadvanced/symbol=BTC-USDC
    """
    partitions = []
    for channel in channels:
        exchange_prefix = storage.join_path(
            DATA_PREFIX, channel, f"exchange={EXCHANGE}"
        )
        try:
            subdirs = storage.list_dirs(exchange_prefix)
        except Exception:
            continue

        for d in subdirs:
            # d is a relative path; normalize separators and extract last component
            normalized = d.replace("\\", "/")
            dir_name = normalized.rstrip("/").split("/")[-1]
            if dir_name.startswith("symbol=") and dir_name.endswith("-USDC"):
                partitions.append(normalized)

    return sorted(partitions)


def collect_parquet_files(
    storage: StorageBackend,
    usdc_prefix: str,
) -> list[dict]:
    """
    Recursively list all parquet files under a USDC partition prefix.

    Returns list of dicts with 'path' and 'size' keys (relative to storage root).
    """
    files = storage.list_files(
        path=usdc_prefix,
        pattern="*.parquet",
        recursive=True,
    )
    # Normalize separators for cross-platform consistency
    for f in files:
        f["path"] = f["path"].replace("\\", "/")
    return files


# ─── Path manipulation ──────────────────────────────────────────────────────

def usdc_path_to_usd(path: str) -> str:
    """
    Rewrite a file path, replacing the symbol=X-USDC partition component
    with symbol=X-USD.

    Works on both local and S3 paths (uses '/' separator).
    e.g. .../symbol=BTC-USDC/year=2026/... → .../symbol=BTC-USD/year=2026/...
    """
    parts = path.replace("\\", "/").split("/")
    for i, part in enumerate(parts):
        if part.startswith("symbol=") and part.endswith("-USDC"):
            parts[i] = part[:-5] + "-USD"
            break
    return "/".join(parts)


# ─── Migration logic ────────────────────────────────────────────────────────

def migrate_one_file(
    storage: StorageBackend,
    src_path: str,
    dry_run: bool = False,
) -> dict:
    """
    Migrate a single parquet file from USDC → USD.

    - Reads via read_bytes (works for both local & S3)
    - Rewrites the 'symbol' column values
    - Writes to the USD path via write_bytes
    - Does NOT delete originals here (deferred to batch_delete)

    Returns a status dict.
    """
    dst_path = usdc_path_to_usd(src_path)

    if dry_run:
        return {"status": "would_migrate", "src": src_path, "dst": dst_path}

    try:
        # Read parquet from storage
        raw_bytes = storage.read_bytes(src_path)
        table = pq.read_table(io.BytesIO(raw_bytes))

        # Rewrite symbol column
        if "symbol" in table.column_names:
            symbols = table.column("symbol")
            new_symbols = pa.array(
                [
                    s[:-1] if s and s.endswith("-USDC") else s
                    for s in symbols.to_pylist()
                ],
                type=pa.string(),
            )
            col_idx = table.schema.get_field_index("symbol")
            table = table.set_column(col_idx, "symbol", new_symbols)

        # Check if destination already exists (merge to avoid data loss)
        if storage.exists(dst_path):
            try:
                existing_bytes = storage.read_bytes(dst_path)
                existing_table = pq.read_table(io.BytesIO(existing_bytes))
                if existing_table.schema.equals(table.schema):
                    table = pa.concat_tables([existing_table, table])
                else:
                    logger.warning(f"Schema mismatch at {dst_path}, overwriting")
            except Exception as e:
                logger.warning(f"Could not read existing {dst_path}: {e}, overwriting")

        # Write to destination
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd", compression_level=4)
        storage.write_bytes(buf.getvalue(), dst_path)

        return {
            "status": "migrated",
            "src": src_path,
            "dst": dst_path,
            "rows": table.num_rows,
        }

    except Exception as e:
        return {"status": "error", "src": src_path, "error": str(e)}


# ─── Cleanup ────────────────────────────────────────────────────────────────

def cleanup_empty_dirs_local(storage: StorageBackend, partition_prefix: str):
    """
    Remove empty directories left behind after migration.
    Only meaningful for local storage — S3 has no real directories.
    """
    if storage.backend_type != "local":
        return

    full_path = Path(storage.get_full_path(partition_prefix))
    if not full_path.exists():
        return

    # Walk bottom-up and remove empties
    for dirpath in sorted(full_path.rglob("*"), reverse=True):
        if dirpath.is_dir():
            try:
                if not any(dirpath.iterdir()):
                    dirpath.rmdir()
            except OSError:
                pass

    # Remove root itself if empty
    try:
        if full_path.is_dir() and not any(full_path.iterdir()):
            full_path.rmdir()
    except OSError:
        pass


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Migrate coinbaseadvanced USDC → USD symbols across storage backends"
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["local", "s3"],
        default="local",
        help="Storage backend to operate on (default: local)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying files")
    parser.add_argument("--channel", type=str, choices=CHANNELS, help="Process single channel only")
    parser.add_argument("--workers", type=int, default=None, help="Parallel workers (auto-tuned per backend)")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Auto-tune workers by backend (S3 is network-bound, can go higher)
    workers = args.workers or (DEFAULT_WORKERS_S3 if args.backend == "s3" else DEFAULT_WORKERS_LOCAL)
    channels = [args.channel] if args.channel else CHANNELS

    # Load config and create storage backend
    config = load_config(args.config)
    storage = create_storage(config, args.backend)

    print(f"Backend:  {storage.backend_type}  ({storage.base_path})")
    print(f"Workers:  {workers}")
    print(f"Channels: {', '.join(channels)}")

    if args.dry_run:
        print("\n*** DRY RUN — no files will be modified ***\n")

    # ── Step 1: Discover USDC partitions ─────────────────────────────────
    usdc_partitions = find_usdc_partitions(storage, channels)
    if not usdc_partitions:
        print("No symbol=*-USDC partitions found for coinbaseadvanced. Nothing to do.")
        return

    print(f"\nFound {len(usdc_partitions)} USDC partition directories:")
    for p in usdc_partitions:
        print(f"  {p}")

    # ── Step 2: Collect all parquet files ────────────────────────────────
    all_files: list[str] = []
    for partition in usdc_partitions:
        files = collect_parquet_files(storage, partition)
        all_files.extend(f["path"] for f in files)

    print(f"\nTotal parquet files to process: {len(all_files)}")
    if not all_files:
        print("No parquet files found. Nothing to do.")
        return

    # ── Step 3: Migrate files in parallel ────────────────────────────────
    start = time.time()
    results = {"migrated": 0, "would_migrate": 0, "error": 0, "total_rows": 0}
    errors = []
    files_to_delete: list[str] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(migrate_one_file, storage, path, args.dry_run): path
            for path in all_files
        }
        for future in as_completed(futures):
            result = future.result()
            status = result["status"]

            if status == "migrated":
                results["migrated"] += 1
                results["total_rows"] += result.get("rows", 0)
                files_to_delete.append(result["src"])
            elif status == "would_migrate":
                results["would_migrate"] += 1
            elif status == "error":
                results["error"] += 1
                errors.append(result)

            # Progress every 500 files
            done = results["migrated"] + results["would_migrate"] + results["error"]
            if done % 500 == 0 and done > 0:
                print(f"  ... processed {done}/{len(all_files)} files")

    # ── Step 4: Batch-delete originals ───────────────────────────────────
    if files_to_delete and not args.dry_run:
        print(f"\nDeleting {len(files_to_delete)} original USDC files...")
        del_result = storage.batch_delete(files_to_delete, max_workers=workers)
        print(
            f"  Deleted: {del_result['deleted']}, "
            f"Failed: {del_result['failed']}"
        )
        if del_result["errors"]:
            for path, err in del_result["errors"][:10]:
                print(f"    Error: {path}: {err}")

    # ── Step 5: Cleanup empty directories (local only) ───────────────────
    if not args.dry_run and storage.backend_type == "local":
        print("\nCleaning up empty directories...")
        for partition in usdc_partitions:
            cleanup_empty_dirs_local(storage, partition)

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - start
    print(f"\n{'=' * 80}")
    print(f"MIGRATION SUMMARY  [{storage.backend_type.upper()}]")
    print(f"{'=' * 80}")
    if args.dry_run:
        print(f"  Files that would be migrated: {results['would_migrate']}")
    else:
        print(f"  Files migrated:  {results['migrated']}")
        print(f"  Total rows:      {results['total_rows']:,}")
    print(f"  Errors:          {results['error']}")
    print(f"  Duration:        {elapsed:.1f}s")
    print(f"{'=' * 80}")

    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for e in errors[:20]:
            print(f"  {e['src']}: {e['error']}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")

    if not args.dry_run and results["migrated"] > 0:
        if storage.backend_type == "local":
            print("\nNext step: sync updated local data to S3:")
            print("  python scripts/archived/sync_and_repair.py --sync-only")
        else:
            print("\nS3 data has been migrated in-place.")
        print("\nRemember to update config.yaml symbols from USDC to USD pairs.")


if __name__ == "__main__":
    main()
