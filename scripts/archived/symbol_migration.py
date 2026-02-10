#!/usr/bin/env python3
"""
General-purpose Symbol Migration — Multi-Backend.

Migrates parquet data between symbol partition variants (e.g. USD↔USDC)
for any exchange and channel combination. Supports bidirectional migration
and can run concurrently for different channels.

For each parquet file in matching symbol=*{from_suffix} partitions:
  1. Reads the file via StorageBackend.read_bytes()
  2. Rewrites the 'symbol' column: replaces {from_suffix} → {to_suffix}
  3. Writes to the new symbol=*{to_suffix} path via StorageBackend.write_bytes()
  4. Deletes originals (batch_delete for S3 efficiency)
  5. Cleans up empty directories (local only)

Usage Examples:
    # Orderbook: USD → USDC (coinbaseadvanced, S3)
    python scripts/archived/symbol_migration.py \\
        --from USD --to USDC --channel orderbook --backend s3

    # Ticker + Trades: USDC → USD (coinbaseadvanced, S3)
    python scripts/archived/symbol_migration.py \\
        --from USDC --to USD --channel ticker trades --backend s3

    # Dry run (preview only)
    python scripts/archived/symbol_migration.py \\
        --from USD --to USDC --channel orderbook --backend s3 --dry-run

    # Different exchange
    python scripts/archived/symbol_migration.py \\
        --from USD --to USDT --exchange binanceus --channel trades --backend local

    # All channels at once
    python scripts/archived/symbol_migration.py \\
        --from USDC --to USD --backend local
"""
import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Project imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DaedalusConfig, load_config
from storage.base import StorageBackend
from storage.factory import _create_backend_from_layer_config

logger = logging.getLogger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────
ALL_CHANNELS = ["orderbook", "trades", "ticker"]
DATA_PREFIX = "raw/ready/ccxt"
DEFAULT_WORKERS_LOCAL = 16
DEFAULT_WORKERS_S3 = 32


# ─── Storage factory ────────────────────────────────────────────────────────

def create_storage(config: DaedalusConfig, backend: str) -> StorageBackend:
    if backend == "local":
        return _create_backend_from_layer_config(
            config.storage.ingestion_storage, "migration_local"
        )
    elif backend == "s3":
        return _create_backend_from_layer_config(
            config.storage.sync.destination, "migration_s3"
        )
    else:
        raise ValueError(f"Unknown backend '{backend}'. Supported: local, s3")


# ─── Discovery ──────────────────────────────────────────────────────────────

def find_partitions(
    storage: StorageBackend,
    exchange: str,
    channels: list[str],
    from_suffix: str,
) -> list[str]:
    """
    Find all symbol=*-{from_suffix} partition prefixes for the given exchange.
    """
    partitions = []
    match_suffix = f"-{from_suffix}"

    for channel in channels:
        exchange_prefix = storage.join_path(
            DATA_PREFIX, channel, f"exchange={exchange}"
        )
        try:
            subdirs = storage.list_dirs(exchange_prefix)
        except Exception:
            continue

        for d in subdirs:
            normalized = d.replace("\\", "/")
            dir_name = normalized.rstrip("/").split("/")[-1]
            if dir_name.startswith("symbol=") and dir_name.endswith(match_suffix):
                # Guard: don't match -USDC when looking for -USD
                # e.g. if from_suffix="USD", skip dirs ending in "-USDC"
                symbol_val = dir_name[len("symbol="):]
                if symbol_val.endswith(match_suffix) and not symbol_val.endswith(f"-{from_suffix}C"):
                    partitions.append(normalized)

    return sorted(partitions)


def collect_parquet_files(
    storage: StorageBackend,
    prefix: str,
) -> list[dict]:
    files = storage.list_files(path=prefix, pattern="*.parquet", recursive=True)
    for f in files:
        f["path"] = f["path"].replace("\\", "/")
    return files


# ─── Path manipulation ──────────────────────────────────────────────────────

def rewrite_partition_path(path: str, from_suffix: str, to_suffix: str) -> str:
    """
    Rewrite file path: symbol=X-{from_suffix} → symbol=X-{to_suffix}
    """
    parts = path.replace("\\", "/").split("/")
    match = f"-{from_suffix}"
    replacement = f"-{to_suffix}"
    for i, part in enumerate(parts):
        if part.startswith("symbol=") and part.endswith(match):
            # Verify exact suffix match (not partial)
            symbol_val = part[len("symbol="):]
            if symbol_val.endswith(match) and not symbol_val.endswith(f"-{from_suffix}C"):
                parts[i] = part[: -len(match)] + replacement
                break
    return "/".join(parts)


# ─── Migration logic ────────────────────────────────────────────────────────

def migrate_one_file(
    storage: StorageBackend,
    src_path: str,
    from_suffix: str,
    to_suffix: str,
    dry_run: bool = False,
) -> dict:
    """
    Migrate a single parquet file between symbol variants.

    Rewrites both the partition path and the symbol column values.
    Merges with existing destination if present (avoids data loss).
    """
    dst_path = rewrite_partition_path(src_path, from_suffix, to_suffix)

    if dry_run:
        return {"status": "would_migrate", "src": src_path, "dst": dst_path}

    try:
        raw_bytes = storage.read_bytes(src_path)
        table = pq.read_table(io.BytesIO(raw_bytes))

        # Rewrite symbol column values
        col_from = f"-{from_suffix}"
        col_to = f"-{to_suffix}"
        if "symbol" in table.column_names:
            symbols = table.column("symbol")
            new_symbols = pa.array(
                [
                    s[: -len(col_from)] + col_to
                    if s and s.endswith(col_from)
                    else s
                    for s in symbols.to_pylist()
                ],
                type=pa.string(),
            )
            col_idx = table.schema.get_field_index("symbol")
            table = table.set_column(col_idx, "symbol", new_symbols)

        # Merge with existing destination to avoid data loss
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
    if storage.backend_type != "local":
        return

    full_path = Path(storage.get_full_path(partition_prefix))
    if not full_path.exists():
        return

    for dirpath in sorted(full_path.rglob("*"), reverse=True):
        if dirpath.is_dir():
            try:
                if not any(dirpath.iterdir()):
                    dirpath.rmdir()
            except OSError:
                pass

    try:
        if full_path.is_dir() and not any(full_path.iterdir()):
            full_path.rmdir()
    except OSError:
        pass


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="General-purpose symbol partition migration across storage backends",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Orderbook USD → USDC on S3
  %(prog)s --from USD --to USDC --channel orderbook --backend s3

  # Ticker & Trades USDC → USD on S3
  %(prog)s --from USDC --to USD --channel ticker trades --backend s3

  # Dry run
  %(prog)s --from USD --to USDC --channel orderbook --backend s3 --dry-run

  # Different exchange, local storage
  %(prog)s --from USD --to USDT --exchange binanceus --backend local
""",
    )
    parser.add_argument(
        "--from", dest="from_suffix", type=str, required=True,
        help="Source symbol suffix to migrate FROM (e.g. USD, USDC, USDT)",
    )
    parser.add_argument(
        "--to", dest="to_suffix", type=str, required=True,
        help="Target symbol suffix to migrate TO (e.g. USDC, USD, USDT)",
    )
    parser.add_argument(
        "--exchange", type=str, default="coinbaseadvanced",
        help="Exchange partition value (default: coinbaseadvanced)",
    )
    parser.add_argument(
        "--channel", type=str, nargs="+", choices=ALL_CHANNELS,
        default=ALL_CHANNELS,
        help="Channel(s) to process (default: all)",
    )
    parser.add_argument(
        "--backend", type=str, choices=["local", "s3"], default="local",
        help="Storage backend (default: local)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying files")
    parser.add_argument("--workers", type=int, default=None, help="Parallel workers (auto-tuned per backend)")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    args = parser.parse_args()

    if args.from_suffix == args.to_suffix:
        print(f"Error: --from and --to are the same ('{args.from_suffix}'). Nothing to do.")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    workers = args.workers or (DEFAULT_WORKERS_S3 if args.backend == "s3" else DEFAULT_WORKERS_LOCAL)

    config = load_config(args.config)
    storage = create_storage(config, args.backend)

    direction = f"{args.from_suffix} → {args.to_suffix}"
    print(f"Migration:  {direction}")
    print(f"Exchange:   {args.exchange}")
    print(f"Backend:    {storage.backend_type}  ({storage.base_path})")
    print(f"Channels:   {', '.join(args.channel)}")
    print(f"Workers:    {workers}")

    if args.dry_run:
        print("\n*** DRY RUN — no files will be modified ***\n")

    # ── Step 1: Discover partitions ──────────────────────────────────────
    partitions = find_partitions(storage, args.exchange, args.channel, args.from_suffix)
    if not partitions:
        print(f"\nNo symbol=*-{args.from_suffix} partitions found for {args.exchange}. Nothing to do.")
        return

    print(f"\nFound {len(partitions)} partition directories to migrate:")
    for p in partitions:
        print(f"  {p}")

    # ── Step 2: Collect all parquet files ────────────────────────────────
    all_files: list[str] = []
    for partition in partitions:
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
            executor.submit(
                migrate_one_file, storage, path,
                args.from_suffix, args.to_suffix, args.dry_run,
            ): path
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

            done = results["migrated"] + results["would_migrate"] + results["error"]
            if done % 500 == 0 and done > 0:
                print(f"  ... processed {done}/{len(all_files)} files")

    # ── Step 4: Batch-delete originals ───────────────────────────────────
    if files_to_delete and not args.dry_run:
        print(f"\nDeleting {len(files_to_delete)} original {args.from_suffix} files...")
        del_result = storage.batch_delete(files_to_delete, max_workers=workers)
        print(f"  Deleted: {del_result['deleted']}, Failed: {del_result['failed']}")
        if del_result["errors"]:
            for path, err in del_result["errors"][:10]:
                print(f"    Error: {path}: {err}")

    # ── Step 5: Cleanup empty directories (local only) ───────────────────
    if not args.dry_run and storage.backend_type == "local":
        print("\nCleaning up empty directories...")
        for partition in partitions:
            cleanup_empty_dirs_local(storage, partition)

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - start
    print(f"\n{'=' * 80}")
    print(f"MIGRATION SUMMARY  [{direction}]  [{storage.backend_type.upper()}]")
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


if __name__ == "__main__":
    main()
