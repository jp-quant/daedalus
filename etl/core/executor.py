"""
Transform Executor
==================

Orchestrates transform execution: resolves inputs, runs transform, writes outputs.

The executor is the central component that:
1. Reads inputs from storage based on InputConfig
2. Applies FilterSpec for partition pruning and row filtering
3. Passes resolved LazyFrames to the transform
4. Writes outputs to storage based on OutputConfig
5. Manages execution context, logging, and statistics

Usage:
    from etl.core.executor import TransformExecutor
    from etl.core.config import FilterSpec
    
    executor = TransformExecutor()
    result = executor.execute(
        transform=my_transform,
        filter_spec=FilterSpec(exchange="binanceus", symbol="BTC/USD"),
    )
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import polars as pl

from etl.core.base import BaseTransform, TransformContext
from etl.core.config import (
    FeatureConfig,
    FilterSpec,
    InputConfig,
    OutputConfig,
    StorageConfig,
    TransformConfig,
)
from etl.core.enums import (
    CompressionCodec,
    DataFormat,
    ProcessingMode,
    StorageBackendType,
    WriteMode,
)

# Shared partitioning utilities (used by both ingestion and ETL)
from shared.partitioning import (
    build_partition_path,
    format_partition_value,
    sanitize_dataframe_partition_values,
    ensure_partition_columns_exist,
    DEFAULT_PARTITION_COLUMNS,
)

if TYPE_CHECKING:
    from config.config import DaedalusConfig

# Type alias for Polars compression literals
ParquetCompression = Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"]

logger = logging.getLogger(__name__)


class TransformExecutor:
    """
    Executes transforms by resolving inputs, running transform logic, and writing outputs.
    
    The executor handles the complete lifecycle:
    - Reading inputs from storage (local or S3) with partition pruning
    - Applying FilterSpec for efficient data loading
    - Passing resolved data to the transform
    - Writing transform outputs to storage with proper partitioning
    - Managing execution context and metadata
    
    This is the PRIMARY interface for running transforms. ETL scripts should use
    the executor rather than manually scanning data and writing outputs.
    
    Example:
        # Create executor with feature configuration
        executor = TransformExecutor(
            feature_config=FeatureConfig(
                categories={FeatureCategory.STRUCTURAL, FeatureCategory.DYNAMIC},
                depth_levels=20,
            )
        )
        
        # Create filter for specific partition
        filter_spec = FilterSpec(exchange="binanceus", symbol="BTC/USD")
        
        # Execute transform - executor handles I/O
        result = executor.execute(
            transform=OrderbookFeatureTransform(config),
            filter_spec=filter_spec,
        )
        
        print(f"Processed {result['write_stats']['silver']['rows']} rows")
    """
    
    def __init__(
        self,
        default_storage: Optional[StorageConfig] = None,
        feature_config: Optional[FeatureConfig] = None,
    ):
        """
        Initialize executor.
        
        Args:
            default_storage: Default storage config if not specified per input/output.
                           Used when InputConfig/OutputConfig don't specify storage.
            feature_config: Feature computation configuration. Passed to transforms
                          via TransformContext for feature-specific parameters.
        """
        self.default_storage = default_storage or StorageConfig()
        self.feature_config = feature_config or FeatureConfig()
        self._storage_backend = None  # Lazy loaded
    
    @classmethod
    def from_config(cls, config: "DaedalusConfig") -> "TransformExecutor":
        """
        Create executor from Daedalus configuration.
        
        This factory method creates a properly configured TransformExecutor
        from the main application configuration file.
        
        Args:
            config: DaedalusConfig instance loaded from config.yaml
            
        Returns:
            Configured TransformExecutor ready for execution
        
        Example:
            from config.config import load_config
            from etl.core.executor import TransformExecutor
            
            config = load_config()
            executor = TransformExecutor.from_config(config)
            result = executor.execute(my_transform)
        """
        # Build storage config from ETL input settings
        storage_config = StorageConfig(
            backend_type=StorageBackendType.LOCAL,
            base_path=config.storage.etl_storage_input.base_dir,
        )
        
        # Use the new to_feature_config() method if available
        if hasattr(config, 'to_feature_config'):
            feature_config = config.to_feature_config()
        else:
            # Fallback for older configs
            feature_config = FeatureConfig()
        
        return cls(
            default_storage=storage_config,
            feature_config=feature_config,
        )
    
    # =========================================================================
    # Main Execution
    # =========================================================================
    
    def execute(
        self,
        transform: BaseTransform,
        context: Optional[TransformContext] = None,
        filter_spec: Optional[FilterSpec] = None,
        dry_run: bool = False,
        limit_rows: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Execute a transform end-to-end.
        
        This is the main entry point for running transforms. The executor:
        1. Creates execution context if not provided
        2. Resolves inputs from storage using InputConfig paths
        3. Applies FilterSpec for partition pruning and row filtering
        4. Calls transform.transform() with resolved inputs
        5. Writes outputs to storage using OutputConfig paths
        
        Args:
            transform: The transform to execute. Must have config with inputs/outputs.
            context: Execution context (created if not provided). Contains feature_config,
                    execution_id, and partition_values for the transform.
            filter_spec: Optional filter for partition pruning. If provided, only
                        partitions matching the filter are read. Significantly reduces
                        I/O for large datasets.
            dry_run: If True, don't write outputs. Useful for testing transforms
                    without modifying storage.
            limit_rows: If provided, limit each input to this many rows.
                       Useful for testing with small data samples.
        
        Returns:
            Execution result dictionary with:
            - success: bool
            - execution_id: str
            - transform: str (transform name)
            - duration_seconds: float
            - inputs_read: list[str]
            - outputs_written: list[str]
            - write_stats: dict[str, dict] (rows/files per output)
        
        Raises:
            ValueError: If inputs cannot be resolved.
            RuntimeError: If transform execution fails.
        
        Example:
            result = executor.execute(
                transform=my_transform,
                filter_spec=FilterSpec(exchange="binanceus"),
                limit_rows=10000,  # For testing
            )
            if result["success"]:
                print(f"Wrote {result['write_stats']['silver']['rows']} rows")
        """
        execution_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        logger.info(f"[{execution_id}] Starting transform: {transform.name}")
        
        # Build partition values from filter_spec for context
        partition_values = None
        if filter_spec:
            partition_values = filter_spec.to_dict()
            logger.info(f"[{execution_id}] Filter: {partition_values}")
        
        # Create context if not provided
        if context is None:
            context = TransformContext(
                execution_id=execution_id,
                execution_time=start_time,
                feature_config=self.feature_config,
                partition_values=partition_values,
            )
        
        try:
            # Initialize transform
            transform.initialize(context)
            
            # Resolve inputs with filter
            logger.info(f"[{execution_id}] Resolving {len(transform.config.inputs)} inputs")
            inputs = self._resolve_inputs(
                config=transform.config,
                context=context,
                filter_spec=filter_spec,
                limit_rows=limit_rows,
            )
            
            # Log input statistics
            for name, lf in inputs.items():
                try:
                    schema = lf.collect_schema()
                    logger.info(f"[{execution_id}] Input '{name}': {len(schema.names())} columns")
                except Exception:
                    logger.info(f"[{execution_id}] Input '{name}': schema pending")
            
            # Validate inputs
            transform.validate_inputs(inputs)
            
            # Execute transform
            logger.info(f"[{execution_id}] Executing transform logic")
            outputs = transform.transform(inputs, context)
            
            # Validate outputs
            transform.validate_outputs(outputs)
            
            # Write outputs (unless dry run)
            if not dry_run:
                logger.info(f"[{execution_id}] Writing {len(outputs)} outputs")
                write_stats = self._write_outputs(transform.config, outputs, context)
            else:
                logger.info(f"[{execution_id}] Dry run - skipping output writes")
                # Still collect outputs to get row counts
                write_stats = {}
                for name, lf in outputs.items():
                    try:
                        df = lf.collect()
                        write_stats[name] = {"rows": len(df), "files": 0, "dry_run": True}
                    except Exception as e:
                        write_stats[name] = {"error": str(e)}
            
            # Finalize transform
            transform.finalize(context)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "success": True,
                "execution_id": execution_id,
                "transform": transform.name,
                "duration_seconds": duration,
                "inputs_read": list(inputs.keys()),
                "outputs_written": list(outputs.keys()),
                "write_stats": write_stats,
            }
            
            logger.info(f"[{execution_id}] Transform completed in {duration:.2f}s")
            self._log_write_stats(execution_id, write_stats)
            
            return result
            
        except Exception as e:
            logger.error(f"[{execution_id}] Transform failed: {e}", exc_info=True)
            raise RuntimeError(f"Transform '{transform.name}' failed: {e}") from e
    
    def _log_write_stats(self, execution_id: str, write_stats: dict[str, dict]) -> None:
        """Log write statistics in a readable format."""
        for name, stats in write_stats.items():
            if "error" in stats:
                logger.warning(f"[{execution_id}]   {name}: ERROR - {stats['error']}")
            elif stats.get("dry_run"):
                logger.info(f"[{execution_id}]   {name}: {stats['rows']:,} rows (dry run)")
            else:
                logger.info(f"[{execution_id}]   {name}: {stats['rows']:,} rows -> {stats['files']} files")
    
    # =========================================================================
    # Input Resolution
    # =========================================================================
    
    def _resolve_inputs(
        self,
        config: TransformConfig,
        context: TransformContext,
        filter_spec: Optional[FilterSpec] = None,
        limit_rows: Optional[int] = None,
    ) -> dict[str, pl.LazyFrame]:
        """
        Resolve all inputs to LazyFrames with optional filtering.
        
        Args:
            config: Transform configuration with input definitions.
            context: Execution context.
            filter_spec: Optional filter for partition pruning.
            limit_rows: Optional row limit for testing.
        
        Returns:
            Dictionary mapping input names to LazyFrames.
        """
        inputs = {}
        
        for name, input_config in config.inputs.items():
            logger.debug(f"Resolving input: {name} from {input_config.path}")
            
            lf = self._read_input(
                input_config=input_config,
                default_storage=config.storage,
                filter_spec=filter_spec,
            )
            
            # Apply row limit if specified (for testing)
            if limit_rows is not None:
                lf = lf.head(limit_rows)
            
            inputs[name] = lf
        
        return inputs
    
    def _read_input(
        self,
        input_config: InputConfig,
        default_storage: Optional[StorageConfig],
        filter_spec: Optional[FilterSpec] = None,
    ) -> pl.LazyFrame:
        """
        Read a single input to a LazyFrame with filtering.
        
        Args:
            input_config: Input configuration.
            default_storage: Fallback storage config.
            filter_spec: Optional filter specification for partition pruning.
        
        Returns:
            LazyFrame with input data (filtered if filter_spec provided).
        """
        storage = input_config.storage or default_storage or self.default_storage
        path = input_config.path
        
        # Build full path if needed
        if storage.base_path:
            path = str(Path(storage.base_path) / path)
        
        # Read based on format
        if input_config.format == DataFormat.PARQUET:
            lf = self._read_parquet(
                path=path,
                storage=storage,
                columns=input_config.columns,
            )
        elif input_config.format == DataFormat.NDJSON:
            lf = self._read_ndjson(path=path, storage=storage)
        else:
            raise ValueError(f"Unsupported input format: {input_config.format}")
        
        # Apply FilterSpec if provided
        if filter_spec is not None:
            filter_expr = filter_spec.to_polars_filter()
            if filter_expr is not None:
                lf = lf.filter(filter_expr)
        
        # Apply InputConfig filters if provided (legacy support)
        if input_config.filters:
            for col, op, val in input_config.filters:
                lf = self._apply_filter(lf, col, op, val)
        
        # Apply column selection if provided
        if input_config.columns:
            lf = lf.select(input_config.columns)
        
        return lf
    
    def _apply_filter(
        self,
        lf: pl.LazyFrame,
        col: str,
        op: str,
        val: Any,
    ) -> pl.LazyFrame:
        """
        Apply a single filter condition to LazyFrame.
        
        Supported operators:
            =, !=, >, >=, <, <=, in, between
        
        Args:
            lf: LazyFrame to filter.
            col: Column name to filter on.
            op: Filter operator.
            val: Filter value (or tuple for 'between').
        
        Returns:
            Filtered LazyFrame.
        
        Raises:
            ValueError: If operator is not supported.
        """
        if op == "=":
            return lf.filter(pl.col(col) == val)
        elif op == "!=":
            return lf.filter(pl.col(col) != val)
        elif op == ">":
            return lf.filter(pl.col(col) > val)
        elif op == ">=":
            return lf.filter(pl.col(col) >= val)
        elif op == "<":
            return lf.filter(pl.col(col) < val)
        elif op == "<=":
            return lf.filter(pl.col(col) <= val)
        elif op == "in":
            return lf.filter(pl.col(col).is_in(val))
        elif op == "between":
            # val should be (low, high) tuple
            if not isinstance(val, tuple) or len(val) != 2:
                raise ValueError("'between' operator requires (low, high) tuple")
            return lf.filter((pl.col(col) >= val[0]) & (pl.col(col) <= val[1]))
        else:
            raise ValueError(f"Unsupported filter operator: {op}")
    
    def _read_parquet(
        self,
        path: str,
        storage: StorageConfig,
        columns: Optional[list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Read Parquet file(s) to LazyFrame.
        
        Supports:
        - Single file
        - Directory with Hive partitioning  
        - Glob patterns (*.parquet, **/*.parquet)
        
        Args:
            path: Path to Parquet file, directory, or glob pattern.
            storage: Storage configuration.
            columns: Optional columns to select (projection pushdown).
        
        Returns:
            LazyFrame with Parquet data.
        """
        if storage.backend_type == StorageBackendType.S3:
            # Build S3 URI
            s3_path = f"s3://{storage.bucket}/{path}"
            return pl.scan_parquet(
                s3_path,
                hive_partitioning=True,
            )
        else:
            # Local filesystem
            p = Path(path)
            
            if p.is_dir():
                # Directory - scan all parquet files recursively
                glob_pattern = str(p / "**/*.parquet")
                logger.debug(f"Scanning directory: {glob_pattern}")
                return pl.scan_parquet(
                    glob_pattern,
                    hive_partitioning=True,
                )
            elif "*" in str(p):
                # Glob pattern provided directly
                logger.debug(f"Scanning glob: {path}")
                return pl.scan_parquet(
                    path,
                    hive_partitioning=True,
                )
            elif p.exists():
                # Single file
                logger.debug(f"Scanning file: {path}")
                return pl.scan_parquet(path)
            else:
                # Path doesn't exist - try as glob in parent
                parent = p.parent
                if parent.exists():
                    glob_pattern = str(parent / "*.parquet")
                    return pl.scan_parquet(
                        glob_pattern,
                        hive_partitioning=True,
                    )
                raise FileNotFoundError(f"Input path not found: {path}")
    
    def _read_ndjson(
        self,
        path: str,
        storage: StorageConfig,
    ) -> pl.LazyFrame:
        """
        Read NDJSON file(s) to LazyFrame.
        
        Args:
            path: Path to NDJSON file.
            storage: Storage configuration.
        
        Returns:
            LazyFrame with NDJSON data.
        """
        # NDJSON reading is typically eager, wrap in lazy
        if storage.backend_type == StorageBackendType.LOCAL:
            return pl.scan_ndjson(path)
        else:
            raise ValueError("S3 NDJSON reading not yet implemented")
    
    # =========================================================================
    # Output Writing
    # =========================================================================
    
    def _write_outputs(
        self,
        config: TransformConfig,
        outputs: dict[str, pl.LazyFrame],
        context: TransformContext,
    ) -> dict[str, dict[str, Any]]:
        """
        Write all outputs to storage.
        
        Args:
            config: Transform configuration.
            outputs: Dictionary of output LazyFrames.
            context: Execution context.
        
        Returns:
            Statistics about written data per output.
        """
        stats = {}
        
        for name, lf in outputs.items():
            output_config = config.outputs[name]
            logger.debug(f"Writing output: {name} to {output_config.path}")
            
            stats[name] = self._write_output(
                lf=lf,
                output_config=output_config,
                default_storage=config.storage,
                context=context,
            )
        
        return stats
    
    def _write_output(
        self,
        lf: pl.LazyFrame,
        output_config: OutputConfig,
        default_storage: Optional[StorageConfig],
        context: TransformContext,
    ) -> dict[str, Any]:
        """
        Write a single output.
        
        Args:
            lf: LazyFrame to write.
            output_config: Output configuration.
            default_storage: Fallback storage config.
            context: Execution context.
        
        Returns:
            Statistics about written data.
        """
        storage = output_config.storage or default_storage or self.default_storage
        path = output_config.path
        
        # Build full path if needed
        if storage.base_path:
            path = str(Path(storage.base_path) / path)
        
        # Collect the LazyFrame
        df = lf.collect()
        row_count = len(df)
        
        if row_count == 0:
            logger.warning(f"Output '{output_config.name}' has no data to write")
            return {"rows": 0, "files": 0}
        
        # Write based on format
        if output_config.format == DataFormat.PARQUET:
            file_count = self._write_parquet(
                df=df,
                path=path,
                output_config=output_config,
                storage=storage,
            )
        else:
            raise ValueError(f"Unsupported output format: {output_config.format}")
        
        return {"rows": row_count, "files": file_count}
    
    def _write_parquet(
        self,
        df: pl.DataFrame,
        path: str,
        output_config: OutputConfig,
        storage: StorageConfig,
    ) -> int:
        """
        Write DataFrame to Parquet.
        
        Args:
            df: DataFrame to write.
            path: Output path.
            output_config: Output configuration.
            storage: Storage configuration.
        
        Returns:
            Number of files written.
        """
        # Get compression - cast to literal type that polars expects
        compression_value = output_config.compression.value if output_config.compression else "zstd"
        compression: ParquetCompression = compression_value  # type: ignore
        compression_level = output_config.compression_level
        
        # Handle partitioned writes
        if output_config.partition_cols:
            return self._write_partitioned_parquet(
                df=df,
                path=path,
                partition_cols=output_config.partition_cols,
                compression=compression,
                compression_level=compression_level,
                mode=output_config.mode,
            )
        else:
            # Single file write
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle write mode
            if output_config.mode == WriteMode.APPEND and p.exists():
                # Read existing, concatenate, write
                existing = pl.read_parquet(str(p))
                df = pl.concat([existing, df])
            
            df.write_parquet(
                str(p),
                compression=compression,
                compression_level=compression_level,
                row_group_size=output_config.row_group_size,
            )
            return 1
    
    def _write_partitioned_parquet(
        self,
        df: pl.DataFrame,
        path: str,
        partition_cols: list[str],
        compression: ParquetCompression,
        compression_level: int,
        mode: WriteMode,
    ) -> int:
        """
        Write DataFrame to partitioned Parquet (Hive-style).
        
        Uses shared partitioning utilities for consistent behavior with ingestion.
        Supports Directory-Aligned Partitioning where partition values exist
        in data AND match directory paths.
        
        Args:
            df: DataFrame to write.
            path: Base output path.
            partition_cols: Columns to partition by.
            compression: Compression codec name (e.g., 'zstd', 'snappy').
            compression_level: Compression level (mainly for zstd).
            mode: Write mode.
        
        Returns:
            Number of files written.
        """
        file_count = 0
        base_path = Path(path)
        
        # Ensure all partition columns exist (derive datetime cols if needed)
        df = ensure_partition_columns_exist(
            df, 
            partition_cols,
            timestamp_col="timestamp" if "timestamp" in df.columns else "capture_ts",
        )
        
        # Sanitize partition column values for filesystem compatibility
        df = sanitize_dataframe_partition_values(df, partition_cols)
        
        # Group by partition columns
        for partition_values, group_df in df.group_by(partition_cols):
            # Ensure partition_values is always a tuple
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
            
            # Build partition path using shared utility
            partition_path = build_partition_path(
                base_path=base_path,
                partition_cols=partition_cols,
                partition_values=partition_values,
                # zero_pad_datetime=False,
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = partition_path / f"data_{timestamp}.parquet"
            
            # Handle write mode for this partition
            if mode == WriteMode.OVERWRITE_PARTITION:
                # Remove existing files in this partition
                for existing in partition_path.glob("*.parquet"):
                    existing.unlink()
            elif mode == WriteMode.OVERWRITE:
                # Same as OVERWRITE_PARTITION for partitioned writes
                for existing in partition_path.glob("*.parquet"):
                    existing.unlink()
            # APPEND mode: just add new file
            
            # Keep partition columns in data (Directory-Aligned Partitioning)
            # This differs from traditional Hive where partition cols are dropped
            write_df = group_df
            
            write_df.write_parquet(
                str(file_path),
                compression=compression,
                compression_level=compression_level,
            )
            file_count += 1
        
        return file_count


class BatchExecutor(TransformExecutor):
    """
    Executor for large datasets that need to be processed in batches.
    
    Extends TransformExecutor with batching capabilities for
    datasets that don't fit in memory.
    """
    
    def execute_batched(
        self,
        transform: BaseTransform,
        batch_size: int,
        context: Optional[TransformContext] = None,
    ) -> dict[str, Any]:
        """
        Execute transform in batches.
        
        Args:
            transform: The transform to execute.
            batch_size: Number of rows per batch.
            context: Execution context.
        
        Returns:
            Aggregated execution result.
        """
        # This is a scaffold for future implementation
        # For v1, we use the standard execute() method
        raise NotImplementedError("Batch execution not yet implemented")
