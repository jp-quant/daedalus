"""
Transform Executor
==================

Orchestrates transform execution: resolves inputs, runs transform, writes outputs.
"""

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import polars as pl

from etl.core.base import BaseTransform, TransformContext
from etl.core.config import (
    FeatureConfig,
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

logger = logging.getLogger(__name__)


class TransformExecutor:
    """
    Executes transforms by resolving inputs, running transform logic, and writing outputs.
    
    The executor handles:
    - Reading inputs from storage (local or S3)
    - Passing resolved data to the transform
    - Writing transform outputs to storage
    - Managing execution context and metadata
    
    Example:
        executor = TransformExecutor()
        result = executor.execute(my_transform)
    """
    
    def __init__(
        self,
        default_storage: Optional[StorageConfig] = None,
        feature_config: Optional[FeatureConfig] = None,
    ):
        """
        Initialize executor.
        
        Args:
            default_storage: Default storage config if not specified per input/output
            feature_config: Feature computation configuration
        """
        self.default_storage = default_storage or StorageConfig()
        self.feature_config = feature_config or FeatureConfig()
        self._storage_backend = None  # Lazy loaded
    
    # =========================================================================
    # Main Execution
    # =========================================================================
    
    def execute(
        self,
        transform: BaseTransform,
        context: Optional[TransformContext] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Execute a transform.
        
        Args:
            transform: The transform to execute.
            context: Execution context (created if not provided).
            dry_run: If True, don't write outputs (useful for testing).
        
        Returns:
            Execution result with metadata.
        
        Raises:
            ValueError: If inputs cannot be resolved.
            RuntimeError: If transform execution fails.
        """
        execution_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        logger.info(f"[{execution_id}] Starting transform: {transform.name}")
        
        # Create context if not provided
        if context is None:
            context = TransformContext(
                execution_id=execution_id,
                execution_time=start_time,
                feature_config=self.feature_config,
            )
        
        try:
            # Initialize transform
            transform.initialize(context)
            
            # Resolve inputs
            logger.info(f"[{execution_id}] Resolving {len(transform.config.inputs)} inputs")
            inputs = self._resolve_inputs(transform.config, context)
            
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
                write_stats = {}
            
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
            return result
            
        except Exception as e:
            logger.error(f"[{execution_id}] Transform failed: {e}")
            raise RuntimeError(f"Transform '{transform.name}' failed: {e}") from e
    
    # =========================================================================
    # Input Resolution
    # =========================================================================
    
    def _resolve_inputs(
        self,
        config: TransformConfig,
        context: TransformContext,
    ) -> dict[str, pl.LazyFrame]:
        """
        Resolve all inputs to LazyFrames.
        
        Args:
            config: Transform configuration.
            context: Execution context.
        
        Returns:
            Dictionary mapping input names to LazyFrames.
        """
        inputs = {}
        
        for name, input_config in config.inputs.items():
            logger.debug(f"Resolving input: {name} from {input_config.path}")
            inputs[name] = self._read_input(input_config, config.storage)
        
        return inputs
    
    def _read_input(
        self,
        input_config: InputConfig,
        default_storage: Optional[StorageConfig],
    ) -> pl.LazyFrame:
        """
        Read a single input to a LazyFrame.
        
        Args:
            input_config: Input configuration.
            default_storage: Fallback storage config.
        
        Returns:
            LazyFrame with input data.
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
                filters=input_config.filters,
                columns=input_config.columns,
            )
        elif input_config.format == DataFormat.NDJSON:
            lf = self._read_ndjson(path=path, storage=storage)
        else:
            raise ValueError(f"Unsupported input format: {input_config.format}")
        
        return lf
    
    def _read_parquet(
        self,
        path: str,
        storage: StorageConfig,
        filters: Optional[list[tuple[str, str, Any]]] = None,
        columns: Optional[list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Read Parquet file(s) to LazyFrame.
        
        Args:
            path: Path to Parquet file or directory.
            storage: Storage configuration.
            filters: Partition filters [(col, op, val), ...].
            columns: Columns to select.
        
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
            # Check if path is a directory (hive partitioned) or single file
            p = Path(path)
            if p.is_dir():
                # Hive partitioned dataset
                return pl.scan_parquet(
                    str(p / "**/*.parquet"),
                    hive_partitioning=True,
                )
            else:
                return pl.scan_parquet(path)
    
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
        # Determine compression string
        compression = str(output_config.compression)
        if output_config.compression == CompressionCodec.ZSTD:
            compression = f"zstd({output_config.compression_level})"
        
        # Handle partitioned writes
        if output_config.partition_cols:
            return self._write_partitioned_parquet(
                df=df,
                path=path,
                partition_cols=output_config.partition_cols,
                compression=compression,
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
                row_group_size=output_config.row_group_size,
            )
            return 1
    
    def _write_partitioned_parquet(
        self,
        df: pl.DataFrame,
        path: str,
        partition_cols: list[str],
        compression: str,
        mode: WriteMode,
    ) -> int:
        """
        Write DataFrame to partitioned Parquet (Hive-style).
        
        Args:
            df: DataFrame to write.
            path: Base output path.
            partition_cols: Columns to partition by.
            compression: Compression codec.
            mode: Write mode.
        
        Returns:
            Number of files written.
        """
        file_count = 0
        base_path = Path(path)
        
        # Group by partition columns
        for partition_values, group_df in df.group_by(partition_cols):
            # Build partition path
            if isinstance(partition_values, tuple):
                partition_parts = [
                    f"{col}={val}" 
                    for col, val in zip(partition_cols, partition_values)
                ]
            else:
                partition_parts = [f"{partition_cols[0]}={partition_values}"]
            
            partition_path = base_path / "/".join(partition_parts)
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
            
            # Drop partition columns from data (they're in the path)
            data_cols = [c for c in group_df.columns if c not in partition_cols]
            write_df = group_df.select(data_cols)
            
            write_df.write_parquet(str(file_path), compression=compression)
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
