"""
Ingestion Pipeline Orchestrator.

Manages multiple data collectors and coordinates data writing across sources.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Union

from config import DaedalusConfig
from storage.base import StorageBackend
from storage.factory import create_ingestion_storage, get_ingestion_path
from ingestion.writers.log_writer import LogWriter
from ingestion.writers.parquet_writer import StreamingParquetWriter
from ingestion.collectors.coinbase_ws import CoinbaseCollector
from ingestion.collectors.ccxt_collector import CcxtCollector

logger = logging.getLogger(__name__)

# Type alias for any writer
Writer = Union[LogWriter, StreamingParquetWriter]


class IngestionPipeline:
    """
    Orchestrates ingestion from multiple data sources.
    
    Manages:
    - Storage backend initialization
    - Writer creation per source
    - Collector lifecycle management
    - Graceful shutdown coordination
    
    Example:
        config = load_config()
        pipeline = IngestionPipeline(config)
        
        await pipeline.start()
        await pipeline.wait_for_shutdown()
        await pipeline.stop()
    """
    
    def __init__(self, config: DaedalusConfig, sources: Optional[List[str]] = None):
        """
        Initialize ingestion pipeline.
        
        Args:
            config: Daedalus configuration
            sources: Optional list of sources to enable (e.g. ["coinbase", "ccxt"]).
                     If None, all configured sources are enabled.
        """
        self.config = config
        self.enabled_sources = sources
        
        # Initialize storage backend for ingestion
        self.storage = create_ingestion_storage(config)
        logger.info(f"Ingestion storage backend: {self.storage.backend_type}")
        logger.info(f"Raw format: {config.ingestion.raw_format}")
        
        # Collectors and writers (writer type depends on raw_format config)
        self.writers: Dict[str, Writer] = {}
        self.collectors: List = []
        
        self._shutdown_event = asyncio.Event()
    
    def _create_writer(self, source_name: str, active_path: str, ready_path: str) -> Writer:
        """
        Create the appropriate writer based on config.ingestion.raw_format.
        
        Returns:
            LogWriter for 'ndjson' format, StreamingParquetWriter for 'parquet' format.
        """
        if self.config.ingestion.raw_format == "parquet":
            return StreamingParquetWriter(
                storage=self.storage,
                active_path=active_path,
                ready_path=ready_path,
                source_name=source_name,
                batch_size=self.config.ingestion.batch_size,
                flush_interval_seconds=self.config.ingestion.flush_interval_seconds,
                queue_maxsize=self.config.ingestion.queue_maxsize,
                segment_max_mb=self.config.ingestion.segment_max_mb,
                compression=self.config.ingestion.parquet_compression,
                compression_level=self.config.ingestion.parquet_compression_level,
                partition_by=self.config.ingestion.partition_by,
            )
        else:
            return LogWriter(
                storage=self.storage,
                active_path=active_path,
                ready_path=ready_path,
                source_name=source_name,
                batch_size=self.config.ingestion.batch_size,
                flush_interval_seconds=self.config.ingestion.flush_interval_seconds,
                queue_maxsize=self.config.ingestion.queue_maxsize,
                enable_fsync=self.config.ingestion.enable_fsync,
                segment_max_mb=self.config.ingestion.segment_max_mb,
            )
    
    async def start(self):
        """Start all configured collectors."""
        logger.info("=" * 80)
        logger.info("Daedalus Ingestion Pipeline Starting")
        if self.enabled_sources:
            logger.info(f"Enabled sources: {self.enabled_sources}")
        logger.info("=" * 80)
        
        # Helper to check if source should run
        def should_run(source_name):
            if self.enabled_sources is None:
                return True
            return source_name in self.enabled_sources

        # Start Coinbase if configured
        if should_run("coinbase") and self.config.coinbase and self.config.coinbase.api_key:
            await self._start_coinbase()
        
        # Start CCXT if configured
        if should_run("ccxt") and self.config.ccxt and self.config.ccxt.exchanges:
            await self._start_ccxt()
        
        logger.info("=" * 80)
        logger.info(f"Ingestion pipeline running with {len(self.collectors)} collector(s)")
        logger.info(f"Storage: {self.storage.backend_type} @ {self.storage.base_path}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
    
    async def _start_coinbase(self):
        """Start Coinbase collector and writer."""
        logger.info("Initializing Coinbase collector...")
        
        # Get paths for this source
        active_path = get_ingestion_path(self.config, "coinbase", state="active")
        ready_path = get_ingestion_path(self.config, "coinbase", state="ready")
        
        # Create writer (type depends on config)
        coinbase_writer = self._create_writer("coinbase", active_path, ready_path)
        await coinbase_writer.start()
        self.writers["coinbase"] = coinbase_writer
        
        # Create collector
        coinbase_collector = CoinbaseCollector(
            log_writer=coinbase_writer,
            api_key=self.config.coinbase.api_key,
            api_secret=self.config.coinbase.api_secret,
            product_ids=self.config.coinbase.product_ids,
            channels=self.config.coinbase.channels,
            ws_url=self.config.coinbase.ws_url,
            level2_batch_size=getattr(self.config.coinbase, 'level2_batch_size', 10),
            auto_reconnect=self.config.ingestion.auto_reconnect,
            max_reconnect_attempts=self.config.ingestion.max_reconnect_attempts,
            reconnect_delay=self.config.ingestion.reconnect_delay,
        )
        await coinbase_collector.start()
        self.collectors.append(coinbase_collector)
        
        logger.info(f"✓ Coinbase collector started ({self.config.ingestion.raw_format} format)")
        logger.info(f"  Active: {active_path}")
        logger.info(f"  Ready:  {ready_path}")
    
    async def _start_ccxt(self):
        """Start CCXT collectors for all configured exchanges."""
        logger.info("Initializing CCXT collectors...")
        
        # Create a single unified writer for all CCXT exchanges
        source_name = "ccxt"
        active_path = get_ingestion_path(self.config, source_name, state="active")
        ready_path = get_ingestion_path(self.config, source_name, state="ready")
        
        # Create writer (type depends on config)
        ccxt_writer = self._create_writer(source_name, active_path, ready_path)
        await ccxt_writer.start()
        self.writers[source_name] = ccxt_writer
        
        logger.info(f"✓ CCXT unified writer started ({self.config.ingestion.raw_format} format)")
        logger.info(f"  Active: {active_path}")
        logger.info(f"  Ready:  {ready_path}")

        successful_collectors = 0
        failed_collectors = []

        for exchange_id, exchange_config in self.config.ccxt.exchanges.items():
            try:
                logger.info(f"Starting CCXT collector for {exchange_id}...")
                
                # Create collector using the shared writer
                ccxt_collector = CcxtCollector(
                    log_writer=ccxt_writer,
                    exchange_id=exchange_id,
                    channels=exchange_config.channels,
                    api_key=exchange_config.api_key,
                    api_secret=exchange_config.api_secret,
                    password=exchange_config.password,
                    options=exchange_config.options,
                    max_orderbook_depth=exchange_config.max_orderbook_depth,
                    auto_reconnect=self.config.ingestion.auto_reconnect,
                    max_reconnect_attempts=self.config.ingestion.max_reconnect_attempts,
                    reconnect_delay=self.config.ingestion.reconnect_delay,
                )
                await ccxt_collector.start()
                self.collectors.append(ccxt_collector)
                successful_collectors += 1
                
                logger.info(f"✓ CCXT collector started for {exchange_id}")
                
            except Exception as e:
                failed_collectors.append((exchange_id, str(e)))
                logger.error(
                    f"✗ Failed to start CCXT collector for {exchange_id}: {e}",
                    exc_info=True
                )
        
        # Log summary
        logger.info(
            f"CCXT collectors: {successful_collectors} started, "
            f"{len(failed_collectors)} failed"
        )
        
        if failed_collectors:
            logger.warning("Failed exchanges:")
            for exchange_id, error in failed_collectors:
                logger.warning(f"  - {exchange_id}: {error}")
            logger.warning(
                "Note: Failed exchanges will NOT auto-start later. "
                "Restart the pipeline to retry."
            )

    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()
    
    def request_shutdown(self):
        """Request pipeline shutdown."""
        logger.info("Shutdown requested")
        self._shutdown_event.set()
    
    async def stop(self):
        """Stop all collectors and writers."""
        logger.info("Shutting down ingestion pipeline...")
        
        # Stop collectors
        for collector in self.collectors:
            try:
                await collector.stop()
            except Exception as e:
                logger.error(f"Error stopping collector: {e}")
        
        # Stop writers
        for name, writer in self.writers.items():
            try:
                await writer.stop()
            except Exception as e:
                logger.error(f"Error stopping writer {name}: {e}")
        
        logger.info("Ingestion pipeline stopped")
    
    async def print_stats(self):
        """Print statistics for all collectors and writers with health indicators."""
        logger.info("=" * 80)
        logger.info("Pipeline Statistics")
        logger.info("=" * 80)
        
        # Collector stats with health indicators
        healthy_collectors = 0
        unhealthy_collectors = 0
        
        for collector in self.collectors:
            stats = collector.get_stats()
            is_healthy = stats.get("is_healthy", False)
            health_icon = "✓" if is_healthy else "✗"
            
            if is_healthy:
                healthy_collectors += 1
            else:
                unhealthy_collectors += 1
            
            # Format stats for logging
            connected = "🟢" if stats.get("connected") else "🔴"
            msgs = stats.get("messages_received", 0)
            errors = stats.get("errors", 0)
            reconnections = stats.get("reconnections", 0)
            uptime = stats.get("uptime_seconds", 0)
            
            logger.info(
                f"{health_icon} {connected} {collector.source_name}: "
                f"msgs={msgs:,}, errors={errors}, reconnects={reconnections}, "
                f"uptime={uptime:.0f}s"
            )
            
            # Show last error if present
            if stats.get("last_error"):
                logger.warning(f"  └─ Last error: {stats['last_error']}")
            
            # Show seconds since last message if available
            if stats.get("seconds_since_last_message") is not None:
                secs = stats["seconds_since_last_message"]
                if secs > 60:
                    logger.warning(f"  └─ No messages for {secs}s")
        
        # Writer stats
        for name, writer in self.writers.items():
            stats = writer.get_stats()
            logger.info(
                f"Writer {name}: written={stats.get('messages_written', 0):,}, "
                f"queue={stats.get('queue_size', 0)}, flushes={stats.get('flushes', 0)}"
            )
        
        # Health summary
        total_collectors = len(self.collectors)
        logger.info("=" * 80)
        logger.info(
            f"Health: {healthy_collectors}/{total_collectors} collectors healthy"
        )
        
        if unhealthy_collectors > 0:
            logger.warning(f"⚠️  {unhealthy_collectors} collector(s) unhealthy - check logs")
        
        logger.info("=" * 80)
