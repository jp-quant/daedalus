"""
Base collector interface for market data streaming.

Design Philosophy:
    Collectors are PURE I/O - they only:
    1. Connect to data sources (WebSocket, REST)
    2. Capture raw messages with timestamps
    3. Push to writer queue (non-blocking)
    4. Handle reconnection logic
    
    NO processing, transformation, or business logic.
    This separation ensures:
    - Minimal latency in the hot path
    - Easy testing and mocking
    - Collector-agnostic writer implementation

Reconnection Strategy:
    Uses exponential backoff with jitter to avoid thundering herd:
    - Base delay: 5s
    - Exponential growth: 5s → 10s → 20s → 40s → ...
    - Maximum delay: 5 minutes
    - Jitter: ±20% to spread reconnection attempts
    - For 24/7 operation, use max_reconnect_attempts=-1 (infinite)

Health Tracking:
    - Messages received counter
    - Last message timestamp (for staleness detection)
    - Connection state (connected/disconnected)
    - Error history for debugging

Subclass Implementation:
    Override _collect() to implement source-specific logic:
    
    async def _collect(self):
        async with websockets.connect(url) as ws:
            while not self._shutdown.is_set():
                msg = await ws.recv()
                await self.log_writer.write(msg, block=False)
                self._mark_message_received()
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from ..writers.log_writer import LogWriter


logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for all market data collectors.
    
    Collectors are I/O-only:
    - Connect to websocket
    - Capture raw messages
    - Push to LogWriter queue
    - Zero processing/transformation
    
    Attributes:
        source_name: Identifier for logging and metrics
        log_writer: Writer instance for data persistence
        auto_reconnect: Enable automatic reconnection on disconnect
        max_reconnect_attempts: -1 for infinite (24/7 operation)
        reconnect_delay: Base delay between reconnection attempts
        stats: Dictionary of health/performance metrics
    """
    
    def __init__(
        self,
        source_name: str,
        log_writer: LogWriter,
        auto_reconnect: bool = True,
        max_reconnect_attempts: int = -1,  # -1 = infinite for 24/7 operation
        reconnect_delay: float = 5.0,
    ):
        """
        Initialize base collector.
        
        Args:
            source_name: Identifier for this data source (e.g., "ccxt_binance")
            log_writer: Writer instance to push messages to (LogWriter or StreamingParquetWriter)
            auto_reconnect: Whether to automatically reconnect on disconnect
            max_reconnect_attempts: Maximum reconnection attempts (-1 for infinite)
            reconnect_delay: Initial seconds to wait between reconnect attempts
        """
        self.source_name = source_name
        self.log_writer = log_writer
        self.auto_reconnect = auto_reconnect
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        
        self._collector_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        self._connected = False
        self._last_message_time = 0.0
        
        self.stats = {
            "messages_received": 0,
            "connection_attempts": 0,
            "successful_connections": 0,
            "disconnections": 0,
            "errors": 0,
            "reconnections": 0,
            "last_error": None,
            "uptime_seconds": 0,
            "connected": False,
        }
        
        infinite_str = "infinite" if max_reconnect_attempts <= 0 else str(max_reconnect_attempts)
        logger.info(
            f"[{self.source_name}] Collector initialized: "
            f"auto_reconnect={auto_reconnect}, max_attempts={infinite_str}"
        )
    
    async def start(self):
        """
        Start the collector task.
        
        Creates an asyncio Task that runs the _run() loop.
        Safe to call multiple times - will warn if already running.
        """
        if self._collector_task is not None:
            logger.warning(f"[{self.source_name}] Collector already running")
            return
        
        self._shutdown.clear()
        self._collector_task = asyncio.create_task(self._run())
        logger.info(f"[{self.source_name}] Collector started")
    
    async def stop(self):
        """
        Stop the collector task gracefully.
        
        Sets shutdown event and waits up to 10s for clean exit.
        If timeout, forcefully cancels the task.
        """
        if self._collector_task is None:
            return
        
        logger.info(f"[{self.source_name}] Stopping collector")
        self._shutdown.set()
        
        try:
            await asyncio.wait_for(self._collector_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.error(f"[{self.source_name}] Collector did not stop within timeout")
            self._collector_task.cancel()
        
        logger.info(f"[{self.source_name}] Collector stopped. Stats: {self.stats}")
    
    async def _run(self):
        """
        Main collector loop with exponential backoff reconnection logic.
        
        Flow:
        1. Increment connection attempt counter
        2. Call _collect() (subclass implementation)
        3. On clean exit: calculate backoff, sleep, retry
        4. On error: log, calculate backoff, sleep, retry
        5. Repeat until shutdown or max attempts reached
        """
        attempt = 0
        consecutive_failures = 0
        connection_start_time = 0.0
        
        while not self._shutdown.is_set():
            try:
                self.stats["connection_attempts"] += 1
                attempt += 1
                
                # Check if we should stop (only if max_reconnect_attempts > 0)
                if self.max_reconnect_attempts > 0 and attempt > self.max_reconnect_attempts:
                    logger.error(
                        f"[{self.source_name}] Max reconnection attempts ({self.max_reconnect_attempts}) reached"
                    )
                    break
                
                attempt_desc = f"{attempt}" if self.max_reconnect_attempts <= 0 else f"{attempt}/{self.max_reconnect_attempts}"
                logger.info(f"[{self.source_name}] Connection attempt {attempt_desc}")
                
                # Mark connection start
                connection_start_time = asyncio.get_event_loop().time()
                self._connected = False
                self.stats["connected"] = False
                
                # Call subclass-specific collection logic
                await self._collect()
                
                # If we get here, connection closed normally
                connection_duration = asyncio.get_event_loop().time() - connection_start_time
                self.stats["uptime_seconds"] += connection_duration
                self.stats["disconnections"] += 1
                self._connected = False
                self.stats["connected"] = False
                
                logger.warning(
                    f"[{self.source_name}] Connection closed normally after "
                    f"{connection_duration:.1f}s, {self.stats['messages_received']} messages"
                )
                
                # Reset consecutive failures on clean disconnect
                if connection_duration > 60:  # Connection lasted > 1 min
                    consecutive_failures = 0
                
                if not self.auto_reconnect:
                    logger.info(f"[{self.source_name}] Auto-reconnect disabled, stopping")
                    break
                
                # Calculate backoff delay with exponential increase and jitter
                delay = self._calculate_backoff_delay(consecutive_failures)
                consecutive_failures += 1
                
                logger.info(f"[{self.source_name}] Reconnecting in {delay:.1f}s...")
                self.stats["reconnections"] += 1
                await asyncio.sleep(delay)
            
            except asyncio.CancelledError:
                logger.info(f"[{self.source_name}] Collector task cancelled")
                self._connected = False
                self.stats["connected"] = False
                break
            
            except Exception as e:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
                self._connected = False
                self.stats["connected"] = False
                
                # Categorize error for better logging
                error_type = type(e).__name__
                logger.error(
                    f"[{self.source_name}] {error_type}: {e}",
                    exc_info=True
                )
                
                if not self.auto_reconnect:
                    logger.error(f"[{self.source_name}] Auto-reconnect disabled, stopping after error")
                    break
                
                # Calculate backoff delay
                consecutive_failures += 1
                delay = self._calculate_backoff_delay(consecutive_failures)
                
                logger.warning(
                    f"[{self.source_name}] Reconnecting after error in {delay:.1f}s "
                    f"(consecutive failures: {consecutive_failures})..."
                )
                await asyncio.sleep(delay)
    
    def _calculate_backoff_delay(self, consecutive_failures: int) -> float:
        """
        Calculate exponential backoff delay with jitter.
        
        Formula: min(base * 2^failures, max_delay) + jitter
        
        Args:
            consecutive_failures: Number of consecutive connection failures
        
        Returns:
            Delay in seconds
        """
        import random
        
        # Exponential backoff: 5s, 10s, 20s, 40s, 80s, 160s (cap at 300s = 5min)
        base_delay = self.reconnect_delay
        max_delay = 300.0  # 5 minutes max
        
        exponential_delay = base_delay * (2 ** min(consecutive_failures, 6))
        capped_delay = min(exponential_delay, max_delay)
        
        # Add jitter (±20%) to avoid thundering herd
        jitter = capped_delay * 0.2 * (random.random() - 0.5)
        
        return max(1.0, capped_delay + jitter)  # Never less than 1s
    
    @abstractmethod
    async def _collect(self):
        """
        Subclass implements actual websocket connection and message handling.
        
        This method should:
        1. Connect to websocket
        2. Subscribe to channels
        3. Read messages in a loop
        4. Push raw messages to log_writer
        5. Handle disconnections gracefully
        """
        pass
    
    def get_stats(self):
        """Get collector statistics."""
        stats = self.stats.copy()
        
        # Add health metrics
        import time
        current_time = time.time()
        
        if self._last_message_time > 0:
            stats["seconds_since_last_message"] = int(current_time - self._last_message_time)
        else:
            stats["seconds_since_last_message"] = None
        
        stats["is_healthy"] = self._is_healthy()
        
        return stats
    
    def _is_healthy(self) -> bool:
        """
        Check if collector is healthy.
        
        A collector is considered healthy if:
        - It's connected OR
        - It received a message in the last 60 seconds
        
        Returns:
            True if healthy
        """
        import time
        
        if self._connected:
            return True
        
        if self._last_message_time > 0:
            time_since_message = time.time() - self._last_message_time
            return time_since_message < 60
        
        return False
    
    def _mark_message_received(self):
        """Mark that a message was received (for health tracking)."""
        import time
        self._last_message_time = time.time()
        self.stats["messages_received"] += 1
        
        # Mark as connected if not already
        if not self._connected:
            self._connected = True
            self.stats["connected"] = True
            self.stats["successful_connections"] += 1
            logger.info(f"[{self.source_name}] Connection established successfully")
