"""CCXT Pro Collector for streaming market data.

Optimized for high-throughput WebSocket streaming:
- Concurrent subscription initialization with staggered starts
- Bulk subscription methods for exchanges that support them
- Non-blocking writes with backpressure handling
- Minimal CPU overhead in the hot path

Architecture:
    WebSocket -> CcxtCollector._bulk_loop() -> Writer.queue -> Parquet
    
    Each exchange gets one CcxtCollector instance.
    Each channel/symbol batch gets one asyncio task (_bulk_loop or _symbol_loop).
    All tasks share a single writer queue with backpressure.

Performance Considerations:
    - Capture timestamps immediately after recv() to minimize latency
    - Avoid CPU work in the hot path (defer to writer)
    - Use bulk methods when available (reduces connection overhead)
    - Stagger subscription starts to avoid rate limits
"""
import asyncio
import logging
import gc
import os
import time
from typing import List, Optional, Dict, Any

import ccxt.pro as ccxtpro

from .base_collector import BaseCollector
from ..utils.time import utc_now

logger = logging.getLogger(__name__)

# Performance tuning constants
# Stagger delay between subscription batches (ms) - prevents rate limiting
SUBSCRIPTION_STAGGER_MS = 100

# Default: No limit on concurrent subscription initializations
# Set to a positive integer (e.g., 5) if exchange rate-limits during startup
DEFAULT_MAX_CONCURRENT_SUBSCRIPTIONS = None

# CPU cores available (used for thread pool sizing)
CPU_COUNT = os.cpu_count() or 4

# How often to clear CCXT internal caches (seconds)
# CCXT Pro maintains internal ArrayCache objects for trades, tickers, etc.
# Even with tradesLimit=1, the internal orderbook objects accumulate
# Python dict/list overhead. Periodic clearing helps on memory-constrained devices.
CCXT_CACHE_CLEAR_INTERVAL = 300  # 5 minutes


class CcxtCollector(BaseCollector):
    """
    CCXT Pro collector that streams market data via WebSocket.
    
    Supports multiple channels (methods) per exchange.
    """
    
    # Map CCXT method names to internal channel names
    METHOD_TO_CHANNEL = {
        "watchTicker": "ticker",
        "watchTrades": "trades",
        "watchOrderBook": "orderbook",
        "watchBidsAsks": "bidask",
        "watchOHLCV": "ohlcv",
    }

    # Map singular methods to their bulk counterparts
    BULK_METHODS = {
        "watchTicker": "watchTickers",
        "watchTrades": "watchTradesForSymbols",
        "watchOrderBook": "watchOrderBookForSymbols",
        "watchOHLCV": "watchOHLCVForSymbols",
    }
    
    def __init__(
        self,
        log_writer,
        exchange_id: str,
        channels: Dict[str, List[str]],
        api_key: str = "",
        api_secret: str = "",
        password: str = "",
        options: Dict[str, Any] = None,
        max_orderbook_depth: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize CCXT collector.
        
        Args:
            log_writer: LogWriter instance
            exchange_id: CCXT exchange ID (e.g., 'binance', 'kraken')
            channels: Map of method name to symbols (e.g. {'watchTicker': ['BTC/USDT']})
            api_key: API Key (optional)
            api_secret: API Secret (optional)
            password: API Password (optional)
            options: Extra exchange options
            max_orderbook_depth: Max number of bids/asks to capture (None = unlimited)
        """
        super().__init__(
            source_name=f"ccxt_{exchange_id}",
            log_writer=log_writer,
            **kwargs
        )
        self.exchange_id = exchange_id
        self.channels = channels
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.options = options or {}
        self.max_orderbook_depth = max_orderbook_depth
        
        self.exchange = None
        self._tasks = []
        
        # Optional semaphore for controlled concurrent subscription initialization
        # Set via options['max_concurrent_subscriptions'] if exchange rate-limits during startup
        # None = no limit (default), positive int = limit concurrent handshakes
        max_concurrent = self.options.get('max_concurrent_subscriptions', DEFAULT_MAX_CONCURRENT_SUBSCRIPTIONS)
        self._subscription_semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent else None

    async def _collect(self):
        """
        Main collection loop.
        Initializes exchange, spawns channel loops, and handles cleanup.
        """
        try:
            exchange_class = getattr(ccxtpro, self.exchange_id)
        except AttributeError:
            logger.error(f"Exchange {self.exchange_id} not found in ccxt.pro")
            return

        # Initialize exchange
        # enableRateLimit: True is recommended by CCXT to handle the initial REST handshake 
        # and internal throttling to avoid 429s. It generally does not affect WebSocket 
        # throughput once connected, but ensures connection stability.
        #
        # MEMORY OPTIMIZATION: Set aggressive cache limits.
        # CCXT Pro maintains internal ArrayCache per symbol for trades, tickers, etc.
        # Without limits, these caches grow unbounded and leak hundreds of MB over hours.
        # We write everything to Parquet immediately, so we don't need CCXT's caches.
        exchange_options = dict(self.options)  # Copy to avoid mutating original
        exchange_options.setdefault('tradesLimit', 1)       # Don't cache trades (we persist to Parquet)
        exchange_options.setdefault('OHLCVLimit', 1)        # Don't cache OHLCV
        exchange_options.setdefault('ordersLimit', 1)        # Don't cache orders
        exchange_options.setdefault('tickersLimit', 1)       # Don't cache tickers (available in ccxt >= 4.x)
        
        config = {
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'password': self.password,
            'enableRateLimit': self.options.get('enableRateLimit', True),
            'options': exchange_options,
        }
        self.exchange = exchange_class(config)
        logger.info(f"Initialized CCXT exchange {self.exchange_id}")

        loops = []
        delay_counter = 0
        # 100ms delay between subscriptions to avoid rate limits
        # Adjust this if the exchange is still strict
        delay_step = 1
        
        for method, symbols in self.channels.items():
            if not hasattr(self.exchange, method):
                logger.warning(f"Exchange {self.exchange_id} does not support {method}")
                continue
                
            # Check for bulk method support
            bulk_method = self.BULK_METHODS.get(method)
            use_bulk = False
            if bulk_method and hasattr(self.exchange, bulk_method) and symbols:
                # Check if exchange capability explicitly says True (optional but good)
                if self.exchange.has.get(bulk_method):
                    use_bulk = True
            
            if use_bulk:
                # Batch symbols to avoid rate limits (especially for Coinbase)
                # Default to 10 as requested for Coinbase Advanced Trade
                batch_size = self.options.get('orderbook_watch_batch_size', 20)
                
                for i in range(0, len(symbols), batch_size):
                    batch = symbols[i:i + batch_size]
                    loops.append(self._bulk_loop(method, bulk_method, batch, start_delay=delay_counter * delay_step))
                    delay_counter += 1
                    
            elif symbols:
                # Create a loop for each symbol if the method requires it
                for symbol in symbols:
                    loops.append(self._symbol_loop(method, symbol, start_delay=delay_counter * delay_step))
                    delay_counter += 1
            else:
                # Method without symbols (e.g. watchBalance)
                loops.append(self._method_loop(method))
        
        if not loops:
            logger.warning(f"No valid channels configured for {self.exchange_id}")
            return

        # Add periodic cache-clearing task to prevent CCXT internal memory growth
        loops.append(self._cache_clearing_loop())

        # Run all loops concurrently
        try:
            # Stagger the start of each loop to avoid rate limits
            # We wrap the loops in a way that they start with a delay
            # But asyncio.gather expects coroutines. 
            # We can modify _symbol_loop to accept a delay.
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in CCXT collector main loop for {self.exchange_id}: {e}")
        finally:
            # Close exchange connection
            if self.exchange:
                await self.exchange.close()
                self.exchange = None
            logger.info(f"Closed CCXT exchange {self.exchange_id}")

    async def _cache_clearing_loop(self):
        """
        Periodically clear CCXT Pro internal caches to prevent memory growth.
        
        CCXT Pro maintains internal ArrayCache objects for trades, tickers, OHLCV, etc.
        Even with tradesLimit=1, the Python dict/list overhead from the exchange's
        internal state (orderbooks dict, tickers dict, trades dict) accumulates
        over hours of operation. This is especially critical on memory-constrained
        devices like Raspberry Pi 4.
        
        This task runs alongside the data collection loops and periodically:
        1. Clears the internal trades cache (we already wrote data to Parquet)
        2. Clears the internal tickers cache
        3. Forces Python garbage collection
        """
        logger.info(f"[{self.exchange_id}] Cache clearing task started (interval={CCXT_CACHE_CLEAR_INTERVAL}s)")
        
        while not self._shutdown.is_set():
            try:
                await asyncio.sleep(CCXT_CACHE_CLEAR_INTERVAL)
                
                if self.exchange is None:
                    continue
                
                cleared_items = 0
                
                # Clear trades cache - we've already written these to Parquet
                if hasattr(self.exchange, 'trades') and self.exchange.trades:
                    cleared_items += len(self.exchange.trades)
                    self.exchange.trades.clear()
                
                # Clear tickers cache
                if hasattr(self.exchange, 'tickers') and self.exchange.tickers:
                    cleared_items += len(self.exchange.tickers)
                    self.exchange.tickers.clear()
                
                # Clear OHLCV cache
                if hasattr(self.exchange, 'ohlcvs') and self.exchange.ohlcvs:
                    cleared_items += len(self.exchange.ohlcvs)
                    self.exchange.ohlcvs.clear()
                
                # Force garbage collection to reclaim freed memory
                collected = gc.collect()
                
                logger.info(
                    f"[{self.exchange_id}] Cache cleared: {cleared_items} entries removed, "
                    f"gc collected {collected} objects"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[{self.exchange_id}] Error in cache clearing: {e}")
                await asyncio.sleep(60)  # Back off on error

    async def _symbol_loop(self, method: str, symbol: str, start_delay: float = 0.0):
        """
        Loop for a specific symbol and method.
        
        Optionally uses semaphore to limit concurrent subscription initializations
        if max_concurrent_subscriptions is set in options.
        """
        # Optional semaphore for exchanges that rate-limit during subscription setup
        if self._subscription_semaphore:
            async with self._subscription_semaphore:
                if start_delay > 0:
                    await asyncio.sleep(start_delay)
                logger.info(f"Starting {self.exchange_id} {method} {symbol}")
        else:
            # No semaphore - initialize immediately (maximum throughput)
            if start_delay > 0:
                await asyncio.sleep(start_delay)
            logger.info(f"Starting {self.exchange_id} {method} {symbol}")
        
        # Resolve internal channel name (outside semaphore - fast operation)
        channel_type = self.METHOD_TO_CHANNEL.get(method, method.replace("watch", "").lower())
        
        while not self._shutdown.is_set():
            try:
                # Call the watch method
                # e.g. await exchange.watchTicker(symbol)
                response = await getattr(self.exchange, method)(symbol)
                
                # Prepare data for storage
                data_to_store = response

                # Handle orderbook depth truncation if configured
                if channel_type == "orderbook" and self.max_orderbook_depth:
                    if isinstance(response, dict):
                        # Create a shallow copy to avoid modifying CCXT internal state
                        data_to_store = response.copy()
                        if 'bids' in response:
                            data_to_store['bids'] = response['bids'][:self.max_orderbook_depth]
                        if 'asks' in response:
                            data_to_store['asks'] = response['asks'][:self.max_orderbook_depth]
                
                # Capture timestamp immediately after receipt
                capture_ts = utc_now()

                # Standardize and write
                # MEMORY OPTIMIZATION: Strip 'info' dict from data before queuing.
                # The 'info' field contains the full raw exchange response which can be
                # very large. We serialize it to JSON in the writer anyway, but holding
                # the full Python dict in the queue wastes memory. Strip it here.
                if isinstance(data_to_store, dict) and 'info' in data_to_store:
                    data_to_store = {k: v for k, v in data_to_store.items() if k != 'info'}
                
                msg = {
                    "type": channel_type,
                    "exchange": self.exchange_id,
                    "symbol": symbol,
                    "method": method,
                    "data": data_to_store,
                    "collected_at": self.exchange.milliseconds(),
                    "capture_ts": capture_ts.isoformat()
                }
                
                # Write to log writer (non-blocking to avoid stalling the websocket loop)
                try:
                    await self.log_writer.write(msg, block=False)
                    self._mark_message_received()  # Track health
                except asyncio.QueueFull:
                    logger.warning(f"[{self.source_name}] Queue full, dropping message")
                
            except Exception as e:
                if self._shutdown.is_set():
                    break
                logger.error(f"Error in {self.exchange_id} {method} {symbol}: {e}")
                # Simple backoff
                await asyncio.sleep(self.reconnect_delay)

    async def _method_loop(self, method: str):
        """Loop for a method without symbol arguments."""
        logger.info(f"Starting {self.exchange_id} {method}")
        
        # Resolve internal channel name
        channel_type = self.METHOD_TO_CHANNEL.get(method, method.replace("watch", "").lower())
        
        while not self._shutdown.is_set():
            try:
                response = await getattr(self.exchange, method)()
                
                # Capture timestamp immediately after receipt
                capture_ts = utc_now()
                
                # Strip 'info' to reduce memory pressure
                data_to_store = response
                if isinstance(response, dict) and 'info' in response:
                    data_to_store = {k: v for k, v in response.items() if k != 'info'}
                
                msg = {
                    "type": channel_type,
                    "exchange": self.exchange_id,
                    "method": method,
                    "data": data_to_store,
                    "collected_at": self.exchange.milliseconds(),
                    "capture_ts": capture_ts.isoformat()
                }
                
                # Write to log writer (non-blocking)
                try:
                    await self.log_writer.write(msg, block=False)
                    self._mark_message_received()  # Track health
                except asyncio.QueueFull:
                    logger.warning(f"[{self.source_name}] Queue full, dropping message")
                
            except Exception as e:
                if self._shutdown.is_set():
                    break
                logger.error(f"Error in {self.exchange_id} {method}: {e}")
                await asyncio.sleep(self.reconnect_delay)

    async def _bulk_loop(self, method: str, bulk_method: str, symbols: List[str], start_delay: float = 0.0):
        """
        Loop for bulk subscription methods.
        
        Optionally uses semaphore to limit concurrent subscription initializations
        if max_concurrent_subscriptions is set in options.
        """
        # Optional semaphore for exchanges that rate-limit during subscription setup
        if self._subscription_semaphore:
            async with self._subscription_semaphore:
                if start_delay > 0:
                    await asyncio.sleep(start_delay)
                logger.info(f"Starting {self.exchange_id} {bulk_method} for {len(symbols)} symbols")
        else:
            # No semaphore - initialize immediately (maximum throughput)
            if start_delay > 0:
                await asyncio.sleep(start_delay)
            logger.info(f"Starting {self.exchange_id} {bulk_method} for {len(symbols)} symbols")
        
        # Resolve channel type (outside semaphore - fast operation)
        channel_type = self.METHOD_TO_CHANNEL.get(method, method.replace("watch", "").lower())
        
        while not self._shutdown.is_set():
            try:
                # Call bulk method
                if bulk_method == "watchOrderBookForSymbols" and self.max_orderbook_depth:
                     response = await getattr(self.exchange, bulk_method)(symbols, limit=self.max_orderbook_depth)
                else:
                     response = await getattr(self.exchange, bulk_method)(symbols)
                
                capture_ts = utc_now()
                msgs = []
                
                if bulk_method == "watchTickers":
                    # response is Dict[symbol, Ticker] or List[Ticker]
                    if isinstance(response, dict):
                        for sym, ticker in response.items():
                            # Strip 'info' to reduce memory pressure
                            ticker_data = {k: v for k, v in ticker.items() if k != 'info'} if isinstance(ticker, dict) else ticker
                            msgs.append({
                                "type": channel_type,
                                "exchange": self.exchange_id,
                                "symbol": sym,
                                "method": method,
                                "data": ticker_data,
                                "collected_at": self.exchange.milliseconds(),
                                "capture_ts": capture_ts.isoformat()
                            })
                    elif isinstance(response, list):
                         for ticker in response:
                            sym = ticker.get('symbol')
                            ticker_data = {k: v for k, v in ticker.items() if k != 'info'} if isinstance(ticker, dict) else ticker
                            msgs.append({
                                "type": channel_type,
                                "exchange": self.exchange_id,
                                "symbol": sym,
                                "method": method,
                                "data": ticker_data,
                                "collected_at": self.exchange.milliseconds(),
                                "capture_ts": capture_ts.isoformat()
                            })

                elif bulk_method == "watchTradesForSymbols":
                    # response is List[Trade]
                    if isinstance(response, list) and response:
                        # Group by symbol to be safe
                        by_symbol = {}
                        for trade in response:
                            s = trade.get('symbol')
                            # Strip 'info' from each trade to reduce memory
                            trade_clean = {k: v for k, v in trade.items() if k != 'info'} if isinstance(trade, dict) else trade
                            if s not in by_symbol:
                                by_symbol[s] = []
                            by_symbol[s].append(trade_clean)
                        
                        for s, trades in by_symbol.items():
                            msgs.append({
                                "type": channel_type,
                                "exchange": self.exchange_id,
                                "symbol": s,
                                "method": method,
                                "data": trades,
                                "collected_at": self.exchange.milliseconds(),
                                "capture_ts": capture_ts.isoformat()
                            })

                elif bulk_method == "watchOrderBookForSymbols":
                    # response is OrderBook (dict) for the updated symbol
                    sym = response.get('symbol')
                    data_to_store = response
                    if self.max_orderbook_depth:
                        if isinstance(response, dict):
                            data_to_store = response.copy()
                            if 'bids' in response:
                                data_to_store['bids'] = response['bids'][:self.max_orderbook_depth]
                            if 'asks' in response:
                                data_to_store['asks'] = response['asks'][:self.max_orderbook_depth]
                    
                    # Strip 'info' to reduce memory
                    if isinstance(data_to_store, dict) and 'info' in data_to_store:
                        data_to_store = {k: v for k, v in data_to_store.items() if k != 'info'}
                    
                    msgs.append({
                        "type": channel_type,
                        "exchange": self.exchange_id,
                        "symbol": sym,
                        "method": method,
                        "data": data_to_store,
                        "collected_at": self.exchange.milliseconds(),
                        "capture_ts": capture_ts.isoformat()
                    })
                
                else:
                    # Fallback for other bulk methods (e.g. OHLCV)
                    # Assuming they return a structure with 'symbol' or we can't easily map
                    logger.warning(f"Unhandled bulk method return for {bulk_method}")

                # Write messages
                for msg in msgs:
                    try:
                        await self.log_writer.write(msg, block=False)
                        self._mark_message_received()  # Track health
                    except asyncio.QueueFull:
                        pass

            except Exception as e:
                if self._shutdown.is_set():
                    break
                logger.error(f"Error in {self.exchange_id} {bulk_method}: {e}")
                await asyncio.sleep(self.reconnect_delay)
