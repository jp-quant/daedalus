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
# Additionally, exchange.orderbooks holds full OrderBook objects per symbol
# that accumulate Python overhead. Periodic clearing is critical on Pi4.
CCXT_CACHE_CLEAR_INTERVAL = 120  # 2 minutes (aggressive for Pi4)

# Maximum symbols per bulk subscription call.
# Coinbase Advanced has practical limits — too many symbols in one
# subscription overloads the WebSocket with L2 updates, causing ping-pong
# timeouts. Individual symbol loops are more resilient to disconnections.
# For orderbooks, we ALWAYS use per-symbol loops to isolate failures.
DEFAULT_BULK_BATCH_SIZE = 10


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
        
        Subscription strategy:
        - watchOrderBook: ALWAYS per-symbol loops (isolates failures, prevents
          cascading timeouts when one symbol's updates overwhelm the connection)
        - watchTicker/watchTrades: Use bulk methods when available, with small
          batch sizes to limit per-subscription bandwidth
        """
        try:
            exchange_class = getattr(ccxtpro, self.exchange_id)
        except AttributeError:
            logger.error(f"Exchange {self.exchange_id} not found in ccxt.pro")
            return

        # Initialize exchange with memory-optimized settings
        #
        # CCXT Pro maintains internal caches (ArrayCache) per symbol for trades,
        # tickers, OHLCV. We write everything to Parquet immediately, so we set
        # aggressive limits. Additionally, we increase WebSocket tolerance for
        # ping-pong keepalive on Pi4 where CPU spikes can delay pong responses.
        exchange_options = dict(self.options)  # Copy to avoid mutating original
        exchange_options.setdefault('tradesLimit', 1)       # Don't cache trades
        exchange_options.setdefault('OHLCVLimit', 1)        # Don't cache OHLCV
        exchange_options.setdefault('ordersLimit', 1)        # Don't cache orders
        
        config = {
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'password': self.password,
            'enableRateLimit': self.options.get('enableRateLimit', True),
            'options': exchange_options,
        }
        self.exchange = exchange_class(config)
        
        # Increase ping-pong tolerance for Pi4 (CPU spikes delay pong responses)
        # Default: keepAlive=30000ms, maxPingPongMisses=2 → 60s timeout
        # Pi4 setting: keepAlive=30000ms, maxPingPongMisses=4 → 120s timeout
        if hasattr(self.exchange, 'streaming'):
            self.exchange.streaming = {
                **self.exchange.streaming,
                'maxPingPongMisses': 4,  # More tolerant on constrained hardware
            }
        
        logger.info(f"Initialized CCXT exchange {self.exchange_id}")

        loops = []
        delay_counter = 0
        delay_step = 0.5  # 500ms between subscription starts
        
        for method, symbols in self.channels.items():
            if not hasattr(self.exchange, method):
                logger.warning(f"Exchange {self.exchange_id} does not support {method}")
                continue
            
            # ORDERBOOK: Always use per-symbol loops.
            # Reason: Bulk orderbook subscriptions create enormous bandwidth on a single
            # WebSocket. When one symbol's deep book generates heavy traffic, ping-pong
            # keepalive gets delayed, causing RequestTimeout for ALL subscribed symbols.
            # Per-symbol loops isolate failures — one symbol's timeout doesn't affect others.
            if method == "watchOrderBook":
                for symbol in symbols:
                    loops.append(self._symbol_loop(method, symbol, start_delay=delay_counter * delay_step))
                    delay_counter += 1
                continue
            
            # TICKER/TRADES: Use bulk methods with small batch sizes
            bulk_method = self.BULK_METHODS.get(method)
            use_bulk = False
            if bulk_method and hasattr(self.exchange, bulk_method) and symbols:
                if self.exchange.has.get(bulk_method):
                    use_bulk = True
            
            if use_bulk:
                batch_size = self.options.get('bulk_batch_size', DEFAULT_BULK_BATCH_SIZE)
                for i in range(0, len(symbols), batch_size):
                    batch = symbols[i:i + batch_size]
                    loops.append(self._bulk_loop(method, bulk_method, batch, start_delay=delay_counter * delay_step))
                    delay_counter += 1
            elif symbols:
                for symbol in symbols:
                    loops.append(self._symbol_loop(method, symbol, start_delay=delay_counter * delay_step))
                    delay_counter += 1
            else:
                loops.append(self._method_loop(method))
        
        if not loops:
            logger.warning(f"No valid channels configured for {self.exchange_id}")
            return

        # Add periodic cache-clearing task to prevent CCXT internal memory growth
        loops.append(self._cache_clearing_loop())

        # Run all loops concurrently
        try:
            await asyncio.gather(*loops, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in CCXT collector main loop for {self.exchange_id}: {e}")
        finally:
            # Close exchange connection and clear all internal state
            if self.exchange:
                try:
                    await self.exchange.close()
                except Exception as e:
                    logger.warning(f"Error closing exchange {self.exchange_id}: {e}")
                self.exchange = None
            logger.info(f"Closed CCXT exchange {self.exchange_id}")

    async def _cache_clearing_loop(self):
        """
        Periodically clear CCXT Pro internal caches to prevent memory growth.
        
        CCXT Pro maintains several internal data structures that grow unboundedly:
        
        1. exchange.trades: ArrayCacheBySymbolById with hashmap + index duplicates
           - Even with tradesLimit=1, the hashmap/index structures have per-symbol overhead
        2. exchange.tickers: Dict of ticker dicts, one per symbol
        3. exchange.orderbooks: Dict of OrderBook objects, each with:
           - Two OrderBookSide lists (bids/asks) with parallel _index lists  
           - A cache list for pending updates
           - NOT bounded by depth parameter between limit() calls
        4. exchange.ohlcvs: Similar ArrayCache structure
        
        Since we write all data to Parquet immediately, none of these caches serve 
        any purpose. We clear them aggressively.
        
        Note: We do NOT clear exchange.orderbooks because CCXT uses them for
        incremental orderbook maintenance. Clearing them would force full snapshots
        on every update. Instead, we just let the depth limit control their size.
        """
        logger.info(f"[{self.exchange_id}] Cache clearing task started (interval={CCXT_CACHE_CLEAR_INTERVAL}s)")
        
        while not self._shutdown.is_set():
            try:
                await asyncio.sleep(CCXT_CACHE_CLEAR_INTERVAL)
                
                if self.exchange is None:
                    continue
                
                cleared_items = 0
                
                # Clear trades cache — we've already written these to Parquet
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
                
                # Clear myTrades cache (if any)
                if hasattr(self.exchange, 'myTrades') and self.exchange.myTrades:
                    cleared_items += len(self.exchange.myTrades)
                    self.exchange.myTrades.clear()
                
                # Trim orderbook internal caches.
                # OrderBook objects have a .cache list for pending delta updates
                # that can grow between processing cycles. Clear these.
                if hasattr(self.exchange, 'orderbooks'):
                    for symbol, ob in self.exchange.orderbooks.items():
                        if hasattr(ob, 'cache') and ob.cache:
                            cleared_items += len(ob.cache)
                            ob.cache.clear()
                
                # Force garbage collection (gen 0+1+2) to reclaim freed memory
                gc.collect(0)
                gc.collect(1)
                collected = gc.collect(2)
                
                logger.info(
                    f"[{self.exchange_id}] Cache cleared: {cleared_items} entries, "
                    f"orderbooks={len(getattr(self.exchange, 'orderbooks', {}))}, "
                    f"gc_collected={collected}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[{self.exchange_id}] Error in cache clearing: {e}")
                await asyncio.sleep(60)  # Back off on error

    async def _symbol_loop(self, method: str, symbol: str, start_delay: float = 0.0):
        """
        Loop for a specific symbol and method.
        
        Handles CCXT RequestTimeout gracefully — these occur when the WebSocket
        ping-pong keepalive fails. CCXT internally reconnects on the next watch*()
        call, so we just log and retry with a short delay.
        """
        if start_delay > 0:
            await asyncio.sleep(start_delay)
        logger.info(f"Starting {self.exchange_id} {method} {symbol}")
        
        channel_type = self.METHOD_TO_CHANNEL.get(method, method.replace("watch", "").lower())
        
        while not self._shutdown.is_set():
            try:
                # Call the watch method (e.g. await exchange.watchOrderBook(symbol))
                if method == "watchOrderBook" and self.max_orderbook_depth:
                    response = await getattr(self.exchange, method)(symbol, limit=self.max_orderbook_depth)
                else:
                    response = await getattr(self.exchange, method)(symbol)
                
                # Prepare data for storage — strip info dict and truncate depth
                data_to_store = response
                if channel_type == "orderbook" and self.max_orderbook_depth:
                    if isinstance(response, dict):
                        data_to_store = {
                            k: v for k, v in response.items() 
                            if k != 'info'
                        }
                        if 'bids' in response:
                            data_to_store['bids'] = response['bids'][:self.max_orderbook_depth]
                        if 'asks' in response:
                            data_to_store['asks'] = response['asks'][:self.max_orderbook_depth]
                elif isinstance(data_to_store, dict) and 'info' in data_to_store:
                    data_to_store = {k: v for k, v in data_to_store.items() if k != 'info'}
                
                capture_ts = utc_now()
                msg = {
                    "type": channel_type,
                    "exchange": self.exchange_id,
                    "symbol": symbol,
                    "method": method,
                    "data": data_to_store,
                    "collected_at": self.exchange.milliseconds(),
                    "capture_ts": capture_ts.isoformat()
                }
                
                try:
                    await self.log_writer.write(msg, block=False)
                    self._mark_message_received()
                except asyncio.QueueFull:
                    pass  # Drop silently — stats tracked by writer
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break
                # Classify the error for appropriate handling:
                # - Timeouts: ping-pong failure, CCXT reconnects on next call
                # - Connection closed (1006): server-side disconnect, also auto-reconnects
                # - Other: genuine errors, log at ERROR and back off
                error_name = type(e).__name__
                error_str = str(e).lower()
                if ('Timeout' in error_name or 'timeout' in error_str
                        or 'connection closed' in error_str or '1006' in str(e)):
                    logger.debug(f"[{self.exchange_id}] {method} {symbol}: {error_name}, reconnecting...")
                    await asyncio.sleep(1.0)
                else:
                    logger.error(f"Error in {self.exchange_id} {method} {symbol}: {e}")
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
                    self._mark_message_received()
                except asyncio.QueueFull:
                    pass
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break
                error_name = type(e).__name__
                error_str = str(e).lower()
                if ('Timeout' in error_name or 'timeout' in error_str
                        or 'connection closed' in error_str or '1006' in str(e)):
                    logger.debug(f"[{self.exchange_id}] {method}: {error_name}, reconnecting...")
                    await asyncio.sleep(1.0)
                else:
                    logger.error(f"Error in {self.exchange_id} {method}: {e}")
                    await asyncio.sleep(self.reconnect_delay)

    async def _bulk_loop(self, method: str, bulk_method: str, symbols: List[str], start_delay: float = 0.0):
        """
        Loop for bulk subscription methods (tickers, trades).
        
        Handles CCXT RequestTimeout gracefully — these occur when the WebSocket
        ping-pong keepalive fails. CCXT internally reconnects on the next watch*()
        call, so we just log and retry with a short delay.
        """
        if start_delay > 0:
            await asyncio.sleep(start_delay)
        logger.info(f"Starting {self.exchange_id} {bulk_method} for {len(symbols)} symbols")
        
        channel_type = self.METHOD_TO_CHANNEL.get(method, method.replace("watch", "").lower())
        
        # Build symbol remap for exchanges where WS API returns different
        # symbols than subscribed (e.g., Coinbase returns BTC/USD for BTC/USDC).
        # CCXT's tryResolveUsdc fixes subscription resolution but NOT the
        # symbol field in returned data. Since the canonical symbol is USD,
        # no remap is needed if config uses USD pairs. If config uses USDC,
        # remap CCXT's USD response back to the configured USDC symbol.
        symbol_remap = {}
        for s in symbols:
            if s.endswith('/USDC'):
                usd_variant = s[:-1]  # BTC/USDC -> BTC/USD
                symbol_remap[usd_variant] = s
        if symbol_remap:
            logger.info(f"[{self.exchange_id}] Symbol remap active: {len(symbol_remap)} USD->USDC mappings")
        
        while not self._shutdown.is_set():
            try:
                # Call bulk method
                if bulk_method == "watchOrderBookForSymbols" and self.max_orderbook_depth:
                    response = await getattr(self.exchange, bulk_method)(symbols, limit=self.max_orderbook_depth)
                else:
                    response = await getattr(self.exchange, bulk_method)(symbols)
                
                capture_ts = utc_now()
                
                if bulk_method == "watchTickers":
                    items = response.items() if isinstance(response, dict) else \
                            ((t.get('symbol'), t) for t in response)
                    for sym, ticker in items:
                        sym = symbol_remap.get(sym, sym)
                        ticker_data = {k: v for k, v in ticker.items() if k != 'info'} if isinstance(ticker, dict) else ticker
                        try:
                            await self.log_writer.write({
                                "type": channel_type, "exchange": self.exchange_id,
                                "symbol": sym, "method": method, "data": ticker_data,
                                "collected_at": self.exchange.milliseconds(),
                                "capture_ts": capture_ts.isoformat()
                            }, block=False)
                            self._mark_message_received()
                        except asyncio.QueueFull:
                            pass

                elif bulk_method == "watchTradesForSymbols":
                    if isinstance(response, list) and response:
                        by_symbol = {}
                        for trade in response:
                            s = symbol_remap.get(trade.get('symbol'), trade.get('symbol'))
                            trade_clean = {k: v for k, v in trade.items() if k != 'info'} if isinstance(trade, dict) else trade
                            by_symbol.setdefault(s, []).append(trade_clean)
                        
                        for s, trades in by_symbol.items():
                            try:
                                await self.log_writer.write({
                                    "type": channel_type, "exchange": self.exchange_id,
                                    "symbol": s, "method": method, "data": trades,
                                    "collected_at": self.exchange.milliseconds(),
                                    "capture_ts": capture_ts.isoformat()
                                }, block=False)
                                self._mark_message_received()
                            except asyncio.QueueFull:
                                pass
                        del by_symbol

                elif bulk_method == "watchOrderBookForSymbols":
                    sym = symbol_remap.get(response.get('symbol'), response.get('symbol'))
                    data_to_store = {k: v for k, v in response.items() if k != 'info'}
                    if self.max_orderbook_depth:
                        if 'bids' in data_to_store:
                            data_to_store['bids'] = data_to_store['bids'][:self.max_orderbook_depth]
                        if 'asks' in data_to_store:
                            data_to_store['asks'] = data_to_store['asks'][:self.max_orderbook_depth]
                    
                    try:
                        await self.log_writer.write({
                            "type": channel_type, "exchange": self.exchange_id,
                            "symbol": sym, "method": method, "data": data_to_store,
                            "collected_at": self.exchange.milliseconds(),
                            "capture_ts": capture_ts.isoformat()
                        }, block=False)
                        self._mark_message_received()
                    except asyncio.QueueFull:
                        pass
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break
                error_name = type(e).__name__
                error_str = str(e).lower()
                if ('Timeout' in error_name or 'timeout' in error_str
                        or 'connection closed' in error_str or '1006' in str(e)):
                    logger.debug(f"[{self.exchange_id}] {bulk_method}: {error_name}, reconnecting...")
                    await asyncio.sleep(1.0)
                else:
                    logger.error(f"Error in {self.exchange_id} {bulk_method}: {e}")
                    await asyncio.sleep(self.reconnect_delay)
