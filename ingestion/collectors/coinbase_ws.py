"""Coinbase Advanced Trade WebSocket collector - I/O only."""
import asyncio
import json
import logging
import time
import secrets
import hashlib
from typing import List, Optional

import websockets
import jwt
from cryptography.hazmat.primitives import serialization

from .base_collector import BaseCollector
from ..utils.time import utc_now
from ..utils.serialization import serialize_json_message


logger = logging.getLogger(__name__)


class CoinbaseCollector(BaseCollector):
    """
    Coinbase Advanced Trade WebSocket collector.
    
    This collector is I/O-only:
    - Connects to Coinbase Advanced Trade WS API
    - Subscribes to specified channels
    - Captures raw JSON messages
    - Pushes to LogWriter with minimal metadata
    
    NO PROCESSING:
    - No parsing beyond JSON decode
    - No transformation or aggregation
    - No business logic
    """
    
    def __init__(
        self,
        log_writer,
        api_key: str,
        api_secret: str,
        product_ids: List[str],
        channels: Optional[List[str]] = None,
        ws_url: str = "wss://advanced-trade-ws.coinbase.com",
        level2_batch_size: int = 10,  # Max products per level2 subscription
        **kwargs
    ):
        """
        Initialize Coinbase collector.
        
        Args:
            log_writer: LogWriter instance
            api_key: Coinbase Advanced Trade API key
            api_secret: Coinbase Advanced Trade API secret (PEM format)
            product_ids: List of product IDs to subscribe to (e.g., ['BTC-USD'])
            channels: List of channels (default: ['ticker', 'level2', 'market_trades'])
            ws_url: WebSocket URL
            level2_batch_size: Max products per level2 subscription (default: 10)
            **kwargs: Passed to BaseCollector
        """
        super().__init__(source_name="coinbase", log_writer=log_writer, **kwargs)
        
        self.api_key = api_key
        self.api_secret = api_secret
        self.product_ids = product_ids
        self.channels = channels or ["ticker", "level2", "market_trades"]
        self.ws_url = ws_url
        self.level2_batch_size = level2_batch_size
        
        # JWT token refresh (tokens expire every 2 minutes)
        self._jwt_token: Optional[str] = None
        self._jwt_expiry: float = 0
        
        logger.info(
            f"[CoinbaseCollector] Initialized: products={len(product_ids)} products, "
            f"channels={self.channels}, level2_batch_size={level2_batch_size}"
        )
    
    def _build_jwt(self) -> str:
        """
        Build JWT token for WebSocket authentication.
        
        Coinbase Advanced Trade requires JWT tokens that expire every 2 minutes.
        Uses ES256 algorithm with private key.
        """
        private_key_bytes = self.api_secret.encode('utf-8')
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None
        )
        
        now = int(time.time())
        jwt_payload = {
            'sub': self.api_key,
            'iss': "coinbase-cloud",
            'nbf': now,
            'exp': now + 120,  # 2 minutes
        }
        
        jwt_token = jwt.encode(
            jwt_payload,
            private_key,
            algorithm='ES256',
            headers={
                'kid': self.api_key,
                'nonce': hashlib.sha256(secrets.token_bytes(16)).hexdigest()
            },
        )
        
        self._jwt_token = jwt_token
        self._jwt_expiry = now + 120
        
        return jwt_token
    
    async def _collect(self):
        """
        Main collection loop - connects to Coinbase WS and streams messages.
        
        Handles channel-specific product limits:
        - level2: Batches products (default 10 per subscription)
        - Other channels: All products in single subscription
        """
        # Generate JWT token
        jwt_token = self._build_jwt()
        
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=10,
            max_size=10 * 1024 * 1024  # 10 MB limit for level2 snapshots
        ) as websocket:
            logger.info(f"[CoinbaseCollector] Connected to {self.ws_url}")
            
            # Subscribe to each channel
            for channel in self.channels:
                # Level2 has strict product limits - batch subscriptions
                if channel == "level2":
                    await self._subscribe_level2_batched(websocket, jwt_token)
                else:
                    # Other channels can handle more products
                    await self._subscribe_channel(websocket, channel, self.product_ids, jwt_token)
            
            # Message loop
            while not self._shutdown.is_set():
                try:
                    # Wait for message with timeout for shutdown check
                    message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=1.0
                    )
                    
                    # Capture timestamp immediately
                    capture_ts = utc_now()
                    
                    # Minimal parsing - just decode JSON
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        logger.error(f"[CoinbaseCollector] JSON decode error: {e}")
                        continue
                    
                    # Skip heartbeats at ingestion layer (no value in logs)
                    if data.get("channel") == "heartbeats":
                        continue
                    
                    # Serialize with metadata
                    record = serialize_json_message(
                        data=data,
                        timestamp=capture_ts,
                        metadata={
                            "source": "coinbase",
                            "channel": data.get("channel", "unknown"),
                        }
                    )
                    
                    # Push to log writer (non-blocking)
                    try:
                        await self.log_writer.write(record, block=False)
                        self._mark_message_received()  # Track health
                    except asyncio.QueueFull:
                        logger.warning(
                            "[CoinbaseCollector] LogWriter queue full - "
                            "message dropped (backpressure)"
                        )
                
                except asyncio.TimeoutError:
                    # Timeout is expected for shutdown checks
                    continue
                
                except websockets.exceptions.ConnectionClosed as e:
                    logger.error(
                        f"[CoinbaseCollector] WebSocket connection closed: "
                        f"code={e.code}, reason={e.reason}"
                    )
                    break
                
                except Exception as e:
                    logger.error(
                        f"[CoinbaseCollector] Error receiving message: {e}",
                        exc_info=True
                    )
                    self.stats["errors"] += 1
    
    async def _subscribe_channel(
        self,
        websocket,
        channel: str,
        product_ids: List[str],
        jwt_token: str
    ):
        """
        Subscribe to a single channel with given products.
        
        Args:
            websocket: Active WebSocket connection
            channel: Channel name (ticker, market_trades, candles, etc.)
            product_ids: List of products to subscribe
            jwt_token: JWT authentication token
        """
        subscribe_msg = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": product_ids,
            "jwt": jwt_token,
        }
        
        await websocket.send(json.dumps(subscribe_msg))
        logger.info(
            f"[CoinbaseCollector] Sent subscribe to channel={channel}, "
            f"products={len(product_ids)} products: {product_ids[:3]}..."
        )
        
        # Wait for subscription response
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            response_data = json.loads(response)
            logger.info(
                f"[CoinbaseCollector] Subscribe response for {channel}: "
                f"type={response_data.get('type')}, "
                f"channel={response_data.get('channel')}"
            )
            if response_data.get('type') == 'error':
                logger.error(
                    f"[CoinbaseCollector] Subscription error for {channel}: "
                    f"{response_data.get('message')}"
                )
        except asyncio.TimeoutError:
            logger.warning(
                f"[CoinbaseCollector] No immediate response for {channel} subscription"
            )
        except Exception as e:
            logger.error(
                f"[CoinbaseCollector] Error reading subscribe response: {e}"
            )
    
    async def _subscribe_level2_batched(
        self,
        websocket,
        jwt_token: str
    ):
        """
        Subscribe to level2 channel with product batching.
        
        Level2 has stricter limits on products per subscription than other channels.
        This method splits product_ids into batches of level2_batch_size.
        
        Args:
            websocket: Active WebSocket connection
            jwt_token: JWT authentication token
        """
        total_products = len(self.product_ids)
        batch_size = self.level2_batch_size
        
        # Split products into batches
        for i in range(0, total_products, batch_size):
            batch = self.product_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_products + batch_size - 1) // batch_size
            
            logger.info(
                f"[CoinbaseCollector] Subscribing to level2 batch {batch_num}/{total_batches}: "
                f"{len(batch)} products: {batch}"
            )
            
            await self._subscribe_channel(websocket, "level2", batch, jwt_token)
            
            # Small delay between batches to avoid rate limiting
            if i + batch_size < total_products:
                await asyncio.sleep(0.5)
