# Python Example for subscribing to a channel
import time
import json
import jwt
import hashlib
import os
import websocket
import threading
from datetime import datetime, timedelta

# Derived from your Coinbase CDP API Key
# SIGNING_KEY: the signing key provided as a part of your API key. Also called the "SECRET KEY"
# API_KEY: the api key provided as a part of your API key. also called the "API KEY NAME"
API_KEY = "deactivated wont work lol (my mistake on first commit)"
SIGNING_KEY = "deactivated wont work lol (my mistake on first commit)"
ALGORITHM = "ES256"

if not SIGNING_KEY or not API_KEY:
    raise ValueError("Missing mandatory environment variable(s)")

CHANNEL_NAMES = {
    "level2": "level2",
    "user": "user",
    "tickers": "ticker",
    "ticker_batch": "ticker_batch",
    "status": "status",
    "market_trades": "market_trades",
    "candles": "candles",
}

WS_API_URL = "wss://advanced-trade-ws.coinbase.com"

def sign_with_jwt(message, channel, products=[]):
    payload = {
        "iss": "coinbase-cloud",
        "nbf": int(time.time()),
        "exp": int(time.time()) + 120,
        "sub": API_KEY,
    }
    headers = {
        "kid": API_KEY,
        "nonce": hashlib.sha256(os.urandom(16)).hexdigest()
    }
    token = jwt.encode(payload, SIGNING_KEY, algorithm=ALGORITHM, headers=headers)
    message['jwt'] = token
    return message

def on_message(ws, message):
    data = json.loads(message)
    with open("Output1.txt", "a") as f:
        f.write(json.dumps(data) + "\n")

def subscribe_to_products(ws, products, channel_name):
    message = {
        "type": "subscribe",
        "channel": channel_name,
        "product_ids": products
    }
    signed_message = sign_with_jwt(message, channel_name, products)
    ws.send(json.dumps(signed_message))

def unsubscribe_to_products(ws, products, channel_name):
    message = {
        "type": "unsubscribe",
        "channel": channel_name,
        "product_ids": products
    }
    signed_message = sign_with_jwt(message, channel_name, products)
    ws.send(json.dumps(signed_message))

def on_open(ws):
    products = ["BTC-USD"]
    subscribe_to_products(ws, products, CHANNEL_NAMES["level2"])

def start_websocket():
    ws = websocket.WebSocketApp(WS_API_URL, on_open=on_open, on_message=on_message)
    ws.run_forever()

def main():
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.start()

    sent_unsub = False
    start_time = datetime.utcnow()

    try:
        while True:
            if (datetime.utcnow() - start_time).total_seconds() > 5 and not sent_unsub:
                # Unsubscribe after 5 seconds
                ws = websocket.create_connection(WS_API_URL)
                unsubscribe_to_products(ws, ["BTC-USD"], CHANNEL_NAMES["level2"])
                ws.close()
                sent_unsub = True
            time.sleep(1)
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    main()