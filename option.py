import json
import time
import threading
import requests
import websocket
from datetime import datetime, timezone

BASE_URL = "https://api.india.delta.exchange"
WS_URL = "wss://socket.india.delta.exchange"  # Correct: no /live path in the URL

# =========================
# GLOBAL STORAGE
# =========================
orderbooks = {}
orderbooks_lock = threading.Lock()
symbols_map = {}
ws_ready = threading.Event()

MAX_SYMBOLS_PER_CONNECTION = 20  # l2_orderbook hard limit per connection


# =========================
# FETCH PRODUCTS
# =========================
def get_products():
    products = []
    url = BASE_URL + "/v2/products"
    params = {
        "contract_types": "call_options,put_options",
        "page_size": 100
    }

    while True:
        try:
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(f"[ERROR] Failed to fetch products: {e}")
            break

        products.extend(data.get("result", []))

        after = data.get("meta", {}).get("after")
        if not after:
            break

        params["after"] = after

    return products


# =========================
# GROUP OPTIONS
# =========================
def prepare_symbols():
    global symbols_map

    products = get_products()
    temp = {}

    for p in products:
        if p.get("underlying_asset", {}).get("symbol") != "ETH":
            continue

        strike = p.get("strike_price")
        expiry = p.get("settlement_time")
        symbol = p.get("symbol")

        if not strike or not expiry or not symbol:
            continue

        strike = float(strike)

        # Normalize settlement_time to date string
        try:
            expiry_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            expiry_key = expiry_dt.strftime("%d%b%y").upper()  # e.g., "31JAN25"
        except Exception:
            expiry_key = expiry

        key = (strike, expiry_key)

        if key not in temp:
            temp[key] = {}

        if p["contract_type"] == "call_options":
            temp[key]["call"] = symbol
        else:
            temp[key]["put"] = symbol

    # Keep only valid pairs
    symbols_map = {
        k: v for k, v in temp.items()
        if "call" in v and "put" in v
    }

    print(f"[INFO] Loaded {len(symbols_map)} valid call/put pairs for ETH")


# =========================
# GET FUTURES PRICE
# Uses /v2/tickers/ETHUSD directly and mark_price for accuracy
# =========================
def get_futures_price():
    url = BASE_URL + "/v2/tickers/ETHUSD"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        result = data.get("result", {})

        mark_price = result.get("mark_price")
        if mark_price:
            return float(mark_price)

        print("[WARN] mark_price not found in ETHUSD ticker response.")

    except Exception as e:
        print(f"[ERROR] Failed to fetch futures price: {e}")

    return None


# =========================
# WEBSOCKET HANDLERS
# =========================
def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")

    if msg_type == "heartbeat":
        return

    if msg_type == "l2_orderbook":
        symbol = data.get("symbol")
        bids = data.get("buy", [])
        asks = data.get("sell", [])

        if not symbol:
            return

        # Handle one-sided or empty orderbook gracefully
        if not bids or not asks:
            print(f"[WARN] One-sided or empty orderbook for {symbol}. Skipping update.")
            return

        # Correct field name is "limit_price", not "price"
        with orderbooks_lock:
            orderbooks[symbol] = {
                "bid": float(bids[0]["limit_price"]),
                "ask": float(asks[0]["limit_price"])
            }

        # Mark ws_ready only after first real orderbook data is received
        if not ws_ready.is_set():
            ws_ready.set()


def on_open(ws):
    print("[INFO] WebSocket connected")

    ws.send(json.dumps({"type": "enable_heartbeat"}))

    futures_price = get_futures_price()

    if not futures_price:
        print("[ERROR] Could not fetch futures price. No subscriptions made.")
        return

    ATM_RANGE = futures_price * 0.05
    symbols = []

    for (strike, expiry), pair in symbols_map.items():
        if abs(strike - futures_price) > ATM_RANGE:
            continue
        symbols.append(pair["call"])
        symbols.append(pair["put"])

    if not symbols:
        print("[WARN] No symbols found within ATM range. Check futures price or symbols_map.")
        return

    print(f"[INFO] Found {len(symbols)} symbols near ATM (futures: {futures_price})")

    # Enforce 20-symbol limit per connection by chunking subscriptions
    for i in range(0, len(symbols), MAX_SYMBOLS_PER_CONNECTION):
        chunk = symbols[i:i + MAX_SYMBOLS_PER_CONNECTION]
        sub_msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": "l2_orderbook",
                        "symbols": chunk
                    }
                ]
            }
        }
        ws.send(json.dumps(sub_msg))
        print(f"[INFO] Subscribed to chunk {i // MAX_SYMBOLS_PER_CONNECTION + 1}: {len(chunk)} symbols")


def on_error(ws, error):
    print(f"[ERROR] WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WARN] WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
    ws_ready.clear()


# =========================
# WEBSOCKET WITH RECONNECTION
# =========================
def start_websocket():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[ERROR] WebSocket crashed: {e}")

        print("[INFO] Reconnecting WebSocket in 5 seconds...")
        ws_ready.clear()
        time.sleep(0)


# =========================
# ARBITRAGE ENGINE
# =========================
def arbitrage_loop():
    print("[INFO] Waiting for WebSocket to be ready...")
    ws_ready.wait(timeout=30)

    if not ws_ready.is_set():
        print("[ERROR] WebSocket did not become ready in time. Exiting arbitrage loop.")
        return

    print("[INFO] Starting arbitrage loop...")

    while True:
        futures_price = get_futures_price()
        if not futures_price:
            time.sleep(1)
            continue

        with orderbooks_lock:
            snapshot = dict(orderbooks)

        for (strike, expiry), pair in symbols_map.items():
            call = pair["call"]
            put = pair["put"]

            if call not in snapshot or put not in snapshot:
                continue

            call_bid = snapshot[call]["bid"]
            call_ask = snapshot[call]["ask"]
            put_bid = snapshot[put]["bid"]
            put_ask = snapshot[put]["ask"]

            # =========================
            # ARBITRAGE CALCULATION
            # =========================
            edge_rev = (call_bid - put_ask) - (futures_price - strike)
            edge_conv = (put_bid - call_ask) - (strike - futures_price)

            # =========================
            # FILTER
            # =========================
            if edge_rev > 2:
                print(f"\n[REVERSAL OPPORTUNITY]")
                print(f"  Strike : {strike}")
                print(f"  Expiry : {expiry}")
                print(f"  Edge   : {edge_rev:.2f}")
                # Execution logic: sell call, buy put, buy futures

            if edge_conv > 2:
                print(f"\n[CONVERSION OPPORTUNITY]")
                print(f"  Strike : {strike}")
                print(f"  Expiry : {expiry}")
                print(f"  Edge   : {edge_conv:.2f}")
                # Execution logic: buy call, sell put, sell futures

        time.sleep(0.2)


# =========================
# START BOT
# =========================
def start():
    prepare_symbols()

    if not symbols_map:
        print("[ERROR] No valid option pairs found. Exiting.")
        return

    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    arbitrage_loop()


if __name__ == "__main__":
    start()
