# ============================================================
#                PAXG / XAUT ARBITRAGE BOT
# ============================================================

import asyncio
import websockets
import json
import time
import hmac
import hashlib
import requests
import logging
import uuid
import traceback
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_KEY = os.getenv("API_KEY1")
API_SECRET = os.getenv("API_SECRET1")

BASE_URL = "https://api.india.delta.exchange"
WS_URL = "wss://socket.india.delta.exchange"

TELEGRAM_BOT_TOKEN =  os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID =  os.getenv("TELEGRAM_CHAT_ID")
# ============================================================
# SYMBOLS
# ============================================================

SYMBOL_1 = "PAXGUSD"
SYMBOL_2 = "XAUTUSD"

# ============================================================
# STRATEGY
# ============================================================

ENTRY_SPREAD =10 
EXIT_SPREAD = 0.05
STOP_SPREAD = 100

# ============================================================
# RISK
# ============================================================

LEVERAGE = 100
CAPITAL_PERCENT = 1

MAX_POSITION_HOLD_SEC = 3600000
COOLDOWN_AFTER_EXIT = 0

# ============================================================
# ORDERBOOK FILTERS
# ============================================================

MAX_SLIPPAGE = 0.0025
MIN_ORDERBOOK_USD = 5000

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================================
# TELEGRAM
# ============================================================

last_telegram_time = 0


def send_telegram(msg):

    global last_telegram_time

    try:

        if time.time() - last_telegram_time < 2:
            return

        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg
            },
            timeout=5
        )

        last_telegram_time = time.time()

    except Exception as e:

        logging.error(f"Telegram Error: {e}")


# ============================================================
# DELTA CLIENT
# ============================================================

class DeltaClient:

    def __init__(self):

        self.key = API_KEY
        self.secret = API_SECRET.encode()

    def sign(
        self,
        method,
        path,
        query_string="",
        payload=""
    ):

        timestamp = str(int(time.time()))

        signature_data = (
            method +
            timestamp +
            path +
            query_string +
            payload
        )

        signature = hmac.new(
            self.secret,
            signature_data.encode(),
            hashlib.sha256
        ).hexdigest()

        return signature, timestamp

    def request(
        self,
        method,
        path,
        data=None,
        auth=False,
        params=None
    ):

        payload = json.dumps(data) if data else ""

        query_string = ""

        if params:

            query_string = "?" + "&".join(
                f"{k}={v}"
                for k, v in params.items()
            )

        headers = {
            "Content-Type": "application/json"
        }

        if auth:

            signature, timestamp = self.sign(
                method,
                path,
                query_string,
                payload
            )

            headers.update({
                "api-key": self.key,
                "timestamp": timestamp,
                "signature": signature
            })

        try:

            response = requests.request(
                method,
                BASE_URL + path,
                data=payload,
                headers=headers,
                params=params,
                timeout=10
            )

            return response.json()

        except Exception as e:

            logging.error(e)

            return None

    # ========================================================
    # API
    # ========================================================

    def get_product(self, symbol):

        return self.request(
            "GET",
            f"/v2/products/{symbol}"
        )

    def get_balance(self):

        return self.request(
            "GET",
            "/v2/wallet/balances",
            auth=True
        )

    def get_positions(self):

        return self.request(
            "GET",
            "/v2/positions/margined",
            auth=True
        )

    def set_leverage(
        self,
        product_id,
        leverage
    ):

        body = {
            "leverage": str(leverage)
        }

        return self.request(
            "POST",
            f"/v2/products/{product_id}/orders/leverage",
            body,
            auth=True
        )

    def place_market_order(
        self,
        product_id,
        side,
        size
    ):

        body = {
            "product_id": product_id,
            "size": size,
            "side": side,
            "order_type": "market_order",
            "client_order_id": uuid.uuid4().hex[:32]
        }

        return self.request(
            "POST",
            "/v2/orders",
            body,
            auth=True
        )


# ============================================================
# BOT
# ============================================================

class PAXGXAUTBot:

    def __init__(self):

        self.client = DeltaClient()

        self.products = {}

        self.orderbooks = {}

        self.prices = {
            SYMBOL_1: None,
            SYMBOL_2: None
        }

        self.positions_open = False

        self.entry_time = None

        self.last_exit_time = 0

        self.leverage_done = set()

        self.load_products()

        self.recover_positions_on_startup()

    # ========================================================
    # LOAD PRODUCTS
    # ========================================================

    def load_products(self):

        for symbol in [SYMBOL_1, SYMBOL_2]:

            data = self.client.get_product(symbol)

            if not data or not data.get("success"):

                raise Exception(
                    f"Cannot load product: {symbol}"
                )

            self.products[symbol] = data["result"]

            logging.info(
                f"Loaded Product: {symbol}"
            )

    # ========================================================
    # RECOVER POSITIONS
    # ========================================================

    def recover_positions_on_startup(self):

        logging.info(
            "Checking existing positions..."
        )

        positions = self.fetch_open_positions()

        if not positions:

            logging.info(
                "No existing positions found."
            )

            self.positions_open = False

            return

        found_symbols = []

        for p in positions:

            pid = p["product_id"]

            for symbol, product in self.products.items():

                if product["id"] == pid:

                    found_symbols.append(symbol)

        # ====================================================
        # BOTH LEGS EXIST
        # ====================================================

        if (
            SYMBOL_1 in found_symbols
            and
            SYMBOL_2 in found_symbols
        ):

            self.positions_open = True

            self.entry_time = None

            logging.warning(
                "Recovered Existing Hedge"
            )

            send_telegram(
                "Recovered Existing Hedge"
            )

        # ====================================================
        # PARTIAL POSITION
        # ====================================================

        else:

            logging.error(
                "Partial Hedge Found!"
            )

            send_telegram(
                "WARNING: Partial Hedge Found!"
            )

            self.close_all(
                "Startup Cleanup"
            )

    # ========================================================
    # LEVERAGE
    # ========================================================

    def ensure_leverage(self, symbol):

        if symbol in self.leverage_done:
            return

        pid = self.products[symbol]["id"]

        res = self.client.set_leverage(
            pid,
            LEVERAGE
        )

        if res and res.get("success"):

            self.leverage_done.add(symbol)

            logging.info(
                f"Leverage Set: {symbol}"
            )

    # ========================================================
    # BALANCE
    # ========================================================

    def get_usdt_balance(self):

        data = self.client.get_balance()

        if not data:
            return 0

        for asset in data.get("result", []):

            if asset["asset_symbol"] == "USDT":

                return float(
                    asset["available_balance"]
                )

        return 0

    # ========================================================
    # FETCH POSITIONS
    # ========================================================

    def fetch_open_positions(self):

        data = self.client.get_positions()

        if not data:
            return []

        positions = []

        for p in data.get("result", []):

            if abs(float(p.get("size", 0))) > 0:

                positions.append(p)

        return positions

    # ========================================================
    # ORDERBOOK FILTER
    # ========================================================

    def orderbook_metrics(self, symbol):

        if symbol not in self.orderbooks:
            return None

        ob = self.orderbooks[symbol]

        bids = ob["bids"]
        asks = ob["asks"]

        if not bids or not asks:
            return None

        best_bid = float(
            bids[0]["limit_price"]
        )

        best_ask = float(
            asks[0]["limit_price"]
        )

        mid = (best_bid + best_ask) / 2

        slippage = abs(
            best_ask - best_bid
        ) / mid

        cv = float(
            self.products[symbol]["contract_value"]
        )

        bid_liq = sum(
            float(level["limit_price"])
            *
            float(level["size"])
            *
            cv
            for level in bids[:5]
        )

        ask_liq = sum(
            float(level["limit_price"])
            *
            float(level["size"])
            *
            cv
            for level in asks[:5]
        )

        liquidity = min(
            bid_liq,
            ask_liq
        )

        ok = (
            slippage <= MAX_SLIPPAGE
            and
            liquidity >= MIN_ORDERBOOK_USD
        )

        return {
            "ok": ok,
            "slippage": slippage,
            "liquidity": liquidity
        }

    # ========================================================
    # SIZE
    # ========================================================

    def compute_sizes(self):

        balance = self.get_usdt_balance()

        capital = balance * CAPITAL_PERCENT

        capital_each = capital / 2

        p1 = self.prices[SYMBOL_1]
        p2 = self.prices[SYMBOL_2]

        cv1 = float(
            self.products[SYMBOL_1]["contract_value"]
        )

        cv2 = float(
            self.products[SYMBOL_2]["contract_value"]
        )

        size1 = max(
            int(
                (
                    capital_each
                    *
                    LEVERAGE
                    /
                    p1
                ) / cv1
            ),
            1
        )

        size2 = max(
            int(
                (
                    capital_each
                    *
                    LEVERAGE
                    /
                    p2
                ) / cv2
            ),
            1
        )

        return size1, size2

    # ========================================================
    # OPEN TRADE
    # ========================================================

    def open_trade(self, direction):

        if (
            time.time()
            -
            self.last_exit_time
            <
            COOLDOWN_AFTER_EXIT
        ):
            return

        metrics1 = self.orderbook_metrics(SYMBOL_1)
        metrics2 = self.orderbook_metrics(SYMBOL_2)

        if not metrics1 or not metrics2:
            return

        if not metrics1["ok"]:
            return

        if not metrics2["ok"]:
            return

        self.ensure_leverage(SYMBOL_1)
        self.ensure_leverage(SYMBOL_2)

        size1, size2 = self.compute_sizes()

        pid1 = self.products[SYMBOL_1]["id"]
        pid2 = self.products[SYMBOL_2]["id"]

        # ====================================================
        # SHORT PAXG
        # ====================================================

        if direction == "SHORT_PAXG":

            res1 = self.client.place_market_order(
                pid1,
                "sell",
                size1
            )

            res2 = self.client.place_market_order(
                pid2,
                "buy",
                size2
            )

            signal = (
                "SHORT PAXG / LONG XAUT"
            )

        # ====================================================
        # LONG PAXG
        # ====================================================

        else:

            res1 = self.client.place_market_order(
                pid1,
                "buy",
                size1
            )

            res2 = self.client.place_market_order(
                pid2,
                "sell",
                size2
            )

            signal = (
                "LONG PAXG / SHORT XAUT"
            )

        if (
            not res1
            or
            not res1.get("success")
            or
            not res2
            or
            not res2.get("success")
        ):

            logging.error(
                f"Order Failed"
            )

            send_telegram(
                "Order Failed"
            )

            return

        self.positions_open = True

        self.entry_time = time.time()

        logging.info(signal)

        send_telegram(
            f"ENTRY\n{signal}"
        )

    # ========================================================
    # CLOSE ALL
    # ========================================================

    def close_all(self, reason):

        positions = self.fetch_open_positions()

        if not positions:

            self.positions_open = False

            self.entry_time = None

            self.last_exit_time = time.time()

            return

        for p in positions:

            size = abs(float(p["size"]))

            side = (
                "sell"
                if float(p["size"]) > 0
                else "buy"
            )

            self.client.place_market_order(
                p["product_id"],
                side,
                int(size)
            )

        self.positions_open = False

        self.entry_time = None

        self.last_exit_time = time.time()

        send_telegram(
            f"CLOSED\nReason: {reason}"
        )

    # ========================================================
    # DASHBOARD
    # ========================================================

    def dashboard(self, spread, signal):

        print("\033c", end="")

        print("=" * 70)
        print("PAXG/XAUT ARBITRAGE BOT")
        print("=" * 70)
        print()

        print(
            f"PAXG : "
            f"{round(self.prices[SYMBOL_1], 2)}"
        )

        print(
            f"XAUT : "
            f"{round(self.prices[SYMBOL_2], 2)}"
        )

        print()

        print(
            f"Spread : "
            f"{round(spread, 2)}"
        )

        print()

        print(
            f"Entry Spread : "
            f"{ENTRY_SPREAD}"
        )

        print(
            f"Exit Spread : "
            f"{EXIT_SPREAD}"
        )

        print(
            f"Stop Spread : "
            f"{STOP_SPREAD}"
        )

        print()

        metrics1 = self.orderbook_metrics(SYMBOL_1)
        metrics2 = self.orderbook_metrics(SYMBOL_2)

        if metrics1:

            print(
                f"PAXG Slippage : "
                f"{round(metrics1['slippage'] * 100, 4)}%"
            )

            print(
                f"PAXG Liquidity : "
                f"${round(metrics1['liquidity'], 2)}"
            )

        print()

        if metrics2:

            print(
                f"XAUT Slippage : "
                f"{round(metrics2['slippage'] * 100, 4)}%"
            )

            print(
                f"XAUT Liquidity : "
                f"${round(metrics2['liquidity'], 2)}"
            )

        print()

        print(
            f"Positions Open : "
            f"{self.positions_open}"
        )

        print(
            f"Signal : "
            f"{signal}"
        )

        print()
        print("=" * 70)

    # ========================================================
    # STRATEGY
    # ========================================================

    def evaluate(self):

        if self.prices[SYMBOL_1] is None:
            return

        if self.prices[SYMBOL_2] is None:
            return

        real_positions = self.fetch_open_positions()

        if len(real_positions) >= 1:

            self.positions_open = True

        spread = (
            self.prices[SYMBOL_1]
            -
            self.prices[SYMBOL_2]
        )

        signal = "WAIT"

        # ====================================================
        # ENTRY
        # ====================================================

        if not self.positions_open:

            # PAXG expensive

            if spread >= ENTRY_SPREAD:

                signal = (
                    "SHORT PAXG / LONG XAUT"
                )

                self.open_trade(
                    "SHORT_PAXG"
                )

            # XAUT expensive

            elif spread <= -ENTRY_SPREAD:

                signal = (
                    "LONG PAXG / SHORT XAUT"
                )

                self.open_trade(
                    "LONG_PAXG"
                )

        # ====================================================
        # EXIT
        # ====================================================

        else:

            hold_time = 0

            if self.entry_time:

                hold_time = (
                    time.time()
                    -
                    self.entry_time
                )

            # EXIT

            if abs(spread) <= EXIT_SPREAD:

                signal = "EXIT"

                self.close_all(
                    "Spread Normalized"
                )

            # STOP

            elif abs(spread) >= STOP_SPREAD:

                signal = "STOP LOSS"

                self.close_all(
                    "Spread Explosion"
                )

            # TIME EXIT

            elif (
                self.entry_time
                and
                hold_time
                >=
                MAX_POSITION_HOLD_SEC
            ):

                signal = "TIME EXIT"

                self.close_all(
                    "Max Hold Time"
                )

            else:

                signal = "HOLDING"

        self.dashboard(
            spread,
            signal
        )

    # ========================================================
    # WEBSOCKET
    # ========================================================

    async def websocket_loop(self):

        logging.info(
            "Connecting WebSocket..."
        )

        async with websockets.connect(
            WS_URL,
            ping_interval=10,
            ping_timeout=20
        ) as ws:

            logging.info(
                "WebSocket Connected"
            )

            send_telegram(
                "WebSocket Connected"
            )

            subscribe_message = {
                "type": "subscribe",
                "payload": {
                    "channels": [
                        {
                            "name": "v2/ticker",
                            "symbols": [
                                SYMBOL_1,
                                SYMBOL_2
                            ]
                        },
                        {
                            "name": "l2_orderbook",
                            "symbols": [
                                SYMBOL_1,
                                SYMBOL_2
                            ]
                        }
                    ]
                }
            }

            await ws.send(
                json.dumps(
                    subscribe_message
                )
            )

            while True:

                msg = await ws.recv()

                data = json.loads(msg)

                msg_type = data.get("type")

                # ====================================================
                # TICKER
                # ====================================================

                if msg_type == "v2/ticker":

                    symbol = data.get("symbol")

                    if symbol not in self.prices:
                        continue

                    mark_price = data.get(
                        "mark_price"
                    )

                    if mark_price is None:
                        continue

                    self.prices[symbol] = float(
                        mark_price
                    )

                    self.evaluate()

                # ====================================================
                # ORDERBOOK
                # ====================================================

                elif msg_type == "l2_orderbook":

                    symbol = data.get("symbol")

                    if symbol not in [
                        SYMBOL_1,
                        SYMBOL_2
                    ]:
                        continue

                    self.orderbooks[symbol] = {
                        "bids": data.get(
                            "buy",
                            []
                        ),
                        "asks": data.get(
                            "sell",
                            []
                        )
                    }


# ============================================================
# MAIN
# ============================================================

async def main():

    bot = PAXGXAUTBot()

    while True:

        try:

            await bot.websocket_loop()

        except Exception as e:

            logging.error(
                traceback.format_exc()
            )

            send_telegram(
                f"WebSocket Crash\n{e}"
            )

            await asyncio.sleep(0)


if __name__ == "__main__":

    asyncio.run(main())
