import time
import hmac
import hashlib
import requests
import logging
import uuid
import json
import os

from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = "https://api.india.delta.exchange"

# TELEGRAM
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# STRATEGY
ETH_THRESHOLD = 0.005
ETC_THRESHOLD = 0.000800

TARGET_PNL_PERCENT =2.5
CAPITAL_PERCENT = 0.0
LEVERAGE = 50

RETRY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============ TELEGRAM ============
last_msg_time = 0


def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg
            },
            timeout=5
        )
    except Exception as e:
        logging.error(f"Telegram error: {e}")


def send_telegram_safe(msg, cooldown=5):
    global last_msg_time

    if time.time() - last_msg_time > cooldown:
        send_telegram(msg)
        last_msg_time = time.time()


# ============ DELTA CLIENT ============
class DeltaClient:

    def __init__(self):
        self.key = API_KEY
        self.secret = API_SECRET.encode()

    def _sign(self, method, path, payload=""):
        ts = str(int(time.time()))

        message = method + ts + path + payload

        signature = hmac.new(
            self.secret,
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        return signature, ts

    def _request(self, method, path, data=None, auth=False):

        url = BASE_URL + path

        payload = json.dumps(data) if data else ""

        for i in range(RETRY):

            try:

                headers = {
                    "Content-Type": "application/json"
                }

                if auth:
                    sig, ts = self._sign(method, path, payload)

                    headers.update({
                        "api-key": self.key,
                        "timestamp": ts,
                        "signature": sig
                    })

                response = requests.request(
                    method,
                    url,
                    data=payload,
                    headers=headers,
                    timeout=10
                )

                if response.status_code == 200:

                    result = response.json()

                    if result.get("success", False):
                        return result

                logging.error(
                    f"HTTP ERROR [{response.status_code}] : {response.text}"
                )

            except Exception as e:
                logging.error(f"Request failed: {e}")

            time.sleep(2 ** i)

        send_telegram_safe(f"⚠️ API ERROR: {path}")

        return None

    # ===== PUBLIC API =====

    def get_products(self):
        return self._request("GET", "/v2/products")

    def get_ticker(self, symbol):
        return self._request("GET", f"/v2/tickers/{symbol}")

    def get_balance(self):
        return self._request(
            "GET",
            "/v2/wallet/balances",
            auth=True
        )

    def get_positions(self):
        return self._request(
            "GET",
            "/v2/positions/margined",
            auth=True
        )

    def get_order(self, client_order_id):
        return self._request(
            "GET",
            f"/v2/orders/client_order_id/{client_order_id}",
            auth=True
        )

    def place_order(self, product_id, side, size, client_order_id):

        body = {
            "product_id": product_id,
            "size": size,
            "side": side,
            "order_type": "market_order",
            "client_order_id": client_order_id
        }

        return self._request(
            "POST",
            "/v2/orders",
            body,
            auth=True
        )

    def set_leverage(self, product_id):

        body = {
            "leverage": LEVERAGE
        }

        return self._request(
            "POST",
            f"/v2/products/{product_id}/orders/leverage",
            body,
            auth=True
        )


# ============ BOT ============
class FundingArbitrageBot:

    def __init__(self):

        self.client = DeltaClient()

        self.products = {}

        self.leverage_set = set()

        self.last_pnl_alert = 0

        self.last_trade_time = 0

        self.load_products()

    def gen_id(self):
        return uuid.uuid4().hex[:32]

    # =========================
    # LOAD PRODUCTS
    # =========================
    def load_products(self):

        data = self.client.get_products()

        if not data:
            raise Exception("Failed to load products")

        for p in data["result"]:
            self.products[p["symbol"]] = p

        logging.info("Products loaded")

    # =========================
    # BALANCE
    # =========================
    def get_balance(self):

        data = self.client.get_balance()

        if not data:
            return 0

        for b in data["result"]:

            if b["asset_symbol"] == "USDT":
                return float(b["balance"])

        return 0

    # =========================
    # ACTIVE POSITIONS
    # =========================
    def get_active_positions(self):

        data = self.client.get_positions()

        if not data:
            return []

        return [
            p for p in data["result"]
            if abs(float(p.get("size", 0))) > 0
        ]

    # =========================
    # ENSURE LEVERAGE
    # =========================
    def ensure_leverage(self, symbol):

        if symbol in self.leverage_set:
            return

        product_id = self.products[symbol]["id"]

        result = self.client.set_leverage(product_id)

        if result:
            self.leverage_set.add(symbol)

            send_telegram_safe(
                f"⚙️ Leverage set: {symbol} ({LEVERAGE}x)"
            )

    # =========================
    # POSITION SIZE
    # =========================
    def compute_equal_notional_sizes(self):

        balance = self.get_balance()

        total_capital = balance * CAPITAL_PERCENT

        capital_each = total_capital / 2

        eth = self.client.get_ticker("ETHUSD")
        etc = self.client.get_ticker("ETCUSD")

        if not eth or not etc:
            return 0, 0

        eth_price = float(eth["result"]["mark_price"])
        etc_price = float(etc["result"]["mark_price"])

        eth_cv = float(self.products["ETHUSD"]["contract_value"])
        etc_cv = float(self.products["ETCUSD"]["contract_value"])

        # ETH size
        eth_size = max(
            int((capital_each / eth_price) / eth_cv),
            1
        )

        eth_notional = eth_size * eth_cv * eth_price

        # Match ETC notional to ETH
        etc_size = max(
            int((eth_notional / etc_price) / etc_cv),
            1
        )

        return eth_size, etc_size

    # =========================
    # PLACE TRADE
    # =========================
    def place_trade(self):

        eth_size, etc_size = self.compute_equal_notional_sizes()

        if eth_size <= 0 or etc_size <= 0:
            logging.error("Invalid position size")
            return

        self.ensure_leverage("ETHUSD")
        self.ensure_leverage("ETCUSD")

        eth_id = self.gen_id()
        etc_id = self.gen_id()

        logging.info(
            f"Opening positions | LONG ETH: {eth_size} | SHORT ETC: {etc_size}"
        )

        # ====================================
        # LONG ETH
        # ====================================
        eth_order = self.client.place_order(
            self.products["ETHUSD"]["id"],
            "buy",
            eth_size,
            eth_id
        )

        if not eth_order:
            send_telegram_safe("❌ Failed LONG ETH order")
            return

        # ====================================
        # SHORT ETC
        # ====================================
        etc_order = self.client.place_order(
            self.products["ETCUSD"]["id"],
            "sell",
            etc_size,
            etc_id
        )

        # Rollback if ETC fails
        if not etc_order:

            send_telegram_safe(
                "⚠️ ETC SHORT failed. Rolling back ETH LONG."
            )

            self.client.place_order(
                self.products["ETHUSD"]["id"],
                "sell",
                eth_size,
                self.gen_id()
            )

            return

        send_telegram(
            f"🚀 TRADE EXECUTED\n\n"
            f"LONG ETH : {eth_size}\n"
            f"SHORT ETC: {etc_size}"
        )

        self.last_trade_time = time.time()

    # =========================
    # CLOSE ALL POSITIONS
    # =========================
    def close_all(self):

        positions = self.get_active_positions()

        if not positions:
            return

        for p in positions:

            size = float(p["size"])

            if size == 0:
                continue

            side = "sell" if size > 0 else "buy"

            self.client.place_order(
                p["product_id"],
                side,
                int(abs(size)),
                self.gen_id()
            )

        send_telegram("🔒 ALL POSITIONS CLOSED")

    # =========================
    # MAIN LOOP
    # =========================
    def run(self):

        send_telegram("✅ FUNDING ARBITRAGE BOT STARTED")

        while True:

            try:

                eth_data = self.client.get_ticker("ETHUSD")
                etc_data = self.client.get_ticker("ETCUSD")

                if not eth_data or not etc_data:
                    time.sleep(3)
                    continue

                eth_funding = float(
                    eth_data["result"]["funding_rate"]
                )

                etc_funding = float(
                    etc_data["result"]["funding_rate"]
                )

                logging.info(
                    f"ETH Funding: {eth_funding} | "
                    f"ETC Funding: {etc_funding}"
                )

                active_positions = self.get_active_positions()

                # ====================================
                # ENTRY CONDITION
                # LONG ETH + SHORT ETC
                # ====================================
                if (
                    eth_funding >= ETH_THRESHOLD and
                    etc_funding <= ETC_THRESHOLD and
                    not active_positions and
                    time.time() - self.last_trade_time > 10
                ):

                    send_telegram(
                        f"📊 ENTRY SIGNAL\n\n"
                        f"LONG ETH\n"
                        f"SHORT ETC\n\n"
                        f"ETH Funding: {eth_funding}\n"
                        f"ETC Funding: {etc_funding}"
                    )

                    self.place_trade()

                # ====================================
                # PNL MANAGEMENT
                # ====================================
                active_positions = self.get_active_positions()

                if active_positions or len(active_positions) == 2:

                    pnl = sum(
                        float(p.get("unrealized_pnl", 0))
                        for p in active_positions
                    )

                    total_margin = sum(
                        float(p.get("margin", 0))
                        for p in active_positions
                    )

                    target = total_margin * TARGET_PNL_PERCENT

                    logging.info(
                        f"PnL: {pnl} | "
                        f"Margin: {total_margin} | "
                        f"Target: {target}"
                    )

                    # Telegram PnL update every 10 mins
                    if time.time() - self.last_pnl_alert > 600:

                        send_telegram_safe(
                            f"📈 LIVE STATUS\n\n"
                            f"PnL: {round(pnl, 4)}\n"
                            f"Target: {round(target, 4)}\n"
                            f"Margin: {round(total_margin, 4)}"
                        )

                        self.last_pnl_alert = time.time()

                    # Target hit
                    if pnl >= target:

                        send_telegram(
                            f"🎯 TARGET HIT\n\n"
                            f"PnL: {round(pnl, 4)}"
                        )

                        self.close_all()

            except Exception as e:

                logging.error(f"MAIN LOOP ERROR: {e}")

                send_telegram_safe(
                    f"❌ BOT ERROR\n{str(e)}"
                )

            time.sleep(1)


# ============ RUN ============
if __name__ == "__main__":

    while True:

        try:

            FundingArbitrageBot().run()

        except Exception as e:

            logging.error(f"CRASH: {e}")

            send_telegram_safe(
                f"💥 BOT CRASHED\n{str(e)}"
            )

            time.sleep(1)
