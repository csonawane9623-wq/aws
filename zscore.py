import time
import hmac
import hashlib
import requests
import logging
import uuid
import json
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timezone
import os

from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
API_KEY = os.getenv("API_KEY1")
API_SECRET = os.getenv("API_SECRET1")
BASE_URL = "https://api.india.delta.exchange"

# TELEGRAM
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ── Z-score thresholds ──
ENTRY_Z   = 3.4     # |Z| to open a position
EXIT_Z    = 0.1     # |Z| to close (mean reversion complete)
     # |Z| hard stop-loss

# ── Binance data settings ──
SYMBOL_A    = "ETCUSDT"
SYMBOL_B    = "ETHUSDT"
INTERVAL    = "15m"
LOOKBACK    = 400

# ── Position sizing ──
TARGET_PNL_PERCENT = 0.60
CAPITAL_PERCENT    = 0.50
LEVERAGE           = 50

RETRY = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BINANCE_BASE = "https://api.binance.com/api/v3"


# ============ BINANCE Z-SCORE ============
def fetch_klines(symbol: str, limit: int = LOOKBACK) -> pd.Series:
    """Fetch closing prices from Binance."""
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    raw = r.json()
    return pd.Series(
        [float(k[4]) for k in raw],
        index=pd.to_datetime([k[0] for k in raw], unit="ms", utc=True),
        name=symbol,
    )


def compute_zscore(closes_a: pd.Series, closes_b: pd.Series) -> dict:
    """OLS hedge ratio spread Z-score."""
    aligned = pd.concat([closes_a, closes_b], axis=1).dropna()
    a = aligned.iloc[:, 0].values
    b = aligned.iloc[:, 1].values

    beta, alpha, *_ = stats.linregress(b, a)
    spread = a - (beta * b + alpha)

    mean = spread.mean()
    std  = spread.std(ddof=1)
    z    = (spread[-1] - mean) / std if std > 0 else 0.0

    return {
        "z_score":     z,
        "spread_now":  spread[-1],
        "spread_mean": mean,
        "spread_std":  std,
        "beta":        beta,
        "alpha":       alpha,
        "price_a":     a[-1],
        "price_b":     b[-1],
        "timestamp":   aligned.index[-1],
    }


def get_current_zscore() -> dict | None:
    """Fetch Binance data and return Z-score dict, or None on failure."""
    try:
        closes_a = fetch_klines(SYMBOL_A)
        closes_b = fetch_klines(SYMBOL_B)
        return compute_zscore(closes_a, closes_b)
    except Exception as e:
        log.error(f"Z-score fetch failed: {e}")
        return None


# ============ TELEGRAM ============
last_msg_time = 0

def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")

def send_telegram_safe(msg, cooldown=5):
    global last_msg_time
    if time.time() - last_msg_time > cooldown:
        send_telegram(msg)
        last_msg_time = time.time()


# ============ DELTA CLIENT ============
class DeltaClient:
    def __init__(self):
        self.key    = API_KEY
        self.secret = API_SECRET.encode()

    def _sign(self, method, path, payload=""):
        ts  = str(int(time.time()))
        msg = method + ts + path + payload
        sig = hmac.new(self.secret, msg.encode(), hashlib.sha256).hexdigest()
        return sig, ts

    def _request(self, method, path, data=None, auth=False):
        url     = BASE_URL + path
        payload = json.dumps(data) if data else ""

        for i in range(RETRY):
            try:
                headers = {"Content-Type": "application/json"}
                if auth:
                    sig, ts = self._sign(method, path, payload)
                    headers.update({"api-key": self.key, "timestamp": ts, "signature": sig})

                r = requests.request(method, url, data=payload, headers=headers)
                if r.status_code == 200:
                    res = r.json()
                    if res.get("success", False):
                        return res

                log.error(f"HTTP error: {r.text}")
            except Exception as e:
                log.error(f"Request failed: {e}")

            time.sleep(2 ** i)

        send_telegram_safe(f"⚠️ API ERROR: {path}")
        return None

    def get_products(self):            return self._request("GET", "/v2/products")
    def get_ticker(self, s):           return self._request("GET", f"/v2/tickers/{s}")
    def get_balance(self):             return self._request("GET", "/v2/wallet/balances", auth=True)
    def get_positions(self):           return self._request("GET", "/v2/positions/margined", auth=True)

    def place_order(self, product_id, side, size, cid):
        body = {
            "product_id":     product_id,
            "size":           size,
            "side":           side,
            "order_type":     "market_order",
            "client_order_id": cid,
        }
        return self._request("POST", "/v2/orders", body, auth=True)

    def set_leverage(self, product_id):
        return self._request("POST", f"/v2/products/{product_id}/orders/leverage",
                             {"leverage": LEVERAGE}, auth=True)


# ============ BOT ============
class FundingArbitrageBot:
    def __init__(self):
        self.client       = DeltaClient()
        self.products     = {}
        self.leverage_set = set()
        self.last_pnl_alert  = 0
        self.last_trade_time = 0
        self.entry_spread    = None   # spread value when position was opened
        self.entry_z         = None
        self.position_side   = None   # "LONG_ETC" | "SHORT_ETC"
        self.load_products()

    def gen_id(self):
        return uuid.uuid4().hex[:32]

    def load_products(self):
        data = self.client.get_products()
        if not data:
            raise Exception("Failed to load products")
        for p in data["result"]:
            self.products[p["symbol"]] = p
        log.info("Products loaded")

    def get_balance(self):
        data = self.client.get_balance()
        if not data:
            return 0
        for b in data["result"]:
            if b["asset_symbol"] == "USDT":
                return float(b["balance"])
        return 0

    def get_active_positions(self):
        data = self.client.get_positions()
        if not data:
            return []
        return [p for p in data["result"] if abs(float(p.get("size", 0))) > 0]

    def ensure_leverage(self, symbol):
        if symbol in self.leverage_set:
            return
        pid = self.products[symbol]["id"]
        if self.client.set_leverage(pid):
            self.leverage_set.add(symbol)
            send_telegram_safe(f"⚙️ Leverage set: {symbol}")

    def compute_equal_notional_sizes(self):
        balance      = self.get_balance()
        total_capital = balance * CAPITAL_PERCENT
        target_each  = total_capital / 2

        eth = self.client.get_ticker("ETHUSD")
        etc = self.client.get_ticker("ETCUSD")
        if not eth or not etc:
            return 0, 0

        eth_price = float(eth["result"]["mark_price"])
        etc_price = float(etc["result"]["mark_price"])
        eth_cv    = float(self.products["ETHUSD"]["contract_value"])
        etc_cv    = float(self.products["ETCUSD"]["contract_value"])

        eth_size     = max(int((target_each / eth_price) / eth_cv), 1)
        eth_notional = eth_size * eth_cv * eth_price
        etc_size     = max(int((eth_notional / etc_price) / etc_cv), 1)

        return eth_size, etc_size

    def place_trade(self, signal: str, z_data: dict):
        """
        signal == "LONG_ETC"  → BUY ETC  / SELL ETH   (Z very negative, spread below mean)
        signal == "SHORT_ETC" → SELL ETC / BUY  ETH   (Z very positive, spread above mean)
        """
        eth_size, etc_size = self.compute_equal_notional_sizes()
        if eth_size <= 0 or etc_size <= 0:
            log.warning("Size computation returned 0 — skipping trade")
            return

        self.ensure_leverage("ETHUSD")
        self.ensure_leverage("ETCUSD")

        if signal == "LONG_ETC":
            etc_side, eth_side = "buy", "sell"
        else:  # SHORT_ETC
            etc_side, eth_side = "sell", "buy"

        eth_cid = self.gen_id()
        etc_cid = self.gen_id()

        # Place ETC leg first
        if not self.client.place_order(self.products["ETCUSD"]["id"], etc_side, etc_size, etc_cid):
            log.error("ETC order failed — aborting")
            return

        # Place ETH leg
        if not self.client.place_order(self.products["ETHUSD"]["id"], eth_side, eth_size, eth_cid):
            log.error("ETH order failed — rolling back ETC")
            rollback_side = "sell" if etc_side == "buy" else "buy"
            self.client.place_order(self.products["ETCUSD"]["id"], rollback_side, etc_size, self.gen_id())
            return

        self.entry_spread  = z_data["spread_now"]
        self.entry_z       = z_data["z_score"]
        self.position_side = signal
        self.last_trade_time = time.time()

        send_telegram(
            f"🚀 <b>TRADE ENTERED</b>  |  {signal}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Z-score : <code>{z_data['z_score']:+.4f}</code>\n"
            f"📉 Spread  : <code>{z_data['spread_now']:+.6f}</code>\n"
            f"⚖️  Beta    : <code>{z_data['beta']:.5f}</code>\n"
            f"📦 ETC qty : <code>{etc_size}</code>  ({etc_side})\n"
            f"📦 ETH qty : <code>{eth_size}</code>  ({eth_side})\n"
            f"<i>Lookback: {LOOKBACK} × {INTERVAL} candles</i>"
        )

    def close_all(self, reason: str = "EXIT", z_data: dict | None = None):
        positions = self.get_active_positions()
        for p in positions:
            size = float(p["size"])
            if size == 0:
                continue
            side = "sell" if size > 0 else "buy"
            self.client.place_order(p["product_id"], side, int(abs(size)), self.gen_id())

        z_str = f"\nExit Z: <code>{z_data['z_score']:+.4f}</code>" if z_data else ""
        send_telegram(
            f"🔒 <b>POSITIONS CLOSED</b>  [{reason}]{z_str}\n"
            f"Entry Z: <code>{self.entry_z:+.4f}</code>"
            if self.entry_z is not None else f"🔒 <b>POSITIONS CLOSED</b>  [{reason}]"
        )

        # Reset state
        self.entry_spread  = None
        self.entry_z       = None
        self.position_side = None

    def run(self):
        send_telegram(
            f"✅ <b>Bot Started</b>\n"
            f"Entry |Z| ≥ <code>{ENTRY_Z}</code>  "
            f"Exit |Z| ≤ <code>{EXIT_Z}</code>  "
          
            f"Pair: <code>{SYMBOL_A} / {SYMBOL_B}</code>  "
            f"Interval: <code>{INTERVAL}</code>  Lookback: <code>{LOOKBACK}</code>"
        )

        tick = 0
        while True:
            try:
                tick += 1

                # ── Fetch Z-score from Binance ──────────────────────────────
                z_data = get_current_zscore()
                if z_data is None:
                    send_telegram_safe("⚠️ Z-score fetch failed, skipping tick")
                    time.sleep(1)
                    continue

                z = z_data["z_score"]
                positions = self.get_active_positions()  # FIX 1: removed stray `total_margin` line
                in_trade  = len(positions) > 0

                log.info(
                    f"Z={z:+.4f}  spread={z_data['spread_now']:+.6f}  "
                    f"pos={'FLAT' if not in_trade else self.position_side}"
                )

                # ── ENTRY (no open position) ────────────────────────────────
                if not in_trade and time.time() - self.last_trade_time > 10:
                    if z <= -ENTRY_Z:
                        send_telegram(f"📊 <b>ENTRY SIGNAL</b>  Z = <code>{z:+.4f}</code>\nLONG ETC / SHORT ETH")
                        self.place_trade("LONG_ETC", z_data)

                    elif z >= ENTRY_Z:
                        send_telegram(f"📊 <b>ENTRY SIGNAL</b>  Z = <code>{z:+.4f}</code>\nSHORT ETC / LONG ETH")
                        self.place_trade("SHORT_ETC", z_data)

                # ── MANAGE OPEN POSITION ────────────────────────────────────
                elif in_trade:
                    positions = self.get_active_positions()
                    pnl    = 0.0   # FIX 2: initialise before the conditional block
                    target = 0.0   # so the elif branches below can always reference them
                    amount = 0.0
                    if positions and len(positions) == 2:
                        pnl = sum(float(p.get("unrealized_pnl", 0)) for p in positions)

                        total_margin = 0
                        for p in positions:
                            margin = float(p.get("margin"))
                            total_margin += margin
                        
                        target = total_margin * TARGET_PNL_PERCENT
                        amount=total_margin

                        if time.time() - self.last_pnl_alert > 600:
                            send_telegram_safe(f"📈 PnL: {pnl} | Target: {target}\nTotal Margin:{amount}")
                            self.last_pnl_alert = time.time()

                        if pnl >= target:
                            send_telegram(f"🎯 TARGET HIT\nPnL: {pnl}")
                            self.close_all()

                    # Z-score mean reversion exit
                    elif (
                        (self.position_side == "LONG_ETC"  and z >= -EXIT_Z) or
                        (self.position_side == "SHORT_ETC" and z <=  EXIT_Z)
                    ):
                        log.info(f"EXIT: Z reverted to {z:+.4f}")
                        self.close_all("MEAN REVERSION ⚪", z_data)

                    # Periodic PnL alert
                    elif time.time() - self.last_pnl_alert > 600:
                        send_telegram_safe(
                            f"📈 <b>PnL Update</b>\n"
                            f"PnL: <code>{pnl:.4f}</code>  Target: <code>{target:.4f}</code>\n"
                       
                            f"Z: <code>{z:+.4f}</code>"
                        )
                        self.last_pnl_alert = time.time()

                # ── Heartbeat every 20 ticks ────────────────────────────────
                if tick % 60 == 0:
                    send_telegram_safe(
                        f"💓 <b>Heartbeat</b>\n"
                        f"Z: <code>{z:+.4f}</code>  "
                        f"Spread: <code>{z_data['spread_now']:+.6f}</code>\n"
                        f"Position: <code>{'FLAT' if not in_trade else self.position_side}</code>"
                    )

            except Exception as e:
                log.error(e)
                send_telegram_safe(f"❌ ERROR: {e}")

            time.sleep(1)


# ============ RUN ============
if __name__ == "__main__":
    while True:
        try:
            FundingArbitrageBot().run()
        except Exception as e:
            print("CRASH:", e)
            time.sleep(1)
