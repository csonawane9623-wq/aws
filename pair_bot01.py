"""
ETC/USDT ↔ ETH/USDT  │  Fully Automated Pairs Trading Bot
==========================================================
Exchange  : Delta Exchange India (USDT-margined Perpetual Futures)
Strategy  : Z-score mean reversion on ETC/ETH spread
Signals   : ENTER when |Z| > 2σ  │  EXIT when |Z| < 0.5σ
Execution : Market orders on both legs simultaneously
Sizing    : % of available USDT wallet balance
Persistence: Position saved to disk + validated against exchange on restart

Setup
-----
1. pip install requests pandas numpy scipy

2. Delta Exchange India API key:
   https://www.delta.exchange/app/account/manageapikeys
   ⚠️  Enable BOTH "Read Data" AND "Trading" permissions.
   ⚠️  Whitelist your server IP in the API key settings.

3. Telegram bot:
   - Message @BotFather → /newbot → copy token
   - Start your bot, then:
       python -c "import requests; print(requests.get('https://api.telegram.org/bot<TOKEN>/getUpdates').json())"
   - Copy chat_id from response.

4. Set SYMBOL_A / SYMBOL_B to the exact perpetual symbols from Delta.
   You can verify at: https://api.india.delta.exchange/v2/products?contract_type=perpetual_futures

5. Run: python pairs_bot_delta_auto.py

Key API endpoints
-----------------
  GET  /v2/history/candles          resolution param uses "1m","15m","1h" format
  GET  /v2/wallet/balances          USDT free balance
  GET  /v2/products                 contract metadata (product_id, contract_size)
  GET  /v2/positions                open positions (requires product_id param)
  POST /v2/orders                   place market order
"""

import hashlib
import hmac
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import requests
from scipy import stats

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  — edit these values before running
# ═══════════════════════════════════════════════════════════════════════════════
DELTA_API_KEY    = ""          # from delta.exchange/app/account/manageapikeys
DELTA_API_SECRET = ""          # keep secret — never share or commit

TELEGRAM_TOKEN   = ""
TELEGRAM_CHAT_ID = ""

# Exact perpetual futures symbol names on Delta Exchange India
# Verify: curl "https://api.india.delta.exchange/v2/products?contract_type=perpetual_futures"
SYMBOL_A = "ETCUSDT"
SYMBOL_B = "ETHUSDT"

# Candle resolution — Delta Exchange format: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 1d, 1w, 2w
RESOLUTION = "15m"
LOOKBACK   = 400               # number of closed candles for Z-score calculation

ENTRY_Z = 2.0                  # open trade when |Z| crosses this
EXIT_Z  = 0.5                  # close trade when |Z| falls below this

# Position sizing
WALLET_FRACTION = 0.20         # fraction of free USDT balance to deploy (0.20 = 20%)
LEVERAGE        = 5            # must match leverage set on Delta Exchange for both symbols

POLL_SECONDS    = 0          # 300s = 5 min between ticks
HEARTBEAT_TICKS = 5           # send status telegram every N ticks (~60 min)

POSITION_FILE = "pairs_position.json"

# Resolution string → seconds (for computing candle start time)
RESOLUTION_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900,
    "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
    "6h": 21600, "1d": 86400, "1w": 604800, "2w": 1209600,
}
# ═══════════════════════════════════════════════════════════════════════════════

DELTA_BASE = "https://api.india.delta.exchange/v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pairs_bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH — Delta Exchange HMAC-SHA256 signing
# ═══════════════════════════════════════════════════════════════════════════════
def _generate_signature(method: str, path: str, query_string: str, body: str, timestamp: str) -> str:
    """
    Official Delta Exchange signature:
      HMAC-SHA256(secret, method + timestamp + path + query_string + body)
    query_string must include the leading '?' if present.
    """
    msg = method + timestamp + path + query_string + body
    return hmac.new(
        DELTA_API_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _auth_headers(method: str, path: str, query_string: str = "", body: str = "") -> dict:
    ts  = str(int(time.time()))
    sig = _generate_signature(method, path, query_string, body, ts)
    return {
        "api-key":      DELTA_API_KEY,
        "timestamp":    ts,
        "signature":    sig,
        "Content-Type": "application/json",
        "User-Agent":   "pairs-bot/2.0",
    }


def delta_get(path: str, params: dict | None = None) -> dict:
    """Authenticated GET — params dict is encoded into query string."""
    qs = ("?" + urlencode(params)) if params else ""
    headers = _auth_headers("GET", path, qs)
    url = DELTA_BASE + path + qs
    r = requests.get(url, headers=headers, timeout=15)
    if not r.ok:
        raise RuntimeError(f"GET {path} → HTTP {r.status_code}: {r.text[:300]}")
    body = r.json()
    if not body.get("success", True):
        raise RuntimeError(f"GET {path} API error: {body}")
    return body


def delta_post(path: str, payload: dict) -> dict:
    """Authenticated POST."""
    body_str = json.dumps(payload, separators=(",", ":"))
    headers  = _auth_headers("POST", path, "", body_str)
    r = requests.post(DELTA_BASE + path, headers=headers, data=body_str, timeout=15)
    if not r.ok:
        raise RuntimeError(f"POST {path} → HTTP {r.status_code}: {r.text[:300]}")
    body = r.json()
    if not body.get("success", True):
        raise RuntimeError(f"POST {path} API error: {body}")
    return body


# ═══════════════════════════════════════════════════════════════════════════════
#  MARKET DATA
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_klines(symbol: str, limit: int = LOOKBACK) -> pd.Series:
    """
    Fetch closed OHLCV candles from Delta Exchange India.
    Endpoint: GET /v2/history/candles
    Params  : symbol, resolution (e.g. "15m"), start, end (Unix seconds)
    """
    res_secs = RESOLUTION_SECONDS.get(RESOLUTION, 900)
    end_ts   = int(datetime.now(timezone.utc).timestamp())
    # Request extra candles to guarantee we have `limit` closed ones
    start_ts = end_ts - res_secs * (limit + 10)

    params = {
        "symbol":     symbol,
        "resolution": RESOLUTION,      # e.g. "15m"  NOT "15"
        "start":      start_ts,
        "end":        end_ts,
    }
    # Candles are public — no auth needed
    qs = "?" + urlencode(params)
    r  = requests.get(DELTA_BASE + "/history/candles" + qs,
                      headers={"User-Agent": "pairs-bot/2.0"}, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Candle fetch {symbol} → HTTP {r.status_code}: {r.text[:300]}")

    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"Candle API error for {symbol}: {body}")

    candles = body.get("result", [])
    if not candles:
        raise RuntimeError(f"No candles returned for {symbol}")

    # Sort ascending, drop the still-open (last) candle, keep most recent `limit`
    candles = sorted(candles, key=lambda c: c["time"])
    candles = candles[:-1][-limit:]

    return pd.Series(
        [float(c["close"]) for c in candles],
        index=pd.to_datetime([c["time"] for c in candles], unit="s", utc=True),
        name=symbol,
    )


def compute_zscore(closes_a: pd.Series, closes_b: pd.Series) -> dict:
    """OLS spread Z-score."""
    aligned = pd.concat([closes_a, closes_b], axis=1).dropna()
    a = aligned.iloc[:, 0].values
    b = aligned.iloc[:, 1].values

    beta, alpha, *_ = stats.linregress(b, a)
    spread = a - (beta * b + alpha)
    mean   = spread.mean()
    std    = spread.std(ddof=1)
    z      = (spread[-1] - mean) / std if std > 0 else 0.0

    return {
        "spread_now":  spread[-1],
        "spread_mean": mean,
        "spread_std":  std,
        "z_score":     z,
        "beta":        beta,
        "alpha":       alpha,
        "price_a":     a[-1],
        "price_b":     b[-1],
        "ratio":       a[-1] / b[-1] if b[-1] else 0,
        "timestamp":   aligned.index[-1],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ACCOUNT & CONTRACT INFO
# ═══════════════════════════════════════════════════════════════════════════════
_product_cache: dict[str, dict] = {}


def get_product(symbol: str) -> dict:
    """
    Fetch and cache perpetual futures product metadata.
    Uses GET /v2/products?contract_type=perpetual_futures and filters by symbol.
    Caches result to avoid repeated API calls.
    """
    if symbol in _product_cache:
        return _product_cache[symbol]

    # Fetch all perpetual futures (paginate if needed — usually fits in one page)
    body = delta_get("/products", {"contract_type": "perpetual_futures", "page_size": 100})
    for p in body.get("result", []):
        _product_cache[p["symbol"]] = p

    if symbol not in _product_cache:
        raise RuntimeError(
            f"Symbol '{symbol}' not found in perpetual futures products. "
            f"Check exact symbol name at: https://api.india.delta.exchange/v2/products"
        )

    p = _product_cache[symbol]
    log.info(
        f"Product {symbol}: id={p['id']}  "
        f"contract_size={p.get('contract_size', 1)}  "
        f"min_size={p.get('min_size', 1)}  "
        f"tick_size={p.get('tick_size')}"
    )
    return p


def get_free_usdt_balance() -> float:
    """Return free (available) USDT balance."""
    body = delta_get("/wallet/balances")
    for asset in body.get("result", []):
        if asset.get("asset_symbol") == "USDT":
            bal = float(asset.get("available_balance", 0))
            log.info(f"Free USDT balance: {bal:.4f}")
            return bal
    raise RuntimeError("USDT not found in wallet balances")


def compute_contracts(symbol: str, price: float, usdt_notional: float) -> int:
    """
    Convert USDT notional → integer contract count.
      contracts = floor(notional / (price × contract_size))
    Enforces the exchange minimum size.
    """
    product       = get_product(symbol)
    contract_size = float(product.get("contract_size", 1))
    min_size      = int(product.get("min_size", 1))
    qty           = math.floor(usdt_notional / (price * contract_size))
    return max(qty, min_size)


# ═══════════════════════════════════════════════════════════════════════════════
#  POSITION QUERY (for startup validation)
# ═══════════════════════════════════════════════════════════════════════════════
def get_live_position(symbol: str) -> dict | None:
    """
    Query a single open position by product_id.
    GET /v2/positions?product_id=<id>
    Returns the position dict or None if flat / zero size.
    """
    product = get_product(symbol)
    try:
        body = delta_get("/positions", {"product_id": product["id"]})
        pos  = body.get("result", {})
        size = int(float(pos.get("size", 0)))
        if size > 0:
            return pos
    except Exception as e:
        log.warning(f"Could not fetch position for {symbol}: {e}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  ORDER EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════
def place_market_order(symbol: str, side: str, qty: int) -> dict:
    """
    Place a market order.
    side: "buy" | "sell"
    qty : integer number of contracts
    """
    product = get_product(symbol)
    payload = {
        "product_id":     product["id"],
        "product_symbol": symbol,
        "order_type":     "market_order",
        "side":           side,
        "size":           qty,
    }
    log.info(f"→ Market order: {side.upper()} {qty} × {symbol}  (product_id={product['id']})")
    result = delta_post("/orders", payload)
    order  = result.get("result", {})
    log.info(
        f"  ✓ id={order.get('id')}  "
        f"avg_fill={order.get('average_fill_price', '?')}  "
        f"state={order.get('state')}"
    )
    return order


def open_pair(signal: str, data: dict) -> tuple[dict, dict, int, int]:
    """
    Open both legs as market orders.
    LONG_A  → BUY  SYMBOL_A, SELL SYMBOL_B
    SHORT_A → SELL SYMBOL_A, BUY  SYMBOL_B
    Sizing: (free_balance × WALLET_FRACTION × LEVERAGE) / 2 per leg,
            with leg B beta-adjusted for dollar-neutrality.
    """
    free_usdt      = get_free_usdt_balance()
    total_notional = free_usdt * WALLET_FRACTION * LEVERAGE
    leg_notional   = total_notional / 2.0

    price_a = data["price_a"]
    price_b = data["price_b"]
    beta    = abs(data["beta"])

    qty_a = compute_contracts(SYMBOL_A, price_a, leg_notional)
    qty_b = compute_contracts(SYMBOL_B, price_b, leg_notional * beta)

    side_a = "buy"  if signal == "LONG_A" else "sell"
    side_b = "sell" if signal == "LONG_A" else "buy"

    log.info(
        f"Opening [{signal}]: "
        f"{side_a.upper()} {qty_a}×{SYMBOL_A} ~${price_a:.4f}  |  "
        f"{side_b.upper()} {qty_b}×{SYMBOL_B} ~${price_b:.2f}  "
        f"(beta={beta:.4f})"
    )

    order_a = place_market_order(SYMBOL_A, side_a, qty_a)
    order_b = place_market_order(SYMBOL_B, side_b, qty_b)
    return order_a, order_b, qty_a, qty_b


def close_pair(position) -> tuple[dict, dict]:
    """Close both legs by reversing saved directions."""
    close_a = "sell" if position.direction_a == "buy" else "buy"
    close_b = "sell" if position.direction_b == "buy" else "buy"

    log.info(
        f"Closing [{position.type}]: "
        f"{close_a.upper()} {position.qty_a}×{SYMBOL_A}  |  "
        f"{close_b.upper()} {position.qty_b}×{SYMBOL_B}"
    )
    order_a = place_market_order(SYMBOL_A, close_a, position.qty_a)
    order_b = place_market_order(SYMBOL_B, close_b, position.qty_b)
    return order_a, order_b


# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════
def send_telegram(message: str) -> bool:
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram ✓")
        return True
    except Exception as e:
        log.error(f"Telegram ✗ {e}")
        return False


def _fill_price(order: dict, fallback: float) -> float:
    v = order.get("average_fill_price")
    return float(v) if v else fallback


def fmt_entry_msg(signal: str, data: dict, order_a: dict, order_b: dict,
                  qty_a: int, qty_b: int) -> str:
    ts    = data["timestamp"].strftime("%Y-%m-%d %H:%M UTC")
    z     = data["z_score"]
    icon  = "🟢" if signal == "LONG_A" else "🔴"
    dir_a = "LONG" if signal == "LONG_A" else "SHORT"
    dir_b = "SHORT" if signal == "LONG_A" else "LONG"
    fa    = _fill_price(order_a, data["price_a"])
    fb    = _fill_price(order_b, data["price_b"])
    return (
        f"{icon} <b>PAIR OPENED — {signal}</b>  |  {ts}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Z-score: <code>{z:+.4f}</code>   Spread: <code>{data['spread_now']:+.6f}</code>\n"
        f"📐 Beta: <code>{data['beta']:.5f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🅰️  <b>{dir_a} {SYMBOL_A}</b>  {qty_a} contracts @ <code>${fa:.4f}</code>\n"
        f"   Order ID: <code>{order_a.get('id', '—')}</code>\n"
        f"🅱️  <b>{dir_b} {SYMBOL_B}</b>  {qty_b} contracts @ <code>${fb:.2f}</code>\n"
        f"   Order ID: <code>{order_b.get('id', '—')}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{WALLET_FRACTION*100:.0f}% wallet × {LEVERAGE}× leverage</i>"
    )


def fmt_exit_msg(data: dict, position, order_a: dict, order_b: dict) -> str:
    ts      = data["timestamp"].strftime("%Y-%m-%d %H:%M UTC")
    z       = data["z_score"]
    pnl     = position.unrealised_pnl(data["spread_now"])
    ico     = "✅" if pnl >= 0 else "❌"
    fa      = _fill_price(order_a, data["price_a"])
    fb      = _fill_price(order_b, data["price_b"])
    return (
        f"⚪ <b>PAIR CLOSED</b>  |  {ts}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Exit Z: <code>{z:+.4f}</code>  Entry Z: <code>{position.entry_z:+.4f}</code>\n"
        f"{ico} Spread PnL: <code>{pnl:+.6f}</code>   Age: <code>{position.age_str()}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🅰️  Closed {SYMBOL_A} @ <code>${fa:.4f}</code>  ID: <code>{order_a.get('id','—')}</code>\n"
        f"🅱️  Closed {SYMBOL_B} @ <code>${fb:.2f}</code>  ID: <code>{order_b.get('id','—')}</code>\n"
    )


def fmt_heartbeat_msg(data: dict, position=None) -> str:
    ts   = data["timestamp"].strftime("%Y-%m-%d %H:%M UTC")
    z    = data["z_score"]
    body = "📋 <b>Status:</b> FLAT"
    if position:
        pnl = position.unrealised_pnl(data["spread_now"])
        ico = "✅" if pnl >= 0 else "❌"
        body = (
            f"📋 <b>Position:</b> {position.type}\n"
            f"   Entry Z: <code>{position.entry_z:+.4f}</code>  "
            f"Current Z: <code>{z:+.4f}</code>\n"
            f"   {ico} Spread PnL: <code>{pnl:+.6f}</code>  Age: <code>{position.age_str()}</code>"
        )
    return (
        f"💓 <b>HEARTBEAT</b>  |  {ts}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Z: <code>{z:+.4f}</code>   "
        f"Spread: <code>{data['spread_now']:+.6f}</code>\n"
        f"💰 {SYMBOL_A}: <code>${data['price_a']:.4f}</code>   "
        f"{SYMBOL_B}: <code>${data['price_b']:.2f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{body}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Lookback {LOOKBACK}×{RESOLUTION}  |  Delta Exchange India</i>"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  POSITION  — in-memory object + disk persistence
# ═══════════════════════════════════════════════════════════════════════════════
class Position:
    def __init__(self, sig_type: str, entry_z: float, entry_spread: float,
                 direction_a: str, direction_b: str,
                 qty_a: int, qty_b: int,
                 opened_at: datetime | None = None):
        self.type         = sig_type
        self.entry_z      = entry_z
        self.entry_spread = entry_spread
        self.direction_a  = direction_a    # "buy" | "sell"
        self.direction_b  = direction_b
        self.qty_a        = qty_a
        self.qty_b        = qty_b
        self.opened_at    = opened_at or datetime.now(timezone.utc)

    def unrealised_pnl(self, current_spread: float) -> float:
        if self.type == "LONG_A":
            return current_spread - self.entry_spread
        return self.entry_spread - current_spread

    def age_str(self) -> str:
        delta = datetime.now(timezone.utc) - self.opened_at
        h, r  = divmod(int(delta.total_seconds()), 3600)
        return f"{h}h {r//60}m"

    def to_dict(self) -> dict:
        return {
            "type": self.type, "entry_z": self.entry_z,
            "entry_spread": self.entry_spread,
            "direction_a": self.direction_a, "direction_b": self.direction_b,
            "qty_a": self.qty_a, "qty_b": self.qty_b,
            "opened_at": self.opened_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(
            sig_type     = d["type"],
            entry_z      = float(d["entry_z"]),
            entry_spread = float(d["entry_spread"]),
            direction_a  = d["direction_a"],
            direction_b  = d["direction_b"],
            qty_a        = int(d["qty_a"]),
            qty_b        = int(d["qty_b"]),
            opened_at    = datetime.fromisoformat(d["opened_at"]),
        )


def save_position(pos: Position | None) -> None:
    if pos is None:
        if os.path.exists(POSITION_FILE):
            os.remove(POSITION_FILE)
            log.info("Position file removed (flat)")
        return
    with open(POSITION_FILE, "w") as f:
        json.dump(pos.to_dict(), f, indent=2)
    log.info(f"Position saved: {pos.type}  A={pos.qty_a}  B={pos.qty_b}")


def load_position() -> Position | None:
    if not os.path.exists(POSITION_FILE):
        log.info("No saved position file — starting flat")
        return None
    try:
        with open(POSITION_FILE) as f:
            data = json.load(f)
        pos = Position.from_dict(data)
        log.info(
            f"Loaded saved position: {pos.type}  "
            f"entry_z={pos.entry_z:.4f}  "
            f"qty_A={pos.qty_a}  qty_B={pos.qty_b}  "
            f"opened={pos.opened_at.strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return pos
    except Exception as e:
        log.error(f"Failed to load position file: {e} — starting flat")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  BOT
# ═══════════════════════════════════════════════════════════════════════════════
class PairsBot:
    def __init__(self):
        self.position: Position | None = load_position()
        self.iteration = 0
        self.last_z    = 0.0

    # ── startup: sync local state with exchange ────────────────────────────
    def sync_position_with_exchange(self) -> None:
        """
        On startup, query actual open positions on Delta Exchange and reconcile
        with the saved local state. Three outcomes:

        1. Saved file exists + exchange confirms open → keep saved state, notify.
        2. Saved file exists + exchange is FLAT → someone closed manually; clear file.
        3. No saved file + exchange has open positions → reconstruct state from exchange
           (qty recovered; entry_z/spread unknown, set to 0 as placeholders).
        """
        log.info("Syncing position state with Delta Exchange …")

        live_a = get_live_position(SYMBOL_A)
        live_b = get_live_position(SYMBOL_B)
        has_live = (live_a is not None) or (live_b is not None)

        if self.position and has_live:
            # ── Case 1: saved state matches exchange ───────────────────────
            log.info("✅ Saved position confirmed on exchange — continuing.")
            send_telegram(
                f"🔄 <b>Bot restarted — position restored</b>\n"
                f"Type: <code>{self.position.type}</code>  "
                f"A: <code>{self.position.qty_a} contracts</code>  "
                f"B: <code>{self.position.qty_b} contracts</code>\n"
                f"Entry Z: <code>{self.position.entry_z:+.4f}</code>  "
                f"Age: <code>{self.position.age_str()}</code>\n"
                f"⚠️ Bot will manage this position normally."
            )

        elif self.position and not has_live:
            # ── Case 2: saved state but exchange is flat ───────────────────
            log.warning("⚠️  Saved position found but exchange is FLAT — clearing local state.")
            send_telegram(
                f"⚠️ <b>Mismatch on restart</b>\n"
                f"Saved position <code>{self.position.type}</code> found locally, "
                f"but Delta Exchange shows NO open positions.\n"
                f"Clearing local state — bot starting FLAT."
            )
            self.position = None
            save_position(None)

        elif not self.position and has_live:
            # ── Case 3: no saved file but exchange has open positions ───────
            log.warning("⚠️  No saved file but exchange has open positions — reconstructing state.")

            # Determine direction from exchange position data
            # Delta position: side="buy" means long, side="sell" means short
            dir_a = live_a.get("side", "buy") if live_a else "buy"
            dir_b = live_b.get("side", "buy") if live_b else "sell"
            qty_a = int(float(live_a.get("size", 0))) if live_a else 0
            qty_b = int(float(live_b.get("size", 0))) if live_b else 0

            sig_type = "LONG_A" if dir_a == "buy" else "SHORT_A"

            self.position = Position(
                sig_type     = sig_type,
                entry_z      = 0.0,          # unknown — was opened before bot ran
                entry_spread = 0.0,          # unknown
                direction_a  = dir_a,
                direction_b  = dir_b,
                qty_a        = qty_a,
                qty_b        = qty_b,
                opened_at    = datetime.now(timezone.utc),
            )
            save_position(self.position)
            send_telegram(
                f"⚠️ <b>Open positions found on exchange (no local record)</b>\n"
                f"Reconstructed: <code>{sig_type}</code>\n"
                f"A: <code>{qty_a} contracts ({dir_a})</code>  "
                f"B: <code>{qty_b} contracts ({dir_b})</code>\n"
                f"Entry Z/Spread unknown (set to 0). Bot will manage exit normally."
            )
        else:
            # ── Both flat ──────────────────────────────────────────────────
            log.info("Exchange and local state both flat — starting fresh.")

    # ── main tick ──────────────────────────────────────────────────────────
    def tick(self):
        self.iteration += 1
        log.info(f"── tick #{self.iteration}  {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} ──")

        # 1. Fetch market data
        try:
            closes_a = fetch_klines(SYMBOL_A)
            closes_b = fetch_klines(SYMBOL_B)
        except Exception as e:
            log.error(f"Data fetch failed: {e}")
            return

        # 2. Compute Z-score
        try:
            data = compute_zscore(closes_a, closes_b)
        except Exception as e:
            log.error(f"Z-score calculation failed: {e}")
            return

        z = data["z_score"]
        log.info(
            f"Z={z:+.4f}  spread={data['spread_now']:+.6f}  "
            f"pos={'FLAT' if not self.position else self.position.type}"
        )

        # 3. Signal logic
        signal = None

        if self.position is None:
            if z <= -ENTRY_Z:
                signal = "LONG_A"
            elif z >= ENTRY_Z:
                signal = "SHORT_A"
        else:
            if (
                (self.position.type == "LONG_A"  and z >= -EXIT_Z) or
                (self.position.type == "SHORT_A" and z <=  EXIT_Z)
            ):
                signal = "EXIT"

        # 4. Execute
        if signal in ("LONG_A", "SHORT_A"):
            log.info(f"SIGNAL → {signal} — placing entry orders")
            try:
                order_a, order_b, qty_a, qty_b = open_pair(signal, data)
                dir_a = "buy"  if signal == "LONG_A" else "sell"
                dir_b = "sell" if signal == "LONG_A" else "buy"
                self.position = Position(signal, z, data["spread_now"],
                                         dir_a, dir_b, qty_a, qty_b)
                save_position(self.position)
                send_telegram(fmt_entry_msg(signal, data, order_a, order_b, qty_a, qty_b))
            except Exception as e:
                log.exception(f"Entry failed: {e}")
                send_telegram(f"🚨 <b>ENTRY FAILED</b>\n<code>{e}</code>\nBot still running.")

        elif signal == "EXIT":
            log.info("SIGNAL → EXIT — placing close orders")
            try:
                order_a, order_b = close_pair(self.position)
                msg = fmt_exit_msg(data, self.position, order_a, order_b)
                self.position = None
                save_position(None)
                send_telegram(msg)
            except Exception as e:
                log.exception(f"Exit failed: {e}")
                send_telegram(
                    f"🚨 <b>EXIT FAILED — position may still be open!</b>\n"
                    f"<code>{e}</code>\n"
                    f"⚠️ Check Delta Exchange manually."
                )

        elif self.iteration % HEARTBEAT_TICKS == 0:
            send_telegram(fmt_heartbeat_msg(data, self.position))

        self.last_z = z

    # ── run loop ───────────────────────────────────────────────────────────
    def run(self):
        log.info("═" * 65)
        log.info("  Pairs Bot  (Delta Exchange India — Fully Automated)")
        log.info(f"  Pair       : {SYMBOL_A} / {SYMBOL_B}")
        log.info(f"  Resolution : {RESOLUTION}  |  Lookback: {LOOKBACK}")
        log.info(f"  Entry Z    : ±{ENTRY_Z}  |  Exit Z: ±{EXIT_Z}")
        log.info(f"  Sizing     : {WALLET_FRACTION*100:.0f}% wallet × {LEVERAGE}× leverage")
        log.info("═" * 65)

        # Pre-flight: sync with exchange before first tick
        try:
            self.sync_position_with_exchange()
        except Exception as e:
            log.error(f"Position sync failed: {e}")
            send_telegram(f"⚠️ <b>Startup position sync failed</b>: <code>{e}</code>")

        pos_line = (
            f"Position: <code>{self.position.type}</code>"
            if self.position else "Starting FLAT"
        )
        send_telegram(
            f"🤖 <b>Pairs Bot LIVE</b>  (Delta Exchange India)\n"
            f"Pair: <code>{SYMBOL_A}/{SYMBOL_B}</code>  "
            f"Resolution: <code>{RESOLUTION}</code>  Lookback: <code>{LOOKBACK}</code>\n"
            f"Entry: <code>±{ENTRY_Z}σ</code>  Exit: <code>±{EXIT_Z}σ</code>\n"
            f"Sizing: <code>{WALLET_FRACTION*100:.0f}%</code> wallet × "
            f"<code>{LEVERAGE}×</code> leverage\n"
            f"{pos_line}"
        )

        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                log.info("Stopped by user.")
                send_telegram("🛑 <b>Pairs Bot stopped</b>")
                break
            except Exception as e:
                log.exception(f"Unexpected tick error: {e}")
                send_telegram(f"⚠️ <b>Tick error</b>: <code>{e}</code>  Retrying next poll.")

            log.info(f"Sleeping {POLL_SECONDS}s …\n")
            time.sleep(POLL_SECONDS)


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    bot = PairsBot()
    bot.run()
