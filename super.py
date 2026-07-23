"""
====================================================================
  Delta Exchange India — Supertrend Trading Bot
  Timeframe : 5-minute candles
  Data      : Binance via CCXT
  Exchange  : Delta Exchange India (REST + WebSocket)
  Strategy  : Supertrend flip (bull/bear) + ADX confirmation
  Risk      : 10% balance, 25x leverage, 0.7% trailing SL

  FIXES APPLIED:
  1. detect_flip() now requires FLIP_CONFIRMATION_CANDLES consecutive
     candles in the new direction — eliminates single-wick fakeouts.
  2. _enter_trade() returns bool; clears _pending_entry_side on ADX
     rejection to stop 5-second retry spam into weak-trend moves.
  3. adx_is_rising() check added — ADX must be climbing, not just
     above threshold, to confirm a strengthening trend.
  4. Signature fix: "?" prepended inside _sign() when query non-empty.
  5. get_open_orders uses "product_id" (singular) per API docs.
  6. tick_size fallback removed — raises ValueError on missing product.
  7. Exit order placed BEFORE TSL deactivation to avoid unprotected
     position if market order fails.
  8. recover_state() does not set _pending_entry_side when position
     already exists.
  9. MIN_SL_MOVE_PCT threshold prevents excessive SL update API calls.
  10. stop_trigger_method explicitly set to "mark_price".
====================================================================
"""

import os
import sys
import time
import math
import hmac
import json
import logging
import hashlib
import threading
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
from urllib.parse import urlencode

import ccxt
import numpy as np
import pandas as pd
import requests
import websocket
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_KEY = os.getenv("API_KEY1")
API_SECRET = os.getenv("API_SECRET1")

DELTA_TICKER  = "XRPUSD"
SYMBOL_CCXT   = "XRP/USDT"
TIMEFRAME     = "5m"
CANDLE_LIMIT  = 200

ATR_PERIOD      = 10
ATR_MULTIPLIER  = 3.0

ADX_PERIOD    = 14
ADX_THRESHOLD = 20
ADX_RISING_LOOKBACK = 3   # ADX must be rising over this many candles

# FIX 1: require this many consecutive closed candles in the new
# supertrend direction before treating the move as a confirmed flip.
# Set to 1 to restore original single-candle behaviour (not recommended).
FLIP_CONFIRMATION_CANDLES = 2

RISK_PCT        = 0.10
LEVERAGE        = 25
TRAILING_SL_PCT = 0.007

# Profit-lock SL constants
PROFIT_LOCK_TRIGGER = 0.005   # uPnL % that activates profit-lock SL
PROFIT_LOCK_BUFFER  = 0.0006  # buffer subtracted from uPnL to set locked-in profit

# Only update SL if it moves by at least this fraction (avoids excessive API calls)
MIN_SL_MOVE_PCT = 0.0002

LOOP_INTERVAL   = 5
TRADE_COOLDOWN  = 0
MAX_API_RETRIES = 5
RETRY_DELAY     = 2

DELTA_BASE_URL = "https://api.india.delta.exchange"
LOG_FILE       = "trading_bot.log"

# ─────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────
try:
    import colorama
    colorama.init(autoreset=True)
    _ANSI = True
except ImportError:
    _ANSI = False

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _ANSI else text

def _green(t):  return _c("92", t)
def _red(t):    return _c("91", t)
def _yellow(t): return _c("93", t)
def _cyan(t):   return _c("96", t)
def _bold(t):   return _c("1",  t)
def _dim(t):    return _c("2",  t)
def _white(t):  return _c("97", t)

SEP_THICK = "=" * 68
SEP_THIN  = "-" * 68
SEP_DOT   = "·" * 68

def print_banner():
    print()
    print(_bold(_cyan(SEP_THICK)))
    print(_bold(_cyan("  DELTA EXCHANGE — SUPERTREND + ADX TRADING BOT")))
    print(_bold(_cyan("  India Perpetuals  |  REST + WebSocket")))
    print(_bold(_cyan(SEP_THICK)))
    print()

def print_config_table(cfg: dict):
    print(_bold("  CONFIGURATION"))
    print(_dim("  " + SEP_THIN))
    for label, value in cfg.items():
        print(f"  {_cyan(label):<30} {_white(str(value))}")
    print(_dim("  " + SEP_THIN))
    print()

def print_section(title: str):
    print()
    print(_bold(_yellow(f"  ── {title} " + "─" * max(0, 60 - len(title)))))

def print_tick_header(tick_no: int, price: float, timestamp: str):
    print()
    print(_dim(SEP_DOT))
    print(
        f"  {_bold('TICK')} #{tick_no:<6}"
        f"  {_dim('time:')} {_white(timestamp)}"
        f"  {_dim('price:')} {_bold(_white(f'{price:.4f}'))}"
    )
    print(_dim(SEP_DOT))

def print_indicator_row(supertrend_dir: int, adx: float,
                        flip: Optional[str], adx_rising: bool):
    st_label  = _green("BULLISH") if supertrend_dir == 1 else _red("BEARISH")
    adx_str   = f"{adx:.2f}"
    trend_str = _green("[STRONG↑]") if adx >= ADX_THRESHOLD and adx_rising else \
                _yellow("[STRONG→]") if adx >= ADX_THRESHOLD else \
                _yellow("[WEAK]")
    adx_label = f"{adx_str}  {trend_str}"
    flip_label = (
        _green("BULLISH FLIP") if flip == "bullish_flip" else
        _red("BEARISH FLIP")   if flip == "bearish_flip" else
        _dim("none")
    )
    print(
        f"  {_dim('Supertrend:')} {st_label:<20}"
        f"  {_dim('ADX:')} {adx_label:<34}"
        f"  {_dim('Flip:')} {flip_label}"
    )

def print_position_status(side: Optional[str], size: int,
                           entry: float, pnl_pct: float):
    if side is None:
        print(f"  {_dim('Position:')} {_yellow('FLAT  (no open position)')}")
        return
    side_label = _green("LONG") if side == "buy" else _red("SHORT")
    pnl_label  = (
        _green(f"+{pnl_pct*100:.2f}%") if pnl_pct >= 0
        else _red(f"{pnl_pct*100:.2f}%")
    )
    print(
        f"  {_dim('Position:')} {side_label}"
        f"  {_dim('size:')} {_white(str(size))}"
        f"  {_dim('entry:')} {_white(f'{entry:.4f}')}"
        f"  {_dim('uPnL:')} {pnl_label}"
    )

def print_trade_event(event: str, side: str, contracts: int,
                      price: float, reason: str = ""):
    side_label = _green("BUY  / LONG") if side == "buy" else _red("SELL / SHORT")
    tag = f"[{reason}]" if reason else ""
    print()
    print(_bold(SEP_THICK))
    if event == "ENTRY":
        print(_bold(_green(
            f"  *** ENTRY {side_label}  |  contracts={contracts}"
            f"  |  price={price:.4f}  {tag}"
        )))
    elif event == "EXIT":
        print(_bold(_red(
            f"  *** EXIT  {side_label}  |  contracts={contracts}"
            f"  |  price={price:.4f}  {tag}"
        )))
    print(_bold(SEP_THICK))
    print()

def print_sl_event(sl_price: float, order_id):
    print(
        f"  {_yellow('SL placed')}  "
        f"stop={_white(f'{sl_price:.4f}')}  "
        f"order_id={_dim(str(order_id))}"
    )

def print_adx_rejection(adx_val: float, side: str, rising: bool):
    reason = "trend too weak" if adx_val < ADX_THRESHOLD else "ADX not rising"
    print(
        f"  {_yellow('ADX FILTER')}  "
        f"adx={_white(f'{adx_val:.2f}')}  "
        f"rising={_white(str(rising))}  "
        f"threshold={_white(str(ADX_THRESHOLD))}  "
        f"side={_white(side.upper())}  "
        f"{_red(f'ENTRY REJECTED — {reason}')}"
    )

def print_recovery_status(case: str, details: str):
    icon = _green("OK") if "clean" in details.lower() or "restored" in details.lower() \
           else _yellow("!")
    print(f"  [{icon}] {_bold('RECOVERY')}  {_cyan(case)}  {_dim(details)}")

def print_balance(balance: float, asset: str):
    print(f"  {_dim('Balance:')} {_bold(_white(f'{balance:.4f} {asset}'))}")

def print_warning(msg: str):
    print(f"  {_yellow('WARNING')}  {msg}")

def print_error(msg: str):
    print(f"  {_red('ERROR')}  {msg}")

def print_cooldown(remaining: float):
    print(f"  {_dim('Cooldown:')} {_yellow(f'{remaining:.0f}s remaining — skipping entry')}")


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("DeltaBot")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

log = setup_logger()


# ─────────────────────────────────────────────
# DELTA EXCHANGE REST CLIENT
# ─────────────────────────────────────────────
class DeltaClient:
    def __init__(self, api_key: str, api_secret: str,
                 base_url: str = DELTA_BASE_URL):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self.session    = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent":   "python-rest-client",
        })

    def _sign(self, method: str, path: str, query: str,
              body: str, timestamp: str) -> str:
        # Per Delta Exchange docs the signed string is:
        # method + timestamp + path + "?" + query_string + body
        # "?" is included only when query is non-empty.
        payload = method.upper() + timestamp + path
        if query:
            payload += "?" + query
        payload += body
        return hmac.new(
            self.api_secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()

    def _request(self, method: str, path: str,
                 params: Optional[dict] = None,
                 data:   Optional[dict] = None) -> Optional[dict]:

        if params:
            query_str = urlencode(sorted(params.items()))
            url = f"{self.base_url}{path}?{query_str}"
        else:
            query_str = ""
            url = self.base_url + path

        body_str = json.dumps(data, separators=(",", ":")) if data else ""

        for attempt in range(1, MAX_API_RETRIES + 1):
            timestamp = str(int(time.time()))
            signature = self._sign(method.upper(), path, query_str,
                                   body_str, timestamp)
            headers = {
                "api-key":   self.api_key,
                "timestamp": timestamp,
                "signature": signature,
            }
            try:
                resp = self.session.request(
                    method, url,
                    data    = body_str if body_str else None,
                    headers = headers,
                    timeout = 15,
                )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                log.warning("HTTP %s on %s %s (attempt %d/%d): %s",
                            resp.status_code, method, path,
                            attempt, MAX_API_RETRIES, e)
                if resp.status_code != 429 and 400 <= resp.status_code < 500:
                    log.error("Client error %s — not retrying: %s",
                              resp.status_code, resp.text[:300])
                    return None
            except Exception as e:
                log.warning("Request error %s %s (attempt %d/%d): %s",
                            method, path, attempt, MAX_API_RETRIES, e)
            if attempt < MAX_API_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

        log.error("All %d attempts failed for %s %s",
                  MAX_API_RETRIES, method, path)
        return None

    def get(self, path: str,
            params: Optional[dict] = None) -> Optional[dict]:
        return self._request("GET", path, params=params)

    def post(self, path: str,
             data: Optional[dict] = None) -> Optional[dict]:
        return self._request("POST", path, data=data)

    def delete(self, path: str,
               data: Optional[dict] = None) -> Optional[dict]:
        return self._request("DELETE", path, data=data)

    @staticmethod
    def result(resp: Optional[dict],
               key: str = "result") -> Optional[dict]:
        if resp is None:
            return None
        if key not in resp:
            log.warning("Key '%s' missing in response: %s",
                        key, str(resp)[:200])
            return None
        return resp[key]


# ─────────────────────────────────────────────
# PRODUCT REGISTRY
# ─────────────────────────────────────────────
class ProductRegistry:
    def __init__(self, client: DeltaClient):
        self.client  = client
        self._cache: Dict[str, dict] = {}

    def get_product(self, ticker: str) -> Optional[dict]:
        if ticker in self._cache:
            return self._cache[ticker]
        resp = self.client.get(f"/v2/products/{ticker}")
        prod = self.client.result(resp)
        if not prod:
            log.error("Ticker '%s' not found or request failed", ticker)
            return None
        self._cache[ticker] = prod
        return prod

    def get_product_id(self, ticker: str) -> Optional[int]:
        p = self.get_product(ticker)
        return p["id"] if p else None

    def get_tick_size(self, ticker: str) -> float:
        p = self.get_product(ticker)
        if not p:
            raise ValueError(
                f"Cannot determine tick_size for '{ticker}': product not found. "
                "Ensure the ticker is valid and the API is reachable."
            )
        return float(p["tick_size"])

    def get_contract_size(self, ticker: str) -> float:
        p = self.get_product(ticker)
        if not p:
            raise ValueError(
                f"Cannot determine contract_value for '{ticker}': "
                "product not found."
            )
        return float(p["contract_value"])

    def get_settling_asset(self, ticker: str) -> str:
        p = self.get_product(ticker)
        if not p:
            return "USD"
        sa = p.get("settling_asset")
        if isinstance(sa, dict):
            return sa.get("symbol", "USD")
        if isinstance(sa, str):
            return sa
        return "USD"


# ─────────────────────────────────────────────
# ACCOUNT
# ─────────────────────────────────────────────
class Account:
    def __init__(self, client: DeltaClient):
        self.client = client

    def get_balance(self, asset: str = "USD") -> float:
        resp     = self.client.get("/v2/wallet/balances")
        balances = self.client.result(resp)
        if not balances:
            return 0.0
        for b in (balances if isinstance(balances, list) else [balances]):
            sym = b.get("asset_symbol") or b.get("currency") or ""
            if sym.upper() == asset.upper():
                val = b.get("available_balance") or b.get("balance") or 0
                return float(val)
        log.warning("Asset %s not found in balances", asset)
        return 0.0

    def set_leverage(self, product_id: int, leverage: int) -> bool:
        path = f"/v2/products/{product_id}/orders/leverage"
        resp = self.client.post(path, {"leverage": str(leverage)})
        return resp is not None and resp.get("success", False)


# ─────────────────────────────────────────────
# POSITIONS
# ─────────────────────────────────────────────
class PositionManager:
    def __init__(self, client: DeltaClient):
        self.client = client

    def get_position(self, product_id: int) -> Optional[dict]:
        resp   = self.client.get("/v2/positions",
                                 params={"product_id": product_id})
        result = self.client.result(resp)
        if result is None:
            return None
        if isinstance(result, list):
            for p in result:
                if p.get("product_id") == product_id:
                    return p
            return None
        return result

    def has_open_position(self, product_id: int) -> bool:
        pos = self.get_position(product_id)
        if not pos:
            return False
        return float(pos.get("size") or 0) != 0

    def get_side(self, product_id: int) -> Optional[str]:
        pos = self.get_position(product_id)
        if not pos:
            return None
        size = float(pos.get("size") or 0)
        if size > 0:
            return "buy"
        if size < 0:
            return "sell"
        return None

    def get_entry_price(self, product_id: int) -> float:
        pos = self.get_position(product_id)
        if not pos:
            return 0.0
        return float(pos.get("entry_price") or 0)

    def get_unrealized_pnl_pct(self, product_id: int,
                                current_price: float) -> float:
        pos = self.get_position(product_id)
        if not pos:
            return 0.0
        entry = float(pos.get("entry_price") or 0)
        size  = float(pos.get("size") or 0)
        if entry == 0:
            return 0.0
        return (current_price - entry) / entry if size > 0 \
               else (entry - current_price) / entry


# ─────────────────────────────────────────────
# ORDER MANAGER
# ─────────────────────────────────────────────
class OrderManager:
    def __init__(self, client: DeltaClient, registry: ProductRegistry):
        self.client   = client
        self.registry = registry

    def _round_to_tick(self, price: float, tick_size: float) -> float:
        if tick_size <= 0:
            return round(price, 4)
        return round(round(price / tick_size) * tick_size, 8)

    def place_market_order(self, product_id: int, side: str,
                           size: int,
                           reduce_only: bool = False) -> Optional[dict]:
        payload = {
            "product_id":  product_id,
            "side":        side,
            "order_type":  "market_order",
            "size":        size,
            "reduce_only": "true" if reduce_only else "false",
        }
        log.debug("Placing MARKET %s | product_id=%s | size=%s | reduce_only=%s",
                  side.upper(), product_id, size, reduce_only)
        resp   = self.client.post("/v2/orders", payload)
        result = self.client.result(resp)
        if result:
            log.debug("Order placed: id=%s status=%s",
                      result.get("id"), result.get("state"))
        return result

    def place_stop_limit_order(self, product_id: int, side: str,
                               size: int, stop_price: float,
                               limit_price: float) -> Optional[dict]:
        ticker    = self._get_ticker(product_id)
        tick_size = self.registry.get_tick_size(ticker) if ticker else None
        if tick_size is None:
            log.error("Cannot place SL: tick_size unavailable for "
                      "product_id=%s", product_id)
            return None

        stop_price  = self._round_to_tick(stop_price,  tick_size)
        limit_price = self._round_to_tick(limit_price, tick_size)

        if stop_price <= 0 or limit_price <= 0:
            log.error("Invalid SL price: stop=%.6f limit=%.6f",
                      stop_price, limit_price)
            return None

        payload = {
            "product_id":            product_id,
            "side":                  side,
            "order_type":            "limit_order",
            "size":                  size,
            "stop_order_type":       "stop_loss_order",
            "stop_price":            str(stop_price),
            "limit_price":           str(limit_price),
            "reduce_only":           "true",
            "stop_trigger_method":   "mark_price",
        }
        log.debug("Placing SL %s | stop=%.6f limit=%.6f",
                  side.upper(), stop_price, limit_price)
        resp = self.client.post("/v2/orders", payload)
        return self.client.result(resp)

    def get_open_orders(self, product_id: int) -> list:
        # "product_id" (singular) is the correct query param per API docs
        resp = self.client.get("/v2/orders", params={
            "product_id": product_id,
            "state":      "open",
            "page_size":  100,
        })
        result = self.client.result(resp)
        if result is None:
            return []
        return result if isinstance(result, list) else []

    def cancel_all_orders(self, product_id: int) -> bool:
        resp = self.client.delete("/v2/orders/all",
                                  data={"product_id": product_id})
        return resp is not None

    def cancel_stop_orders_only(self, product_id: int) -> bool:
        payload = {
            "product_id":                product_id,
            "cancel_stop_orders":        "true",
            "cancel_reduce_only_orders": "true",
        }
        resp = self.client.delete("/v2/orders/all", data=payload)
        return resp is not None

    def _get_ticker(self, product_id: int) -> Optional[str]:
        for sym, prod in self.registry._cache.items():
            if prod.get("id") == product_id:
                return sym
        return None

    def calculate_contracts(self, balance: float, price: float,
                            contract_value: float) -> int:
        notional  = balance * RISK_PCT * LEVERAGE
        contracts = notional / (price * contract_value)
        contracts = max(1, math.floor(contracts))
        log.debug(
            "Sizing: balance=%.2f price=%.4f notional=%.2f contracts=%d",
            balance, price, notional, contracts
        )
        return contracts


# ─────────────────────────────────────────────
# SUPERTREND INDICATOR
# ─────────────────────────────────────────────
def compute_supertrend(df: pd.DataFrame,
                       atr_period: int = ATR_PERIOD,
                       multiplier: float = ATR_MULTIPLIER) -> pd.DataFrame:
    df    = df.copy()
    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr         = tr.ewm(alpha=1 / atr_period, adjust=False).mean()
    hl2         = (high + low) / 2
    upper_basic = hl2 + multiplier * atr
    lower_basic = hl2 - multiplier * atr

    upper      = upper_basic.copy()
    lower      = lower_basic.copy()
    direction  = pd.Series(1, index=df.index)
    supertrend = pd.Series(np.nan, index=df.index)

    for i in range(1, len(df)):
        upper.iat[i] = (upper_basic.iat[i]
                        if upper_basic.iat[i] < upper.iat[i - 1]
                        or close.iat[i - 1] > upper.iat[i - 1]
                        else upper.iat[i - 1])

        lower.iat[i] = (lower_basic.iat[i]
                        if lower_basic.iat[i] > lower.iat[i - 1]
                        or close.iat[i - 1] < lower.iat[i - 1]
                        else lower.iat[i - 1])

        if direction.iat[i - 1] == -1 and close.iat[i] > upper.iat[i]:
            direction.iat[i] = 1
        elif direction.iat[i - 1] == 1 and close.iat[i] < lower.iat[i]:
            direction.iat[i] = -1
        else:
            direction.iat[i] = direction.iat[i - 1]

        supertrend.iat[i] = (lower.iat[i] if direction.iat[i] == 1
                             else upper.iat[i])

    df.loc[:, "supertrend"]           = supertrend
    df.loc[:, "supertrend_direction"] = direction
    return df


def detect_flip(df: pd.DataFrame,
                confirmation: int = FLIP_CONFIRMATION_CANDLES) -> Optional[str]:
    """
    FIX 1: Requires `confirmation` consecutive closed candles all agreeing
    on the new supertrend direction before signalling a flip.

    With confirmation=2 (default):
      - Checks that candle[-2] and candle[-1] are both in the new direction
      - Checks that candle[-3] was in the opposite direction
    This eliminates single-wick fakeouts that flip for one candle and reverse.

    Set FLIP_CONFIRMATION_CANDLES=1 to restore the original single-candle
    behaviour (not recommended for live trading).
    """
    if len(df) < confirmation + 2:
        return None

    prev_dir = df["supertrend_direction"].iloc[-(confirmation + 1)]
    recent   = df["supertrend_direction"].iloc[-confirmation:]

    if all(d == 1 for d in recent) and prev_dir == -1:
        return "bullish_flip"
    if all(d == -1 for d in recent) and prev_dir == 1:
        return "bearish_flip"
    return None


# ─────────────────────────────────────────────
# ADX INDICATOR
# ─────────────────────────────────────────────
def compute_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.DataFrame:
    df         = df.copy()
    high       = df["high"]
    low        = df["low"]
    close      = df["close"]
    prev_close = close.shift(1)
    prev_high  = high.shift(1)
    prev_low   = low.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    up_move   = high - prev_high
    down_move = prev_low - low

    plus_dm  = pd.Series(
        np.where((up_move > down_move)   & (up_move > 0),   up_move,   0.0),
        index=df.index)
    minus_dm = pd.Series(
        np.where((down_move > up_move)   & (down_move > 0), down_move, 0.0),
        index=df.index)

    alpha             = 1.0 / period
    smoothed_tr       = tr.ewm(alpha=alpha,       adjust=False).mean()
    smoothed_plus_dm  = plus_dm.ewm(alpha=alpha,  adjust=False).mean()
    smoothed_minus_dm = minus_dm.ewm(alpha=alpha, adjust=False).mean()

    plus_di  = 100 * smoothed_plus_dm  / smoothed_tr.replace(0, np.nan)
    minus_di = 100 * smoothed_minus_dm / smoothed_tr.replace(0, np.nan)

    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx     = 100 * (plus_di - minus_di).abs() / di_sum
    adx    = dx.ewm(alpha=alpha, adjust=False).mean()

    df.loc[:, "adx"]      = adx
    df.loc[:, "plus_di"]  = plus_di
    df.loc[:, "minus_di"] = minus_di
    return df


def get_adx_value(df: pd.DataFrame) -> float:
    if "adx" not in df.columns:
        return 0.0
    val = df["adx"].iloc[-1]
    return float(val) if not pd.isna(val) else 0.0


def adx_is_rising(df: pd.DataFrame,
                  lookback: int = ADX_RISING_LOOKBACK) -> bool:
    """
    FIX 3: Returns True only when ADX has been strictly increasing over
    the last `lookback` candles. An ADX above threshold but falling means
    momentum is fading — entering into that is a common cause of bad fills.
    """
    if "adx" not in df.columns or len(df) < lookback + 1:
        return False
    recent_adx = df["adx"].iloc[-lookback:]
    return bool(recent_adx.is_monotonic_increasing)


# ─────────────────────────────────────────────
# CANDLE FETCHER  (Binance via CCXT)
# ─────────────────────────────────────────────
BINANCE_TIMEFRAMES = {
    "1m","3m","5m","15m","30m",
    "1h","2h","4h","6h","8h","12h",
    "1d","3d","1w","1M",
}

_binance_exchange: Optional[ccxt.binance] = None

def _get_binance() -> ccxt.binance:
    global _binance_exchange
    if _binance_exchange is None:
        _binance_exchange = ccxt.binance({"enableRateLimit": True})
        _binance_exchange.load_markets()
        log.debug("Binance markets loaded (%d symbols)",
                  len(_binance_exchange.markets))
    return _binance_exchange


def fetch_candles(symbol: str    = SYMBOL_CCXT,
                  timeframe: str = TIMEFRAME,
                  limit: int     = CANDLE_LIMIT) -> Optional[pd.DataFrame]:
    if timeframe not in BINANCE_TIMEFRAMES:
        log.error("Invalid timeframe '%s'", timeframe)
        return None
    try:
        exchange = _get_binance()
    except Exception as e:
        log.error("Failed to initialise Binance: %s", e)
        return None

    if symbol not in exchange.markets:
        log.error("Symbol '%s' not found on Binance", symbol)
        return None

    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv:
                raise ValueError("Empty OHLCV response")
            df = pd.DataFrame(ohlcv,
                              columns=["timestamp", "open", "high",
                                       "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"],
                                             unit="ms", utc=True)
            df = df.set_index("timestamp").astype(float)
            # Always drop the last (still-forming) candle
            df = df.iloc[:-1]
            log.debug("Fetched %d candles | last close=%.5f",
                      len(df), df["close"].iloc[-1])
            return df
        except ccxt.BadSymbol as e:
            log.error("Bad symbol '%s': %s", symbol, e)
            return None
        except Exception as e:
            log.warning("Candle fetch attempt %d/%d failed: %s",
                        attempt, MAX_API_RETRIES, e)
            if attempt < MAX_API_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    log.error("All candle fetch attempts failed for %s %s", symbol, timeframe)
    return None


# ─────────────────────────────────────────────
# TRAILING STOP-LOSS MANAGER
# ─────────────────────────────────────────────
class TrailingStopLoss:
    def __init__(self, order_mgr: OrderManager):
        self.order_mgr                    = order_mgr
        self._sl_order_id: Optional[int]  = None
        self._best_price:  float          = 0.0
        self._initial_side: Optional[str] = None
        self._active                      = False
        self._product_id: Optional[int]   = None
        self._size: int                   = 0
        self._entry_price: float          = 0.0

        self._profit_lock_active: bool  = False
        self._locked_sl_price:    float = 0.0
        self._last_placed_sl_price: float = 0.0

    def activate(self, product_id: int, side: str,
                 entry_price: float, size: int):
        self._product_id            = product_id
        self._initial_side          = side
        self._best_price            = entry_price
        self._entry_price           = entry_price
        self._size                  = size
        self._active                = True
        self._profit_lock_active    = False
        self._locked_sl_price       = 0.0
        self._last_placed_sl_price  = 0.0
        log.debug("TSL activated | side=%s entry=%.6f size=%d",
                  side, entry_price, size)
        self._place_sl(entry_price)

    def activate_from_recovery(self, product_id: int, side: str,
                                entry_price: float, size: int):
        self._product_id            = product_id
        self._initial_side          = side
        self._best_price            = entry_price
        self._entry_price           = entry_price
        self._size                  = size
        self._active                = True
        self._profit_lock_active    = False
        self._locked_sl_price       = 0.0
        self._last_placed_sl_price  = 0.0
        log.debug("TSL state restored (no order placed) | side=%s", side)

    def deactivate(self):
        self._active                = False
        self._sl_order_id           = None
        self._profit_lock_active    = False
        self._locked_sl_price       = 0.0
        self._last_placed_sl_price  = 0.0
        if self._product_id:
            self.order_mgr.cancel_all_orders(self._product_id)
        log.debug("TSL deactivated")

    def update(self, current_price: float):
        if not self._active or not self._product_id:
            return

        price_improved = (
            (self._initial_side == "buy"  and current_price > self._best_price) or
            (self._initial_side == "sell" and current_price < self._best_price)
        )
        if price_improved:
            self._best_price = current_price
            log.debug("TSL ratchet | best_price=%.6f", self._best_price)

        if self._initial_side == "buy":
            trailing_sl_price = self._best_price * (1 - TRAILING_SL_PCT)
        else:
            trailing_sl_price = self._best_price * (1 + TRAILING_SL_PCT)

        upnl_pct               = self._compute_upnl_pct(current_price)
        final_sl_price         = trailing_sl_price
        profit_lock_sl_updated = False

        if upnl_pct >= PROFIT_LOCK_TRIGGER and self._entry_price > 0:
            locked_profit_pct = upnl_pct - PROFIT_LOCK_BUFFER

            if self._initial_side == "buy":
                candidate_pl_sl = self._entry_price * (1 + locked_profit_pct)
            else:
                candidate_pl_sl = self._entry_price * (1 - locked_profit_pct)

            if not self._profit_lock_active:
                self._profit_lock_active = True
                self._locked_sl_price    = candidate_pl_sl
                profit_lock_sl_updated   = True
                log.debug(
                    "Profit-lock SL activated | uPnL=%.4f%% sl_price=%.6f",
                    upnl_pct * 100, candidate_pl_sl
                )
                print(
                    f"  {_yellow('PROFIT-LOCK activated')}  "
                    f"uPnL={_white(f'{upnl_pct*100:.4f}%')}  "
                    f"locking={_white(f'{(upnl_pct - PROFIT_LOCK_BUFFER)*100:.4f}%')}  "
                    f"sl_price={_white(f'{candidate_pl_sl:.6f}')}"
                )
            else:
                if (self._initial_side == "buy" and
                        candidate_pl_sl > self._locked_sl_price):
                    self._locked_sl_price  = candidate_pl_sl
                    profit_lock_sl_updated = True
                    log.debug("Profit-lock SL ratcheted UP | new_sl=%.6f",
                              self._locked_sl_price)
                    print(
                        f"  {_yellow('PROFIT-LOCK ratchet')}  "
                        f"uPnL={_white(f'{upnl_pct*100:.4f}%')}  "
                        f"sl_price={_white(f'{self._locked_sl_price:.6f}')}"
                    )
                elif (self._initial_side == "sell" and
                      candidate_pl_sl < self._locked_sl_price):
                    self._locked_sl_price  = candidate_pl_sl
                    profit_lock_sl_updated = True
                    log.debug("Profit-lock SL ratcheted DOWN | new_sl=%.6f",
                              self._locked_sl_price)
                    print(
                        f"  {_yellow('PROFIT-LOCK ratchet')}  "
                        f"uPnL={_white(f'{upnl_pct*100:.4f}%')}  "
                        f"sl_price={_white(f'{self._locked_sl_price:.6f}')}"
                    )

            if self._initial_side == "buy":
                final_sl_price = max(trailing_sl_price, self._locked_sl_price)
            else:
                final_sl_price = min(trailing_sl_price, self._locked_sl_price)

        should_update_sl = False
        if price_improved or profit_lock_sl_updated:
            if self._last_placed_sl_price == 0.0:
                should_update_sl = True
            else:
                move_pct = abs(final_sl_price - self._last_placed_sl_price) \
                           / self._last_placed_sl_price
                if move_pct >= MIN_SL_MOVE_PCT:
                    should_update_sl = True

        if should_update_sl:
            self.order_mgr.cancel_stop_orders_only(self._product_id)
            self._place_sl_at_price(final_sl_price)

    def _compute_upnl_pct(self, current_price: float) -> float:
        if self._entry_price <= 0:
            return 0.0
        if self._initial_side == "buy":
            return (current_price - self._entry_price) / self._entry_price
        else:
            return (self._entry_price - current_price) / self._entry_price

    def _place_sl(self, reference_price: float):
        if self._initial_side == "buy":
            sl_price = reference_price * (1 - TRAILING_SL_PCT)
        else:
            sl_price = reference_price * (1 + TRAILING_SL_PCT)
        self._place_sl_at_price(sl_price)

    def _place_sl_at_price(self, sl_price: float):
        slippage = 0.002
        if self._initial_side == "buy":
            close_side  = "sell"
            limit_price = sl_price * (1 - slippage)
        else:
            close_side  = "buy"
            limit_price = sl_price * (1 + slippage)

        result = self.order_mgr.place_stop_limit_order(
            self._product_id, close_side, self._size, sl_price, limit_price
        )
        if result:
            self._sl_order_id          = result.get("id")
            self._last_placed_sl_price = sl_price
            print_sl_event(sl_price, self._sl_order_id)

    @property
    def active(self) -> bool:
        return self._active


# ─────────────────────────────────────────────
# WEBSOCKET PRICE FEED
# ─────────────────────────────────────────────
class PriceFeed:
    WS_URL = "wss://socket.india.delta.exchange"

    def __init__(self, ticker: str, product_id: int):
        self.ticker     = ticker
        self.product_id = product_id
        self._price     = 0.0
        self._lock      = threading.Lock()
        self._ws        = None
        self._running   = False

    def start(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def get_price(self) -> float:
        with self._lock:
            return self._price

    def _run(self):
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open    = self._on_open,
                    on_message = self._on_message,
                    on_error   = self._on_error,
                    on_close   = self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.warning("WebSocket crashed: %s", e)
            if self._running:
                time.sleep(5)

    def _on_open(self, ws):
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name":    "mark_price",
                        "symbols": [f"MARK:{self.ticker}"]
                    }
                ]
            },
        }
        ws.send(json.dumps(sub))
        log.debug("WS subscribed to MARK:%s", self.ticker)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if (data.get("type") == "mark_price" and
                    data.get("symbol") == f"MARK:{self.ticker}"):
                price = float(data.get("price", 0))
                if price > 0:
                    with self._lock:
                        self._price = price
        except Exception:
            pass

    def _on_error(self, ws, error):
        log.warning("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg):
        log.debug("WebSocket closed (%s %s)", code, msg)


# ─────────────────────────────────────────────
# TRADING BOT  (main orchestrator)
# ─────────────────────────────────────────────
class TradingBot:

    def __init__(self):
        self.client    = DeltaClient(API_KEY, API_SECRET)
        self.registry  = ProductRegistry(self.client)
        self.account   = Account(self.client)
        self.positions = PositionManager(self.client)
        self.orders    = OrderManager(self.client, self.registry)
        self.tsl       = TrailingStopLoss(self.orders)

        self.product_id:     Optional[int]      = None
        self.contract_value: float               = 1.0
        self.settling_asset: str                 = "USD"
        self.price_feed:     Optional[PriceFeed] = None

        self._last_trade_time:    float         = 0.0
        self._active_signal:      Optional[str] = None
        self._tick_count:         int           = 0
        self._pending_entry_side: Optional[str] = None

    # ── Restart recovery ──────────────────────────────────────────────
    def recover_state(self):
        print_section("RESTART RECOVERY")
        has_pos = self.positions.has_open_position(self.product_id)

        if not has_pos:
            open_orders = self.orders.get_open_orders(self.product_id)
            if open_orders:
                print_recovery_status(
                    "Case A — no position",
                    f"found {len(open_orders)} orphaned order(s) — cancelling"
                )
                self.orders.cancel_all_orders(self.product_id)
            else:
                print_recovery_status(
                    "Case A — no position",
                    "clean start — no orphaned orders"
                )
            return

        pos         = self.positions.get_position(self.product_id)
        size        = int(pos.get("size") or 0)
        entry_price = float(pos.get("entry_price") or 0)
        abs_size    = abs(size)
        side        = "buy" if size > 0 else "sell"

        print_recovery_status(
            f"Case B — open {'LONG' if side == 'buy' else 'SHORT'}",
            f"size={abs_size}  entry={entry_price:.6f}"
        )

        open_orders = self.orders.get_open_orders(self.product_id)
        if open_orders:
            print_recovery_status(
                "Stale orders",
                f"cancelling {len(open_orders)} order(s) from previous session"
            )
            self.orders.cancel_all_orders(self.product_id)
        else:
            print_recovery_status("Stale orders", "none found")

        self._active_signal   = "long" if side == "buy" else "short"
        self._last_trade_time = time.time()

        if entry_price > 0:
            print_recovery_status(
                "Trailing SL",
                f"placing fresh SL at entry {entry_price:.6f} "
                f"± {TRAILING_SL_PCT*100:.3f}%"
            )
            self.tsl.activate(self.product_id, side, entry_price, abs_size)
        else:
            print_recovery_status(
                "Trailing SL",
                "entry_price=0 — SL deferred until first price tick"
            )
            self.tsl.activate_from_recovery(
                self.product_id, side, entry_price, abs_size
            )

        # FIX 8: do NOT set _pending_entry_side when a position already exists
        self._pending_entry_side = None

        print_recovery_status(
            "Signal restored",
            f"_active_signal='{self._active_signal}'  "
            f"_pending_entry_side='None' (position already open)"
        )

    # ── Initialise ────────────────────────────────────────────────────
    def initialise(self) -> bool:
        print_banner()

        prod = self.registry.get_product(DELTA_TICKER)
        if not prod:
            print_error(f"Cannot find product for ticker {DELTA_TICKER}")
            return False

        self.product_id     = prod["id"]
        self.contract_value = float(prod.get("contract_value", 1.0))
        self.settling_asset = self.registry.get_settling_asset(DELTA_TICKER)

        print_config_table({
            "Ticker (Delta)":         DELTA_TICKER,
            "Symbol (Binance)":       SYMBOL_CCXT,
            "Timeframe":              TIMEFRAME,
            "Product ID":             self.product_id,
            "Contract value":         f"{self.contract_value} "
                                      f"{prod.get('contract_unit_currency','')}",
            "Margin asset":           self.settling_asset,
            "Leverage":               f"{LEVERAGE}x",
            "Risk per trade":         f"{RISK_PCT*100}%",
            "Trailing SL":            f"{TRAILING_SL_PCT*100}%",
            "Min SL move":            f"{MIN_SL_MOVE_PCT*100}%",
            "Profit-lock trigger":    f"{PROFIT_LOCK_TRIGGER*100}% uPnL",
            "Profit-lock buffer":     f"{PROFIT_LOCK_BUFFER*100}%",
            "Flip confirmation":      f"{FLIP_CONFIRMATION_CANDLES} candles",
            "ADX period":             ADX_PERIOD,
            "ADX threshold":          ADX_THRESHOLD,
            "ADX rising lookback":    ADX_RISING_LOOKBACK,
            "Loop interval":          f"{LOOP_INTERVAL}s",
            "Trade cooldown":         f"{TRADE_COOLDOWN}s",
            "Log file":               LOG_FILE,
        })

        ok = self.account.set_leverage(self.product_id, LEVERAGE)
        if ok:
            print(f"  {_green('OK')}  Leverage set to {LEVERAGE}x")
        else:
            print_warning(f"Leverage set call returned non-success "
                          f"(may already be {LEVERAGE}x — continuing)")

        self.price_feed = PriceFeed(DELTA_TICKER, self.product_id)
        self.price_feed.start()
        print(f"  {_green('OK')}  WebSocket price feed started "
              f"(MARK:{DELTA_TICKER})")
        time.sleep(2)

        bal = self.account.get_balance(self.settling_asset)
        print_balance(bal, self.settling_asset)

        if bal <= 0.0:
            print_error(f"Balance is zero for {self.settling_asset} — "
                        f"deposit funds and restart.")
            return False
        if bal < 1.0:
            print_warning(f"Low balance ({bal:.4f} {self.settling_asset}) — "
                          f"bot will continue but may not be able to trade.")

        self.recover_state()

        print_section("BOT RUNNING")
        print(f"  Loop interval : {_white(str(LOOP_INTERVAL)+'s')}")
        print(f"  Press          {_bold('Ctrl+C')} to stop")
        print()
        return True

    # ── Get current price ─────────────────────────────────────────────
    def _get_price(self) -> float:
        price = self.price_feed.get_price() if self.price_feed else 0.0
        if price <= 0:
            resp   = self.client.get(f"/v2/tickers/{DELTA_TICKER}")
            result = self.client.result(resp)
            if result:
                price = float(result.get("mark_price",
                                         result.get("close", 0)))
        return price

    # ── Entry logic ───────────────────────────────────────────────────
    def _enter_trade(self, side: str, price: float,
                     df: pd.DataFrame) -> bool:
        """
        Attempt to enter a trade.

        FIX 2: Returns True if the order was placed successfully, False
        otherwise. On ADX rejection, _pending_entry_side is cleared so
        the retry path does not fire every 5 seconds into a weak trend.

        FIX 3: ADX must also be rising (adx_is_rising()) to confirm that
        momentum is building, not fading.
        """
        now = time.time()
        remaining = TRADE_COOLDOWN - (now - self._last_trade_time)
        if remaining > 0:
            print_cooldown(remaining)
            return False

        if self.positions.has_open_position(self.product_id):
            print(f"  {_dim('Entry skipped — position already open')}")
            return False

        adx_df   = compute_adx(df, period=ADX_PERIOD)
        adx_val  = get_adx_value(adx_df)
        rising   = adx_is_rising(adx_df, lookback=ADX_RISING_LOOKBACK)

        if adx_val < ADX_THRESHOLD or not rising:
            print_adx_rejection(adx_val, side, rising)
            # FIX 2: clear pending so we don't spam retries on weak/falling ADX
            self._pending_entry_side = None
            return False

        balance   = self.account.get_balance(self.settling_asset)
        contracts = self.orders.calculate_contracts(
            balance, price, self.contract_value
        )

        result = self.orders.place_market_order(
            self.product_id, side, contracts
        )
        if not result:
            print_error("Entry market order failed — check logs")
            return False

        self._last_trade_time = time.time()
        self._active_signal   = "long" if side == "buy" else "short"
        self.tsl.activate(self.product_id, side, price, contracts)
        print_trade_event("ENTRY", side, contracts, price,
                          f"adx={adx_val:.2f}")
        return True

    # ── Exit logic ────────────────────────────────────────────────────
    def _exit_trade(self, reason: str):
        pos_side = self.positions.get_side(self.product_id)
        if not pos_side:
            print(f"  {_dim('Exit skipped — no open position')}")
            return

        pos  = self.positions.get_position(self.product_id)
        size = abs(int(pos.get("size") or 0)) if pos else 0
        if size == 0:
            return

        close_side = "sell" if pos_side == "buy" else "buy"

        # FIX 7: place the exit market order BEFORE deactivating the TSL.
        # If the order fails, the position remains protected by the existing SL.
        result = self.orders.place_market_order(
            self.product_id, close_side, size, reduce_only=True
        )
        if result:
            self.tsl.deactivate()
            self._active_signal   = None
            self._last_trade_time = time.time()
            price = self._get_price()
            print_trade_event("EXIT", close_side, size, price, reason)
        else:
            print_error(
                "Exit market order failed — position still open, SL remains active"
            )

    # ── Main loop iteration ───────────────────────────────────────────
    def _tick(self):
        self._tick_count += 1
        price = self._get_price()
        if price <= 0:
            print_warning("Could not obtain current price — skipping tick")
            return

        ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        ts = ist_time.strftime("%Y-%m-%d %H:%M:%S IST")

        print_tick_header(self._tick_count, price, ts)

        if self.tsl.active:
            self.tsl.update(price)

        df = fetch_candles()
        min_candles = max(ATR_PERIOD, ADX_PERIOD) + FLIP_CONFIRMATION_CANDLES + 5
        if df is None or len(df) < min_candles:
            print_warning(f"Insufficient candle data "
                          f"(need {min_candles}, got "
                          f"{len(df) if df is not None else 0})")
            return

        df   = compute_supertrend(df)
        flip = detect_flip(df)  # uses FLIP_CONFIRMATION_CANDLES

        adx_df   = compute_adx(df, period=ADX_PERIOD)
        adx_val  = get_adx_value(adx_df)
        rising   = adx_is_rising(adx_df, lookback=ADX_RISING_LOOKBACK)

        st_dir = int(df["supertrend_direction"].iloc[-1])
        print_indicator_row(st_dir, adx_val, flip, rising)

        pos      = self.positions.get_position(self.product_id)
        pos_side = self.positions.get_side(self.product_id)
        pos_size = abs(int(pos.get("size") or 0)) if pos else 0
        entry    = float(pos.get("entry_price") or 0) if pos else 0.0
        pnl_pct  = self.positions.get_unrealized_pnl_pct(
            self.product_id, price
        )
        print_position_status(pos_side, pos_size, entry, pnl_pct)

        has_position  = pos_size > 0
        intended_side = "buy" if st_dir == 1 else "sell"

        # Reset stale _active_signal if position closed externally and
        # the trend has already reversed
        if not has_position and self._active_signal is not None:
            active_side = "buy" if self._active_signal == "long" else "sell"
            if active_side != intended_side:
                log.debug(
                    "Signal reset: active=%s intended=%s (trend reversed)",
                    self._active_signal, intended_side
                )
                self._active_signal      = None
                self._pending_entry_side = None

        # ── Flip handling ──────────────────────────────────────────────
        # detect_flip() now requires FLIP_CONFIRMATION_CANDLES consecutive
        # candles in the new direction, so single-wick fakeouts are ignored.
        if flip == "bullish_flip":
            self._pending_entry_side = "buy"
            if has_position and self._active_signal == "short":
                self._exit_trade("supertrend_flip")
                has_position = self.positions.has_open_position(
                    self.product_id
                )
            if not has_position:
                self._enter_trade("buy", price, df)

        elif flip == "bearish_flip":
            self._pending_entry_side = "sell"
            if has_position and self._active_signal == "long":
                self._exit_trade("supertrend_flip")
                has_position = self.positions.has_open_position(
                    self.product_id
                )
            if not has_position:
                self._enter_trade("sell", price, df)

        else:
            trend = _green("BULLISH") if st_dir == 1 else _red("BEARISH")

            if not has_position:
                if self._pending_entry_side == intended_side:
                    print(
                        f"  {_yellow('RETRY ENTRY')}  "
                        f"trend={trend}  "
                        f"side={_white(intended_side.upper())}  "
                        f"{_dim('(missed flip — retrying)')}"
                    )
                    self._enter_trade(intended_side, price, df)
                    # Note: _enter_trade clears _pending_entry_side on ADX
                    # rejection, so a bad signal won't retry indefinitely
                else:
                    if (self._pending_entry_side is not None and
                            self._pending_entry_side != intended_side):
                        log.debug(
                            "Pending entry side '%s' cleared — "
                            "trend reversed to '%s'",
                            self._pending_entry_side, intended_side
                        )
                        self._pending_entry_side = None
                    print(
                        f"  {_dim('No flip — holding')}  trend={trend}  "
                        f"signal={_dim(self._active_signal or 'none')}"
                    )
            else:
                print(
                    f"  {_dim('No flip — holding')}  trend={trend}  "
                    f"signal={_dim(self._active_signal or 'none')}"
                )

        # Clear pending entry once position is confirmed open
        if self.positions.has_open_position(self.product_id):
            self._pending_entry_side = None

    # ── Run forever ───────────────────────────────────────────────────
    def run(self):
        if not self.initialise():
            print_error("Initialisation failed — exiting")
            sys.exit(1)

        while True:
            try:
                self._tick()
            except KeyboardInterrupt:
                print()
                print(_bold(_yellow("  Keyboard interrupt — shutting down...")))
                break
            except Exception as e:
                print_error(f"Unhandled exception: {e}")
                log.error("Unhandled exception in tick: %s\n%s",
                          e, traceback.format_exc())
                time.sleep(5)

            time.sleep(LOOP_INTERVAL)

        if self.price_feed:
            self.price_feed.stop()
        print()
        print(_bold(_cyan(SEP_THICK)))
        print(_bold(_cyan("  Bot stopped.")))
        print(_bold(_cyan(SEP_THICK)))
        print()


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
