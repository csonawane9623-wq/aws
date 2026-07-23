import time
import hmac
import hashlib
import requests
import logging
import uuid
import math
from urllib.parse import urlencode
import os

from dotenv import load_dotenv
load_dotenv()

# ============ CONFIG ============
API_KEY    = os.getenv("API_KEY3")
API_SECRET = os.getenv("API_SECRET3")
BASE_URL   = "https://fapi.binance.com"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

ETH_THRESHOLD  = 0.0001
ETC_THRESHOLD  = 0.0004
TARGET_PNL_PCT = 2
CAPITAL_PCT    = 0.10
LEVERAGE       = 50
MIN_NOTIONAL   = 20.0

RETRY       = 5
RECV_WINDOW = 5000
ETH_SYMBOL  = "ETHUSDT"
ETC_SYMBOL  = "ETCUSDT"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ============ TELEGRAM ============
_last_tg_time = 0

def send_telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def send_telegram_safe(msg, cooldown=5):
    global _last_tg_time
    if time.time() - _last_tg_time > cooldown:
        send_telegram(msg)
        _last_tg_time = time.time()

# ============ BINANCE CLIENT ============
class BinanceClient:
    _NO_RETRY = {-1022, -1100, -1102, -2010, -4164}

    def __init__(self):
        self.key    = API_KEY
        self.secret = API_SECRET.encode()
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.key,
                                     "Content-Type": "application/x-www-form-urlencoded"})
        self._hedge_mode = None   # cached after first call

    def _ts(self):
        return int(time.time() * 1000)

    def _sign(self, params):
        p = dict(params)   # ALWAYS copy — never mutate caller's dict
        p["signature"] = hmac.new(self.secret, urlencode(p).encode(), hashlib.sha256).hexdigest()
        return p

    def _get(self, path, params=None, auth=False):
        url, params = BASE_URL + path, params or {}
        for i in range(RETRY):
            try:
                p = dict(params)
                if auth:
                    p["timestamp"] = self._ts(); p["recvWindow"] = RECV_WINDOW
                    p = self._sign(p)
                r = self.session.get(url, params=p, timeout=10)
                if r.status_code == 200: return r.json()
                logging.error(f"GET {path}: {r.status_code} {r.text}")
            except Exception as e:
                logging.error(f"GET exception: {e}")
            time.sleep(2 ** i)
        send_telegram_safe(f"API GET failed: {path}")
        return None

    def _post(self, path, params=None):
        url, params = BASE_URL + path, params or {}
        for i in range(RETRY):
            try:
                p = dict(params)
                p["timestamp"] = self._ts(); p["recvWindow"] = RECV_WINDOW
                p = self._sign(p)
                r = self.session.post(url, data=p, timeout=10)
                if r.status_code == 200: return r.json()
                logging.error(f"POST {path}: {r.status_code} {r.text}")
                try:
                    code = r.json().get("code")
                    if code in self._NO_RETRY:
                        logging.error(f"Non-retriable {code}, stopping retries")
                        return None
                except Exception: pass
            except Exception as e:
                logging.error(f"POST exception: {e}")
            time.sleep(2 ** i)
        send_telegram_safe(f"API POST failed: {path}")
        return None

    # ── Market data ──
    def get_premium_index(self, symbol):
        return self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
    def get_mark_price(self, symbol):
        d = self.get_premium_index(symbol); return float(d["markPrice"]) if d else None
    def get_funding_rate(self, symbol):
        d = self.get_premium_index(symbol); return float(d["lastFundingRate"]) if d else None
    def get_exchange_info(self):
        return self._get("/fapi/v1/exchangeInfo")

    # ── Account ──
    def get_balance(self):
        data = self._get("/fapi/v2/balance", auth=True)
        if data:
            for b in data:
                if b["asset"] == "USDT": return float(b["availableBalance"])
        return 0.0

    def get_positions(self):
        data = self._get("/fapi/v2/positionRisk", auth=True)
        return [p for p in (data or []) if float(p["positionAmt"]) != 0]

    def is_hedge_mode(self):
        """Detect dual/hedge position mode once, then cache it."""
        if self._hedge_mode is None:
            d = self._get("/fapi/v1/positionSide/dual", auth=True)
            self._hedge_mode = bool(d and d.get("dualSidePosition", False))
            logging.info(f"Position mode: {'HEDGE' if self._hedge_mode else 'ONE-WAY'}")
        return self._hedge_mode

    # ── Config ──
    def set_leverage(self, symbol, leverage):
        return self._post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
    def set_margin_type(self, symbol, margin_type="ISOLATED"):
        return self._post("/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})

    # ── Orders ──
    def place_market_order(self, symbol, side, quantity, position_side=None, client_order_id=None):
        params = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": quantity}
        if position_side:    params["positionSide"]     = position_side
        if client_order_id:  params["newClientOrderId"] = client_order_id[:36]
        return self._post("/fapi/v1/order", params)

    def close_position(self, symbol, position_amt):
        """
        THE KEY FIX — two completely different close methods depending on account mode:

        ONE-WAY mode → reduceOnly=true
          • Bypasses the $20 minimum notional check entirely (Binance's own exception).
          • Using positionSide here would raise -4061 (invalid for one-way mode).

        HEDGE mode   → positionSide=LONG or SHORT  (NO reduceOnly)
          • reduceOnly is FORBIDDEN in hedge mode and returns -4164 even for closes.
          • Must specify which side to close via positionSide.
        """
        side = "SELL" if position_amt > 0 else "BUY"
        qty  = abs(position_amt)

        if self.is_hedge_mode():
            pos_side = "LONG" if position_amt > 0 else "SHORT"
            params = {"symbol": symbol, "side": side,
                      "positionSide": pos_side, "type": "MARKET", "quantity": qty}
        else:
            params = {"symbol": symbol, "side": side,
                      "type": "MARKET", "quantity": qty, "reduceOnly": "true"}

        logging.info(f"close_position {symbol} amt={position_amt} hedge={self._hedge_mode}")
        return self._post("/fapi/v1/order", params)


# ============ HELPERS ============
class SymbolInfo:
    def __init__(self, qty_precision, min_qty, step_size):
        self.qty_precision = qty_precision
        self.min_qty       = min_qty
        self.step_size     = step_size

def floor_step(qty, step):
    return math.floor(qty / step) * step


# ============ BOT ============
class FundingArbitrageBot:
    def __init__(self):
        self.client          = BinanceClient()
        self.sym_info        = {}
        self.leverage_set    = set()
        self.last_pnl_alert  = 0.0
        self.last_trade_time = 0.0
        self._load_symbol_info()

    def _gen_id(self): return uuid.uuid4().hex[:32]

    def _load_symbol_info(self):
        data = self.client.get_exchange_info()
        if not data: raise RuntimeError("Failed to load exchange info")
        for s in data["symbols"]:
            if s["symbol"] not in (ETH_SYMBOL, ETC_SYMBOL): continue
            min_qty = step = 1.0
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    min_qty = float(f["minQty"]); step = float(f["stepSize"])
            self.sym_info[s["symbol"]] = SymbolInfo(s["quantityPrecision"], min_qty, step)
        logging.info(f"Symbol info loaded: {list(self.sym_info)}")

    def _ensure_leverage(self, symbol):
        if symbol in self.leverage_set: return
        self.client.set_margin_type(symbol, "ISOLATED")
        if self.client.set_leverage(symbol, LEVERAGE):
            self.leverage_set.add(symbol)
            send_telegram_safe(f"Leverage {LEVERAGE}x set: {symbol}")

    def _compute_sizes(self):
        balance = self.client.get_balance()
        if balance <= 0: logging.warning("Zero balance"); return 0, 0

        target_each     = max(balance * CAPITAL_PCT * LEVERAGE / 2, MIN_NOTIONAL * 1.05)
        required_margin = (target_each * 2) / LEVERAGE
        if balance < required_margin:
            send_telegram_safe(f"Insufficient balance ${balance:.2f}, need ${required_margin:.2f}")
            return 0, 0

        eth_price = self.client.get_mark_price(ETH_SYMBOL)
        etc_price = self.client.get_mark_price(ETC_SYMBOL)
        if not eth_price or not etc_price: return 0, 0

        ei = self.sym_info.get(ETH_SYMBOL); ci = self.sym_info.get(ETC_SYMBOL)
        if not ei or not ci: return 0, 0

        eth_qty = max(floor_step(target_each / eth_price, ei.step_size), ei.min_qty)
        etc_qty = max(floor_step((eth_qty * eth_price) / etc_price, ci.step_size), ci.min_qty)

        eth_not = eth_qty * eth_price; etc_not = etc_qty * etc_price

        if eth_not < MIN_NOTIONAL:
            send_telegram_safe(f"ETH notional ${eth_not:.2f} < min ${MIN_NOTIONAL}"); return 0, 0
        if etc_not < MIN_NOTIONAL:
            send_telegram_safe(f"ETC notional ${etc_not:.2f} < min ${MIN_NOTIONAL}"); return 0, 0

        logging.info(f"ETH {eth_qty} (${eth_not:.2f})  ETC {etc_qty} (${etc_not:.2f})")
        return eth_qty, etc_qty

    def _place_trade(self):
        eth_qty, etc_qty = self._compute_sizes()
        if not eth_qty or not etc_qty: return
        self._ensure_leverage(ETH_SYMBOL); self._ensure_leverage(ETC_SYMBOL)
        hedge = self.client.is_hedge_mode()

        eth_order = self.client.place_market_order(
            ETH_SYMBOL, "SELL", eth_qty,
            position_side="SHORT" if hedge else None,
            client_order_id=self._gen_id())
        if not eth_order:
            send_telegram("ETH short FAILED — no trade placed"); return

        etc_order = self.client.place_market_order(
            ETC_SYMBOL, "BUY", etc_qty,
            position_side="LONG" if hedge else None,
            client_order_id=self._gen_id())
        if not etc_order:
            logging.error("ETC long failed — rolling back ETH short")
            self.client.close_position(ETH_SYMBOL, -eth_qty)
            send_telegram("ROLLBACK: ETC long failed, ETH short reversed"); return

        send_telegram(f"TRADE EXECUTED\nSHORT {ETH_SYMBOL}: {eth_qty}\n"
                      f"LONG {ETC_SYMBOL}: {etc_qty}\n"
                      f"Balance: ${self.client.get_balance():.2f} USDT")
        self.last_trade_time = time.time()

    def _close_all(self):
        positions = self.client.get_positions()
        if not positions: logging.info("No open positions"); return
        for p in positions:
            symbol = p["symbol"]; amt = float(p["positionAmt"])
            if amt == 0: continue
            ok = self.client.close_position(symbol, amt)
            if ok: logging.info(f"Closed {symbol} amt={amt}")
            else:  send_telegram_safe(f"Close FAILED: {symbol} amt={amt}")
        send_telegram("ALL POSITIONS CLOSED")

    def _pnl_and_margin(self, positions):
        pnl = sum(float(p.get("unRealizedProfit", 0)) for p in positions)
        margin = 0.0
        for p in positions:
            # Binance isolated margin is in 'isolatedWallet', not 'isolatedMargin'
            # 'isolatedMargin' = wallet + unrealized PnL (inflated), not true margin
            isolated_wallet = float(p.get("isolatedWallet") or 0)
            initial_margin  = float(p.get("initialMargin") or 0)
            notional        = abs(float(p.get("notional") or 0))
            if isolated_wallet > 0:
                margin += isolated_wallet
            elif initial_margin > 0:
                margin += initial_margin
            elif notional > 0:
            # Fallback: derive margin from notional and leverage
                margin += notional / LEVERAGE

        return pnl, margin

    def run(self):
        send_telegram(f"BOT STARTED | ETH<={ETH_THRESHOLD} ETC>={ETC_THRESHOLD} "
                      f"| {LEVERAGE}x | {CAPITAL_PCT*100:.0f}% capital | min ${MIN_NOTIONAL}")
        while True:
            try:
                eth_rate = self.client.get_funding_rate(ETH_SYMBOL)
                etc_rate = self.client.get_funding_rate(ETC_SYMBOL)
                if eth_rate is None or etc_rate is None:
                    time.sleep(3); continue

                logging.info(f"Funding -> {ETH_SYMBOL}: {eth_rate:.6f} | {ETC_SYMBOL}: {etc_rate:.6f}")

                positions   = self.client.get_positions()
                no_pos      = len(positions) == 0
                cooldown_ok = time.time() - self.last_trade_time > 10

                if eth_rate <= ETH_THRESHOLD and etc_rate >= ETC_THRESHOLD and no_pos and cooldown_ok:
                    send_telegram(f"ENTRY SIGNAL ETH:{eth_rate:.6f} ETC:{etc_rate:.6f}")
                    self._place_trade()

                positions = self.client.get_positions()
                if len(positions) == 2:
                    pnl, margin = self._pnl_and_margin(positions)
                    target = margin * TARGET_PNL_PCT
                    if time.time() - self.last_pnl_alert > 600:
                        send_telegram_safe(f"PnL: {pnl:.4f} | Margin: {margin:.4f} "
                                           f"| Target: {target:.4f} | {pnl/target*100 if target else 0:.1f}%")
                        self.last_pnl_alert = time.time()
                    if pnl >= target:
                        send_telegram(f"TARGET HIT PnL:{pnl:.4f} Target:{target:.4f}")
                        self._close_all()

            except Exception as e:
                logging.error(f"Loop error: {e}"); send_telegram_safe(f"BOT ERROR: {e}")
            time.sleep(3)

if __name__ == "__main__":
    while True:
        try:
            FundingArbitrageBot().run()
        except Exception as e:
            logging.error(f"CRASH: {e}")
            send_telegram_safe(f"BOT CRASHED: {e} - Restarting in 5s")
            time.sleep(5)
