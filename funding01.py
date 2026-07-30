import hashlib
import hmac
import requests
import time
import json
import math
import threading
import websocket
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_KEY    = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_URL             = 'https://api.india.delta.exchange'
WS_URL               = 'wss://socket.india.delta.exchange'

LEVERAGE             = 20
POSITION_PCT         = 0.20      # 10% of available balance
ENTRY_BEFORE_S       = 15        # Enter 15 seconds before funding
CLOSE_AFTER_MS       = 100       # Close 100ms after funding
SCAN_INTERVAL_S      = 300       # Re-scan every 5 minutes
WINDOW_S             = 14400       # Only act if funding <= 5 min away
MIN_FUNDING_RATE_ABS = 0.1000    # Minimum absolute funding rate % to trade

# ---- STOP LOSS CONFIG -------------------------------------------------------
STOP_LOSS_PCT    = 0.05          # % adverse move from live entry price
STOP_LOSS_ENABLED = True         # master switch
MIN_STOP_TICKS   = 2             # minimum ticks away from market price for SL
# -----------------------------------------------------------------------------

IST = pytz.timezone('Asia/Kolkata')

# =============================================================================
# SESSION
# =============================================================================
session = requests.Session()
session.headers.update({
    'User-Agent': 'rest-client',
    'Content-Type': 'application/json'
})

# =============================================================================
# AUTHENTICATION
# =============================================================================
def generate_signature(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

def auth_headers(method: str, path: str, payload: str = '', query: str = '') -> dict:
    ts  = str(int(time.time()))
    sig = generate_signature(API_SECRET, method + ts + path + query + payload)
    return {
        'api-key':      API_KEY,
        'timestamp':    ts,
        'signature':    sig,
        'User-Agent':   'rest-client',
        'Content-Type': 'application/json'
    }

# =============================================================================
# HELPERS
# =============================================================================
def now_ist() -> datetime:
    return datetime.now(IST)

def ts_to_ist(ts_micro: int) -> datetime:
    return datetime.fromtimestamp(ts_micro / 1_000_000, tz=IST)

def fmt_ist(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%d %H:%M:%S IST')

def separator(char='=', width=110):
    print(char * width)

# =============================================================================
# TICK SIZE UTILITIES
# Handles very small prices like 1000SATSUSD (0.000001 tick size)
# =============================================================================
def tick_decimals(tick_size: float) -> int:
    """Returns the number of decimal places needed for a given tick size."""
    if tick_size >= 1:
        return 0
    return max(0, -int(math.floor(math.log10(tick_size))))

def round_to_tick(price: float, tick_size: float) -> float:
    """Rounds a price to the nearest tick size increment."""
    decimals = tick_decimals(tick_size)
    return round(round(price / tick_size) * tick_size, decimals)

def format_price(price: float, tick_size: float) -> str:
    """Formats a price as a string with the correct decimal precision for the API."""
    decimals = tick_decimals(tick_size)
    return f"{price:.{decimals}f}"

# =============================================================================
# STOP PRICE CALCULATION
# Computes a valid stop price that:
#   - Is on the correct side of the market
#   - Is rounded to the contract's tick size
#   - Is at least MIN_STOP_TICKS away from the current market price
#   - Is never zero or negative
# =============================================================================
def compute_stop_price(side: str, live_price: float, tick_size: float) -> float | None:
    """
    side       : entry side ('buy' or 'sell')
    live_price : current mark price
    tick_size  : contract tick size

    Returns the stop trigger price, or None if a valid price cannot be computed.

    For a SHORT (sell) entry -> stop is a BUY -> price must be ABOVE market
    For a LONG  (buy)  entry -> stop is a SELL -> price must be BELOW market
    """
    if side == 'buy':
        raw_stop = live_price * (1 - STOP_LOSS_PCT / 100)
    else:
        raw_stop = live_price * (1 + STOP_LOSS_PCT / 100)

    stop_price = round_to_tick(raw_stop, tick_size)

    # Ensure stop is never zero or negative
    if stop_price <= 0:
        print(
            f"[WARN] Computed stop price is zero or negative "
            f"(raw={raw_stop:.10f}, tick={tick_size}). Stop-loss disabled for this trade."
        )
        return None

    # Enforce minimum tick distance from market price
    min_distance = MIN_STOP_TICKS * tick_size
    if side == 'buy':
        # SELL stop must be below market by at least MIN_STOP_TICKS
        if live_price - stop_price < min_distance:
            stop_price = round_to_tick(live_price - min_distance, tick_size)
            print(
                f"[INFO] Stop price adjusted to maintain minimum {MIN_STOP_TICKS}-tick "
                f"distance: {format_price(stop_price, tick_size)}"
            )
    else:
        # BUY stop must be above market by at least MIN_STOP_TICKS
        if stop_price - live_price < min_distance:
            stop_price = round_to_tick(live_price + min_distance, tick_size)
            print(
                f"[INFO] Stop price adjusted to maintain minimum {MIN_STOP_TICKS}-tick "
                f"distance: {format_price(stop_price, tick_size)}"
            )

    # Final zero/negative check after adjustment
    if stop_price <= 0:
        print(f"[WARN] Stop price still invalid after adjustment. Stop-loss disabled for this trade.")
        return None

    return stop_price

# =============================================================================
# NEXT FUNDING TIME VIA WEBSOCKET
# Delta Exchange WS uses abbreviated field names:
#   nfr -> next_funding_realization (microseconds UTC epoch)
#   fr  -> funding_rate
#   fi  -> funding_interval (seconds)
#   sy  -> symbol
#   ts  -> timestamp
# =============================================================================
def get_next_funding_time_ws(symbol: str, timeout: int = 8) -> int | None:
    """
    Subscribes to the funding_rate WebSocket channel for the given symbol
    and returns next_funding_realization in microseconds (UTC epoch).
    Returns None if not received within timeout seconds.
    """
    result = {'value': None}
    done   = threading.Event()

    def on_message(ws_app, message):
        try:
            data = json.loads(message)
            if data.get('type') == 'funding_rate' and 'nfr' in data:
                result['value'] = data['nfr']
                done.set()
                ws_app.close()
        except Exception:
            pass

    def on_open(ws_app):
        ws_app.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name":    "funding_rate",
                    "symbols": [symbol]
                }]
            }
        }))

    def on_error(ws_app, error):
        done.set()

    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error
    )
    t = threading.Thread(target=ws_app.run_forever, daemon=True)
    t.start()
    done.wait(timeout=timeout)
    ws_app.close()
    return result['value']

# =============================================================================
# FALLBACK: epoch-aligned next funding time
# =============================================================================
def get_next_funding_time_fallback(interval_s: int) -> int:
    """
    Calculates the next epoch-aligned funding time in microseconds.
    Example: interval=3600 -> next top-of-hour in UTC epoch microseconds.
    """
    now_s  = int(time.time())
    next_s = ((now_s // interval_s) + 1) * interval_s
    return next_s * 1_000_000

# =============================================================================
# REST API CALLS
# =============================================================================
def get_all_perpetuals() -> list:
    try:
        r = session.get(
            f'{BASE_URL}/v2/tickers',
            params={'contract_types': 'perpetual_futures'},
            timeout=10
        )
        if r.status_code == 429:
            print("[WARN] Rate limit hit on get_all_perpetuals. Sleeping 10s.")
            time.sleep(10)
            return []
        if r.ok and r.json().get('success'):
            return r.json()['result']
    except Exception as e:
        print(f"[ERROR] get_all_perpetuals: {e}")
    return []

def get_product_details(symbol: str) -> dict | None:
    try:
        r = session.get(f'{BASE_URL}/v2/products/{symbol}', timeout=10)
        if r.status_code == 429:
            print("[WARN] Rate limit hit on get_product_details. Sleeping 10s.")
            time.sleep(10)
            return None
        if r.ok and r.json().get('success'):
            return r.json()['result']
    except Exception as e:
        print(f"[ERROR] get_product_details({symbol}): {e}")
    return None

def get_wallet_balance() -> float:
    path = '/v2/wallet/balances'
    try:
        r = session.get(
            f'{BASE_URL}{path}',
            headers=auth_headers('GET', path),
            timeout=10
        )
        if r.status_code == 429:
            print("[WARN] Rate limit hit on get_wallet_balance. Sleeping 10s.")
            time.sleep(10)
            return 0.0
        if r.ok and r.json().get('success'):
            for b in r.json()['result']:
                if b['asset_symbol'] == 'USD':
                    return float(b['available_balance'])
            # If USD not found, log available asset symbols to help diagnose
            symbols = [b['asset_symbol'] for b in r.json()['result']]
            print(f"[WARN] 'USD' not found in wallet. Available asset symbols: {symbols}")
        else:
            print(f"[ERROR] get_wallet_balance API error: {r.text[:300]}")
    except Exception as e:
        print(f"[ERROR] get_wallet_balance: {e}")
    return 0.0

def set_leverage(product_id: int, leverage: int) -> bool:
    path    = f'/v2/products/{product_id}/orders/leverage'
    payload = json.dumps({"leverage": str(leverage)})
    try:
        r = session.post(
            f'{BASE_URL}{path}',
            data=payload,
            headers=auth_headers('POST', path, payload),
            timeout=10
        )
        return r.ok and r.json().get('success')
    except Exception as e:
        print(f"[ERROR] set_leverage: {e}")
    return False

def place_market_order(product_id: int, side: str, size: int) -> dict | None:
    path    = '/v2/orders'
    payload = json.dumps({
        "product_id": product_id,
        "side":       side,
        "size":       size,
        "order_type": "market_order"
    })
    try:
        r = session.post(
            f'{BASE_URL}{path}',
            data=payload,
            headers=auth_headers('POST', path, payload),
            timeout=10
        )
        if r.status_code == 429:
            print("[WARN] Rate limit hit on place_market_order. Sleeping 10s.")
            time.sleep(10)
            return None
        if r.ok and r.json().get('success'):
            return r.json()['result']
        else:
            print(f"[ERROR] place_market_order response: {r.text}")
    except Exception as e:
        print(f"[ERROR] place_market_order: {e}")
    return None

# =============================================================================
# STOP LOSS ORDER
# - reduce_only is a string enum ("true"/"false") on Delta Exchange API
# - stop_price is formatted using tick-aware precision (not hardcoded :.4f)
# - tick_size is passed in to derive correct decimal formatting
# - size should be the actual filled size from the entry order
# =============================================================================
def place_stop_order(
    product_id: int,
    side:       str,
    size:       int,
    stop_price: float,
    tick_size:  float
) -> dict | None:
    """
    Places a reduce-only stop-market order.

    side       : opposite of entry side ('buy' if entry was 'sell', vice versa)
    size       : actual filled size of the entry order
    stop_price : trigger price (must be on correct side of market)
    tick_size  : used to format stop_price with correct decimal precision
    """
    price_str = format_price(stop_price, tick_size)
    path      = '/v2/orders'
    payload   = json.dumps({
        "product_id":      product_id,
        "side":            side,
        "size":            size,
        "order_type":      "market_order",
        "stop_order_type": "stop_loss_order",
        "stop_price":      price_str,
        "reduce_only":     "true"           # string enum, not boolean
    })
    try:
        r = session.post(
            f'{BASE_URL}{path}',
            data=payload,
            headers=auth_headers('POST', path, payload),
            timeout=10
        )
        if r.status_code == 429:
            print("[WARN] Rate limit hit on place_stop_order. Sleeping 10s.")
            time.sleep(10)
            return None
        if r.ok and r.json().get('success'):
            return r.json()['result']
        else:
            print(f"[ERROR] place_stop_order response: {r.text}")
    except Exception as e:
        print(f"[ERROR] place_stop_order: {e}")
    return None

def cancel_order(product_id: int, order_id) -> bool:
    """Cancels a resting order (used to clean up the SL once we close normally)."""
    if not order_id:
        return True
    path    = '/v2/orders'
    payload = json.dumps({"id": order_id, "product_id": product_id})
    try:
        r = session.delete(
            f'{BASE_URL}{path}',
            data=payload,
            headers=auth_headers('DELETE', path, payload),
            timeout=10
        )
        if r.ok and r.json().get('success'):
            return True
        print(f"[WARN] cancel_order response: {r.text}")
    except Exception as e:
        print(f"[ERROR] cancel_order: {e}")
    return False

def get_position(product_id: int) -> dict | None:
    path  = '/v2/positions'
    query = f'?product_id={product_id}'
    try:
        r = session.get(
            f'{BASE_URL}{path}{query}',
            headers=auth_headers('GET', path, '', query),
            timeout=10
        )
        if r.status_code == 429:
            print("[WARN] Rate limit hit on get_position. Sleeping 10s.")
            time.sleep(10)
            return None
        if r.ok and r.json().get('success'):
            res = r.json()['result']
            return res[0] if isinstance(res, list) and res else res
    except Exception as e:
        print(f"[ERROR] get_position: {e}")
    return None

def close_position(product_id: int, symbol: str) -> bool:
    """
    Closes an open position. Retries once if the first close attempt
    results in a residual position (partial fill guard).
    """
    for attempt in range(1, 3):
        pos = get_position(product_id)
        if not pos:
            print(f"[WARN] No position found for {symbol} (attempt {attempt})")
            return False
        size = abs(int(pos.get('size', 0)))
        if size == 0:
            print(f"[INFO] Position fully closed for {symbol}")
            return True
        close_side = 'sell' if int(pos['size']) > 0 else 'buy'
        result     = place_market_order(product_id, close_side, size)
        if result:
            print(
                f"[CLOSE] {symbol} | attempt={attempt} | side={close_side} | "
                f"size={size} | order_id={result.get('id')}"
            )
            time.sleep(0.5)
        else:
            print(f"[ERROR] Close order failed for {symbol} on attempt {attempt}")
            time.sleep(1)

    # Final residual check
    pos      = get_position(product_id)
    residual = abs(int(pos.get('size', 0))) if pos else 0
    if residual > 0:
        print(f"[WARN] Residual position of {residual} contracts remains for {symbol} after 2 close attempts.")
        return False
    return True

# =============================================================================
# CONTRACT SELECTION
# =============================================================================
def select_best_contract() -> dict | None:
    """
    Fetches all perpetual futures, filters by MIN_FUNDING_RATE_ABS,
    sorts by absolute funding rate descending, prints top 10, and
    returns the contract with the highest absolute funding rate.
    """
    contracts = get_all_perpetuals()
    if not contracts:
        print("[ERROR] No perpetual contracts fetched.")
        return None

    rated = []
    for t in contracts:
        try:
            rate     = float(t['funding_rate'])
            abs_rate = abs(rate)
            if abs_rate < MIN_FUNDING_RATE_ABS:
                continue
            rated.append((abs_rate, rate, t))
        except Exception:
            continue

    rated.sort(key=lambda x: x[0], reverse=True)

    separator()
    print(f"  Threshold filter: |funding rate| >= {MIN_FUNDING_RATE_ABS:.4f}%")
    separator('-')

    if not rated:
        print(f"  No contracts meet the >= {MIN_FUNDING_RATE_ABS:.4f}% threshold. Skipping cycle.")
        separator()
        return None

    print(f"{'RANK':<6} {'SYMBOL':<18} {'FUNDING RATE':>15}  {'DIRECTION':<10}")
    separator('-')
    for i, (abs_rate, rate, t) in enumerate(rated[:10], 1):
        direction = 'SHORT' if rate > 0 else 'LONG'
        print(f"{i:<6} {t['symbol']:<18} {rate:>+.6f}%      {direction:<10}")
    separator()

    best                    = rated[0][2]
    best['funding_rate_abs'] = rated[0][0]
    return best

# =============================================================================
# TRADE DETAIL DISPLAY
# Called AFTER stop_price is computed so it is always shown correctly.
# =============================================================================
def display_trade_details(
    symbol:           str,
    product_id:       int,
    side:             str,
    size:             int,
    funding_rate:     float,
    mark_price:       float,
    contract_value:   float,
    contract_unit:    str,
    tick_size:        float,
    initial_margin:   float,
    maint_margin:     float,
    taker_fee:        float,
    maker_fee:        float,
    interval_h:       float,
    next_funding_ist: datetime,
    entry_time_ist:   datetime,
    close_time_ist:   datetime,
    balance:          float,
    leverage:         int,
    stop_price:       float | None = None
):
    notional        = size * contract_value * mark_price
    margin_required = notional * (initial_margin / 100) / leverage
    est_funding_pnl = notional * (abs(funding_rate) / 100)
    est_taker_cost  = notional * taker_fee * 2

    separator()
    print("  TRADE CONFIGURATION & CONTRACT DETAILS")
    separator()
    print(f"  {'Symbol':<35} {symbol}")
    print(f"  {'Product ID':<35} {product_id}")
    print(f"  {'Direction':<35} {side.upper()}")
    print(f"  {'Order Type':<35} Market Order")
    separator('-')
    print(f"  {'Mark Price':<35} {format_price(mark_price, tick_size)}")
    print(f"  {'Contract Value':<35} {contract_value} {contract_unit}")
    print(f"  {'Tick Size':<35} {tick_size}")
    print(f"  {'Initial Margin':<35} {initial_margin}%")
    print(f"  {'Maintenance Margin':<35} {maint_margin}%")
    print(f"  {'Taker Fee':<35} {taker_fee * 100:.4f}%")
    print(f"  {'Maker Fee':<35} {maker_fee * 100:.4f}%")
    separator('-')
    print(f"  {'Funding Rate':<35} {funding_rate:+.6f}%")
    print(f"  {'Funding Rate Threshold':<35} >= {MIN_FUNDING_RATE_ABS:.4f}% (absolute)")
    print(f"  {'Funding Interval':<35} {interval_h:.2f} hour(s)")
    print(f"  {'Next Funding Time (IST)':<35} {fmt_ist(next_funding_ist)}")
    separator('-')
    print(f"  {'Available Balance':<35} ${balance:,.2f}")
    print(f"  {'Leverage':<35} {leverage}x")
    print(f"  {'Position Size (contracts)':<35} {size}")
    print(f"  {'Notional Value':<35} ${notional:,.2f}")
    print(f"  {'Margin Required (approx)':<35} ${margin_required:,.4f}")
    print(f"  {'Est. Funding PnL (approx)':<35} ${est_funding_pnl:,.4f}")
    print(f"  {'Est. Taker Fee Cost (approx)':<35} ${est_taker_cost:,.4f}")
    if stop_price is not None:
        print(
            f"  {'Stop Loss Trigger Price':<35} "
            f"{format_price(stop_price, tick_size)}  "
            f"({STOP_LOSS_PCT:.2f}% adverse move, min {MIN_STOP_TICKS} ticks)"
        )
    else:
        print(f"  {'Stop Loss':<35} DISABLED")
    separator('-')
    print(f"  {'Entry Time (IST)':<35} {fmt_ist(entry_time_ist)}")
    print(f"  {'Funding Settlement (IST)':<35} {fmt_ist(next_funding_ist)}")
    print(f"  {'Close Time (IST)':<35} {fmt_ist(close_time_ist)}")
    separator()

# =============================================================================
# WAIT HELPER
# =============================================================================
def wait_until_ts(target_ts_s: float, label: str):
    """Waits until the given Unix timestamp (seconds). Prints countdown."""
    while True:
        diff = target_ts_s - time.time()
        if diff <= 0:
            break
        remaining_ist = ts_to_ist(int(target_ts_s * 1_000_000))
        print(
            f"\r[WAIT] {label} | Target: {fmt_ist(remaining_ist)} | "
            f"Remaining: {diff:.1f}s   ",
            end='', flush=True
        )
        time.sleep(0.2 if diff < 2 else 1)
    print()

# =============================================================================
# MAIN LOOP
# =============================================================================
def run_bot():
    separator()
    print("  DELTA EXCHANGE - FUNDING ARBITRAGE BOT  (FULLY AUTOMATIC)")
    print(f"  Started at        : {fmt_ist(now_ist())}")
    print(f"  Leverage          : {LEVERAGE}x")
    print(f"  Position          : {int(POSITION_PCT * 100)}% of available balance")
    print(f"  Min Funding Rate  : >= {MIN_FUNDING_RATE_ABS:.4f}% (absolute)")
    print(f"  Entry             : {ENTRY_BEFORE_S}s before funding settlement")
    print(f"  Close             : {CLOSE_AFTER_MS}ms after funding settlement")
    print(f"  Scan every        : {SCAN_INTERVAL_S}s | Act only if funding <= {WINDOW_S}s away")
    print(f"  Stop Loss         : {'ENABLED @ ' + str(STOP_LOSS_PCT) + '% adverse move (min ' + str(MIN_STOP_TICKS) + ' ticks)' if STOP_LOSS_ENABLED else 'DISABLED'}")
    separator()

    active_product_id    = None
    active_stop_order_id = None

    while True:
        try:
            loop_start = time.time()
            print(f"\n[SCAN] {fmt_ist(now_ist())} - Scanning best perpetual contract...")

            # ----------------------------------------------------------------
            # 1. Select best contract by highest absolute funding rate
            # ----------------------------------------------------------------
            best = select_best_contract()
            if not best:
                elapsed = time.time() - loop_start
                sleep_s = max(0, SCAN_INTERVAL_S - elapsed)
                print(f"[WAIT] No qualifying contract found. Next scan in {sleep_s:.0f}s.")
                time.sleep(sleep_s)
                continue

            symbol     = best['symbol']
            product_id = int(best['product_id'])
            rate       = float(best['funding_rate'])
            mark_price = float(best['mark_price'])
            side       = 'sell' if rate > 0 else 'buy'

            # ----------------------------------------------------------------
            # 2. Fetch product details for contract specs
            # ----------------------------------------------------------------
            product = get_product_details(symbol)
            if not product:
                print(f"[WARN] Could not fetch product details for {symbol}. Skipping.")
                time.sleep(60)
                continue

            contract_value = float(product.get('contract_value', 1))
            contract_unit  = product.get('contract_unit_currency', 'USD')
            tick_size      = float(product.get('tick_size', 0.5))
            initial_margin = float(product.get('initial_margin', 1))
            maint_margin   = float(product.get('maintenance_margin', 0.5))
            taker_fee      = float(product.get('taker_commission_rate', 0.0005))
            maker_fee      = float(product.get('maker_commission_rate', 0.0002))
            interval_s     = int(
                product.get('product_specs', {}).get('rate_exchange_interval', 3600)
            )
            interval_h     = interval_s / 3600

            # ----------------------------------------------------------------
            # 3. Get next funding time (WebSocket first, fallback to epoch-align)
            # ----------------------------------------------------------------
            print(f"[INFO] Fetching next funding time for {symbol} via WebSocket...")
            next_funding_micro = get_next_funding_time_ws(symbol)
            if not next_funding_micro:
                print("[WARN] WebSocket timeout. Using epoch-aligned fallback.")
                next_funding_micro = get_next_funding_time_fallback(interval_s)

            next_funding_s   = next_funding_micro / 1_000_000
            next_funding_ist = ts_to_ist(next_funding_micro)
            secs_to_funding  = next_funding_s - time.time()

            print(f"[INFO] Next funding for {symbol}: {fmt_ist(next_funding_ist)}")
            print(f"[INFO] Time to funding: {secs_to_funding:.1f}s")

            # ----------------------------------------------------------------
            # 4. Check if funding is within the action window
            # ----------------------------------------------------------------
            if secs_to_funding > WINDOW_S:
                print(
                    f"[SKIP] Funding is {secs_to_funding:.0f}s away "
                    f"(> {WINDOW_S}s window). Will re-scan in {SCAN_INTERVAL_S}s."
                )
                elapsed = time.time() - loop_start
                sleep_s = max(0, SCAN_INTERVAL_S - elapsed)
                time.sleep(sleep_s)
                continue

            if secs_to_funding <= 0:
                print("[SKIP] Funding already passed. Re-scanning.")
                time.sleep(10)
                continue

            # ----------------------------------------------------------------
            # 5. Close any existing position from a previous cycle
            # ----------------------------------------------------------------
            if active_product_id and active_product_id != product_id:
                print(f"[INFO] Closing previous position (product_id={active_product_id})")
                if active_stop_order_id:
                    cancel_order(active_product_id, active_stop_order_id)
                    active_stop_order_id = None
                close_position(active_product_id, 'previous_contract')
                active_product_id = None

            # ----------------------------------------------------------------
            # 6. Calculate position size
            # ----------------------------------------------------------------
            balance = get_wallet_balance()
            if balance <= 0:
                print("[ERROR] Could not fetch wallet balance or balance is zero.")
                time.sleep(60)
                continue

            raw_size = (balance * POSITION_PCT * LEVERAGE) / (contract_value * mark_price)
            size     = max(1, int(raw_size))

            # ----------------------------------------------------------------
            # 7. Calculate entry and close timestamps
            # ----------------------------------------------------------------
            entry_ts_s     = next_funding_s - ENTRY_BEFORE_S
            close_ts_s     = next_funding_s + (CLOSE_AFTER_MS / 1000.0)
            entry_time_ist = ts_to_ist(int(entry_ts_s * 1_000_000))
            close_time_ist = ts_to_ist(int(close_ts_s * 1_000_000))

            # ----------------------------------------------------------------
            # 8. Set leverage
            # ----------------------------------------------------------------
            if set_leverage(product_id, LEVERAGE):
                print(f"[INFO] Leverage set to {LEVERAGE}x for {symbol}")
            else:
                print(f"[WARN] Could not set leverage for {symbol}. Proceeding anyway.")

            # ----------------------------------------------------------------
            # 9. Wait until ENTRY_BEFORE_S seconds before funding,
            #    then re-verify live funding rate
            # ----------------------------------------------------------------
            wait_until_ts(entry_ts_s, f"Waiting to enter {symbol} ({side.upper()})")

            print(f"\n[VERIFY] Re-checking live funding rate for {symbol} before entry...")
            live_contracts = get_all_perpetuals()
            live_rate      = None
            live_price     = mark_price
            for c in live_contracts:
                if c['symbol'] == symbol:
                    try:
                        live_rate  = float(c['funding_rate'])
                        live_price = float(c.get('mark_price', mark_price))
                    except Exception:
                        pass
                    break

            if live_rate is None:
                print(f"[SKIP] Could not fetch live funding rate for {symbol}. Aborting entry.")
                time.sleep(10)
                continue

            if abs(live_rate) < MIN_FUNDING_RATE_ABS:
                print(
                    f"[SKIP] Live funding rate {live_rate:+.6f}% is below threshold "
                    f"|{MIN_FUNDING_RATE_ABS:.4f}%|. Aborting entry."
                )
                time.sleep(10)
                continue

            # Update side based on live rate (in case rate flipped sign)
            side = 'sell' if live_rate > 0 else 'buy'
            print(
                f"[VERIFY] Live funding rate {live_rate:+.6f}% meets threshold. "
                f"Proceeding with {side.upper()} entry."
            )

            # ----------------------------------------------------------------
            # 9b. Compute stop-loss trigger price using tick-aware calculation.
            #     compute_stop_price() handles:
            #       - Correct side of market
            #       - Tick size rounding
            #       - Minimum tick distance enforcement
            #       - Zero/negative price guard
            # ----------------------------------------------------------------
            stop_price = None
            stop_side  = None
            if STOP_LOSS_ENABLED:
                stop_price = compute_stop_price(side, live_price, tick_size)
                if stop_price is not None:
                    stop_side = 'sell' if side == 'buy' else 'buy'

            # ----------------------------------------------------------------
            # 9c. Display full trade configuration AFTER stop_price is computed
            # ----------------------------------------------------------------
            display_trade_details(
                symbol           = symbol,
                product_id       = product_id,
                side             = side,
                size             = size,
                funding_rate     = live_rate,
                mark_price       = live_price,
                contract_value   = contract_value,
                contract_unit    = contract_unit,
                tick_size        = tick_size,
                initial_margin   = initial_margin,
                maint_margin     = maint_margin,
                taker_fee        = taker_fee,
                maker_fee        = maker_fee,
                interval_h       = interval_h,
                next_funding_ist = next_funding_ist,
                entry_time_ist   = entry_time_ist,
                close_time_ist   = close_time_ist,
                balance          = balance,
                leverage         = LEVERAGE,
                stop_price       = stop_price
            )

            # ----------------------------------------------------------------
            # 9d. Fire ENTRY and STOP-LOSS orders concurrently via threads
            # ----------------------------------------------------------------
            entry_result = {'order': None}
            stop_result  = {'order': None}

            def _entry_worker():
                entry_result['order'] = place_market_order(product_id, side, size)

            def _stop_worker():
                if STOP_LOSS_ENABLED and stop_price is not None:
                    stop_result['order'] = place_stop_order(
                        product_id, stop_side, size, stop_price, tick_size
                    )

            print(f"\n[ENTRY] Placing {side.upper()} market order | {symbol} | size={size}")
            if STOP_LOSS_ENABLED and stop_price is not None:
                print(
                    f"[STOP]  Simultaneously placing {stop_side.upper()} stop-loss | "
                    f"trigger={format_price(stop_price, tick_size)}"
                )
            elif STOP_LOSS_ENABLED and stop_price is None:
                print("[STOP]  Stop-loss skipped (could not compute valid stop price for this contract)")

            t_entry = threading.Thread(target=_entry_worker, name="entry-order")
            t_stop  = threading.Thread(target=_stop_worker,  name="stop-order")

            t_entry.start()
            t_stop.start()
            t_entry.join(timeout=15)
            t_stop.join(timeout=15)

            order      = entry_result['order']
            stop_order = stop_result['order']

            if not order:
                print(f"[ERROR] Entry order placement failed for {symbol}.")
                if stop_order:
                    print("[CLEANUP] Entry failed but stop-loss order exists — cancelling it.")
                    cancel_order(product_id, stop_order.get('id'))
                time.sleep(10)
                continue

            print(
                f"[ENTRY] Order placed successfully | order_id={order.get('id')} | "
                f"avg_fill={order.get('average_fill_price', 'N/A')}"
            )
            active_product_id = product_id

            # ----------------------------------------------------------------
            # 9e. Reconcile stop-loss size against actual filled size.
            #     If entry was partially filled, cancel the concurrent stop
            #     and re-place with the correct filled size.
            # ----------------------------------------------------------------
            requested_size = size
            unfilled       = int(order.get('unfilled_size', 0))
            filled_size    = requested_size - unfilled

            if STOP_LOSS_ENABLED and stop_price is not None:
                if filled_size <= 0:
                    print("[WARN] Entry order filled 0 contracts. Cancelling stop-loss if placed.")
                    if stop_order:
                        cancel_order(product_id, stop_order.get('id'))
                    active_product_id    = None
                    active_stop_order_id = None
                    time.sleep(10)
                    continue

                if filled_size != requested_size:
                    print(
                        f"[WARN] Partial fill: requested={requested_size}, filled={filled_size}. "
                        f"Re-placing stop-loss with correct size."
                    )
                    if stop_order:
                        cancel_order(product_id, stop_order.get('id'))
                    stop_order = place_stop_order(
                        product_id, stop_side, filled_size, stop_price, tick_size
                    )

                if stop_order:
                    active_stop_order_id = stop_order.get('id')
                    print(
                        f"[STOP]  Stop-loss confirmed | order_id={active_stop_order_id} | "
                        f"trigger={format_price(stop_price, tick_size)} | "
                        f"side={stop_side.upper()} | size={filled_size}"
                    )
                else:
                    active_stop_order_id = None
                    print("[WARN]  Stop-loss order FAILED to place — position is currently unprotected!")

            # ----------------------------------------------------------------
            # 10. Wait until CLOSE_AFTER_MS after funding, then close.
            #     Cancel the resting stop-loss first to avoid double-close.
            # ----------------------------------------------------------------
            wait_until_ts(close_ts_s, "Waiting to close after funding settlement")

            if active_stop_order_id:
                print(
                    f"[CLEANUP] Cancelling resting stop-loss "
                    f"order_id={active_stop_order_id} before scheduled close."
                )
                cancel_order(product_id, active_stop_order_id)
                active_stop_order_id = None

            print(f"\n[CLOSE] Closing position for {symbol}...")
            close_position(product_id, symbol)
            active_product_id = None

            # ----------------------------------------------------------------
            # 11. Sleep until next scan cycle
            # ----------------------------------------------------------------
            elapsed = time.time() - loop_start
            sleep_s = max(0, SCAN_INTERVAL_S - elapsed)
            print(f"\n[CYCLE] Trade cycle complete. Next scan in {sleep_s:.0f}s.")
            separator()
            time.sleep(sleep_s)

        except KeyboardInterrupt:
            print("\n[STOP] Bot stopped by user.")
            if active_stop_order_id:
                print(f"[STOP] Cancelling resting stop-loss order_id={active_stop_order_id}...")
                cancel_order(active_product_id, active_stop_order_id)
            if active_product_id:
                print(f"[STOP] Attempting to close open position (product_id={active_product_id})...")
                close_position(active_product_id, 'open_position')
            break

        except Exception as e:
            print(f"[ERROR] Unexpected error in main loop: {e}")
            if active_product_id:
                print(
                    f"[WARN] Open position may exist for product_id={active_product_id}. "
                    f"Will attempt cleanup on next iteration."
                )
            time.sleep(30)

# =============================================================================
if __name__ == "__main__":
    run_bot()
