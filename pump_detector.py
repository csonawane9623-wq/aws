"""
+======================================================================+
|         BINANCE FUTURES PUMP/DUMP SCANNER BOT  v2.1                 |
|         Signals : Short Squeeze | Long Squeeze | Breakout            |
|         Output  : Terminal + Telegram Alerts + CSV Log               |
|         Auth    : Binance API Key (signed requests)                  |
+======================================================================+

SIGNALS DETECTED:
  [PUMP]     Negative funding + Oversold RSI + OI spike + Volume surge
  [DUMP]     Positive funding + Overbought RSI + OI spike + Volume surge
  [BREAKOUT] Bollinger squeeze + Volume explosion + Trend confirmation

SETUP:
  pip install requests pandas numpy colorama

  Then fill in your keys below and run:
  python binance_scanner_bot_v2.py
"""

import sys
import io

# ── Force UTF-8 output on Windows so the terminal never chokes ──────
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  errors="replace", line_buffering=True)
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8",
                                  errors="replace", line_buffering=True)

import requests
import pandas as pd
import numpy as np
import hmac, hashlib, time, os, csv
from datetime import datetime
from colorama import Fore, Back, Style, init

init(autoreset=True, convert=True)   # convert=True forces ANSI on Windows cmd/powershell

# ════════════════════════════════════════════════════════════════
#   YOUR CREDENTIALS  -- fill these in
# ════════════════════════════════════════════════════════════════
API_KEY = ""
API_SECRET = ""

TELEGRAM_BOT_TOKEN ="8798956944:AAGNlEQgOneX5mgOlosVupux_Lunz01NiJo"    # e.g. "7123456789:AAF..."  leave "" to disable
TELEGRAM_CHAT_ID   = "1184234885"    # e.g. "-1001234567890"

# ════════════════════════════════════════════════════════════════
#   SCANNER CONFIGURATION
# ════════════════════════════════════════════════════════════════
CFG = {
    # Scanning
    "SCAN_INTERVAL_SEC"   : 300,        # seconds between full scans (300=5min)
    "TOP_N_COINS"         : 60,         # top-volume futures to scan
    "MIN_VOLUME_USDT"     : 5_000_000,  # ignore pairs below this 24h volume
    "KLINE_INTERVAL"      : "1h",       # candle timeframe for indicators
    "KLINE_LIMIT"         : 100,        # candles to fetch

    # Signal thresholds
    "FUNDING_PUMP_THRESH" : -0.05,  # % negative = shorts paying = pump risk
    "FUNDING_DUMP_THRESH" :  0.05,  # % positive = longs paying  = dump risk
    "RSI_OVERSOLD"        : 30,
    "RSI_OVERBOUGHT"      : 70,
    "VOLUME_SPIKE_MULT"   : 2.0,    # current vol vs 20-bar avg
    "OI_CHANGE_THRESH"    : 5.0,    # % OI change over last 4 hours
    "BB_SQUEEZE_THRESH"   : 3.0,    # band width % below this = squeeze
    "LS_RATIO_LONG_HEAVY" : 1.5,    # above = too many longs  -> dump risk
    "LS_RATIO_SHORT_HEAVY": 0.8,    # below = too many shorts -> pump risk

    # Scoring
    "MIN_SCORE_ALERT"     : 60,     # min score to show in highlights
    "MIN_SCORE_TELEGRAM"  : 70,     # min score to send Telegram message

    # Output
    "SAVE_CSV"            : True,
    "CSV_FILE"            : "scan_log.csv",
    "TOP_DISPLAY"         : 15,     # rows to show in highlights section
}

BASE = "https://fapi.binance.com"

# Signal label constants (pure ASCII -- no emoji)
SIG_PUMP     = "[PUMP]   "
SIG_DUMP     = "[DUMP]   "
SIG_BREAKOUT = "[BRKOUT] "
SIG_NEUTRAL  = "NEUTRAL  "

# ════════════════════════════════════════════════════════════════
#   HTTP HELPERS
# ════════════════════════════════════════════════════════════════

SESSION = requests.Session()
SESSION.headers.update({"X-MBX-APIKEY": API_KEY})

def _sign(params: dict) -> dict:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    sig   = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return params

def get_public(path, params=None):
    for attempt in range(3):
        try:
            r = SESSION.get(BASE + path, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: return None
            time.sleep(1)

def get_signed(path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params = _sign(params)
    for attempt in range(3):
        try:
            r = SESSION.get(BASE + path, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: return None
            time.sleep(1)

# ════════════════════════════════════════════════════════════════
#   DATA FETCHERS
# ════════════════════════════════════════════════════════════════

def fetch_top_symbols():
    data = get_public("/fapi/v1/ticker/24hr")
    if not data: return []
    df = pd.DataFrame(data)
    df = df[df["symbol"].str.endswith("USDT")]
    df["quoteVolume"] = pd.to_numeric(df["quoteVolume"], errors="coerce")
    df = df[df["quoteVolume"] >= CFG["MIN_VOLUME_USDT"]]
    df = df.nlargest(CFG["TOP_N_COINS"], "quoteVolume")
    return df["symbol"].tolist()

def fetch_ticker(symbol):
    return get_public("/fapi/v1/ticker/24hr", {"symbol": symbol})

def fetch_funding_rate(symbol):
    d = get_public("/fapi/v1/premiumIndex", {"symbol": symbol})
    return float(d["lastFundingRate"]) * 100 if d else 0.0

def fetch_mark_price(symbol):
    d = get_public("/fapi/v1/premiumIndex", {"symbol": symbol})
    return float(d.get("markPrice", 0)) if d else 0.0

def fetch_oi_history(symbol):
    """% OI change over last 4 hourly buckets."""
    d = get_public("/futures/data/openInterestHist",
                   {"symbol": symbol, "period": "1h", "limit": 5})
    if d and len(d) >= 2:
        latest = float(d[-1]["sumOpenInterestValue"])
        first  = float(d[0]["sumOpenInterestValue"])
        if first > 0:
            return ((latest - first) / first) * 100
    return 0.0

def fetch_klines(symbol):
    d = get_public("/fapi/v1/klines", {
        "symbol"  : symbol,
        "interval": CFG["KLINE_INTERVAL"],
        "limit"   : CFG["KLINE_LIMIT"],
    })
    if not d: return None
    cols = ["open_time","open","high","low","close","volume",
            "close_time","quote_vol","trades","tbv","tbq","ignore"]
    df = pd.DataFrame(d, columns=cols)
    for c in ["open","high","low","close","volume","quote_vol"]:
        df[c] = pd.to_numeric(df[c])
    return df

def fetch_ls_ratio(symbol):
    d = get_public("/futures/data/globalLongShortAccountRatio",
                   {"symbol": symbol, "period": "1h", "limit": 1})
    if d and len(d) > 0:
        return float(d[0].get("longShortRatio", 1.0))
    return 1.0

def fetch_top_trader_ls(symbol):
    d = get_public("/futures/data/topLongShortPositionRatio",
                   {"symbol": symbol, "period": "1h", "limit": 1})
    if d and len(d) > 0:
        return float(d[0].get("longShortRatio", 1.0))
    return 1.0

def fetch_liquidations_proxy(symbol):
    """Mark/index divergence as liquidation pressure proxy."""
    d = get_public("/fapi/v1/premiumIndex", {"symbol": symbol})
    if d:
        mark = float(d.get("markPrice", 0))
        idx  = float(d.get("indexPrice", 0))
        if idx > 0:
            return abs((mark - idx) / idx) * 100
    return 0.0

def fetch_account_positions():
    return get_signed("/fapi/v2/positionRisk") or []

def fetch_account_balance():
    return get_signed("/fapi/v2/balance") or []

# ════════════════════════════════════════════════════════════════
#   TECHNICAL INDICATORS
# ════════════════════════════════════════════════════════════════

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).iloc[-1]

def calc_bollinger(closes, period=20, k=2):
    mid   = closes.rolling(period).mean()
    std   = closes.rolling(period).std()
    upper = mid + k * std
    lower = mid - k * std
    width = ((upper - lower) / mid * 100).iloc[-1]
    pct_b = ((closes - lower) / (upper - lower)).iloc[-1]
    return width, pct_b

def calc_volume_spike(volumes):
    if len(volumes) < 21: return 1.0
    avg = volumes.iloc[-21:-1].mean()
    return volumes.iloc[-1] / avg if avg > 0 else 1.0

def calc_macd(closes, fast=12, slow=26, sig=9):
    ef = closes.ewm(span=fast, adjust=False).mean()
    es = closes.ewm(span=slow, adjust=False).mean()
    m  = ef - es
    s  = m.ewm(span=sig, adjust=False).mean()
    return (m - s).iloc[-1]

def calc_ema_trend(closes, fast=20, slow=50):
    if len(closes) < slow + 5: return 0
    ef = closes.ewm(span=fast, adjust=False).mean().iloc[-1]
    es = closes.ewm(span=slow, adjust=False).mean().iloc[-1]
    if ef > es * 1.002: return  1
    if ef < es * 0.998: return -1
    return 0

def calc_atr(df, period=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l,
                    (h - c.shift()).abs(),
                    (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]

# ════════════════════════════════════════════════════════════════
#   SCORING ENGINE
# ════════════════════════════════════════════════════════════════

def score_symbol(symbol):
    result = dict(
        symbol=symbol, signal=SIG_NEUTRAL, score=0,
        pump_score=0, dump_score=0, break_score=0,
        funding=0.0, rsi_val=50.0, vol_spike=1.0,
        oi_change=0.0, ls_ratio=1.0, tt_ls=1.0,
        bb_width=0.0, pct_b=0.5, macd_hist=0.0,
        trend=0, liq_div=0.0, price=0.0, change_24h=0.0,
        mark_price=0.0, atr_val=0.0,
        reasons=[], ts=datetime.now().strftime("%H:%M:%S"),
    )

    ticker = fetch_ticker(symbol)
    if ticker:
        result["price"]     = float(ticker.get("lastPrice", 0))
        result["change_24h"]= float(ticker.get("priceChangePercent", 0))

    result["funding"]    = fetch_funding_rate(symbol)
    result["oi_change"]  = fetch_oi_history(symbol)
    result["ls_ratio"]   = fetch_ls_ratio(symbol)
    result["tt_ls"]      = fetch_top_trader_ls(symbol)
    result["liq_div"]    = fetch_liquidations_proxy(symbol)
    result["mark_price"] = fetch_mark_price(symbol)

    klines = fetch_klines(symbol)
    if klines is None or len(klines) < 55:
        return result

    closes  = klines["close"]
    volumes = klines["volume"]

    rsi_v       = calc_rsi(closes)
    bb_w, pct_b = calc_bollinger(closes)
    vol_sp      = calc_volume_spike(volumes)
    macd_h      = calc_macd(closes)
    trend       = calc_ema_trend(closes)
    atr_v       = calc_atr(klines)

    result["rsi_val"]   = round(rsi_v,  1) if not np.isnan(rsi_v)  else 50.0
    result["bb_width"]  = round(bb_w,   2) if not np.isnan(bb_w)   else 0.0
    result["pct_b"]     = round(pct_b,  3) if not np.isnan(pct_b)  else 0.5
    result["vol_spike"] = round(vol_sp, 2)
    result["macd_hist"] = round(macd_h, 6) if not np.isnan(macd_h) else 0.0
    result["trend"]     = trend
    result["atr_val"]   = round(atr_v,  6) if not np.isnan(atr_v)  else 0.0

    f   = result["funding"]
    oic = result["oi_change"]
    ls  = result["ls_ratio"]
    tt  = result["tt_ls"]

    pump = dump = brk = 0
    why  = []

    # ── PUMP signals ────────────────────────────────────────────
    if f <= CFG["FUNDING_PUMP_THRESH"]:
        pts = min(30, int(abs(f) * 400))
        pump += pts
        why.append(f"Funding {f:+.4f}% (shorts overloaded) +{pts}")

    if result["rsi_val"] <= CFG["RSI_OVERSOLD"]:
        pts = int((CFG["RSI_OVERSOLD"] - result["rsi_val"]) * 2)
        pump += pts
        why.append(f"RSI oversold {result['rsi_val']} +{pts}")
    elif result["rsi_val"] <= 40:
        pump += 8
        why.append(f"RSI weak {result['rsi_val']} +8")

    if oic >= CFG["OI_CHANGE_THRESH"]:
        pts = min(20, int(oic * 2))
        pump += pts
        why.append(f"OI +{oic:.1f}% build-up +{pts}")

    if vol_sp >= CFG["VOLUME_SPIKE_MULT"]:
        pts = min(20, int(vol_sp * 4))
        pump += pts
        why.append(f"Volume {vol_sp:.1f}x average +{pts}")

    if ls < CFG["LS_RATIO_SHORT_HEAVY"]:
        pump += 12
        why.append(f"Global L/S {ls:.2f} (short heavy) +12")

    if tt < 0.75:
        pump += 10
        why.append(f"Top-trader L/S {tt:.2f} (smart money short) +10")

    if macd_h > 0:
        pump += 8
        why.append("MACD histogram bullish +8")

    if trend == 1:
        pump += 8
        why.append("EMA20 > EMA50 uptrend +8")

    if pct_b < 0.1:
        pump += 10
        why.append("Price at lower BB band +10")

    if result["liq_div"] > 0.1:
        pump += 8
        why.append(f"Mark/index divergence {result['liq_div']:.3f}% (liq pressure) +8")

    # ── DUMP signals ────────────────────────────────────────────
    if f >= CFG["FUNDING_DUMP_THRESH"]:
        pts = min(30, int(f * 400))
        dump += pts
        why.append(f"Funding {f:+.4f}% (longs overloaded) +{pts}")

    if result["rsi_val"] >= CFG["RSI_OVERBOUGHT"]:
        pts = int((result["rsi_val"] - CFG["RSI_OVERBOUGHT"]) * 2)
        dump += pts
        why.append(f"RSI overbought {result['rsi_val']} +{pts}")
    elif result["rsi_val"] >= 60:
        dump += 8
        why.append(f"RSI elevated {result['rsi_val']} +8")

    if oic >= CFG["OI_CHANGE_THRESH"]:
        pts = min(20, int(oic * 2))
        dump += pts
        why.append(f"OI +{oic:.1f}% (leveraged longs building) +{pts}")

    if vol_sp >= CFG["VOLUME_SPIKE_MULT"]:
        pts = min(20, int(vol_sp * 4))
        dump += pts

    if ls > CFG["LS_RATIO_LONG_HEAVY"]:
        dump += 12
        why.append(f"Global L/S {ls:.2f} (long heavy) +12")

    if tt > 1.5:
        dump += 10
        why.append(f"Top-trader L/S {tt:.2f} (smart money long) +10")

    if macd_h < 0:
        dump += 8
        why.append("MACD histogram bearish +8")

    if trend == -1:
        dump += 8
        why.append("EMA20 < EMA50 downtrend +8")

    if pct_b > 0.9:
        dump += 10
        why.append("Price at upper BB band +10")

    # ── BREAKOUT signals ─────────────────────────────────────────
    if bb_w <= CFG["BB_SQUEEZE_THRESH"]:
        pts = min(40, int((CFG["BB_SQUEEZE_THRESH"] - bb_w) * 600))
        brk += pts
        why.append(f"BB squeeze ({bb_w:.2f}%) +{pts}")

    if vol_sp >= 3.0:
        brk += 20
        why.append(f"Extreme volume {vol_sp:.1f}x +20")

    if abs(result["change_24h"]) >= 5:
        pts = min(20, int(abs(result["change_24h"]) * 2))
        brk += pts
        why.append(f"24h move {result['change_24h']:+.1f}% +{pts}")

    if oic >= 8:
        brk += 15
        why.append(f"Big OI surge {oic:.1f}% +15")

    # ── Final decision ───────────────────────────────────────────
    result["pump_score"]  = min(100, pump)
    result["dump_score"]  = min(100, dump)
    result["break_score"] = min(100, brk)
    best = max(pump, dump, brk)
    result["score"]   = min(100, best)
    result["reasons"] = why

    if best >= CFG["MIN_SCORE_ALERT"]:
        if pump == best:
            result["signal"] = SIG_PUMP
        elif dump == best:
            result["signal"] = SIG_DUMP
        else:
            result["signal"] = SIG_BREAKOUT

    return result

# ════════════════════════════════════════════════════════════════
#   TERMINAL DISPLAY
# ════════════════════════════════════════════════════════════════

def clr():
    os.system("cls" if os.name == "nt" else "clear")

def banner():
    print(Fore.CYAN + Style.BRIGHT + """
  +==========================================================+
  |   BINANCE FUTURES  //  PUMP & DUMP SCANNER  v2.1        |
  |   Signals  : Short Squeeze | Long Squeeze | Breakout     |
  |   Output   : Terminal + Telegram + CSV                   |
  +==========================================================+
""")

def sig_color(sig):
    if SIG_PUMP.strip()     in sig.strip(): return Fore.GREEN  + Style.BRIGHT
    if SIG_DUMP.strip()     in sig.strip(): return Fore.RED    + Style.BRIGHT
    if SIG_BREAKOUT.strip() in sig.strip(): return Fore.YELLOW + Style.BRIGHT
    return Fore.WHITE + Style.DIM

def score_bar(s):
    filled = int(s / 5)
    bar    = "#" * filled + "." * (20 - filled)
    c = Fore.RED if s >= 80 else Fore.YELLOW if s >= 60 else Fore.WHITE
    return c + f"[{bar}] {s:>3}/100" + Style.RESET_ALL

def trend_str(t):
    if t ==  1: return "UP  "
    if t == -1: return "DOWN"
    return "FLAT"

def print_results(results, balance_usdt=None):
    clr()
    banner()

    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    print(Fore.WHITE + f"  Time    : {now}")
    print(Fore.WHITE + f"  Scanned : {len(results)} symbols")
    if balance_usdt is not None:
        print(Fore.WHITE + f"  Balance : {balance_usdt:.2f} USDT available")
    print()

    alerts = sorted([r for r in results if r["signal"] != SIG_NEUTRAL],
                    key=lambda x: x["score"], reverse=True)

    pumps  = [r for r in alerts if SIG_PUMP.strip()     in r["signal"].strip()]
    dumps  = [r for r in alerts if SIG_DUMP.strip()     in r["signal"].strip()]
    breaks = [r for r in alerts if SIG_BREAKOUT.strip() in r["signal"].strip()]

    print(Fore.WHITE + Style.BRIGHT +
          f"  PUMP : {len(pumps):<4}  DUMP : {len(dumps):<4}  BREAKOUT : {len(breaks)}\n")

    if alerts:
        print(Fore.CYAN + Style.BRIGHT +
              "  +------ TOP SIGNALS -----------------------------------------------+\n")
        for i, r in enumerate(alerts[:CFG["TOP_DISPLAY"]], 1):
            c = sig_color(r["signal"])
            print(c +
                  f"  {i:>2}. {r['signal'].strip():<10}  {r['symbol']:<14}"
                  f"  ${r['price']:<14.6f}  {r['change_24h']:>+7.2f}%"
                  f"  Mark: ${r['mark_price']:.6f}")
            print(f"      {score_bar(r['score'])}")
            print(Fore.CYAN +
                  f"      Fund:{r['funding']:+.4f}%"
                  f"  RSI:{r['rsi_val']:>5.1f}"
                  f"  Vol:{r['vol_spike']:>5.1f}x"
                  f"  OI%:{r['oi_change']:>+6.1f}"
                  f"  L/S:{r['ls_ratio']:.2f}"
                  f"  BB%:{r['bb_width']:.2f}"
                  f"  Trend:{trend_str(r['trend'])}"
                  f"  ATR:{r['atr_val']:.5f}")
            for line in r["reasons"][:4]:
                print(Fore.WHITE + Style.DIM + f"      >> {line}")
            print()
    else:
        print(Fore.YELLOW + "  No strong signals yet -- markets are quiet.\n")

    # Full scan table
    print(Fore.CYAN + Style.BRIGHT +
          "\n  +------ FULL SCAN TABLE (top 40 by score) -------------------------+\n")
    hdr = (f"  {'#':<4} {'Symbol':<14} {'Signal':<10} {'Score':<6}"
           f" {'Fund%':<10} {'RSI':<6} {'VolX':<7}"
           f" {'OI%':<8} {'L/S':<7} {'24h%'}")
    print(Fore.WHITE + Style.DIM + hdr)
    print(Fore.WHITE + Style.DIM + "  " + "-" * 85)

    for i, r in enumerate(sorted(results, key=lambda x: x["score"],
                                  reverse=True)[:40], 1):
        c = sig_color(r["signal"])
        print(c +
              f"  {i:<4} {r['symbol']:<14} {r['signal'].strip():<10} {r['score']:<6}"
              f" {r['funding']:+.4f}{'%':<4}  {r['rsi_val']:<6.1f}"
              f" {r['vol_spike']:<7.1f} {r['oi_change']:>+6.1f}{'%':<2} "
              f" {r['ls_ratio']:<7.2f} {r['change_24h']:>+.2f}%")
    print()

# ════════════════════════════════════════════════════════════════
#   TELEGRAM
# ════════════════════════════════════════════════════════════════

_tg_sent = set()

def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID,
                  "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(Fore.RED + f"  [Telegram error] {e}")

def tg_alert(r):
    key = f"{r['symbol']}|{r['signal']}"
    if key in _tg_sent:
        return
    _tg_sent.add(key)

    top_reasons = "\n".join(f"  - {x}" for x in r["reasons"][:5])
    msg = (
        f"<b>{r['signal'].strip()} -- {r['symbol']}</b>\n"
        f"Score: {r['score']}/100\n"
        f"Price: ${r['price']:.6f}  ({r['change_24h']:+.2f}%)\n"
        f"Mark Price: ${r['mark_price']:.6f}\n"
        f"---\n"
        f"Funding Rate : {r['funding']:+.4f}%\n"
        f"RSI          : {r['rsi_val']:.1f}\n"
        f"Volume Spike : {r['vol_spike']:.1f}x\n"
        f"OI Change    : {r['oi_change']:+.1f}%\n"
        f"L/S Ratio    : {r['ls_ratio']:.2f}\n"
        f"Top-Trader LS: {r['tt_ls']:.2f}\n"
        f"BB Width     : {r['bb_width']:.2f}%\n"
        f"ATR          : {r['atr_val']:.5f}\n"
        f"Trend        : {trend_str(r['trend'])}\n"
        f"---\n"
        f"Reasons:\n{top_reasons}\n"
        f"---\n"
        f"<i>Not financial advice. Use stop-losses.</i>"
    )
    tg_send(msg)

# ════════════════════════════════════════════════════════════════
#   CSV LOGGING
# ════════════════════════════════════════════════════════════════

CSV_COLS = [
    "date","time","symbol","signal","score",
    "pump_score","dump_score","break_score",
    "price","mark_price","change_24h",
    "funding","rsi_val","vol_spike",
    "oi_change","ls_ratio","tt_ls",
    "bb_width","pct_b","macd_hist","trend","atr_val",
]

def save_csv(results):
    if not CFG["SAVE_CSV"]: return
    file_exists = os.path.isfile(CFG["CSV_FILE"])
    now = datetime.now()
    with open(CFG["CSV_FILE"], "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in CSV_COLS}
            row["date"] = now.strftime("%Y-%m-%d")
            row["time"] = now.strftime("%H:%M:%S")
            w.writerow(row)

# ════════════════════════════════════════════════════════════════
#   ACCOUNT INFO (signed endpoints)
# ════════════════════════════════════════════════════════════════

def get_usdt_balance():
    balances = fetch_account_balance()
    if balances:
        for b in balances:
            if b.get("asset") == "USDT":
                return float(b.get("availableBalance", 0))
    return None

def print_open_positions():
    positions = fetch_account_positions()
    if not positions: return
    active = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
    if not active: return
    print(Fore.MAGENTA + Style.BRIGHT +
          "\n  +------ YOUR OPEN POSITIONS ----------------------------------------+\n")
    for p in active:
        pnl  = float(p.get("unrealizedProfit", 0))
        side = "LONG " if float(p.get("positionAmt", 0)) > 0 else "SHORT"
        c    = Fore.GREEN if pnl >= 0 else Fore.RED
        print(c +
              f"  {p['symbol']:<16} {side}"
              f"  Entry: ${float(p.get('entryPrice', 0)):.5f}"
              f"  Mark: ${float(p.get('markPrice', 0)):.5f}"
              f"  PnL: {pnl:>+.4f} USDT"
              f"  Lev: {p.get('leverage','?')}x")
    print()

# ════════════════════════════════════════════════════════════════
#   MAIN SCAN LOOP
# ════════════════════════════════════════════════════════════════

def run_scan():
    print(Fore.CYAN + "\n  >> Fetching top futures symbols...")
    symbols = fetch_top_symbols()
    if not symbols:
        print(Fore.RED + "  ERROR: Failed to fetch symbols."
              " Check internet / API key.")
        return

    print(Fore.CYAN + f"  OK  {len(symbols)} symbols found. Scanning...\n")
    results = []
    for i, sym in enumerate(symbols, 1):
        print(Fore.WHITE + Style.DIM +
              f"  Analyzing [{i:>2}/{len(symbols)}]  {sym:<16}", end="\r")
        results.append(score_symbol(sym))
        time.sleep(0.12)    # stay well within Binance rate limits

    balance = get_usdt_balance()
    print_results(results, balance)
    print_open_positions()
    save_csv(results)

    alerts = sorted(
        [r for r in results if r["score"] >= CFG["MIN_SCORE_TELEGRAM"]],
        key=lambda x: x["score"], reverse=True,
    )
    for r in alerts[:10]:
        tg_alert(r)
    if alerts and TELEGRAM_BOT_TOKEN:
        print(Fore.GREEN + f"  {len(alerts)} Telegram alert(s) sent.\n")

    print(Fore.WHITE + Style.DIM +
          f"  Results saved to {CFG['CSV_FILE']}\n"
          f"  Next scan in {CFG['SCAN_INTERVAL_SEC']}s  |  Ctrl+C to stop\n")

# ════════════════════════════════════════════════════════════════
#   ENTRY POINT
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    clr()
    banner()
    print(Fore.YELLOW + Style.BRIGHT + """
  +-------------------------------------------------------------------+
  |  QUICK SETUP                                                      |
  |                                                                   |
  |  1. Set API_KEY and API_SECRET at the top of this file            |
  |  2. Optional: set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID           |
  |     -> Create a Telegram bot via @BotFather                       |
  |  3. pip install requests pandas numpy colorama                    |
  |  4. python binance_scanner_bot_v2.py                              |
  |                                                                   |
  |  SIGNALS                                                          |
  |  [PUMP]   -> Negative funding + oversold RSI + OI spike           |
  |  [DUMP]   -> Positive funding + overbought RSI + OI spike         |
  |  [BRKOUT] -> BB squeeze + volume explosion                        |
  |                                                                   |
  |  SCORE GUIDE                                                      |
  |  60-70 = watch it   |  70-85 = strong   |  85-100 = extreme       |
  |                                                                   |
  |  OUTPUT                                                           |
  |  [OK] Terminal  (color-coded live table)                          |
  |  [OK] Telegram  (alert on every new signal above threshold)       |
  |  [OK] CSV log   (scan_log.csv  --  append-only)                   |
  |                                                                   |
  |  For educational use only. Always use stop-losses!                |
  +-------------------------------------------------------------------+
""")
    

    scan_count = 0
    while True:
        try:
            scan_count += 1
            print(Fore.CYAN + f"\n  === SCAN #{scan_count} ===")
            run_scan()
            time.sleep(CFG["SCAN_INTERVAL_SEC"])
        except KeyboardInterrupt:
            print(Fore.YELLOW + "\n\n  Scanner stopped. Stay profitable!\n")
            break
        except Exception as e:
            print(Fore.RED    + f"\n  ERROR: {e}")
            print(Fore.YELLOW + "  Retrying in 30 seconds...\n")
            time.sleep(30)
