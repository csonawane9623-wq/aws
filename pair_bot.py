"""
ETC/USDT ↔ ETH/USDT  │  Pairs Trading Bot
==========================================
Strategy  : Z-score mean reversion on ETC/ETH spread
Signals   : ENTER when |Z| > 2σ  │  EXIT when |Z| < 0.5σ
Alerts    : Telegram bot message
Schedule  : Polls Binance every 5 minutes (on the candle close)

Setup
-----
1. Install deps:
       pip install requests pandas numpy scipy python-telegram-bot schedule

2. Fill in your credentials in the CONFIG block below.

3. Get a Telegram bot:
   - Message @BotFather on Telegram → /newbot → copy the token
   - Message your new bot once, then run:
       python -c "import requests; print(requests.get('https://api.telegram.org/bot<TOKEN>/getUpdates').json())"
   - Copy your chat_id from the response.

4. Run:
       python pairs_bot.py
"""

import time
import logging
import requests
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  — edit these values
# ═══════════════════════════════════════════════════════════════════════════════
TELEGRAM_TOKEN = "7705635599:AAFWLyKGy9soySHtYN_Hlf68GTIvgWhrJqk"
TELEGRAM_CHAT_ID = "1184234885"        # your personal or group chat id

SYMBOL_A         = "ETCUSDT"                   # legs of the pair
SYMBOL_B         = "ETHUSDT"
INTERVAL         = "15m"
LOOKBACK         = 400                          # candles used to compute mean/std

ENTRY_Z          = 2.0                          # |Z| threshold to open a trade
EXIT_Z           = 0.5                          # |Z| threshold to close a trade
STOP_Z           = 3.5                          # |Z| hard stop-loss threshold

POLL_SECONDS     = 300                          # 300 s = 5 minutes
# ═══════════════════════════════════════════════════════════════════════════════

BINANCE_BASE = "https://api.binance.com/api/v3"

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


# ── Binance ───────────────────────────────────────────────────────────────────
def fetch_klines(symbol: str, limit: int = LOOKBACK) -> pd.Series:
    """Return closing prices as a Series indexed by UTC timestamp."""
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    raw = r.json()
    closes = pd.Series(
        [float(k[4]) for k in raw],
        index=pd.to_datetime([k[0] for k in raw], unit="ms", utc=True),
        name=symbol,
    )
    return closes


# ── Spread / Z-score ──────────────────────────────────────────────────────────
def compute_zscore(closes_a: pd.Series, closes_b: pd.Series):
    """
    OLS hedge ratio:  spread = A - beta * B
    Z-score of the spread over the lookback window.
    """
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


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram ✓ message sent")
        return True
    except Exception as e:
        log.error(f"Telegram ✗ {e}")
        return False


def fmt_signal_message(signal_type: str, data: dict, position: dict | None) -> str:
    ts    = data["timestamp"].strftime("%Y-%m-%d %H:%M UTC")
    z     = data["z_score"]
    ratio = data["ratio"]
    pa    = data["price_a"]
    pb    = data["price_b"]

    ICONS = {
        "LONG_A":   "🟢 ENTRY",
        "SHORT_A":  "🔴 ENTRY",
        "EXIT":     "⚪ EXIT",
        "STOP":     "🛑 STOP-LOSS",
        "HEARTBEAT":"💓 STATUS",
    }
    icon = ICONS.get(signal_type, "ℹ️")

    direction = ""
    if signal_type == "LONG_A":
        direction = (
            f"\n📌 <b>Direction:</b>  LONG {SYMBOL_A} / SHORT {SYMBOL_B}\n"
            f"   (spread below mean → expect reversion upward)"
        )
    elif signal_type == "SHORT_A":
        direction = (
            f"\n📌 <b>Direction:</b>  SHORT {SYMBOL_A} / LONG {SYMBOL_B}\n"
            f"   (spread above mean → expect reversion downward)"
        )
    elif signal_type in ("EXIT", "STOP") and position:
        pnl_emoji = "✅" if position.get("unrealised_pnl", 0) >= 0 else "❌"
        direction = (
            f"\n📌 <b>Close position:</b>  {position['type']}\n"
            f"   Entry Z: <code>{position['entry_z']:.3f}</code>  →  "
            f"Exit Z: <code>{z:.3f}</code>\n"
            f"   {pnl_emoji} Spread PnL: <code>{position.get('unrealised_pnl', 0):+.6f}</code>"
        )

    msg = (
        f"{icon} <b>{signal_type.replace('_', ' ')}</b>  |  {ts}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Z-score:</b>  <code>{z:+.4f}</code>\n"
        f"📉 <b>Spread:</b>   <code>{data['spread_now']:+.6f}</code>  "
        f"(mean <code>{data['spread_mean']:.6f}</code>  "
        f"±<code>{data['spread_std']:.6f}</code>)\n"
        f"⚖️  <b>Ratio:</b>    <code>{ratio:.6f}</code>\n"
        f"💰 <b>{SYMBOL_A}:</b>  <code>${pa:.4f}</code>\n"
        f"💰 <b>{SYMBOL_B}:</b>  <code>${pb:.2f}</code>\n"
        f"📐 <b>Beta:</b>     <code>{data['beta']:.5f}</code>"
        f"{direction}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Lookback: {LOOKBACK} × {INTERVAL} candles</i>"
    )
    return msg


# ── State machine ─────────────────────────────────────────────────────────────
class Position:
    def __init__(self, sig_type: str, entry_z: float, entry_spread: float):
        self.type         = sig_type    # "LONG_A" | "SHORT_A"
        self.entry_z      = entry_z
        self.entry_spread = entry_spread
        self.opened_at    = datetime.now(timezone.utc)

    def unrealised_pnl(self, current_spread: float) -> float:
        """Rough spread PnL (positive = profitable direction)."""
        if self.type == "LONG_A":
            return current_spread - self.entry_spread   # profit if spread rises
        else:
            return self.entry_spread - current_spread   # profit if spread falls


class PairsBot:
    def __init__(self):
        self.position: Position | None = None
        self.iteration   = 0
        self.last_z      = 0.0

    # ── main tick ──────────────────────────────────────────────────────────
    def tick(self):
        self.iteration += 1
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        log.info(f"── tick #{self.iteration}  {now} ──")

        # 1. Fetch data
        try:
            closes_a = fetch_klines(SYMBOL_A)
            closes_b = fetch_klines(SYMBOL_B)
        except Exception as e:
            log.error(f"Data fetch failed: {e}")
            return

        # 2. Compute z-score
        try:
            data = compute_zscore(closes_a, closes_b)
        except Exception as e:
            log.error(f"Z-score calculation failed: {e}")
            return

        z = data["z_score"]
        log.info(
            f"Z={z:+.4f}  spread={data['spread_now']:+.6f}  "
            f"ratio={data['ratio']:.6f}  pos={'FLAT' if not self.position else self.position.type}"
        )

        # 3. Signal logic
        signal = None

        if self.position is None:
            # ── ENTRY ──────────────────────────────────────────────────────
            if z <= -ENTRY_Z:
                signal = "LONG_A"   # spread too low → long ETC, short ETH
            elif z >= ENTRY_Z:
                signal = "SHORT_A"  # spread too high → short ETC, long ETH

        else:
            pnl = self.position.unrealised_pnl(data["spread_now"])
            data["unrealised_pnl"] = pnl

            # ── STOP-LOSS ──────────────────────────────────────────────────
            if abs(z) >= STOP_Z:
                signal = "STOP"

            # ── EXIT ───────────────────────────────────────────────────────
            elif (
                (self.position.type == "LONG_A"  and z >= -EXIT_Z) or
                (self.position.type == "SHORT_A" and z <=  EXIT_Z)
            ):
                signal = "EXIT"

        # 4. Act on signal
        if signal:
            msg = fmt_signal_message(signal, data,
                                     self.position.__dict__ if self.position else None)
            log.info(f"SIGNAL → {signal}")
            send_telegram(msg)

            if signal in ("LONG_A", "SHORT_A"):
                self.position = Position(signal, z, data["spread_now"])
            elif signal in ("EXIT", "STOP"):
                self.position = None

        # 5. Hourly heartbeat (every 12 ticks × 5 min = 60 min)
        elif self.iteration % 1 == 0:
            if self.position:
                data["unrealised_pnl"] = self.position.unrealised_pnl(data["spread_now"])
            msg = fmt_signal_message("HEARTBEAT", data, None)
            send_telegram(msg)

        self.last_z = z

    # ── run loop ───────────────────────────────────────────────────────────
    def run(self):
        log.info("═" * 55)
        log.info(f"  Pairs Bot starting")
        log.info(f"  Pair     : {SYMBOL_A} / {SYMBOL_B}")
        log.info(f"  Interval : {INTERVAL}  |  Lookback: {LOOKBACK} candles")
        log.info(f"  Entry Z  : ±{ENTRY_Z}  |  Exit Z: ±{EXIT_Z}  |  Stop Z: ±{STOP_Z}")
        log.info("═" * 55)

        # Send startup message
        send_telegram(
            f"🤖 <b>Pairs Bot started</b>\n"
            f"Pair: <code>{SYMBOL_A} / {SYMBOL_B}</code>\n"
            f"Interval: <code>{INTERVAL}</code>  Lookback: <code>{LOOKBACK}</code> candles\n"
            f"Entry: <code>±{ENTRY_Z}σ</code>  Exit: <code>±{EXIT_Z}σ</code>  Stop: <code>±{STOP_Z}σ</code>\n"
            f"Polling every {POLL_SECONDS // 60} min ⏱"
        )

        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                log.info("Stopped by user.")
                send_telegram("🛑 <b>Pairs Bot stopped</b> (keyboard interrupt)")
                break
            except Exception as e:
                log.exception(f"Unexpected error: {e}")

            # Wait until next 5-min boundary (or POLL_SECONDS if preferred)
            log.info(f"Sleeping {POLL_SECONDS}s until next candle …\n")
            time.sleep(POLL_SECONDS)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = PairsBot()
    bot.run()
