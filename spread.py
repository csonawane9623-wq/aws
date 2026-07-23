"""
Price Spread Arbitrage Scanner
Binance Perpetuals <-> Delta Exchange India Perpetuals

Scans all common perp pairs, ranks by spread %, logs + sends Telegram alert.
"""

import asyncio
import aiohttp
import time
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ─────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

MIN_SPREAD_PCT      = float(os.getenv("MIN_SPREAD_PCT", "5.0"))   # alert threshold %
SCAN_INTERVAL_SEC   = int(os.getenv("SCAN_INTERVAL_SEC", "15"))   # seconds between scans
TOP_N_RESULTS       = int(os.getenv("TOP_N_RESULTS", "3"))        # top N spreads to log

BINANCE_TICKER_URL  = "https://fapi.binance.com/fapi/v1/ticker/price"
DELTA_TICKER_URL    = "https://api.india.delta.exchange/v2/tickers"

# ─── LOGGING ─────────────────────────────────────────────────────────────────

# Force UTF-8 on Windows console (fixes cp1252 UnicodeEncodeError)
stream_handler = logging.StreamHandler(
    stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False)
    if sys.platform == "win32" else sys.stdout
)
file_handler = logging.FileHandler("arb_scanner.log", encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[stream_handler, file_handler],
)
log = logging.getLogger(__name__)

# ─── DATA ────────────────────────────────────────────────────────────────────

@dataclass
class SpreadResult:
    symbol: str           # e.g. BTC
    binance_symbol: str   # BTCUSDT
    delta_symbol: str     # BTCUSD or BTCUSDT
    binance_price: float
    delta_price: float
    spread_pct: float     # (binance - delta) / delta * 100
    direction: str        # BUY_DELTA_SELL_BINANCE or BUY_BINANCE_SELL_DELTA

# ─── FETCH ───────────────────────────────────────────────────────────────────

async def fetch_binance_perps(session: aiohttp.ClientSession) -> dict[str, float]:
    """Returns {symbol_root: price} e.g. {'BTC': 65000.0, 'ETH': 3200.0}"""
    try:
        async with session.get(BINANCE_TICKER_URL, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
        prices = {}
        for item in data:
            sym = item["symbol"]
            if sym.endswith("USDT"):
                root = sym[:-4]          # strip USDT
                prices[root] = float(item["price"])
        return prices
    except Exception as e:
        log.warning(f"Binance fetch error: {e}")
        return {}


async def fetch_delta_perps(session: aiohttp.ClientSession) -> dict[str, float]:
    """Returns {symbol_root: price} mapped from Delta perp tickers."""
    try:
        async with session.get(DELTA_TICKER_URL, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
        prices = {}
        for item in data.get("result", []):
            # Delta perpetuals have contract_type == "perpetual_futures"
            if item.get("contract_type") not in ("perpetual_futures",):
                continue
            symbol = item.get("symbol", "")          # e.g. BTCUSDT, ETHUSD
            mark_price = item.get("mark_price")
            if not mark_price:
                continue
            # Normalise root: strip USDT or USD suffix
            if symbol.endswith("USDT"):
                root = symbol[:-4]
            elif symbol.endswith("USD"):
                root = symbol[:-3]
            else:
                continue
            prices[root] = float(mark_price)
        return prices
    except Exception as e:
        log.warning(f"Delta fetch error: {e}")
        return {}

# ─── SPREAD LOGIC ────────────────────────────────────────────────────────────

def compute_spreads(
    binance: dict[str, float],
    delta: dict[str, float],
) -> list[SpreadResult]:
    """Find common symbols and compute spread %."""
    results = []
    common = set(binance) & set(delta)
    for root in common:
        bp = binance[root]
        dp = delta[root]
        if bp <= 0 or dp <= 0:
            continue
        spread_pct = (bp - dp) / dp * 100
        direction = (
            "BUY_DELTA  → SELL_BINANCE" if spread_pct > 0
            else "BUY_BINANCE → SELL_DELTA"
        )
        results.append(SpreadResult(
            symbol=root,
            binance_symbol=f"{root}USDT",
            delta_symbol=f"{root}USDT",
            binance_price=bp,
            delta_price=dp,
            spread_pct=spread_pct,
            direction=direction,
        ))
    # Sort by absolute spread descending
    results.sort(key=lambda x: abs(x.spread_pct), reverse=True)
    return results

# ─── TELEGRAM ────────────────────────────────────────────────────────────────

async def send_telegram(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.debug("Telegram not configured, skipping alert.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                log.warning(f"Telegram error: {r.status}")
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# ─── ALERT BUILDER ───────────────────────────────────────────────────────────

def build_alert(results: list[SpreadResult], threshold: float) -> str | None:
    """Build Telegram message for spreads above threshold."""
    hits = [r for r in results if abs(r.spread_pct) >= threshold]
    if not hits:
        return None

    lines = [f"🚨 <b>ARB ALERT</b> — {datetime.now().strftime('%H:%M:%S IST')}\n"]
    lines.append(f"Pairs above {threshold}% spread:\n")
    for r in hits[:05]:
        sign = "+" if r.spread_pct > 0 else ""
        lines.append(
            f"<b>{r.symbol}</b>  {sign}{r.spread_pct:.3f}%\n"
            f"  Binance: <code>${r.binance_price:,.4f}</code>\n"
            f"  Delta:   <code>${r.delta_price:,.4f}</code>\n"
            f"  → {r.direction}\n"
        )
    return "\n".join(lines)

# ─── SCAN LOOP ───────────────────────────────────────────────────────────────

async def scan_once(session: aiohttp.ClientSession) -> list[SpreadResult]:
    binance_prices, delta_prices = await asyncio.gather(
        fetch_binance_perps(session),
        fetch_delta_perps(session),
    )

    if not binance_prices or not delta_prices:
        log.warning("One or both exchanges returned empty data.")
        return []

    results = compute_spreads(binance_prices, delta_prices)

    # ── Print top N to console / log ──
    ts = datetime.now().strftime("%H:%M:%S")
    log.info(f"-- Scan @ {ts}  |  Common pairs: {len(results)}  --")
    header = f"{'#':<3} {'Symbol':<8} {'Spread%':>8}  {'Binance':>12}  {'Delta':>12}  Direction"
    log.info(header)
    log.info("-" * len(header))
    for i, r in enumerate(results[:TOP_N_RESULTS], 1):
        sign = "+" if r.spread_pct > 0 else ""
        log.info(
            f"{i:<3} {r.symbol:<8} {sign}{r.spread_pct:>7.3f}%"
            f"  ${r.binance_price:>11,.4f}  ${r.delta_price:>11,.4f}"
            f"  {r.direction}"
        )

    # ── Telegram alert if above threshold ──
    alert = build_alert(results, MIN_SPREAD_PCT)
    if alert:
        await send_telegram(session, alert)
        log.info(f"[ALERT] Telegram sent ({len([r for r in results if abs(r.spread_pct) >= MIN_SPREAD_PCT])} pairs above {MIN_SPREAD_PCT}%)")

    return results


async def main():
    log.info("=" * 60)
    log.info("  Binance <-> Delta Exchange India  |  Perp Spread Scanner")
    log.info(f"  Threshold: {MIN_SPREAD_PCT}%  |  Interval: {SCAN_INTERVAL_SEC}s")
    log.info("=" * 60)

    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            t0 = time.monotonic()
            try:
                await scan_once(session)
            except Exception as e:
                log.error(f"Scan error: {e}")
            elapsed = time.monotonic() - t0
            wait = max(0, SCAN_INTERVAL_SEC - elapsed)
            await asyncio.sleep(wait)


if __name__ == "__main__":
    asyncio.run(main())
