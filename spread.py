"""
PAXG/USD vs XAUT/USD spread alert bot (Delta Exchange India)
--------------------------------------------------------------
- Polls PAXGUSD and XAUTUSD perpetual futures mark prices from Delta India.
- If |spread| > SPREAD_THRESHOLD_USD -> sends Telegram alert.
- After an alert fires, next alert for the SAME ongoing condition is
  suppressed for ALERT_COOLDOWN_SECONDS (15 min default), but price
  polling continues every POLL_INTERVAL_SECONDS the whole time.
- Runs forever in a while loop with basic error handling/retry.

Setup:
    pip install requests python-dotenv

Create a .env file next to this script:
    TELEGRAM_BOT_TOKEN=123456:ABC-your-bot-token
    TELEGRAM_CHAT_ID=123456789

Get TELEGRAM_CHAT_ID by messaging your bot once, then hitting:
    https://api.telegram.org/bot<TOKEN>/getUpdates
"""

import os
import time
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config ----------------
DELTA_BASE_URL = "https://api.india.delta.exchange"
SYMBOL_1 = "PAXGUSD"
SYMBOL_2 = "XAUTUSD"

SPREAD_THRESHOLD_USD = 10     # alert trigger level
POLL_INTERVAL_SECONDS = 2       # how often to check prices
ALERT_COOLDOWN_SECONDS = 20 * 60  # 15 min between repeat alerts while condition persists
REQUEST_TIMEOUT = 10

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("spread_alert")


# ---------------- Helpers ----------------
def get_mark_price(symbol: str) -> float:
    """Fetch mark price for a given Delta India ticker symbol."""
    url = f"{DELTA_BASE_URL}/v2/tickers/{symbol}"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("result", {})
    mark_price = result.get("mark_price") or result.get("close")
    if mark_price is None:
        raise ValueError(f"No mark_price/close field found for {symbol}: {result}")
    return float(mark_price)


def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing).")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        log.info("Telegram alert sent.")
    except requests.RequestException as e:
        log.error(f"Failed to send Telegram alert: {e}")


def format_alert(paxg: float, xaut: float, spread: float) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"⚠️ <b>PAXG/XAUT Spread Alert</b>\n"
        f"Time: {ts}\n"
        f"PAXGUSD: {paxg:.2f}\n"
        f"XAUTUSD: {xaut:.2f}\n"
        f"Spread: <b>{spread:.2f} USD</b> (threshold {SPREAD_THRESHOLD_USD:.2f})"
    )


# ---------------- Main loop ----------------
def main():
    log.info("Starting PAXG/XAUT spread monitor...")
    last_alert_time = 0.0  # epoch seconds of last alert sent

    while True:
        try:
            paxg_price = get_mark_price(SYMBOL_1)
            xaut_price = get_mark_price(SYMBOL_2)
            spread = paxg_price - xaut_price
            abs_spread = abs(spread)

            log.info(
                f"PAXGUSD={paxg_price:.2f}  XAUTUSD={xaut_price:.2f}  "
                f"spread={spread:+.2f}"
            )

            now = time.time()

            if abs_spread > SPREAD_THRESHOLD_USD:
                since_last_alert = now - last_alert_time
                if since_last_alert >= ALERT_COOLDOWN_SECONDS:
                    msg = format_alert(paxg_price, xaut_price, spread)
                    send_telegram(msg)
                    last_alert_time = now
                else:
                    remaining = ALERT_COOLDOWN_SECONDS - since_last_alert
                    log.info(
                        f"Spread condition active but in cooldown "
                        f"({remaining/60:.1f} min remaining before next alert)."
                    )

        except requests.RequestException as e:
            log.error(f"Network/API error: {e}")
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
