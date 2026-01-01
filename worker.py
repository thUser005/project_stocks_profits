import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import fetch_latest_candle
from telegram_msg import send_message
from time_utils import is_market_time

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 15        # ⛔ DO NOT exceed Groww limits
SLEEP_INTERVAL = 20
ERROR_SLEEP = 15
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = (9, 15)

# =====================================================
# STATE
# =====================================================
companies = load_companies()
alerted = set()
last_reset_date = None


# =====================================================
# HELPERS
# =====================================================
def maybe_reset_alerts():
    """
    Reset alerted symbols once per trading day (after 09:15 IST)
    """
    global alerted, last_reset_date

    now = datetime.now(IST)
    today = now.date()

    if now.time() >= datetime.strptime("09:15", "%H:%M").time():
        if last_reset_date != today:
            alerted.clear()
            last_reset_date = today
            send_message("🔄 Alert state reset for new trading day")


async def fetch_latest_safe(semaphore, session, symbol):
    """
    Fetch latest candle with retry + concurrency guard
    """
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                candle = await fetch_latest_candle(session, symbol)
                return symbol, candle
            except Exception:
                if attempt == MAX_RETRIES:
                    return symbol, None
                await asyncio.sleep(1)


# =====================================================
# WORKER
# =====================================================
async def run_worker():
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        while True:
            try:
                # -------------------------------
                # Market hours guard
                # -------------------------------
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()

                # -------------------------------
                # Fetch signals (source of truth)
                # -------------------------------
                signals = fetch_today_signals()
                if not signals:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                # -------------------------------
                # Filter symbols
                # -------------------------------
                symbols = []
                signal_map = {}

                for s in signals:
                    sym = s["symbol"]
                    if sym not in companies:
                        continue
                    if sym in alerted:
                        continue

                    symbols.append(sym)
                    signal_map[sym] = s

                if not symbols:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                # -------------------------------
                # 🔥 FIRE ALL REQUESTS AT ONCE
                # -------------------------------
                tasks = [
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in symbols
                ]

                results = await asyncio.gather(*tasks)

                # -------------------------------
                # Process results
                # -------------------------------
                for sym, candle in results:
                    if not candle:
                        continue
                    if sym in alerted:
                        continue

                    s = signal_map.get(sym)
                    if not s:
                        continue

                    open_price = s["open"]
                    entry = s["entry"]
                    target = s["target"]
                    sl = s["stoploss"]

                    ltp = candle[4]

                    # 🔔 TRIGGER CONDITION
                    if ltp >= open_price:
                        meta = companies[sym]

                        msg = (
                            f"📢 STOCK TRIGGERED\n\n"
                            f"Company: {meta['company']}\n"
                            f"Symbol: {sym}\n\n"
                            f"Open: {open_price}\n"
                            f"LTP: {ltp}\n"
                            f"Entry: {entry}\n"
                            f"Target: {target}\n"
                            f"SL: {sl}"
                        )

                        send_message(msg)
                        alerted.add(sym)

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker loop error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
