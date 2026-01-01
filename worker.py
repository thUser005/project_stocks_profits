import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone, time as dtime

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import fetch_latest_candle
from telegram_msg import send_message
from time_utils import is_market_time

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 15
SLEEP_INTERVAL = 20
ERROR_SLEEP = 15
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = dtime(9, 15)

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
    Reset alerted symbols once per trading day (after RESET_TIME)
    """
    global alerted, last_reset_date

    now = datetime.now(IST)
    today = now.date()

    if now.time() >= RESET_TIME:
        if last_reset_date != today:
            alerted.clear()
            last_reset_date = today
            send_message("🔄 Alert state reset for new trading day")


async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                candle = await fetch_latest_candle(session, symbol)
                return symbol, candle
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    return symbol, None
                await asyncio.sleep(1)


# =====================================================
# WORKER
# =====================================================
async def run_worker():
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    send_message("🟢 Worker started")

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        while True:
            try:
                # Market hours guard
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()

                # Fetch signals
                signals = fetch_today_signals()
                if not signals:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                symbols = []
                signal_map = {}

                for s in signals:
                    sym = s.get("symbol")
                    if not sym or sym not in companies or sym in alerted:
                        continue

                    symbols.append(sym)
                    signal_map[sym] = s

                if not symbols:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                tasks = [
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in symbols
                ]

                results = await asyncio.gather(*tasks)

                for sym, candle in results:
                    if not candle or len(candle) < 5 or sym in alerted:
                        continue

                    s = signal_map.get(sym)
                    if not s:
                        continue

                    ltp = candle[4]
                    if ltp is None:
                        continue

                    if ltp >= s["open"]:
                        meta = companies[sym]

                        send_message(
                            f"📢 STOCK TRIGGERED\n\n"
                            f"Company: {meta['company']}\n"
                            f"Symbol: {sym}\n\n"
                            f"Open: {s['open']}\n"
                            f"LTP: {ltp}\n"
                            f"Entry: {s['entry']}\n"
                            f"Target: {s['target']}\n"
                            f"SL: {s['stoploss']}"
                        )

                        alerted.add(sym)

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker loop error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
