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
CONCURRENCY = 50          # SAFE for Groww (do NOT exceed 15)
BATCH_SIZE = 100           # how many symbols per gather
SLEEP_INTERVAL = 30       # seconds between cycles
ERROR_SLEEP = 15
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = (9, 15)      # reset alerts daily at 09:15 IST

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


def chunked(iterable, size):
    """
    Yield list chunks of given size
    """
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


async def fetch_with_semaphore(semaphore, session, symbol):
    async with semaphore:
        return await fetch_latest_candle(session, symbol)


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
                # Fetch signals (API = source of truth)
                # -------------------------------
                signals = fetch_today_signals()
                if not signals:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                # -------------------------------
                # Filter symbols to process
                # -------------------------------
                symbols = []
                signal_map = {}

                for s in signals:
                    symbol = s["symbol"]
                    if symbol not in companies:
                        continue
                    if symbol in alerted:
                        continue

                    symbols.append(symbol)
                    signal_map[symbol] = s

                if not symbols:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                # -------------------------------
                # Process in batches
                # -------------------------------
                for batch in chunked(symbols, BATCH_SIZE):
                    retries = 0

                    while retries < MAX_RETRIES:
                        try:
                            tasks = [
                                fetch_with_semaphore(semaphore, session, sym)
                                for sym in batch
                            ]

                            results = await asyncio.gather(*tasks, return_exceptions=True)

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

                                # 🔔 CONDITION
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

                            break  # batch success → exit retry loop

                        except Exception as e:
                            retries += 1
                            if retries >= MAX_RETRIES:
                                send_message(
                                    f"⚠️ Batch failed after {MAX_RETRIES} retries\n{e}"
                                )
                            await asyncio.sleep(3)

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker loop error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
