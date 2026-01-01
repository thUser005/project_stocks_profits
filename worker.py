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

SUMMARY_INTERVAL = 600  # ⏱ 10 minutes (in seconds)

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = dtime(9, 15)

# =====================================================
# STATE
# =====================================================
companies = load_companies()
alerted = set()
last_reset_date = None

# 📊 STATS
stats = {
    "entered": 0,
    "exited": 0,
    "target_hit": 0,
    "sl_hit": 0,
}

last_summary_ts = 0


# =====================================================
# HELPERS
# =====================================================
def maybe_reset_alerts():
    """
    Reset alerted symbols + stats once per trading day
    """
    global alerted, last_reset_date, stats, last_summary_ts

    now = datetime.now(IST)
    today = now.date()

    if now.time() >= RESET_TIME:
        if last_reset_date != today:
            alerted.clear()
            last_reset_date = today
            last_summary_ts = 0

            stats = {
                "entered": 0,
                "exited": 0,
                "target_hit": 0,
                "sl_hit": 0,
            }

            send_message("🔄 Alert & stats reset for new trading day")


def maybe_send_summary():
    """
    Send Telegram summary every 10 minutes
    """
    global last_summary_ts

    now_ts = datetime.now(IST).timestamp()

    if now_ts - last_summary_ts < SUMMARY_INTERVAL:
        return

    last_summary_ts = now_ts

    send_message(
        "📊 *Trade Summary (10 min)*\n\n"
        f"🟢 Entered: {stats['entered']}\n"
        f"🎯 Target Hit: {stats['target_hit']}\n"
        f"🛑 SL Hit: {stats['sl_hit']}\n"
        f"🚪 Exited: {stats['exited']}\n\n"
        f"⏰ Time: {datetime.now(IST).strftime('%H:%M IST')}"
    )


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
                # -------------------------------
                # Market guard
                # -------------------------------
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()
                maybe_send_summary()

                # -------------------------------
                # Fetch signals
                # -------------------------------
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

                    # 🔔 ENTRY
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
                        stats["entered"] += 1

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker loop error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
