import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone, time as dtime

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import (
    fetch_latest_candle,
    fetch_full_day_candles,
)
from telegram_msg import send_message
from time_utils import is_market_time

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 100
SLEEP_INTERVAL = 1
ERROR_SLEEP = 15
MAX_RETRIES = 3

SUMMARY_INTERVAL = 600

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = dtime(9, 15)

# =====================================================
# STATE
# =====================================================
companies = load_companies()
last_reset_date = None

trade_state = {}   # symbol → trade info

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
def now_str():
    return datetime.now(IST).strftime("%H:%M:%S IST")


def maybe_reset_alerts():
    global trade_state, last_reset_date, stats, last_summary_ts

    now = datetime.now(IST)
    today = now.date()

    if now.time() >= RESET_TIME and last_reset_date != today:
        trade_state.clear()
        last_reset_date = today
        last_summary_ts = 0

        stats.update({
            "entered": 0,
            "exited": 0,
            "target_hit": 0,
            "sl_hit": 0,
        })

        send_message("🔄 Alert & trade state reset for new trading day")
        print(f"[{now_str()}] 🔄 Daily reset completed")


def maybe_send_summary():
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


# =====================================================
# COLD START — REPLAY FULL DAY
# =====================================================
def replay_full_day(symbol, candles, signal):
    state = "PENDING"

    for c in candles:
        high = c[2]
        low = c[3]

        if state == "PENDING":
            if high >= signal["entry"]:
                state = "ENTERED"

        elif state == "ENTERED":
            if high >= signal["target"]:
                return "EXITED", "TARGET"
            if low <= signal["stoploss"]:
                return "EXITED", "SL"

    return state, None


# =====================================================
# SAFE FETCH
# =====================================================
async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                _, candle = await fetch_latest_candle(session, symbol)
                return symbol, candle
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    return symbol, None
                await asyncio.sleep(1)


# =====================================================
# WORKER
# =====================================================
async def run_worker():
    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    send_message("🟢 Worker started")
    print(f"[{now_str()}] 🟢 Worker started")

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        while True:
            try:
                print(f"\n[{now_str()}] ▶ Cycle started")

                if not is_market_time():
                    print(f"[{now_str()}] ⛔ Market closed")
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()
                maybe_send_summary()

                signals = fetch_today_signals()
                print(f"[{now_str()}] 📥 Signals: {len(signals)}")

                # -------------------------------
                # COLD START INIT
                # -------------------------------
                for s in signals:
                    sym = s["symbol"]

                    if sym in trade_state:
                        continue

                    _, candles = await fetch_full_day_candles(
                        session,
                        sym,
                        datetime.now(IST).strftime("%Y-%m-%d"),
                    )

                    state, reason = replay_full_day(sym, candles, s)

                    trade_state[sym] = {
                        "state": state,
                        "signal": s,
                        "exit_reason": reason,
                    }

                    if state == "ENTERED":
                        stats["entered"] += 1

                    if state == "EXITED":
                        stats["exited"] += 1
                        stats["target_hit"] += (reason == "TARGET")
                        stats["sl_hit"] += (reason == "SL")

                # -------------------------------
                # LIVE TRACKING
                # -------------------------------
                active = [
                    sym for sym, t in trade_state.items()
                    if t["state"] != "EXITED"
                ]

                tasks = [
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in active
                ]

                results = await asyncio.gather(*tasks)

                for sym, candle in results:
                    if not candle:
                        continue

                    ltp = candle[4]
                    trade = trade_state[sym]
                    s = trade["signal"]

                    if trade["state"] == "PENDING":
                        if ltp >= s["entry"]:
                            trade["state"] = "ENTERED"
                            stats["entered"] += 1

                            send_message(
                                f"🟢 ENTRY\n\n"
                                f"{companies[sym]['company']}\n"
                                f"{sym}\n\n"
                                f"Entry: {s['entry']}\n"
                                f"LTP: {ltp}"
                            )

                    elif trade["state"] == "ENTERED":
                        if ltp >= s["target"]:
                            trade["state"] = "EXITED"
                            trade["exit_reason"] = "TARGET"
                            stats["exited"] += 1
                            stats["target_hit"] += 1

                            send_message(f"🎯 TARGET HIT: {sym}")

                        elif ltp <= s["stoploss"]:
                            trade["state"] = "EXITED"
                            trade["exit_reason"] = "SL"
                            stats["exited"] += 1
                            stats["sl_hit"] += 1

                            send_message(f"🛑 SL HIT: {sym}")

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                print(f"[{now_str()}] ⚠️ Worker error: {e}")
                send_message(f"⚠️ Worker error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
                
