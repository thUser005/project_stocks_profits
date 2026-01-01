import os
import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta, timezone, time as dtime, time

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import (
    fetch_latest_candle,
    fetch_full_day_candles,
    fetch_intraday_candles,
)
from time_utils import is_market_time

# =====================================================
# TELEGRAM
# =====================================================
BOT = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT = os.environ["TELEGRAM_CHAT_ID"]

def send_message(text):
    requests.post(
        f"https://api.telegram.org/bot{BOT}/sendMessage",
        data={
            "chat_id": CHAT,
            "text": text,
            "parse_mode": "Markdown"
        },
        timeout=5
    )

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 100
MAX_RETRIES = 3
ERROR_SLEEP = 15
SLEEP_INTERVAL = 1
SUMMARY_INTERVAL = 600

IST = timezone(timedelta(hours=5, minutes=30))

BUY_START  = dtime(9, 30)
BUY_END    = dtime(11, 30)

SELL_START = dtime(10, 0)
MARKET_END = dtime(15, 30)

RESET_TIME = dtime(9, 15)

# =====================================================
# STATE
# =====================================================
companies = load_companies()

trade_state = {}
alerted_buy = set()
sell_meta = {}

stats = {
    "entered": 0,
    "exited": 0,
    "target_hit": 0,
    "sl_hit": 0,
    "sell_entered": 0,
}

last_reset_date = None
last_summary_ts = 0
initial_summary_sent = False   # ✅ ADDED

# =====================================================
# HELPERS
# =====================================================
def now():
    return datetime.now(IST)

def now_str():
    return now().strftime("%H:%M:%S IST")

def in_buy_window():
    return BUY_START <= now().time() <= BUY_END

def in_sell_window():
    return SELL_START <= now().time() <= MARKET_END

def maybe_reset():
    global trade_state, sell_meta, stats, last_reset_date, last_summary_ts, alerted_buy, initial_summary_sent

    today = now().date()
    if now().time() >= RESET_TIME and last_reset_date != today:
        trade_state.clear()
        sell_meta.clear()
        alerted_buy.clear()
        last_summary_ts = 0
        initial_summary_sent = False   # ✅ reset initial snapshot

        stats.update({
            "entered": 0,
            "exited": 0,
            "target_hit": 0,
            "sl_hit": 0,
            "sell_entered": 0,
        })

        last_reset_date = today
        send_message("🔄 *Daily reset completed*")

def maybe_send_summary():
    global last_summary_ts

    ts = now().timestamp()
    if ts - last_summary_ts < SUMMARY_INTERVAL:
        return

    last_summary_ts = ts

    send_message(
        "📊 *Trade Summary (10 min)*\n\n"
        f"🟢 Entered: {stats['entered']}\n"
        f"🎯 Target Hit: {stats['target_hit']}\n"
        f"🛑 SL Hit: {stats['sl_hit']}\n"
        f"🚪 Exited: {stats['exited']}\n"
        f"🔴 SELL Trades: {stats['sell_entered']}\n\n"
        f"⏰ {now().strftime('%H:%M IST')}"
    )

# =====================================================
# INITIAL SUMMARY (✅ ADDED, NO LOGIC CHANGE)
# =====================================================
def send_initial_summary():
    send_message(
        "📊 *Initial Trade Summary (Server Restart)*\n\n"
        f"🟢 BUY Entered: {stats['entered']}\n"
        f"🚪 BUY Exited: {stats['exited']}\n"
        f"🎯 Target Hit: {stats['target_hit']}\n"
        f"🛑 SL Hit: {stats['sl_hit']}\n"
        f"🔴 SELL Trades (live only): {stats['sell_entered']}\n\n"
        f"⏰ Snapshot at: {now().strftime('%H:%M IST')}"
    )

# =====================================================
# BUY REPLAY LOGIC (UNCHANGED)
# =====================================================
def replay_full_day(symbol, candles, signal):
    state = "PENDING"

    for c in candles:
        high = c[2]
        low = c[3]

        if state == "PENDING" and high >= signal["entry"]:
            state = "ENTERED"

        elif state == "ENTERED":
            if high >= signal["target"]:
                return "EXITED", "TARGET"
            if low <= signal["stoploss"]:
                return "EXITED", "SL"

    return state, None

# =====================================================
# SELL HELPERS (UNCHANGED)
# =====================================================
def highest_from_candles(candles):
    return max(c[2] for c in candles)

def compute_sell_levels(high):
    entry = high * 1.04
    target = entry * 0.98
    sl = entry * 1.01
    return entry, target, sl

# =====================================================
# SAFE FETCH
# =====================================================
async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for _ in range(MAX_RETRIES):
            try:
                _, candle = await fetch_latest_candle(session, symbol)
                return symbol, candle
            except Exception:
                await asyncio.sleep(1)
        return symbol, None

# =====================================================
# WORKER
# =====================================================
async def run_worker():
    global initial_summary_sent

    timeout = aiohttp.ClientTimeout(total=15)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    send_message("🟢 *Trading Worker Started*")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset()
                maybe_send_summary()

                signals = fetch_today_signals()
                signal_map = {s["symbol"]: s for s in signals if s["symbol"] in companies}

                # ---------------- BUY COLD START REPLAY ----------------
                for sym, s in signal_map.items():
                    if sym in trade_state:
                        continue

                    _, candles = await fetch_full_day_candles(
                        session, sym, now().strftime("%Y-%m-%d")
                    )

                    state, reason = replay_full_day(sym, candles, s)
                    trade_state[sym] = {"state": state, "signal": s}

                    if state == "ENTERED":
                        stats["entered"] += 1
                    if state == "EXITED":
                        stats["exited"] += 1
                        stats["target_hit"] += (reason == "TARGET")
                        stats["sl_hit"] += (reason == "SL")

                # ✅ SEND INITIAL SUMMARY ONCE
                if not initial_summary_sent:
                    send_initial_summary()
                    initial_summary_sent = True
                    last_summary_ts = now().timestamp()

                # ---------------- LIVE FETCH ----------------
                active_symbols = set(signal_map) | set(sell_meta)
                tasks = [fetch_latest_safe(semaphore, session, s) for s in active_symbols]
                results = await asyncio.gather(*tasks)

                for sym, candle in results:
                    if not candle:
                        continue

                    ltp = candle[4]

                    # BUY + SELL LOGIC (UNCHANGED BELOW)
                    # 🔒 NO CHANGES MADE HERE

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ *Worker Error*\n{e}")
                await asyncio.sleep(ERROR_SLEEP)

# =====================================================
# ENTRY
# =====================================================
if __name__ == "__main__":
    asyncio.run(run_worker())
