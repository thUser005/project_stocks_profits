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
        data={"chat_id": CHAT, "text": text, "parse_mode": "Markdown"},
        timeout=5,
    )

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 50
SLEEP_INTERVAL = 5
ERROR_SLEEP = 15
MAX_RETRIES = 3

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
trade_state = {}   # symbol → trade dict
last_reset_date = None

stats = {
    "buy_entered": 0,
    "sell_entered": 0,
    "target_hit": 0,
    "sl_hit": 0,
}

# =====================================================
# HELPERS
# =====================================================
def now():
    return datetime.now(IST)

def now_str():
    return now().strftime("%H:%M:%S IST")

def in_buy_window():
    t = now().time()
    return BUY_START <= t <= BUY_END

def in_sell_window():
    t = now().time()
    return SELL_START <= t <= MARKET_END

def maybe_reset():
    global trade_state, last_reset_date, stats

    today = now().date()
    if now().time() >= RESET_TIME and last_reset_date != today:
        trade_state.clear()
        last_reset_date = today
        stats = dict.fromkeys(stats, 0)
        send_message("🔄 *Daily reset completed*")

# =====================================================
# SELL HELPERS (PRIORITY LOGIC)
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

                signals = fetch_today_signals()
                symbols = {
                    s["symbol"]: s
                    for s in signals
                    if s.get("symbol") in companies
                }

                # --------------------------------------------------
                # SELL PREP (09:15–10:00) — ONCE PER SYMBOL
                # --------------------------------------------------
                if now().time() < SELL_START:
                    for sym in symbols:
                        if sym in trade_state:
                            continue

                        _, candles = await fetch_intraday_candles(
                            session,
                            sym,
                            start_time=time(9, 15),
                            end_time=time(10, 0),
                        )
                        if not candles:
                            continue

                        high = highest_from_candles(candles)
                        entry, target, sl = compute_sell_levels(high)

                        trade_state[sym] = {
                            "type": "SELL",
                            "state": "PENDING",
                            "entry": entry,
                            "target": target,
                            "sl": sl,
                            "completed": False,
                        }

                # --------------------------------------------------
                # LIVE PRICE TRACKING
                # --------------------------------------------------
                tasks = [
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in symbols
                ]

                results = await asyncio.gather(*tasks)

                for sym, candle in results:
                    if not candle or sym not in trade_state:
                        continue

                    ltp = candle[4]
                    trade = trade_state[sym]

                    # ==============================
                    # SELL LOGIC (TOP PRIORITY)
                    # ==============================
                    if trade["type"] == "SELL" and in_sell_window():

                        # ENTRY
                        if trade["state"] == "PENDING" and ltp <= trade["entry"]:
                            trade["state"] = "ENTERED"
                            stats["sell_entered"] += 1
                            send_message(
                                f"🔴 *SELL ENTRY*\n{sym}\nEntry: {trade['entry']}\nLTP: {ltp}"
                            )

                        # EXIT (SL FIRST — PRIORITY)
                        elif trade["state"] == "ENTERED":
                            if ltp >= trade["sl"]:
                                trade["state"] = "EXITED"
                                trade["completed"] = True
                                stats["sl_hit"] += 1
                                send_message(f"🛑 *SELL SL HIT*\n{sym}\nLTP: {ltp}")

                            elif ltp <= trade["target"]:
                                trade["state"] = "EXITED"
                                trade["completed"] = True
                                stats["target_hit"] += 1
                                send_message(f"🎯 *SELL TARGET HIT*\n{sym}\nLTP: {ltp}")

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ *Worker Error*\n{e}")
                await asyncio.sleep(ERROR_SLEEP)

# =====================================================
# ENTRY
# =====================================================
if __name__ == "__main__":
    asyncio.run(run_worker())
