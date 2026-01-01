import asyncio
import aiohttp
import time
from datetime import datetime, timedelta, timezone, time as dtime

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import (
    fetch_latest_candle,
    fetch_intraday_candles,
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

# 🔥 Cold start progress config (ADDED)
COLD_PROGRESS_EVERY_SYMBOLS = 100
COLD_PROGRESS_EVERY_SECONDS = 120

IST = timezone(timedelta(hours=5, minutes=30))

RESET_TIME = dtime(9, 15)

BUY_START = dtime(9, 30)
BUY_END   = dtime(11, 30)

SELL_START = dtime(10, 0)
SELL_END   = dtime(15, 30)

# =====================================================
# STATE
# =====================================================
companies = load_companies()
last_reset_date = None

trade_state = {}   # BUY trades
sell_state = {}    # SELL trades

stats = {
    "entered": 0,
    "exited": 0,
    "target_hit": 0,
    "sl_hit": 0,
}

last_summary_ts = 0

# 🔥 Cold start state (ADDED)
cold_start_done = False
cold_start_events = []
cold_start_last_progress_ts = 0
cold_start_task_started = False

# 🔥 Fetch statistics (ADDED)
fetch_stats = {
    "success": 0,
    "empty": 0,
    "failed": 0,
}

# =====================================================
# HELPERS
# =====================================================
def now_str():
    return datetime.now(IST).strftime("%H:%M:%S IST")


def log(msg):
    print(f"[{now_str()}] {msg}")


def safe_send_message(text):
    try:
        send_message(text)
    except Exception as e:
        log(f"❌ Telegram send failed: {e}")


def is_buy_time():
    return BUY_START <= datetime.now(IST).time() <= BUY_END


def is_sell_time():
    return SELL_START <= datetime.now(IST).time() <= SELL_END


def send_trade_message(title, data: dict):
    lines = [f"📊 *{title}*\n"]
    for k, v in data.items():
        lines.append(f"{k:<14}: {v}")
    safe_send_message("\n".join(lines))


def maybe_reset_alerts():
    global trade_state, sell_state, last_reset_date, stats, last_summary_ts

    now = datetime.now(IST)
    today = now.date()

    if now.time() >= RESET_TIME and last_reset_date != today:
        trade_state.clear()
        sell_state.clear()
        last_reset_date = today
        last_summary_ts = 0

        stats.update({
            "entered": 0,
            "exited": 0,
            "target_hit": 0,
            "sl_hit": 0,
        })

        safe_send_message("🔄 Trade state reset for new trading day")
        log("Daily reset completed")


def maybe_send_summary():
    global last_summary_ts

    if not cold_start_done:
        return

    now_ts = datetime.now(IST).timestamp()
    if now_ts - last_summary_ts < SUMMARY_INTERVAL:
        return

    last_summary_ts = now_ts

    safe_send_message(
        "📊 *Trade Summary (10 min)*\n\n"
        f"🟢 Entered: {stats['entered']}\n"
        f"🎯 Target Hit: {stats['target_hit']}\n"
        f"🛑 SL Hit: {stats['sl_hit']}\n"
        f"🚪 Exited: {stats['exited']}\n\n"
        f"⏰ Time: {now_str()}"
    )

# =====================================================
# SELL SETUP (UNCHANGED)
# =====================================================
async def calculate_sell_setup(session, symbol):
    _, candles = await fetch_intraday_candles(
        session,
        symbol,
        start_time=dtime(9, 15),
        end_time=dtime(10, 0),
    )

    if not candles:
        return None

    highest = max(c[2] for c in candles)
    entry = round(highest * 1.04, 2)

    return {
        "entry": entry,
        "target": round(entry * 0.98, 2),
        "stoploss": round(entry * 1.01, 2),
        "state": "WAITING",
    }

# =====================================================
# SAFE FETCH (COUNTING ADDED)
# =====================================================
async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for _ in range(MAX_RETRIES):
            try:
                _, candle = await fetch_latest_candle(session, symbol)
                if candle:
                    fetch_stats["success"] += 1
                    return symbol, candle
                else:
                    fetch_stats["empty"] += 1
            except Exception as e:
                fetch_stats["failed"] += 1
                log(f"⚠️ Candle fetch failed for {symbol}: {e}")
                await asyncio.sleep(1)
        return symbol, None

# =====================================================
# 🔥 COLD START REPLAY (UNCHANGED LOGIC)
# =====================================================
async def replay_symbol(session, sym, signal):
    events = []

    _, candles = await fetch_intraday_candles(
        session,
        sym,
        start_time=dtime(9, 15),
        end_time=datetime.now(IST).time(),
    )

    if not candles:
        return events

    buy_state = "PENDING"

    for c in candles:
        ts = datetime.fromtimestamp(c[0] / 1000, IST)
        high, low = c[2], c[3]

        if buy_state == "PENDING" and BUY_START <= ts.time() <= BUY_END:
            if high >= signal["entry"]:
                buy_state = "ENTERED"
                events.append((ts, sym, "BUY ENTRY", signal["entry"]))

        elif buy_state == "ENTERED":
            if high >= signal["target"]:
                events.append((ts, sym, "BUY TARGET HIT", signal["target"]))
                break
            if low <= signal["stoploss"]:
                events.append((ts, sym, "BUY SL HIT", signal["stoploss"]))
                break

    setup = await calculate_sell_setup(session, sym)
    if not setup:
        return events

    sell_state_local = "WAITING"

    for c in candles:
        ts = datetime.fromtimestamp(c[0] / 1000, IST)
        high, low = c[2], c[3]

        if ts.time() < SELL_START:
            continue

        if sell_state_local == "WAITING" and low <= setup["entry"]:
            sell_state_local = "ENTERED"
            events.append((ts, sym, "SELL ENTRY", setup["entry"]))

        elif sell_state_local == "ENTERED":
            if low <= setup["target"]:
                events.append((ts, sym, "SELL TARGET HIT", setup["target"]))
                break
            if high >= setup["stoploss"]:
                events.append((ts, sym, "SELL SL HIT", setup["stoploss"]))
                break

    return events


def send_cold_start_summary(events):
    if not events:
        safe_send_message("ℹ️ No trades hit before server start")
        return

    lines = ["📊 *COLD START SUMMARY*\n"]

    for ts, sym, event, price in events:
        lines.append(
            f"{ts.strftime('%H:%M')} | {sym}\n"
            f"Event : {event}\n"
            f"Price : {price}\n"
        )

    safe_send_message("\n".join(lines))

# =====================================================
# 🔥 BACKGROUND COLD START TASK (ADDED)
# =====================================================
async def cold_start_task(session, signals):
    global cold_start_done, cold_start_last_progress_ts

    log("Cold start replay started")
    safe_send_message("⏪ Cold start replay started…")

    total = len(signals)
    processed = 0
    detected = 0
    cold_start_last_progress_ts = time.time()

    for s in signals:
        try:
            events = await replay_symbol(session, s["symbol"], s)
            cold_start_events.extend(events)

            processed += 1
            detected += len(events)

            now_ts = time.time()
            if (
                processed % COLD_PROGRESS_EVERY_SYMBOLS == 0
                or now_ts - cold_start_last_progress_ts >= COLD_PROGRESS_EVERY_SECONDS
            ):
                safe_send_message(
                    "⏳ *Cold start in progress…*\n\n"
                    f"Processed : {processed} / {total}\n"
                    f"Events    : {detected}\n"
                    f"Time      : {now_str()}"
                )
                log(f"Cold start progress {processed}/{total}, events={detected}")
                cold_start_last_progress_ts = now_ts

        except Exception as e:
            log(f"❌ Cold start error for {s['symbol']}: {e}")

    safe_send_message(
        "✅ *Cold start replay completed*\n\n"
        f"Total symbols : {total}\n"
        f"Total events  : {detected}\n"
        f"Completed at  : {now_str()}"
    )

    send_cold_start_summary(cold_start_events)
    cold_start_done = True
    log("Cold start replay completed")

# =====================================================
# WORKER
# =====================================================
async def run_worker():
    global cold_start_task_started

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    safe_send_message("🟢 Worker started")
    log("Worker loop started")

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()

                signals = fetch_today_signals()

                # 🔥 Start cold start ONCE in background
                if not cold_start_task_started:
                    asyncio.create_task(cold_start_task(session, signals))
                    cold_start_task_started = True

                maybe_send_summary()

                # ---------------- INIT ----------------
                for s in signals:
                    sym = s["symbol"]

                    if sym not in trade_state:
                        trade_state[sym] = {"state": "PENDING", "signal": s}

                    if sym not in sell_state:
                        setup = await calculate_sell_setup(session, sym)
                        if setup:
                            sell_state[sym] = setup

                active = set(trade_state) | set(sell_state)

                results = await asyncio.gather(*[
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in active
                ])

                for sym, candle in results:
                    if not candle:
                        continue

                    ltp = candle[4]

                    # BUY (UNCHANGED)
                    buy = trade_state.get(sym)
                    if buy:
                        s = buy["signal"]

                        if buy["state"] == "PENDING" and is_buy_time() and ltp >= s["entry"]:
                            buy["state"] = "ENTERED"
                            stats["entered"] += 1
                            send_trade_message("BUY ENTRY", {
                                "Symbol": sym,
                                "Entry": s["entry"],
                                "LTP": ltp,
                                "Time": now_str(),
                            })

                        elif buy["state"] == "ENTERED":
                            if ltp >= s["target"]:
                                buy["state"] = "EXITED"
                                stats["exited"] += 1
                                stats["target_hit"] += 1
                                send_trade_message("BUY TARGET HIT", {
                                    "Symbol": sym,
                                    "Exit LTP": ltp,
                                })

                            elif ltp <= s["stoploss"]:
                                buy["state"] = "EXITED"
                                stats["exited"] += 1
                                stats["sl_hit"] += 1
                                send_trade_message("BUY SL HIT", {
                                    "Symbol": sym,
                                    "Exit LTP": ltp,
                                })

                    # SELL (UNCHANGED)
                    sell = sell_state.get(sym)
                    if sell and is_sell_time():

                        if sell["state"] == "WAITING" and ltp <= sell["entry"]:
                            sell["state"] = "ENTERED"
                            send_trade_message("SELL ENTRY", {
                                "Symbol": sym,
                                "Entry": sell["entry"],
                                "LTP": ltp,
                            })

                        elif sell["state"] == "ENTERED":
                            if ltp <= sell["target"]:
                                sell["state"] = "EXITED"
                                send_trade_message("SELL TARGET HIT", {
                                    "Symbol": sym,
                                    "Exit LTP": ltp,
                                })

                            elif ltp >= sell["stoploss"]:
                                sell["state"] = "EXITED"
                                send_trade_message("SELL SL HIT", {
                                    "Symbol": sym,
                                    "Exit LTP": ltp,
                                })

                # 🔍 Periodic fetch stats log
                log(
                    f"Fetch stats | "
                    f"Success={fetch_stats['success']} "
                    f"Empty={fetch_stats['empty']} "
                    f"Failed={fetch_stats['failed']}"
                )

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                safe_send_message(f"⚠️ Worker error:\n{e}")
                log(f"Worker exception: {e}")
                await asyncio.sleep(ERROR_SLEEP)
