import asyncio
import aiohttp
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

# 🔥 COLD START STATE (NEW)
cold_start_done = False
cold_start_events = []


# =====================================================
# HELPERS
# =====================================================
def now_str():
    return datetime.now(IST).strftime("%H:%M:%S IST")


def is_buy_time():
    return BUY_START <= datetime.now(IST).time() <= BUY_END


def is_sell_time():
    return SELL_START <= datetime.now(IST).time() <= SELL_END


def send_trade_message(title, data: dict):
    lines = [f"📊 *{title}*\n"]
    for k, v in data.items():
        lines.append(f"{k:<14}: {v}")
    send_message("\n".join(lines))


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

        send_message("🔄 Trade state reset for new trading day")


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
# SAFE FETCH (UNCHANGED)
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
# 🔥 COLD START REPLAY (NEW)
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

    # ---- BUY REPLAY ----
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

    # ---- SELL REPLAY ----
    setup = await calculate_sell_setup(session, sym)
    if not setup:
        return events

    sell_state = "WAITING"

    for c in candles:
        ts = datetime.fromtimestamp(c[0] / 1000, IST)
        high, low = c[2], c[3]

        if ts.time() < SELL_START:
            continue

        if sell_state == "WAITING" and low <= setup["entry"]:
            sell_state = "ENTERED"
            events.append((ts, sym, "SELL ENTRY", setup["entry"]))

        elif sell_state == "ENTERED":
            if low <= setup["target"]:
                events.append((ts, sym, "SELL TARGET HIT", setup["target"]))
                break
            if high >= setup["stoploss"]:
                events.append((ts, sym, "SELL SL HIT", setup["stoploss"]))
                break

    return events


def send_cold_start_summary(events):
    if not events:
        send_message("ℹ️ No trades hit before server start")
        return

    lines = ["📊 *COLD START SUMMARY*\n"]

    for ts, sym, event, price in events:
        lines.append(
            f"{ts.strftime('%H:%M')} | {sym}\n"
            f"Event : {event}\n"
            f"Price : {price}\n"
        )

    send_message("\n".join(lines))


# =====================================================
# WORKER
# =====================================================
async def run_worker():
    global cold_start_done

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    send_message("🟢 Worker started")

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()
                maybe_send_summary()

                signals = fetch_today_signals()

                # 🔥 COLD START (RUN ONCE)
                if not cold_start_done:
                    send_message("⏪ Replaying trades since market open...")
                    for s in signals:
                        cold_start_events.extend(
                            await replay_symbol(session, s["symbol"], s)
                        )
                    send_cold_start_summary(cold_start_events)
                    cold_start_done = True

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

                    # ===== BUY =====
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

                    # ===== SELL =====
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

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
