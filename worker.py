import asyncio
import aiohttp
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import fetch_latest_candle
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

ANALYZED_APIS = [
    "https://g1-stock.vercel.app/api/analyze-signals",
    "https://g2-stock.vercel.app/api/analyze-signals",
]

# =====================================================
# STATE
# =====================================================
companies = load_companies()
last_reset_date = None

trade_state = {}

stats = {
    "entered": 0,
    "exited": 0,
    "target_hit": 0,
    "sl_hit": 0,
}

last_summary_ts = 0
cold_start_done = False
cold_start_task_started = False

# =====================================================
# LOGGING
# =====================================================
def now_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")


def log(msg):
    print(f"[{now_str()}] {msg}", flush=True)

# =====================================================
# TELEGRAM
# =====================================================
def safe_send_message(text):
    try:
        send_message(text)
    except Exception as e:
        log(f"TELEGRAM_SEND_FAILED :: {e}")


def send_trade_message(title, data: dict):
    lines = [f"📊 *{title}*\n"]
    for k, v in data.items():
        lines.append(f"{k:<14}: {v}")
    safe_send_message("\n".join(lines))

# =====================================================
# DAILY RESET
# =====================================================
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

        log("DAILY_RESET :: Trade state cleared")
        safe_send_message("🔄 Trade state reset for new trading day")

# =====================================================
# SUMMARY
# =====================================================
def maybe_send_summary():
    global last_summary_ts

    if not cold_start_done:
        return

    now_ts = datetime.now(IST).timestamp()
    if now_ts - last_summary_ts < SUMMARY_INTERVAL:
        return

    last_summary_ts = now_ts

    log("SUMMARY_SENT :: 10-min summary")
    safe_send_message(
        "📊 *Trade Summary (10 min)*\n\n"
        f"🟢 Entered : {stats['entered']}\n"
        f"🎯 Target  : {stats['target_hit']}\n"
        f"🛑 SL      : {stats['sl_hit']}\n"
        f"🚪 Exited  : {stats['exited']}\n\n"
        f"⏱ {now_str()}"
    )

# =====================================================
# SAFE FETCH
# =====================================================
async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for _ in range(MAX_RETRIES):
            try:
                _, candle = await fetch_latest_candle(session, symbol)
                if candle:
                    return symbol, candle
            except Exception as e:
                log(f"CANDLE_FETCH_FAIL :: {symbol} :: {e}")
                await asyncio.sleep(1)
        return symbol, None

# =====================================================
# ANALYZED API MERGE (DEDUP + SAFE)
# =====================================================
def trade_uid(obj):
    return f"{obj['symbol']}|{obj['entry_time']}|{obj['exit_time']}"


def fetch_and_merge_analyzed():
    merged_data = {}

    for url in ANALYZED_APIS:
        log(f"ANALYZED_FETCH_START :: {url}")

        r = requests.get(
            url,
            params={
                "date": datetime.now(IST).strftime("%Y-%m-%d"),
                "end_before": datetime.now(IST).strftime("%H:%M"),
            },
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()

        log(f"ANALYZED_FETCH_OK :: {url}")

        for group, buckets in payload.get("the_data", {}).items():
            merged_data.setdefault(group, {})
            for bucket, symbols in buckets.items():
                merged_data[group].setdefault(bucket, {})
                for obj in symbols.values():
                    if not isinstance(obj, dict):
                        continue
                    if (
                        "symbol" not in obj
                        or "entry_time" not in obj
                        or "exit_time" not in obj
                    ):
                        continue

                    uid = trade_uid(obj)
                    merged_data[group][bucket][uid] = obj

    log("ANALYZED_MERGE_DONE")
    return merged_data

# =====================================================
# COLD START (FULL BACKEND COVERAGE)
# =====================================================
def run_cold_start_from_api():
    global cold_start_done

    log("COLD_START_BEGIN")
    safe_send_message("⏪ Loading cold start snapshot…")

    try:
        data = fetch_and_merge_analyzed()
    except Exception as e:
        log(f"COLD_START_FAILED :: {e}")
        safe_send_message(f"❌ Cold start failed:\n{e}")
        cold_start_done = True
        return

    exited = data.get("1_exited", {})
    entered = data.get("2_entered", {})
    not_entered = data.get("3_not_entered", {})

    target_hits = exited.get("1_profit", {})
    sl_hits = exited.get("2_stoploss", {})

    log(
        f"COLD_START_COUNTS :: "
        f"target={len(target_hits)} "
        f"sl={len(sl_hits)} "
        f"entered={len(entered)} "
        f"not_entered={len(not_entered)}"
    )

    safe_send_message(
        "📊 *COLD START SNAPSHOT*\n\n"
        f"🎯 Target Hit   : {len(target_hits)}\n"
        f"🛑 SL Hit       : {len(sl_hits)}\n"
        f"🟡 Entered     : {len(entered)}\n"
        f"⚪ Not Entered : {len(not_entered)}\n\n"
        f"⏱ Snapshot @ {now_str()}"
    )

    def send_block(title, bucket, limit=20):
        if not bucket:
            return

        lines = [f"📉 *{title}* ({len(bucket)})\n"]
        for i, obj in enumerate(bucket.values()):
            if i >= limit:
                lines.append(f"...and {len(bucket) - limit} more")
                break

            lines.append(
                f"🔹 *{obj['symbol']}*\n"
                f"Entry : {obj.get('entry')} @ {obj.get('entry_time')}\n"
                f"Exit  : {obj.get('exit_ltp')} @ {obj.get('exit_time')}\n"
                f"Qty   : {obj.get('qty')} | "
                f"PnL : ₹{round(obj.get('pnl', 0), 2)}\n"
            )

        safe_send_message("\n".join(lines))

    send_block("TARGET HIT", target_hits)
    send_block("STOPLOSS HIT", sl_hits)
    send_block("ENTERED (OPEN / MARKET CLOSE)", entered)
    send_block("NOT ENTERED", not_entered)

    cold_start_done = True
    log("COLD_START_DONE")

# =====================================================
# WORKER (UNCHANGED)
# =====================================================
async def run_worker():
    global cold_start_task_started

    log("WORKER_START")
    safe_send_message("🟢 Worker started")

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                if not is_market_time():
                    log("MARKET_CLOSED :: sleeping")
                    await asyncio.sleep(60)
                    continue

                maybe_reset_alerts()
                signals = fetch_today_signals()

                if not cold_start_task_started:
                    log("COLD_START_TRIGGER")
                    asyncio.get_running_loop().run_in_executor(
                        None, run_cold_start_from_api
                    )
                    cold_start_task_started = True

                maybe_send_summary()

                for s in signals:
                    sym = s["symbol"]
                    trade_state.setdefault(sym, {"state": "PENDING", "signal": s})

                results = await asyncio.gather(*[
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in trade_state
                ])

                for sym, candle in results:
                    if not candle:
                        continue

                    ltp = candle[4]
                    buy = trade_state.get(sym)
                    s = buy["signal"]

                    if (
                        buy["state"] == "PENDING"
                        and BUY_START <= datetime.now(IST).time() <= BUY_END
                        and ltp >= s["entry"]
                    ):
                        buy["state"] = "ENTERED"
                        stats["entered"] += 1
                        log(f"BUY_ENTERED :: {sym} @ {ltp}")
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
                            log(f"TARGET_HIT :: {sym} @ {ltp}")
                            send_trade_message("BUY TARGET HIT", {
                                "Symbol": sym,
                                "Exit LTP": ltp,
                            })

                        elif ltp <= s["stoploss"]:
                            buy["state"] = "EXITED"
                            stats["exited"] += 1
                            stats["sl_hit"] += 1
                            log(f"SL_HIT :: {sym} @ {ltp}")
                            send_trade_message("BUY SL HIT", {
                                "Symbol": sym,
                                "Exit LTP": ltp,
                            })

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                log(f"WORKER_EXCEPTION :: {e}")
                safe_send_message(f"⚠️ Worker error:\n{e}")
                await asyncio.sleep(ERROR_SLEEP)
