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

# 🔥 Cold start state
cold_start_done = False
cold_start_task_started = False

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


def send_trade_message(title, data: dict):
    lines = [f"📊 *{title}*\n"]
    for k, v in data.items():
        lines.append(f"{k:<14}: {v}")
    safe_send_message("\n".join(lines))


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
# SAFE FETCH
# =====================================================
async def fetch_latest_safe(semaphore, session, symbol):
    async with semaphore:
        for _ in range(MAX_RETRIES):
            try:
                _, candle = await fetch_latest_candle(session, symbol)
                if candle:
                    return symbol, candle
            except Exception:
                await asyncio.sleep(1)
        return symbol, None

# =====================================================
# 🔥 ANALYZED API MERGE
# =====================================================
def fetch_and_merge_analyzed():
    now = datetime.now(IST)

    merged_summary = {
        "entered": 0,
        "target_hit": 0,
        "stoploss_hit": 0,
        "not_entered": 0,
        "market_closed": 0,
    }

    merged_data = {}

    for url in ANALYZED_APIS:
        r = requests.get(
            url,
            params={
                "date": now.strftime("%Y-%m-%d"),
                "end_before": now.strftime("%H:%M"),
            },
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()

        # ---- summary merge ----
        s = payload.get("summary", {})
        for k in merged_summary:
            merged_summary[k] += s.get(k, 0)

        # ---- data merge ----
        for group, buckets in payload.get("the_data", {}).items():
            merged_data.setdefault(group, {})
            for bucket, symbols in buckets.items():
                merged_data[group].setdefault(bucket, {})
                merged_data[group][bucket].update(symbols)

    return merged_summary, merged_data

# =====================================================
# 🔥 API-BASED COLD START (MERGED)
# =====================================================
def run_cold_start_from_api():
    global cold_start_done

    log("Cold start started")
    safe_send_message("⏪ Cold start summary loading…")

    try:
        summary, data = fetch_and_merge_analyzed()
    except Exception as e:
        safe_send_message(f"❌ Cold start API failed:\n{e}")
        log(f"Cold start API error: {e}")
        cold_start_done = True
        return

    # -------- SUMMARY --------
    safe_send_message(
        "📊 *COLD START SUMMARY*\n\n"
        f"🟢 Entered      : {summary['entered']}\n"
        f"🎯 Target Hit   : {summary['target_hit']}\n"
        f"🛑 SL Hit       : {summary['stoploss_hit']}\n"
        f"🚪 Not Entered  : {summary['not_entered']}\n"
        f"🏁 Market Closed: {summary['market_closed']}\n\n"
        f"⏰ Till: {now_str()}"
    )

    # -------- DETAILED EXITS --------
    exited = data.get("1_exited", {})

    def send_exit_block(title, bucket):
        if not bucket:
            return

        blocks = []
        for sym, obj in bucket.items():
            blocks.append(
                f"*{sym}*\n"
                f"Entry : {obj['entry']} @ {obj['entry_time']}\n"
                f"Exit  : {obj['exit_ltp']} @ {obj['exit_time']}\n"
                f"Qty   : {obj['qty']}\n"
                f"PnL   : ₹{round(obj.get('pnl', 0), 2)}\n"
            )

        safe_send_message(f"📉 *{title}*\n\n" + "\n".join(blocks))

    send_exit_block("TARGET HIT", exited.get("1_profit", {}))
    send_exit_block("STOPLOSS HIT", exited.get("2_stoploss", {}))

    cold_start_done = True
    log("Cold start completed")

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

                # 🔥 Cold start ONCE (non-blocking)
                if not cold_start_task_started:
                    asyncio.get_running_loop().run_in_executor(
                        None, run_cold_start_from_api
                    )
                    cold_start_task_started = True

                maybe_send_summary()

                # ---------------- INIT ----------------
                for s in signals:
                    sym = s["symbol"]
                    if sym not in trade_state:
                        trade_state[sym] = {"state": "PENDING", "signal": s}

                results = await asyncio.gather(*[
                    fetch_latest_safe(semaphore, session, sym)
                    for sym in trade_state
                ])

                for sym, candle in results:
                    if not candle:
                        continue

                    ltp = candle[4]
                    buy = trade_state.get(sym)
                    if not buy:
                        continue

                    s = buy["signal"]

                    if (
                        buy["state"] == "PENDING"
                        and BUY_START <= datetime.now(IST).time() <= BUY_END
                        and ltp >= s["entry"]
                    ):
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

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                safe_send_message(f"⚠️ Worker error:\n{e}")
                log(f"Worker exception: {e}")
                await asyncio.sleep(ERROR_SLEEP)
