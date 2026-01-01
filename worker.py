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

from PIL import Image, ImageDraw, ImageFont
import matplotlib.pyplot as plt
import tempfile
import os
import math

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 100
SLEEP_INTERVAL = 1
ERROR_SLEEP = 15
MAX_RETRIES = 3
SUMMARY_INTERVAL = 600

ROWS_PER_IMAGE = 100

IST = timezone(timedelta(hours=5, minutes=30))
RESET_TIME = dtime(9, 15)

BUY_START = dtime(9, 30)
BUY_END   = dtime(11, 30)

SELL_START = dtime(10, 0)
SELL_END   = dtime(15, 30)

LIVE_BUY_START = dtime(9, 25)
LIVE_BUY_END   = dtime(11, 40)

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

# ---- LIVE TRADING STATE (NEW, ADDITIVE) ----
live_alerted = set()
live_trades = {}
day_highs = {}

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
def safe_send_message(text=None, photo=None, caption=None):
    try:
        if photo:
            send_message(photo=photo, caption=caption)
        else:
            send_message(text=text)
        time.sleep(0.4)
    except Exception as e:
        log(f"TELEGRAM_SEND_FAILED :: {e}")

# =====================================================
# HELPERS (FORMAT ONLY)
# =====================================================
def fmt_price(v):
    if v is None:
        return ""
    return f"{v:.2f}".replace(".", "-")

# =====================================================
# SUMMARY PIE IMAGE (UNCHANGED)
# =====================================================
def send_summary_pie(target, sl, entered, not_entered):
    labels_raw = [
        ("Target Hit", target),
        ("SL Hit", sl),
        ("Entered", entered),
        ("Not Entered", not_entered),
    ]

    total = sum(v for _, v in labels_raw) or 1

    labels = [
        f"{name}: {count} ({count/total*100:.1f}%)"
        for name, count in labels_raw
    ]

    values = [count for _, count in labels_raw]
    colors = ["#2ecc71", "#e74c3c", "#f1c40f", "#95a5a6"]

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(values, labels=labels, colors=colors, startangle=140)
    ax.set_title("Cold Start Trade Distribution")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    plt.savefig(tmp.name, bbox_inches="tight")
    plt.close(fig)

    safe_send_message(photo=tmp.name, caption=f"📊 Cold Start Summary\n⏱ {now_str()}")
    os.unlink(tmp.name)

# =====================================================
# META SUMMARY TEXT (UNCHANGED)
# =====================================================
def send_meta_summary_text(meta):
    if not meta:
        return

    summary = meta.get("summary", {})

    msg = (
        "📌 *Strategy Meta Summary*\n\n"
        f"📈 Breakout %: {meta.get('breakout_pct')}\n"
        f"🎯 Profit %: {meta.get('profit_pct')}\n"
        f"⏰ Entry After: {meta.get('entry_after')}\n"
        f"⚡ API Time: {meta.get('response_time_ms')} ms\n\n"
        "📊 *Counts*\n"
        f"• Entered: {summary.get('entered', 0)}\n"
        f"• Target Hit: {summary.get('target_hit', 0)}\n"
        f"• SL Hit: {summary.get('stoploss_hit', 0)}\n"
        f"• Market Closed: {summary.get('market_closed', 0)}\n"
        f"• Not Entered: {summary.get('not_entered', 0)}\n"
    )

    safe_send_message(text=msg)

# =====================================================
# TABLE IMAGE (UNCHANGED)
# =====================================================
# (UNCHANGED CODE – KEPT AS IS)
# =====================================================
def send_table_images(title, bucket):
    if not bucket:
        return

    items = list(bucket.items())
    total_pages = math.ceil(len(items) / ROWS_PER_IMAGE)

    for page in range(total_pages):
        chunk = items[page * ROWS_PER_IMAGE:(page + 1) * ROWS_PER_IMAGE]

        col_headers = [
            "Logo", "Symbol",
            "Entry Px", "Entry Time",
            "Exit Px", "Exit Time",
            "Qty", "PnL",
            "SL", "Target", "Open"
        ]

        col_widths = [
            50, 120,
            110, 90,
            110, 90,
            60, 90,
            90, 90, 90
        ]

        row_h = 40
        header_h = 46
        pad = 15

        width = sum(col_widths) + pad * 2
        height = header_h + row_h * len(chunk) + pad * 2

        img = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(img)

        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 17)
            font_b = ImageFont.truetype("DejaVuSans-Bold.ttf", 18)
        except:
            font = font_b = ImageFont.load_default()

        draw.text((pad, 5), f"{title} (Page {page+1}/{total_pages})",
                  font=font_b, fill="#000000")

        y = pad + 24
        x = pad

        for i, h in enumerate(col_headers):
            draw.rectangle(
                [x, y, x + col_widths[i], y + header_h],
                fill="#eeeeee", outline="black"
            )
            draw.text((x + 6, y + 13), h, font=font_b, fill="#000000")
            x += col_widths[i]

        y += header_h

        for sym, obj in chunk:
            pnl = obj.get("pnl", 0) or 0
            pnl_bg = "#d4edda" if pnl > 0 else "#f8d7da" if pnl < 0 else "white"

            x = pad
            draw.rectangle([x, y, x + col_widths[0], y + row_h], outline="black")
            draw.ellipse([x+15, y+10, x+35, y+30], fill="#3498db")
            x += col_widths[0]

            cells = [
                obj.get("symbol", sym),
                fmt_price(obj.get("entry")),
                obj.get("entry_time", ""),
                fmt_price(obj.get("exit_ltp")),
                obj.get("exit_time", ""),
                str(obj.get("qty", "")),
                fmt_price(pnl),
                fmt_price(obj.get("stoploss")),
                fmt_price(obj.get("target")),
                fmt_price(obj.get("open")),
            ]

            bg_colors = [
                "white", "#e3f2fd", "white", "#e8f5e9",
                "white", "white", pnl_bg,
                "#fdecea", "#e8f5e9", "#f2f2f2"
            ]

            for i, cell in enumerate(cells):
                draw.rectangle(
                    [x, y, x + col_widths[i+1], y + row_h],
                    fill=bg_colors[i], outline="black"
                )
                draw.text(
                    (x + 6, y + 11),
                    str(cell),
                    font=font_b if i in (1, 3, 6) else font,
                    fill="#111111"
                )
                x += col_widths[i+1]

            y += row_h

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        img.save(tmp.name)
        safe_send_message(photo=tmp.name, caption=f"📉 {title}\n⏱ {now_str()}")
        os.unlink(tmp.name)

# =====================================================
# ANALYZED API MERGE (UNCHANGED)
# =====================================================
def trade_uid(obj):
    return f"{obj['symbol']}|{obj['entry_time']}|{obj['exit_time']}"

def fetch_and_merge_analyzed():
    merged = {}
    meta = None

    for url in ANALYZED_APIS:
        r = requests.get(
            url,
            params={
                "date": datetime.now(IST).strftime("%Y-%m-%d"),
                "end_before": datetime.now(IST).strftime("%H:%M"),
            },
            timeout=30,
        )

        payload = r.json()
        meta = meta or payload

        for group, buckets in payload.get("the_data", {}).items():
            merged.setdefault(group, {})
            for bucket, symbols in buckets.items():
                merged[group].setdefault(bucket, {})
                for obj in symbols.values():
                    if isinstance(obj, dict) and "entry_time" in obj:
                        merged[group][bucket][trade_uid(obj)] = obj

    return merged, meta

# =====================================================
# COLD START (UNCHANGED)
# =====================================================
def run_cold_start_from_api():
    global cold_start_done

    data, meta = fetch_and_merge_analyzed()

    exited = data.get("1_exited", {})
    entered = data.get("2_entered", {})
    not_entered = data.get("3_not_entered", {})

    send_meta_summary_text(meta)
    send_summary_pie(
        target=len(exited.get("1_profit", {})),
        sl=len(exited.get("2_stoploss", {})),
        entered=len(entered),
        not_entered=len(not_entered),
    )

    send_table_images("TARGET HIT", exited.get("1_profit", {}))
    send_table_images("STOPLOSS HIT", exited.get("2_stoploss", {}))
    send_table_images("ENTERED", entered)

    cold_start_done = True
    log("COLD_START_DONE")

# =====================================================
# LIVE TRADE WORKER (NEW – ADDITIVE ONLY)
# =====================================================
async def run_live_trade_worker():
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    safe_send_message(text="🟢 Live Trade Worker Started")

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                now = datetime.now(IST)

                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                signals = fetch_today_signals()
                symbols = [s["symbol"] for s in signals if s.get("symbol") in companies]

                tasks = [fetch_latest_candle(session, s) for s in symbols]
                candles = await asyncio.gather(*tasks, return_exceptions=True)

                for sym, candle in zip(symbols, candles):
                    if not candle or not isinstance(candle, list) or len(candle) < 5:
                        continue

                    ltp = candle[4]
                    high = max(candle[:4])

                    if now.time() < dtime(10, 0):
                        day_highs[sym] = max(day_highs.get(sym, 0), high)
                        continue

                    if (
                        LIVE_BUY_START <= now.time() <= LIVE_BUY_END
                        and sym not in live_alerted
                        and sym in day_highs
                        and ltp <= day_highs[sym] * 1.03
                    ):
                        entry = round(day_highs[sym] * 1.03, 2)
                        target = round(entry * 1.03, 2)
                        sl = round(entry * 0.99, 2)

                        live_trades[sym] = {"target": target, "sl": sl}
                        live_alerted.add(sym)
                        stats["entered"] += 1

                        meta = companies[sym]
                        safe_send_message(
                            text=(
                                f"📢 BUY TRIGGERED\n\n"
                                f"{meta['company']} ({sym})\n"
                                f"Entry: {entry}\n"
                                f"Target: {target}\n"
                                f"SL: {sl}\n"
                                f"Time: {now.strftime('%H:%M:%S IST')}"
                            )
                        )

                    if sym in live_trades:
                        trade = live_trades[sym]
                        if ltp >= trade["target"]:
                            stats["target_hit"] += 1
                            safe_send_message(f"🎯 TARGET HIT: {sym} @ {ltp}")
                            del live_trades[sym]
                        elif ltp <= trade["sl"]:
                            stats["sl_hit"] += 1
                            safe_send_message(f"🛑 SL HIT: {sym} @ {ltp}")
                            del live_trades[sym]

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                log(f"LIVE_WORKER_EXCEPTION :: {e}")
                await asyncio.sleep(ERROR_SLEEP)

# =====================================================
# WORKER (SUMMARY + LIVE TOGETHER)
# =====================================================
async def run_worker():
    global cold_start_task_started

    log("WORKER_START")
    safe_send_message(text="🟢 Worker started")

    async with aiohttp.ClientSession():
        await asyncio.gather(
            run_live_trade_worker(),
            asyncio.to_thread(run_cold_start_from_api)
        )
