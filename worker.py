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
# TELEGRAM (SAFE WRAPPER)
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
# SUMMARY PIE IMAGE (COUNT + %)
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

    safe_send_message(
        photo=tmp.name,
        caption=f"📊 Cold Start Summary\n⏱ {now_str()}",
    )

    os.unlink(tmp.name)

# =====================================================
# META SUMMARY TEXT (NEW)
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
def send_table_images(title, bucket):
    if not bucket:
        return

    items = list(bucket.items())
    total_pages = math.ceil(len(items) / ROWS_PER_IMAGE)

    for page in range(total_pages):
        chunk = items[page * ROWS_PER_IMAGE:(page + 1) * ROWS_PER_IMAGE]

        col_headers = ["Logo", "Symbol", "Entry", "Exit", "Qty", "PnL"]
        col_widths = [60, 140, 230, 230, 80, 120]

        row_h = 42
        header_h = 48
        pad = 20

        width = sum(col_widths) + pad * 2
        height = header_h + row_h * (len(chunk) + 1) + pad * 2

        img = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(img)

        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 18)
            font_b = ImageFont.truetype("DejaVuSans-Bold.ttf", 20)
        except:
            font = font_b = ImageFont.load_default()

        draw.text((pad, 5), f"{title} (Page {page+1}/{total_pages})", font=font_b)

        y = pad + 30
        x = pad

        for i, h in enumerate(col_headers):
            draw.rectangle([x, y, x + col_widths[i], y + header_h], fill="#eeeeee", outline="black")
            draw.text((x + 6, y + 12), h, font=font_b)
            x += col_widths[i]

        y += header_h

        for sym, obj in chunk:
            pnl = round(obj.get("pnl", 0) or 0, 2)
            pnl_color = "#2ecc71" if pnl > 0 else "#e74c3c" if pnl < 0 else "#7f8c8d"

            x = pad
            draw.rectangle([x, y, x + col_widths[0], y + row_h], outline="black")
            draw.ellipse([x+20, y+10, x+40, y+30], fill="#3498db")
            x += col_widths[0]

            cells = [
                obj.get("symbol", sym),
                f"{obj.get('entry')} @ {obj.get('entry_time')}",
                f"{obj.get('exit_ltp')} @ {obj.get('exit_time')}",
                str(obj.get("qty", "")),
                f"₹{pnl}",
            ]

            for i, cell in enumerate(cells):
                draw.rectangle([x, y, x + col_widths[i+1], y + row_h], outline="black")
                draw.text((x + 6, y + 10), cell, fill=pnl_color if i == 4 else "black")
                x += col_widths[i+1]

            y += row_h

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        img.save(tmp.name)

        safe_send_message(photo=tmp.name, caption=f"📉 {title}\n⏱ {now_str()}")
        os.unlink(tmp.name)

# =====================================================
# ANALYZED API MERGE (META PRESERVED)
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
        meta = meta or payload  # keep first meta

        for group, buckets in payload.get("the_data", {}).items():
            merged.setdefault(group, {})
            for bucket, symbols in buckets.items():
                merged[group].setdefault(bucket, {})
                for obj in symbols.values():
                    if isinstance(obj, dict) and "entry_time" in obj:
                        merged[group][bucket][trade_uid(obj)] = obj

    return merged, meta

# =====================================================
# COLD START
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
# WORKER (UNCHANGED)
# =====================================================
async def run_worker():
    global cold_start_task_started

    log("WORKER_START")
    safe_send_message(text="🟢 Worker started")

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector):
        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                if not cold_start_task_started:
                    asyncio.get_running_loop().run_in_executor(None, run_cold_start_from_api)
                    cold_start_task_started = True

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                log(f"WORKER_EXCEPTION :: {e}")
                await asyncio.sleep(ERROR_SLEEP)
