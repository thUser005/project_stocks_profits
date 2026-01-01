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

# NEW IMPORTS (SAFE)
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

ROWS_PER_IMAGE = 30

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
def safe_send_message(text=None, photo=None, caption=None):
    try:
        if photo:
            send_message(photo=photo, caption=caption)
        else:
            send_message(text)
    except Exception as e:
        log(f"TELEGRAM_SEND_FAILED :: {e}")

# =====================================================
# SUMMARY PIE IMAGE
# =====================================================
def send_summary_pie(target, sl, entered, not_entered):
    labels = ["Target Hit", "SL Hit", "Entered", "Not Entered"]
    values = [target, sl, entered, not_entered]
    colors = ["#2ecc71", "#e74c3c", "#f1c40f", "#95a5a6"]

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(values, labels=labels, autopct="%1.1f%%", colors=colors)
    ax.set_title("Cold Start Trade Distribution")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    plt.savefig(tmp.name, bbox_inches="tight")
    plt.close(fig)

    safe_send_message(photo=tmp.name, caption=f"📊 Cold Start Summary\n⏱ {now_str()}")
    os.unlink(tmp.name)

# =====================================================
# TABLE IMAGE (PAGINATED + COLORED)
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

        draw.text((pad, 5), f"{title} (Page {page+1}/{total_pages})", font=font_b, fill="black")

        y = pad + 30
        x = pad

        for i, h in enumerate(col_headers):
            draw.rectangle([x, y, x + col_widths[i], y + header_h], fill="#eeeeee", outline="black")
            draw.text((x + 6, y + 12), h, font=font_b, fill="black")
            x += col_widths[i]

        y += header_h

        for sym, obj in chunk:
            symbol = obj.get("symbol", sym)
            pnl = round(obj.get("pnl", 0) or 0, 2)
            pnl_color = "#2ecc71" if pnl > 0 else "#e74c3c" if pnl < 0 else "#7f8c8d"

            x = pad

            # LOGO PLACEHOLDER (FAST & SAFE)
            draw.rectangle([x, y, x + col_widths[0], y + row_h], outline="black")
            draw.ellipse([x+20, y+10, x+40, y+30], fill="#3498db")
            x += col_widths[0]

            cells = [
                symbol,
                f"{obj.get('entry')} @ {obj.get('entry_time')}",
                f"{obj.get('exit_ltp')} @ {obj.get('exit_time')}",
                str(obj.get("qty", "")),
                f"₹{pnl}",
            ]

            for i, cell in enumerate(cells):
                draw.rectangle([x, y, x + col_widths[i+1], y + row_h], outline="black")
                color = pnl_color if i == 4 else "black"
                draw.text((x + 6, y + 10), cell, font=font, fill=color)
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
    merged_data = {}
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
        for group, buckets in payload.get("the_data", {}).items():
            merged_data.setdefault(group, {})
            for bucket, symbols in buckets.items():
                merged_data[group].setdefault(bucket, {})
                for obj in symbols.values():
                    if isinstance(obj, dict) and "entry_time" in obj:
                        merged_data[group][bucket][trade_uid(obj)] = obj
    return merged_data

# =====================================================
# COLD START (IMAGE MODE)
# =====================================================
def run_cold_start_from_api():
    global cold_start_done

    data = fetch_and_merge_analyzed()

    exited = data.get("1_exited", {})
    entered = data.get("2_entered", {})
    not_entered = data.get("3_not_entered", {})

    target_hits = exited.get("1_profit", {})
    sl_hits = exited.get("2_stoploss", {})

    send_summary_pie(
        target=len(target_hits),
        sl=len(sl_hits),
        entered=len(entered),
        not_entered=len(not_entered),
    )

    send_table_images("TARGET HIT", target_hits)
    send_table_images("STOPLOSS HIT", sl_hits)
    send_table_images("ENTERED (OPEN / MARKET CLOSE)", entered)
    send_table_images("NOT ENTERED", not_entered)

    cold_start_done = True
    log("COLD_START_DONE")

# =====================================================
# WORKER (100% UNCHANGED)
# =====================================================
async def run_worker():
    global cold_start_task_started

    log("WORKER_START")
    safe_send_message(text="🟢 Worker started")

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                if not cold_start_task_started:
                    asyncio.get_running_loop().run_in_executor(
                        None, run_cold_start_from_api
                    )
                    cold_start_task_started = True

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                log(f"WORKER_EXCEPTION :: {e}")
                await asyncio.sleep(ERROR_SLEEP)
