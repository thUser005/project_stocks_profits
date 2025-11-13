import asyncio
import json
import os
import time as time_mod
import traceback
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor

import pytz
import yfinance as yf

# ----------------- Config -----------------
DATA_DIR = "stocksData"
FETCH_INTERVAL = 30
MAX_WORKERS = 8
MAX_RETRIES = 3
BASE_BACKOFF = 1

# File patterns
def file_with_date(base):
    """Return a dated filename like stocksData/live_data_2025-11-13.json"""
    today = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
    os.makedirs(DATA_DIR, exist_ok=True)
    base_name = f"{DATA_DIR}/{base}_{today}.json"
    if os.path.exists(base_name):
        return base_name.replace(".json", "_modified.json")
    return base_name

SAVE_LIVE_FILE = file_with_date("live_data")
SAVE_CLOSED_FILE = file_with_date("market_closed_data")
PROFIT_TRACK_FILE = file_with_date("stock_profits")
PROFIT_REPORT_FILE = file_with_date("profit_report")

# ----------------- Utility to load stock list -----------------
def get_stock_data():
    file_name = "stock_ids.json"
    with open(file_name, "r", encoding="utf-8") as f:
        stocks_lst = json.load(f)
    return [f"{obj['symbol']}_{obj['isin']}" for obj in stocks_lst]

SYMBOLS_lst = get_stock_data()
SYMBOLS = [i.split("_")[-1] for i in SYMBOLS_lst]

# ----------------- Market time helper -----------------
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:
        return False
    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)
    return market_open <= now.time() <= market_close

# ----------------- JSON helpers -----------------
def save_json_file(filename, data):
    tmp_name = filename + ".tmp"
    with open(tmp_name, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_name, filename)
    print(f"✅ Saved {len(data)} records → {filename}")

def load_json_file(filename):
    if not os.path.exists(filename):
        return []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

# ----------------- Previous Snapshot -----------------
def load_previous_snapshot():
    if os.path.exists(SAVE_LIVE_FILE):
        fname = SAVE_LIVE_FILE
    elif os.path.exists(SAVE_CLOSED_FILE):
        fname = SAVE_CLOSED_FILE
    else:
        return {}
    try:
        with open(fname, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {item["symbol"]: item for item in data if isinstance(item, dict)}
    except Exception as e:
        print(f"Failed to load previous snapshot: {e}")
        return {}

# ----------------- Fetch helpers -----------------
def fetch_single_symbol(symbol: str, previous_data=None):
    ticker = yf.Ticker(symbol)
    info = ticker.info
    symbol_clean = symbol.replace(".NS", "")
    prev = previous_data.get(symbol_clean, {}) if previous_data else {}

    record = {
        "symbol": symbol_clean,
        "price": info.get("currentPrice"),
        "open": info.get("open"),
        "previousClose": info.get("previousClose") or prev.get("previousClose"),
        "high": info.get("dayHigh") or prev.get("high"),
        "low": info.get("dayLow") or prev.get("low"),
        "volume": info.get("volume"),
        "marketCap": info.get("marketCap"),
        "previousHigh": prev.get("high"),
        "previousLow": prev.get("low"),
        "time": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    }
    return record

def fetch_with_retries(symbol: str, previous_data=None):
    for attempt in range(MAX_RETRIES):
        try:
            return fetch_single_symbol(symbol, previous_data)
        except Exception as e:
            wait = BASE_BACKOFF * (2 ** attempt)
            print(f"[{symbol}] Fetch failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time_mod.sleep(wait)
    return None

# ----------------- Profit Tracking -----------------
def update_profit_tracking(snapshot):
    tracked = {item["symbol"]: item for item in load_json_file(PROFIT_TRACK_FILE)}
    new_entries = 0
    for rec in snapshot:
        open_price = rec.get("open")
        current_price = rec.get("price")
        if not open_price or not current_price:
            continue
        if current_price >= open_price * 1.005 and rec["symbol"] not in tracked:
            rec["profit_flag"] = None
            tracked[rec["symbol"]] = rec
            new_entries += 1
    if new_entries > 0:
        save_json_file(PROFIT_TRACK_FILE, list(tracked.values()))
        print(f"💹 {new_entries} new potential profit stocks saved")

def finalize_profit_report(snapshot):
    tracked = load_json_file(PROFIT_TRACK_FILE)
    current_map = {item["symbol"]: item for item in snapshot}
    report = []
    for item in tracked:
        sym = item["symbol"]
        open_price = item.get("open")
        end_price = current_map.get(sym, {}).get("price")
        profit_flag = False
        if open_price and end_price and end_price >= open_price * 1.01:
            profit_flag = True
        item["final_price"] = end_price
        item["profit_flag"] = profit_flag
        report.append(item)
    save_json_file(PROFIT_REPORT_FILE, report)
    print(f"📊 Profit report generated → {PROFIT_REPORT_FILE}")

# ----------------- Core Scheduler -----------------
async def run_market_day():
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    loop = asyncio.get_event_loop()

    print("📅 Scheduler started. Waiting for market hours...")

    while True:
        if not is_market_open():
            await asyncio.sleep(60)
            continue

        print("📈 Market open — fetching live data...")
        while is_market_open():
            previous_data = load_previous_snapshot()
            tasks = [loop.run_in_executor(executor, fetch_with_retries, symbol, previous_data) for symbol in SYMBOLS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            snapshot = [r for r in results if isinstance(r, dict)]

            save_json_file(SAVE_LIVE_FILE, snapshot)
            update_profit_tracking(snapshot)

            await asyncio.sleep(FETCH_INTERVAL)

        print("⚠️ Market closed — saving final snapshot...")
        previous_data = load_previous_snapshot()
        tasks = [loop.run_in_executor(executor, fetch_with_retries, symbol, previous_data) for symbol in SYMBOLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        snapshot = [r for r in results if isinstance(r, dict)]

        save_json_file(SAVE_CLOSED_FILE, snapshot)
        finalize_profit_report(snapshot)
        print("✅ Market closed — reports finalized. Sleeping until next trading day...")

        # Sleep until next day morning
        await asyncio.sleep(3600 * 8)

# ----------------- Entrypoint -----------------
if __name__ == "__main__":
    try:
        asyncio.run(run_market_day())
    except KeyboardInterrupt:
        print("Shutting down manually...")
    except Exception:
        print("Fatal error:")
        traceback.print_exc()
