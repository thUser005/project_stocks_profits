import asyncio
import json
from datetime import datetime, time as dt_time
import pytz
import os
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import time as time_mod
import traceback
import websockets

# ----------------- Utility to load stock list -----------------
def get_stock_data():
    file_name = 'stock_ids.json'
    with open(file_name, 'r', encoding='utf-8') as f:
        stocks_lst = json.load(f)
    return [f"{obj['symbol']}_{obj['isin']}" for obj in stocks_lst]

# ----------------- Configuration -----------------
SYMBOLS_lst = get_stock_data()
SYMBOLS = [i.split("_")[-1] for i in SYMBOLS_lst]

FETCH_INTERVAL = 30
MAX_RETRIES = 3
BASE_BACKOFF = 1
SAVE_LIVE_FILE = "live_data.json"
SAVE_CLOSED_FILE = "market_closed_data.json"
WEBSOCKET_HOST = "0.0.0.0"
WEBSOCKET_PORT = 8765
MAX_WORKERS = 8

# ----------------- Market time helper -----------------
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:  # Saturday/Sunday
        return False
    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)
    return market_open <= now.time() <= market_close

# ----------------- Load previous snapshot -----------------
def load_previous_snapshot():
    """Load last saved file to keep track of historical highs/lows"""
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

def fetch_with_retries(symbol: str, previous_data=None, max_retries=MAX_RETRIES, base_backoff=BASE_BACKOFF):
    attempt = 0
    while attempt < max_retries:
        try:
            return fetch_single_symbol(symbol, previous_data)
        except Exception as e:
            attempt += 1
            wait = base_backoff * (2 ** (attempt - 1))
            print(f"[{symbol}] fetch failed (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"[{symbol}] retrying in {wait}s...")
                time_mod.sleep(wait)
            else:
                print(f"[{symbol}] giving up after {max_retries} attempts.")
                traceback.print_exc()
                return None

# ----------------- File save -----------------
async def save_snapshot(snapshot_list):
    open_status = is_market_open()
    filename = SAVE_LIVE_FILE if open_status else SAVE_CLOSED_FILE
    tmp_name = filename + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    with open(tmp_name, "w", encoding="utf-8") as f:
        json.dump(snapshot_list, f, indent=2, ensure_ascii=False)
    os.replace(tmp_name, filename)
    print(f"✅ Saved {len(snapshot_list)} symbols to {filename} (market {'open' if open_status else 'closed'})")

# ----------------- WebSocket broadcaster -----------------
class Broadcaster:
    def __init__(self):
        self.clients = set()
        self.lock = asyncio.Lock()

    async def register(self, websocket):
        async with self.lock:
            self.clients.add(websocket)
            print(f"Client connected. Total clients: {len(self.clients)}")

    async def unregister(self, websocket):
        async with self.lock:
            self.clients.discard(websocket)
            print(f"Client disconnected. Total clients: {len(self.clients)}")

    async def broadcast(self, message: str):
        if not self.clients:
            return
        to_remove = []
        async with self.lock:
            for ws in list(self.clients):
                try:
                    await ws.send(message)
                except Exception:
                    to_remove.append(ws)
            for ws in to_remove:
                self.clients.discard(ws)
        if to_remove:
            print(f"Removed {len(to_remove)} dead clients. Active: {len(self.clients)}")

broadcaster = Broadcaster()

async def ws_handler(websocket, path):
    await broadcaster.register(websocket)
    try:
        async for msg in websocket:
            msg = (msg or "").strip().lower()
            if msg in ("ping", "hello"):
                await websocket.send(json.dumps({"pong": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()}))
            elif msg == "get":
                fname = SAVE_LIVE_FILE if is_market_open() else SAVE_CLOSED_FILE
                if os.path.exists(fname):
                    with open(fname, "r", encoding="utf-8") as f:
                        await websocket.send(f.read())
                else:
                    await websocket.send(json.dumps({"error": "no snapshot yet"}))
            else:
                await websocket.send(json.dumps({"error": "unknown command"}))
    except websockets.ConnectionClosed:
        pass
    finally:
        await broadcaster.unregister(websocket)

# ----------------- Main loop -----------------
async def fetch_cycle_loop(interval=FETCH_INTERVAL):
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    loop = asyncio.get_event_loop()

    while True:
        if not is_market_open():
            print("⚠️ Market is closed — fetching one last snapshot and shutting down...")

            previous_data = load_previous_snapshot()
            tasks = [
                loop.run_in_executor(executor, fetch_with_retries, symbol, previous_data)
                for symbol in SYMBOLS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            snapshot = [r for r in results if isinstance(r, dict)]
            await save_snapshot(snapshot)
            print("🛑 Market closed. Exiting program now.")
            return  # <-- graceful stop

        # Market open
        print("📈 Market open — fetching live data...")
        previous_data = load_previous_snapshot()
        start_ts = time_mod.time()

        tasks = [
            loop.run_in_executor(executor, fetch_with_retries, symbol, previous_data)
            for symbol in SYMBOLS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        snapshot = [r for r in results if isinstance(r, dict)]

        await save_snapshot(snapshot)

        try:
            await broadcaster.broadcast(json.dumps(snapshot, ensure_ascii=False))
            print(f"Broadcasted snapshot to {len(broadcaster.clients)} clients")
        except Exception as e:
            print("Broadcast failed:", e)

        elapsed = time_mod.time() - start_ts
        to_wait = max(0, interval - elapsed)
        await asyncio.sleep(to_wait)

# ----------------- Entrypoint -----------------
async def main_async():
    ws_server = await websockets.serve(ws_handler, WEBSOCKET_HOST, WEBSOCKET_PORT)
    print(f"🌐 WebSocket server listening on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")

    await fetch_cycle_loop(FETCH_INTERVAL)

    print("Closing WebSocket server...")
    ws_server.close()
    await ws_server.wait_closed()
    print("✅ WebSocket server closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("Shutting down manually...")
    except Exception:
        print("Fatal error:")
        traceback.print_exc()
