import json
import math,os
import requests
from pymongo import MongoClient
from datetime import datetime, timezone
from typing import List

# =====================================================
# CONFIG
# =====================================================
OPTIONS_URL = (
    "https://groww.in/v1/api/stocks_fo_data/v1/"
    "tr_live_prices/exchange/NSE/segment/FNO/latest_prices_batch"
)

MONGO_DB = "options_data"
STRUCTURAL_COLLECTION = "symbols_structural"
ENTRY_COLLECTION = "entry_plans_live"
STATUS_COLLECTION = "pipeline_status"

UNDERLYING = "NIFTY"        # change safely (BANKNIFTY, FINNIFTY, etc.)
MAX_BATCH_SIZE = 100

# ---- STRATEGY PARAMS ----
STOPLOSS_PCT = 0.01
TARGET_PCT = 0.05
SL_TIME_STR = "14:30"
CAPITAL = 20_000

LOT_SIZES = {
    "NIFTY": 75,
    "BANKNIFTY": 15,
    "FINNIFTY": 40,
    "MIDCPNIFTY": 50,
    "SENSEX": 10,
    "BANKEX": 15,
}

# ---- UNDERLYING-SPECIFIC SAFETY CAPS (₹) ----
MAX_ABS_MOVE_BY_UNDERLYING = {
    "BANKNIFTY": 180,
    "NIFTY": 120,
    "FINNIFTY": 120,
    "MIDCPNIFTY": 100,
    "SENSEX": 250,
    "BANKEX": 250,
}

DEFAULT_MAX_ABS_MOVE = 120   # NIFTY-safe fallback

# =====================================================
# HEADERS
# =====================================================
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "x-app-id": "growwWeb",
}

# =====================================================
# HELPERS
# =====================================================
def chunk_list(lst: List[str], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def pick_nearest_expiry(expiry_map: dict) -> str:
    today = datetime.now(timezone.utc).date()
    future = []

    for k in expiry_map.keys():
        try:
            d = datetime.strptime(k, "%Y-%m-%d").date()
            if d >= today:
                future.append(d)
        except Exception:
            pass

    if not future:
        raise RuntimeError("❌ No valid future expiry found")

    return min(future).strftime("%Y-%m-%d")


# =====================================================
# 🔥 ADAPTIVE BREAKOUT LOGIC
# =====================================================
def adaptive_breakout_pct(open_price: float) -> float:
    if open_price < 500:
        pct = 0.06
    elif open_price < 1000:
        pct = 0.05
    elif open_price < 2000:
        pct = 0.035
    elif open_price < 3500:
        pct = 0.025
    else:
        pct = 0.015

    return max(0.012, min(pct, 0.06))


def capped_move(open_price: float, pct: float, side: str, max_abs_move: float):
    raw = open_price * (1 + pct if side == "BUY" else 1 - pct)

    if side == "BUY":
        return min(raw, open_price + max_abs_move)
    else:
        return max(raw, open_price - max_abs_move)


def build_entry_plan(symbol: str, open_price: float, lot_size: int):
    bpct = adaptive_breakout_pct(open_price)

    max_abs_move = MAX_ABS_MOVE_BY_UNDERLYING.get(
        UNDERLYING,
        DEFAULT_MAX_ABS_MOVE
    )

    buy_trigger = capped_move(open_price, bpct, "BUY", max_abs_move)
    sell_trigger = capped_move(open_price, bpct, "SELL", max_abs_move)

    capital_per_lot = buy_trigger * lot_size
    executable = CAPITAL >= capital_per_lot
    lots = math.floor(CAPITAL / capital_per_lot) if executable else 0

    return {
        "symbol": symbol,
        "open": round(open_price, 2),

        "buy_trigger": round(buy_trigger, 2),
        "buy_sl": round(buy_trigger * (1 - STOPLOSS_PCT), 2),
        "buy_target": round(buy_trigger * (1 + TARGET_PCT), 2),

        "sell_trigger": round(sell_trigger, 2),
        "sell_sl": round(sell_trigger * (1 + STOPLOSS_PCT), 2),
        "sell_target": round(sell_trigger * (1 - TARGET_PCT), 2),

        "breakout_pct_used": round(bpct, 4),
        "max_abs_move_used": max_abs_move,
        "lot_size": lot_size,
        "capital_required": round(capital_per_lot, 2),
        "executable": executable,
        "lots": lots,
        "sl_time": SL_TIME_STR,
        "status": "PLANNED",
        "created_at": datetime.now(timezone.utc),
    }

# =====================================================
# MONGO CONNECT
# =====================================================

# =====================================================
# MONGO
# =====================================================
keys = None
if os.path.exists("keys.json"):
    with open("keys.json") as f:
        keys = json.load(f)

MONGO_URL = os.getenv("MONGO_URL", keys["mongo_url"] if keys else None)

client = MongoClient(MONGO_URL)
db = client[MONGO_DB]

structural_col = db[STRUCTURAL_COLLECTION]
entry_col = db[ENTRY_COLLECTION]
status_col = db[STATUS_COLLECTION]

# =====================================================
# LOAD STRUCTURAL SYMBOLS
# =====================================================
doc = structural_col.find_one(sort=[("updated_at", -1)])
if not doc or "data" not in doc:
    raise RuntimeError("❌ No structural symbols found")

underlying_data = doc["data"].get(UNDERLYING)
if not underlying_data:
    raise RuntimeError(f"❌ No data for {UNDERLYING}")

expiry_key = pick_nearest_expiry(underlying_data)
expiry_data = underlying_data[expiry_key]

ce_symbols = expiry_data.get("ce_symbols") or expiry_data.get("symbols")
if not ce_symbols:
    raise RuntimeError("❌ No CE symbols found")

print(f"📦 Loaded {len(ce_symbols)} CE symbols")
print(f"📅 Using expiry: {expiry_key}")

# =====================================================
# FETCH LIVE PRICES
# =====================================================
session = requests.Session()
live_prices = {}

total_batches = math.ceil(len(ce_symbols) / MAX_BATCH_SIZE)

for i, batch in enumerate(chunk_list(ce_symbols, MAX_BATCH_SIZE), start=1):
    print(f"🔄 Fetching batch {i}/{total_batches} ({len(batch)} symbols)")
    r = session.post(
        OPTIONS_URL,
        headers=HEADERS,
        data=json.dumps(batch),
        timeout=10
    )
    r.raise_for_status()
    live_prices.update(r.json())

# =====================================================
# BUILD ENTRY PLANS
# =====================================================
lot_size = LOT_SIZES.get(UNDERLYING, LOT_SIZES["NIFTY"])
entry_docs = []

for symbol, d in live_prices.items():
    open_price = d.get("open", 0)
    if open_price <= 0:
        continue

    plan = build_entry_plan(symbol, open_price, lot_size)
    plan.update({
        "underlying": UNDERLYING,
        "expiry": expiry_key,
        "volume": d.get("volume"),
        "openInterest": d.get("openInterest"),
        "ltp": d.get("ltp"),
    })

    entry_docs.append(plan)

# =====================================================
# SAVE ENTRY PLANS
# =====================================================
if entry_docs:
    entry_col.delete_many({
        "underlying": UNDERLYING,
        "expiry": expiry_key,
        "status": "PLANNED"
    })
    entry_col.insert_many(entry_docs)

# =====================================================
# SAVE PIPELINE STATUS
# =====================================================
today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

status_col.update_one(
    {
        "underlying": UNDERLYING,
        "trade_date": today_str,
        "run_type": "ENTRY_PLAN"
    },
    {
        "$set": {
            "expiry": expiry_key,
            "entry_data_saved": True,
            "entry_count": len(entry_docs),
            "updated_at": datetime.now(timezone.utc)
        },
        "$setOnInsert": {
            "created_at": datetime.now(timezone.utc)
        }
    },
    upsert=True
)
 
print(f"\n✅ Saved {len(entry_docs)} entry plans")
print(f"📌 Pipeline status recorded for {today_str}")

client.close()
