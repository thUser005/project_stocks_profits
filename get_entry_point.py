import requests
import json
import os
from pymongo import MongoClient
from datetime import datetime, timezone

# ============================
# FLAGS
# ============================
is_replaced = True

# ============================
# NSE CONFIG
# ============================
URL = "https://www.nseindia.com/api/live-analysis-variations"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

# ============================
# TRADE CONFIG
# ============================
CAPITAL = 50000
RISK_PERCENT = 1
ENTRY_RANGE_PERCENT = 0.55
SL_PERCENT = 1.35

# ============================
# MONGODB CONFIG
# ============================
MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise Exception("❌ MONGO_URL not found")

DB_NAME = "nse_data"
COLLECTION_NAME = "entry_points"

# ============================
# HELPER FUNCTIONS
# ============================
def mround(value, multiple):
    return round(value / multiple) * multiple

def fmt(val):
    return round(val, 2)

# ============================
# TRADE CALCULATION
# ============================
def calculate_trade(open_p, high_p, low_p):
    risk_amount = CAPITAL * (RISK_PERCENT / 100)
    range_diff = (high_p - low_p) * ENTRY_RANGE_PERCENT

    buy_entry = mround(open_p + range_diff, 0.05)
    buy_sl = mround(buy_entry - (buy_entry * SL_PERCENT / 100), 0.05)
    buy_diff = buy_entry - buy_sl
    buy_qty = round(risk_amount / buy_diff) if buy_diff > 0 else 0

    sell_entry = mround(open_p - range_diff, 0.05)
    sell_sl = mround(sell_entry + (sell_entry * SL_PERCENT / 100), 0.05)
    sell_diff = sell_sl - sell_entry
    sell_qty = round(risk_amount / sell_diff) if sell_diff > 0 else 0

    return {
        "capital": fmt(CAPITAL),
        "risk_amount": fmt(risk_amount),
        "buy": {
            "entry": fmt(buy_entry),
            "stop_loss": fmt(buy_sl),
            "difference": fmt(buy_diff),
            "quantity": buy_qty
        },
        "sell": {
            "entry": fmt(sell_entry),
            "stop_loss": fmt(sell_sl),
            "difference": fmt(sell_diff),
            "quantity": sell_qty
        }
    }

# ============================
# NSE SESSION
# ============================
session = requests.Session()
session.headers.update(HEADERS)
session.get("https://www.nseindia.com", timeout=10)

# ============================
# FETCH F&O SYMBOLS
# ============================
def get_fo_symbols():
    url = "https://www.nseindia.com/api/equity-stockIndices"
    params = {"index": "SECURITIES IN F&O"}

    res = session.get(url, params=params, timeout=10)
    res.raise_for_status()

    data = res.json()
    return {item["symbol"] for item in data.get("data", [])}

FO_SYMBOLS = get_fo_symbols()
print(f"✅ Loaded {len(FO_SYMBOLS)} F&O symbols")

# ============================
# MONGODB CONNECTION
# ============================
client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ============================
# FETCH + PROCESS DATA
# ============================
final_output = {
    "gainers": [],
    "loosers": []
}

# 🔢 COUNTERS
total_stocks = 0
option_stocks = 0

for index_type in ["gainers", "loosers"]:
    print(f"📡 Fetching {index_type.upper()}")

    response = session.get(
        URL,
        params={"index": index_type, "type": "allSec"},
        timeout=10
    )
    response.raise_for_status()

    raw_json = response.json()

    for index_name, index_data in raw_json.items():
        if index_name == "legends":
            continue
        if not isinstance(index_data, dict):
            continue

        for stock in index_data.get("data", []):
            total_stocks += 1

            symbol = stock.get("symbol")

            # 🔴 FILTER: ONLY OPTION-AVAILABLE STOCKS
            if symbol not in FO_SYMBOLS:
                continue

            option_stocks += 1

            open_p = stock.get("open_price")
            high_p = stock.get("high_price")
            low_p = stock.get("low_price")

            if not all([open_p, high_p, low_p]):
                continue

            final_output[index_type].append({
                "index": index_name,
                "symbol": symbol,
                "series": stock.get("series"),
                "is_option_available": True,
                "open_price": open_p,
                "high_price": high_p,
                "low_price": low_p,
                "ltp": stock.get("ltp"),
                "prev_price": stock.get("prev_price"),
                "entry_data": calculate_trade(open_p, high_p, low_p)
            })

# ============================
# PRINT SUMMARY
# ============================
percentage = (option_stocks / total_stocks * 100) if total_stocks else 0
print(f"📊 Option-eligible stocks loaded: {option_stocks} / {total_stocks} ({percentage:.2f}%)")

# ============================
# SAVE TO MONGODB
# ============================
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

existing_doc = collection.find_one({"date": today})
is_replaced = True if existing_doc else False

document = {
    "date": today,
    "created_at": datetime.now(timezone.utc),
    "is_replaced": is_replaced,
    "capital": CAPITAL,
    "risk_percent": RISK_PERCENT,
    "entry_range_percent": ENTRY_RANGE_PERCENT,
    "sl_percent": SL_PERCENT,
    "data": final_output
}

collection.update_one(
    {"date": today},
    {"$set": document},
    upsert=True
)

print(f"✅ ONLY OPTION STOCKS SAVED | replaced={is_replaced}")
