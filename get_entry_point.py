import os
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from typing import List, Optional, Dict
from pymongo import MongoClient, errors
from datetime import datetime, timezone

# =====================================================
# CONFIG
# =====================================================
UNDERLYINGS = {
    "NIFTY": "https://groww.in/options/nifty",
    "BANKNIFTY": "https://groww.in/options/nifty-bank",
    "SENSEX": "https://groww.in/options/sp-bse-sensex",
    "FINNIFTY": "https://groww.in/options/nifty-financial-services",
    "MIDCPNIFTY": "https://groww.in/options/nifty-midcap-select",
    "BANKEX": "https://groww.in/options/sp-bse-bankex",
}

MAX_RETRIES = 3
RETRY_DELAY = 2

# MongoDB
keys_data = None
if os.path.exists("keys.json"):
    with open("keys.json") as f:
        keys_data = json.load(f)

MONGO_URL = os.getenv("MONGO_URL", keys_data["mongo_url"] if keys_data else None)
DB_NAME = "options_data"
COLLECTION_NAME = "nifty_symbols"

if not MONGO_URL:
    raise RuntimeError("❌ MONGO_URL not found")

# =====================================================
# HEADERS
# =====================================================
HEADERS_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# =====================================================
# CONSTANTS
# =====================================================
MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}
VALID_MONTHS = set(MONTH_MAP.keys())

# =====================================================
# HELPERS
# =====================================================
def fetch_html_with_retry(url: str) -> str:
    for i in range(MAX_RETRIES):
        try:
            print(f"[+] Fetch: {url}")
            r = requests.get(url, headers=HEADERS_HTML, timeout=15)
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if i == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY)


def connect_mongo_with_retry() -> MongoClient:
    for i in range(MAX_RETRIES):
        try:
            print("[+] MongoDB connect")
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client
        except errors.PyMongoError:
            if i == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY)


def normalize_strike(text: str) -> str:
    return text.replace(",", "")


def build_symbol(underlying: str, expiry: str, strike: str) -> str:
    return f"{underlying}{expiry}{strike}CE"


def expiry_text_to_date(text: str, now: datetime) -> Optional[Dict[str, str]]:
    parts = text.split()
    if len(parts) != 2:
        return None

    day, mon = parts
    mon = mon.upper()

    if mon not in VALID_MONTHS or not day.isdigit():
        return None

    expiry_month = int(MONTH_MAP[mon])
    expiry_year = now.year + 1 if expiry_month < now.month else now.year

    full_date = f"{expiry_year}-{MONTH_MAP[mon]}-{day.zfill(2)}"

    return {
        "date_param": full_date,
        "symbol_expiry": f"{str(expiry_year)[-2:]}{mon}",
        "expiry_key": full_date,
    }


def extract_body_texts(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [e.get_text(strip=True) for e in soup.select(".bodyBaseHeavy")]


def extract_expiry_texts(base_url: str) -> List[str]:
    html = fetch_html_with_retry(base_url)
    texts = extract_body_texts(html)
    return list(dict.fromkeys(texts))


def extract_strikes(expiry_url: str) -> List[str]:
    html = fetch_html_with_retry(expiry_url)
    texts = extract_body_texts(html)
    return sorted(
        {normalize_strike(t) for t in texts if re.fullmatch(r"\d{1,3}(,\d{3})+", t)},
        key=int
    )

# =====================================================
# CORE
# =====================================================
def process_all_underlyings():
    now = datetime.now(timezone.utc)
    trade_date = now.strftime("%Y-%m-%d")

    client = connect_mongo_with_retry()
    collection = client[DB_NAME][COLLECTION_NAME]

    final_data: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

    for underlying, base_url in UNDERLYINGS.items():
        print(f"\n===== PROCESSING {underlying} =====")
        final_data[underlying] = {}

        raw_expiries = extract_expiry_texts(base_url)

        for exp_text in raw_expiries:
            exp = expiry_text_to_date(exp_text, now)
            if not exp:
                continue

            expiry_url = f"{base_url}?expiry={exp['date_param']}"
            strikes = extract_strikes(expiry_url)
            if not strikes:
                continue

            symbols = [
                build_symbol(underlying, exp["symbol_expiry"], s)
                for s in strikes
            ]

            final_data[underlying][exp["expiry_key"]] = {
                "symbol_expiry": exp["symbol_expiry"],
                "symbols": symbols
            }

            print(f"[✓] {underlying} {exp['expiry_key']} → {len(symbols)} symbols")

    collection.update_one(
        {"trade_date": trade_date},
        {
            "$set": {
                "data": final_data,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            },
        },
        upsert=True,
    )

    client.close()
    print("\n[✅] ALL underlyings saved in ONE MongoDB document")


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    process_all_underlyings()
