import os
import requests
import re
import time
from bs4 import BeautifulSoup
from typing import List, Dict
from pymongo import MongoClient, errors
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
UNDERLYING = "NIFTY"
YEAR_PREFIX = "20"           # for 2026, 2027 etc

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

BASE_URL = "https://groww.in/options/nifty"

# MongoDB
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = "options_data"
COLLECTION_NAME = "nifty_symbols"

if not MONGO_URL:
    raise RuntimeError("❌ MONGO_URL not found in environment variables")

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
# HELPERS
# =====================================================
MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}

def fetch_html_with_retry(url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[+] Fetch attempt {attempt}: {url}")
            resp = requests.get(url, headers=HEADERS_HTML, timeout=15)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"[⚠️] Fetch failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)

def connect_mongo_with_retry():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client
        except errors.PyMongoError:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)

def normalize_strike(text: str) -> str:
    return text.replace(",", "")

def build_symbol(symbol: str, expiry: str, strike: str, opt_type: str) -> str:
    return f"{symbol}{expiry}{strike}{opt_type}"

def expiry_text_to_date(text: str, year: int) -> Dict[str, str]:
    """
    '06 Jan' -> {
        'date_param': '2026-01-06',
        'symbol_expiry': '26JAN'
    }
    """
    day, mon = text.split()
    mon = mon.upper()

    date_param = f"{year}-{MONTH_MAP[mon]}-{day.zfill(2)}"
    symbol_expiry = f"{str(year)[-2:]}{mon}"

    return {
        "date_param": date_param,
        "symbol_expiry": symbol_expiry
    }

# =====================================================
# STEP 1: FETCH BASE PAGE
# =====================================================
html = fetch_html_with_retry(BASE_URL)
soup = BeautifulSoup(html, "html.parser")
texts = [el.get_text(strip=True) for el in soup.select(".bodyBaseHeavy")]

# =====================================================
# STEP 2: EXTRACT EXPIRY DATES (e.g. 30 Dec, 06 Jan)
# =====================================================
expiry_texts = []
for t in texts:
    if re.fullmatch(r"\d{2}\s[A-Za-z]{3}", t):
        expiry_texts.append(t)

expiry_texts = list(dict.fromkeys(expiry_texts))  # unique, ordered

if not expiry_texts:
    raise RuntimeError("❌ No expiry dates found")

print(f"[+] Found expiries: {expiry_texts}")

# =====================================================
# STEP 3: MONGODB CONNECTION
# =====================================================
client = connect_mongo_with_retry()
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

trade_date = datetime.utcnow().strftime("%Y-%m-%d")
year = datetime.utcnow().year

# =====================================================
# STEP 4: PROCESS EACH EXPIRY
# =====================================================
for exp_text in expiry_texts:
    exp = expiry_text_to_date(exp_text, year)

    expiry_url = f"{BASE_URL}?expiry={exp['date_param']}"
    print(f"\n[▶] Processing expiry {exp_text} → {expiry_url}")

    html = fetch_html_with_retry(expiry_url)
    soup = BeautifulSoup(html, "html.parser")
    texts = [el.get_text(strip=True) for el in soup.select(".bodyBaseHeavy")]

    strike_texts = [
        t for t in texts
        if re.fullmatch(r"\d{1,3}(,\d{3})+", t)
    ]

    if not strike_texts:
        print(f"[⚠️] No strikes for expiry {exp_text}, skipping")
        continue

    strikes = sorted(
        set(normalize_strike(s) for s in strike_texts),
        key=int
    )

    symbols: List[str] = []
    for strike in strikes:
        symbols.append(build_symbol(UNDERLYING, exp["symbol_expiry"], strike, "CE"))
        symbols.append(build_symbol(UNDERLYING, exp["symbol_expiry"], strike, "PE"))

    print(f"[✓] Generated {len(symbols)} symbols for {exp_text}")

    filter_query = {
        "underlying": UNDERLYING,
        "expiry": exp["symbol_expiry"],
        "trade_date": trade_date,
    }

    update_doc = {
        "$set": {
            "symbols": symbols,
            "updated_at": datetime.utcnow(),
        },
        "$setOnInsert": {
            "created_at": datetime.utcnow(),
        },
    }

    collection.update_one(filter_query, update_doc, upsert=True)
    print(f"[💾] Saved expiry {exp['symbol_expiry']}")

print("\n[✅] All expiries processed successfully")
