import os
import requests
import re
import time
from bs4 import BeautifulSoup
from typing import List
from pymongo import MongoClient, errors
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
EXPIRY_YEAR = "26"
UNDERLYING = "NIFTY"

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

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
def normalize_strike(text: str) -> str:
    return text.replace(",", "")

def build_symbol(symbol: str, expiry: str, strike: str, opt_type: str) -> str:
    return f"{symbol}{expiry}{strike}{opt_type}"

def fetch_html_with_retry(url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[+] Fetch attempt {attempt}")
            resp = requests.get(url, headers=HEADERS_HTML, timeout=15)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"[⚠️] Fetch failed (attempt {attempt}): {e}")
            if attempt == MAX_RETRIES:
                raise RuntimeError("❌ HTML fetch failed after max retries")
            time.sleep(RETRY_DELAY)

def connect_mongo_with_retry():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[+] MongoDB connect attempt {attempt}")
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client
        except errors.PyMongoError as e:
            print(f"[⚠️] MongoDB connection failed (attempt {attempt}): {e}")
            if attempt == MAX_RETRIES:
                raise RuntimeError("❌ MongoDB connection failed after max retries")
            time.sleep(RETRY_DELAY)

# =====================================================
# STEP 1: FETCH HTML
# =====================================================
html_url = "https://groww.in/options/nifty"
html = fetch_html_with_retry(html_url)

soup = BeautifulSoup(html, "html.parser")
texts = [el.get_text(strip=True) for el in soup.select(".bodyBaseHeavy")]

if not texts:
    raise RuntimeError("❌ No text extracted from HTML")

print(f"[+] Raw text items: {len(texts)}")

# =====================================================
# STEP 2: DETECT EXPIRY (26DEC, 26JAN, etc.)
# =====================================================
expiry = None
for i, val in enumerate(texts):
    if val == UNDERLYING:
        for j in range(i + 1, len(texts)):
            if re.fullmatch(r"\d{2}\s[A-Za-z]{3}", texts[j]):
                _, mon = texts[j].split(" ")
                expiry = f"{EXPIRY_YEAR}{mon.upper()}"
                break
        break

if not expiry:
    raise RuntimeError("❌ Expiry detection failed")

print(f"[+] Detected expiry: {expiry}")

# =====================================================
# STEP 3: EXTRACT STRIKES
# =====================================================
strike_texts = [
    t for t in texts
    if re.fullmatch(r"\d{1,3}(,\d{3})+", t)
]

if not strike_texts:
    raise RuntimeError("❌ No strikes found")

strikes = sorted(
    set(normalize_strike(s) for s in strike_texts),
    key=int
)

print(f"[+] Found {len(strikes)} strikes")

# =====================================================
# STEP 4: BUILD CE + PE SYMBOLS (EXACT FORMAT)
# =====================================================
symbols: List[str] = []

for strike in strikes:
    symbols.append(build_symbol(UNDERLYING, expiry, strike, "CE"))
    symbols.append(build_symbol(UNDERLYING, expiry, strike, "PE"))

print(f"[+] Generated {len(symbols)} option symbols")
print(symbols)   # optional, matches your local output

# =====================================================
# STEP 5: SAVE TO MONGODB
# =====================================================
client = connect_mongo_with_retry()
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

doc = {
    "underlying": UNDERLYING,
    "expiry": expiry,
    "symbols": symbols,
    "created_at": datetime.utcnow(),
}

try:
    collection.insert_one(doc)
    print(f"[✅] Saved {len(symbols)} symbols to MongoDB")
except errors.PyMongoError as e:
    raise RuntimeError(f"❌ MongoDB insert failed: {e}")
