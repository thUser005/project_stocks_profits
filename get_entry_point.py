import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# =====================================
# CONFIG
# =====================================
CHROMEDRIVER_PATH = r"chromedriver"
URL = "https://www.nseindia.com/market-data/top-gainers-losers"
OUTPUT_FILE = "nse_top_gainers_losers.json"

# TRADE CONFIG
CAPITAL = 10000
RISK_PERCENT = 1
ENTRY_RANGE_PERCENT = 0.55
SL_PERCENT = 1.35

# =====================================
# CHROME OPTIONS (STEALTH)
# =====================================
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-infobars")
options.add_argument("--disable-notifications")
options.add_argument("--disable-extensions")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 40)

# =====================================
# HELPERS
# =====================================
def to_float(val):
    return float(val.replace(",", "").strip())

def mround(value, multiple):
    return round(value / multiple) * multiple

def fmt(val):
    return round(val, 2)

# =====================================
# TRADE LOGIC
# =====================================
def calculate_trade(open_p, high_p, low_p):
    risk_amount = CAPITAL * (RISK_PERCENT / 100)
    range_diff = (high_p - low_p) * ENTRY_RANGE_PERCENT

    # BUY
    buy_entry = mround(open_p + range_diff, 0.05)
    buy_sl = mround(buy_entry - (buy_entry * SL_PERCENT / 100), 0.05)
    buy_diff = buy_entry - buy_sl
    buy_qty = round(risk_amount / buy_diff) if buy_diff > 0 else 0

    # SELL
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

# =====================================
# TABLE EXTRACTION
# =====================================
def extract_table(table_id):
    data = []
    table = wait.until(EC.presence_of_element_located((By.ID, table_id)))
    rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) < 7:
            continue

        open_p = to_float(cols[1].text)
        high_p = to_float(cols[2].text)
        low_p = to_float(cols[3].text)

        stock = {
            "symbol": cols[0].text.strip(),
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "prev_close": cols[4].text.strip(),
            "ltp": cols[5].text.strip(),
            "percent_change": cols[6].text.strip(),
            "entry_data": calculate_trade(open_p, high_p, low_p)
        }

        data.append(stock)

    return data

def click_tab(tab_id):
    tab = wait.until(EC.element_to_be_clickable((By.ID, tab_id)))
    driver.execute_script("arguments[0].click();", tab)
    time.sleep(2)

# =====================================
# START SCRAPING
# =====================================
driver.get(URL)
time.sleep(5)

final_data = {}

index_select = Select(wait.until(EC.presence_of_element_located((By.ID, "index0"))))
index_options = index_select.options

as_on_date = driver.find_element(By.CLASS_NAME, "asondate").text.strip()

for option in index_options:
    value = option.get_attribute("value")
    name = option.text.strip()

    if value == "-1":
        continue

    print(f"Fetching: {name}")

    index_select.select_by_value(value)
    time.sleep(4)

    final_data[name] = {
        "as_on": as_on_date,
        "gainers": [],
        "losers": []
    }

    click_tab("GAINERS")
    final_data[name]["gainers"] = extract_table("topgainer-Table")

    click_tab("LOSERS")
    final_data[name]["losers"] = extract_table("toplosers-Table")

# =====================================
# SAVE JSON
# =====================================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(final_data, f, indent=4)

print(f"\n✅ Data saved to {OUTPUT_FILE}")
driver.quit()
