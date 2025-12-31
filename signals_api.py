import requests
from datetime import datetime, timedelta, timezone

BASE_URL = "https://project-get-entry.vercel.app"

IST = timezone(timedelta(hours=5, minutes=30))


def today_ist():
    """Return today's date in IST (YYYY-MM-DD)"""
    return datetime.now(IST).strftime("%Y-%m-%d")


def fetch_today_signals(date=None):
    """
    Fetch BUY signals for a given date.

    Behavior:
    - If date is None → uses today's IST date
    - If date is provided and data not found → returns []
    - No fallback to previous dates
    """

    trade_date = date or today_ist()

    try:
        r = requests.get(
            f"{BASE_URL}/api/signals",
            params={"date": trade_date},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

    except Exception as e:
        print(f"[signals_api] Request failed: {e}")
        return []

    if not data.get("found"):
        print(f"[signals_api] No data found for {trade_date}")
        return []

    return data.get("data", [])
