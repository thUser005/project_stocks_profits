import aiohttp
from datetime import datetime

GROWW_URL = (
    "https://groww.in/v1/api/charting_service/v2/chart/"
    "delayed/exchange/NSE/segment/CASH"
)

HEADERS = {
    "x-app-id": "growwWeb",
    "user-agent": "Mozilla/5.0"
}


async def fetch_latest_candle(session, symbol):
    end = int(datetime.now().timestamp() * 1000)
    start = end - 3 * 60 * 1000

    params = {
        "intervalInMinutes": 3,
        "startTimeInMillis": start,
        "endTimeInMillis": end,
    }

    try:
        async with session.get(
            f"{GROWW_URL}/{symbol}",
            params=params,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            data = await resp.json()
            candles = data.get("candles") or []
            return symbol, candles[-1] if candles else None
    except Exception:
        return symbol, None
