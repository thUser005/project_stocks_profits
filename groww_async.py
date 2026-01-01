import aiohttp
from datetime import datetime, timedelta, time, timezone

# =====================================================
# CONSTANTS
# =====================================================
IST = timezone(timedelta(hours=5, minutes=30))

GROWW_URL = (
    "https://groww.in/v1/api/charting_service/v2/chart/"
    "delayed/exchange/NSE/segment/CASH"
)

HEADERS = {
    "x-app-id": "growwWeb",
    "user-agent": "Mozilla/5.0",
}

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

# =====================================================
# INTERNAL HELPER
# =====================================================
async def _fetch_candles(
    session,
    symbol,
    start_ms,
    end_ms,
    interval,
):
    try:
        async with session.get(
            f"{GROWW_URL}/{symbol}",
            params={
                "intervalInMinutes": interval,
                "startTimeInMillis": start_ms,
                "endTimeInMillis": end_ms,
            },
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            data = await resp.json()
            return data.get("candles", []) or []
    except Exception:
        return []

# =====================================================
# 1️⃣ FULL MARKET DAY CANDLES
# =====================================================
async def fetch_full_day_candles(
    session,
    symbol,
    date_str,
    interval=3,
):
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)

    start_dt = datetime.combine(d.date(), MARKET_OPEN, tzinfo=IST)
    end_dt = datetime.combine(d.date(), MARKET_CLOSE, tzinfo=IST)

    return symbol, await _fetch_candles(
        session,
        symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        interval,
    )

# =====================================================
# 2️⃣ LAST N MINUTES CANDLES
# =====================================================
async def fetch_last_n_minutes_candles(
    session,
    symbol,
    minutes=5,
    interval=3,
):
    end_dt = datetime.now(IST)
    start_dt = end_dt - timedelta(minutes=minutes)

    return symbol, await _fetch_candles(
        session,
        symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        interval,
    )

# =====================================================
# 3️⃣ LATEST CANDLE ONLY
# =====================================================
async def fetch_latest_candle(
    session,
    symbol,
    interval=3,
):
    _, candles = await fetch_last_n_minutes_candles(
        session,
        symbol,
        minutes=interval,
        interval=interval,
    )

    return symbol, candles[-1] if candles else None

# =====================================================
# 4️⃣ GENERIC RANGE FETCH (FIX)
# =====================================================
async def fetch_candles_for_range(
    session,
    symbol,
    start_ms,
    end_ms,
    interval=3,
):
    return symbol, await _fetch_candles(
        session,
        symbol,
        start_ms,
        end_ms,
        interval,
    )
