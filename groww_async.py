import aiohttp
import time as _time
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
# LOGGING
# =====================================================
def log(msg: str):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"[{ts}] {msg}", flush=True)

# throttle candle logs per symbol
_LAST_CANDLE_LOG_TS = {}

LOG_INTERVAL_SECONDS = 10  # log once per symbol per 10s

# =====================================================
# INTERNAL HELPER (LOW LEVEL)
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
            candles = data.get("candles", []) or []

            # ---------- SERVER LOG (THROTTLED) ----------
            now_ts = _time.time()
            last_ts = _LAST_CANDLE_LOG_TS.get(symbol, 0)

            if now_ts - last_ts >= LOG_INTERVAL_SECONDS:
                log(
                    f"CANDLES_FETCHED :: "
                    f"{symbol} | interval={interval}m | candles={len(candles)}"
                )
                _LAST_CANDLE_LOG_TS[symbol] = now_ts

            return candles

    except Exception as e:
        log(f"CANDLES_FETCH_FAILED :: {symbol} | {e}")
        return []

# =====================================================
# 1️⃣ FULL MARKET DAY CANDLES (09:15–15:30)
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

    candles = await _fetch_candles(
        session,
        symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        interval,
    )

    return symbol, candles

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

    candles = await _fetch_candles(
        session,
        symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        interval,
    )

    return symbol, candles

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

    latest = candles[-1] if candles else None

    if latest is None:
        log(f"LATEST_CANDLE_EMPTY :: {symbol}")

    return symbol, latest

# =====================================================
# 4️⃣ GENERIC RANGE FETCH (RAW TIMESTAMP)
# =====================================================
async def fetch_candles_for_range(
    session,
    symbol,
    start_ms,
    end_ms,
    interval=3,
):
    candles = await _fetch_candles(
        session,
        symbol,
        start_ms,
        end_ms,
        interval,
    )

    return symbol, candles

# =====================================================
# 5️⃣ INTRADAY TIME-RANGE FETCH (🔥 REQUIRED FOR SELL LOGIC)
# =====================================================
async def fetch_intraday_candles(
    session,
    symbol,
    start_time: time,
    end_time: time,
    interval=3,
):
    """
    Fetch candles for today between given IST times
    Example:
        09:15 → 10:00
    """

    today = datetime.now(IST).date()

    start_dt = datetime.combine(today, start_time, tzinfo=IST)
    end_dt = datetime.combine(today, end_time, tzinfo=IST)

    candles = await _fetch_candles(
        session,
        symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        interval,
    )

    return symbol, candles
