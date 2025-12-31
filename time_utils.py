from datetime import datetime, time, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)


def is_market_time():
    """
    Returns True if current IST time is within market hours
    """
    now = datetime.now(IST).time()
    return MARKET_OPEN <= now <= MARKET_CLOSE


def market_window_for_date(date_str):
    """
    Returns (start_ms, end_ms) for market hours of a given date (IST)
    """
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)

    start = datetime.combine(d.date(), MARKET_OPEN, tzinfo=IST)
    end = datetime.combine(d.date(), MARKET_CLOSE, tzinfo=IST)

    return (
        int(start.timestamp() * 1000),
        int(end.timestamp() * 1000),
    )
