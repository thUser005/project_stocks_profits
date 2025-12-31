from datetime import datetime, time, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

START = time(9, 20)
END = time(15, 35)

def is_market_time():
    now = datetime.now(IST).time()
    return START <= now <= END
