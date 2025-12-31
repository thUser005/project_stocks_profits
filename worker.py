import asyncio
import aiohttp
from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import fetch_latest_candle
from telegram_msg import send_message
from time_utils import is_market_time

companies = load_companies()
alerted = set()

CONCURRENCY = 50        # safe for Groww
SLEEP_INTERVAL = 30


async def run_worker():
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:
            try:
                if not is_market_time():
                    await asyncio.sleep(60)
                    continue

                signals = fetch_today_signals()
                if not signals:
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                tasks = []

                for s in signals:
                    symbol = s["symbol"]

                    if symbol not in companies:
                        continue
                    if symbol in alerted:
                        continue

                    tasks.append(fetch_latest_candle(session, symbol))

                results = await asyncio.gather(*tasks)

                price_map = {sym: candle for sym, candle in results}

                for s in signals:
                    symbol = s["symbol"]
                    if symbol in alerted:
                        continue

                    candle = price_map.get(symbol)
                    if not candle:
                        continue

                    open_price = s["open"]
                    entry = s["entry"]
                    target = s["target"]
                    sl = s["stoploss"]

                    ltp = candle[4]

                    if ltp >= open_price:
                        meta = companies[symbol]

                        msg = (
                            f"📢 STOCK TRIGGERED\n\n"
                            f"Company: {meta['company']}\n"
                            f"Symbol: {symbol}\n\n"
                            f"Open: {open_price}\n"
                            f"LTP: {ltp}\n"
                            f"Entry: {entry}\n"
                            f"Target: {target}\n"
                            f"SL: {sl}"
                        )

                        send_message(msg)
                        alerted.add(symbol)

                await asyncio.sleep(SLEEP_INTERVAL)

            except Exception as e:
                send_message(f"⚠️ Worker error:\n{e}")
                await asyncio.sleep(15)
