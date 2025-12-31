import asyncio
import aiohttp
from typing import Dict, List

from companies import load_companies
from signals_api import fetch_today_signals
from groww_async import fetch_candles_for_range
from time_utils import market_window_for_date

# =====================================================
# CONFIG
# =====================================================
CONCURRENCY = 12        # safe for Groww
BATCH_SIZE = 25         # symbols per async batch
MAX_RETRIES = 3
REQUEST_TIMEOUT = 20

# =====================================================
# LOAD METADATA
# =====================================================
companies = load_companies()


# =====================================================
# HELPERS
# =====================================================
def chunked(items: List[str], size: int):
    """Yield list chunks"""
    for i in range(0, len(items), size):
        yield items[i:i + size]


async def fetch_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbols: List[str],
    start_ms: int,
    end_ms: int,
):
    """Fetch candles for a batch of symbols safely"""
    async def _one(symbol):
        async with semaphore:
            return await fetch_candles_for_range(
                session,
                symbol,
                start_ms,
                end_ms,
            )

    tasks = [_one(sym) for sym in symbols]
    return await asyncio.gather(*tasks, return_exceptions=True)


# =====================================================
# MAIN TEST RUNNER
# =====================================================
async def run_test_for_date(
    date: str,
    symbols_filter: List[str] | None = None,
) -> Dict:
    """
    Fetch full market-day candles for all signals on a given date.

    Args:
        date (YYYY-MM-DD)
        symbols_filter (optional list of symbols to limit scope)

    Returns:
        {
          date,
          count,
          data: {
            SYMBOL: {
              company,
              candles
            }
          }
        }
    """

    signals = fetch_today_signals(date)
    if not signals:
        return {
            "date": date,
            "count": 0,
            "data": {},
        }

    start_ms, end_ms = market_window_for_date(date)

    # ---------------------------------
    # Build symbol list
    # ---------------------------------
    symbols = []
    for s in signals:
        sym = s["symbol"]
        if sym not in companies:
            continue
        if symbols_filter and sym not in symbols_filter:
            continue
        symbols.append(sym)

    if not symbols:
        return {
            "date": date,
            "count": 0,
            "data": {},
        }

    # ---------------------------------
    # Async session setup
    # ---------------------------------
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    payload = {}

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
    ) as session:

        # ---------------------------------
        # Process in batches
        # ---------------------------------
        for batch in chunked(symbols, BATCH_SIZE):
            retries = 0

            while retries < MAX_RETRIES:
                try:
                    results = await fetch_batch(
                        session,
                        semaphore,
                        batch,
                        start_ms,
                        end_ms,
                    )

                    for item in results:
                        if isinstance(item, Exception):
                            continue

                        symbol, candles = item
                        if not candles:
                            continue

                        payload[symbol] = {
                            "company": companies[symbol]["company"],
                            "candles": candles,
                        }

                    break  # batch success

                except Exception:
                    retries += 1
                    await asyncio.sleep(2)

    # ---------------------------------
    # Return structured response
    # ---------------------------------
    return {
        "date": date,
        "count": len(payload),
        "data": payload,
    }
