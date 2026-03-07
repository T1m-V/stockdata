import time
from datetime import datetime, timedelta

import pandas as pd
import requests


def _request_llama_price(
    ticker: str,
    timestamp: int,
    timeout_seconds: float = 10.0,
    max_attempts: int = 3,
    backoff_seconds: float = 0.5,
) -> float | None:
    url = f"https://coins.llama.fi/prices/historical/{timestamp}/{ticker}"

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url=url, timeout=timeout_seconds)
        except Exception as exc:
            if attempt == max_attempts:
                print(f"DeFiLlama request error for {ticker}: {exc}")
                return None
            time.sleep(backoff_seconds * attempt)
            continue

        if response.status_code != 200:
            if attempt == max_attempts:
                return None
            time.sleep(backoff_seconds * attempt)
            continue

        data = response.json()
        coins = data.get("coins", {})
        if ticker not in coins:
            return None

        return coins[ticker].get("price")

    return None


def fetch_history_defillama(ticker: str, days_back: int) -> pd.DataFrame | None:
    """
    Fetches historical prices for DeFi assets from DeFiLlama.

    args:
        ticker: DeFiLlama ticker id.
        days_back: Number of daily points to request.

    returns:
        Dataframe with Date and Price columns.
    """
    results: list[dict[str, datetime.date | float]] = []
    end_date = datetime.now()

    for offset in range(days_back):
        target_dt = end_date - timedelta(days=offset)
        timestamp = int(target_dt.timestamp())

        price = _request_llama_price(ticker=ticker, timestamp=timestamp)
        if price is not None:
            results.append({"Date": target_dt.date(), "Price": price})

        # Keep request pace moderate for public API limits.
        time.sleep(0.15)

    if not results:
        return None

    frame = pd.DataFrame(results)
    return frame.sort_values("Date", ascending=False)
