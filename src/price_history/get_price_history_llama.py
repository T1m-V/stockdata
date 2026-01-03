import time
from datetime import datetime, timedelta

import pandas as pd
import requests


def fetch_history_defillama(ticker: str, days_back: int) -> pd.DataFrame | None:
    """Fetches historical prices for DeFi assets from DeFiLlama."""
    results = []
    end_date = datetime.now()

    for i in range(days_back):
        target_dt = end_date - timedelta(days=i)
        timestamp = int(target_dt.timestamp())

        # DeFiLlama historical endpoint
        url = f"https://coins.llama.fi/prices/historical/{timestamp}/{ticker}"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                coins = data.get("coins", {})

                if ticker in coins:
                    price = coins[ticker]["price"]
                    results.append({"Date": target_dt.date(), "Price": price})

            # Short sleep to stay within free tier limits (approx 30-60 requests/min)
            time.sleep(0.15)

        except Exception as e:
            print(f"⚠️ DeFiLlama Error on {target_dt.date()}: {e}")
            continue

    if not results:
        return None

    return pd.DataFrame(results)
