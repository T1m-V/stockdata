import json
import random
import time
from datetime import datetime

import pandas as pd

from price_history import (
    fetch_history_single_stock_ft,
    fetch_history_single_stock_morningstar,
    fetch_history_single_stock_yahoo,
)
from price_history.utils.constants import MAPPING_FILE_PATH, PRICE_DATA_PATH

HISTORY = 90


def load_ticker_map() -> dict[str, str]:
    with open(MAPPING_FILE_PATH, "r") as f:
        return json.load(f)


def get_last_update_date(isin: str) -> pd.Timestamp | None:
    """Checks the local CSV to see the date of the most recent entry."""
    file_path = PRICE_DATA_PATH / f"{isin}.csv"
    if not file_path.exists():
        return None

    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return None
        return pd.to_datetime(df["Date"]).max()
    except Exception:
        return None


def save_and_merge(isin: str, new_data: pd.DataFrame) -> None:
    """Helper to handle the file I/O consistently for all sources."""
    if new_data is None or new_data.empty:
        return

    file_path = PRICE_DATA_PATH / f"{isin}.csv"

    if file_path.exists():
        existing_df = pd.read_csv(file_path)
        existing_df["Date"] = pd.to_datetime(existing_df["Date"]).dt.date
        new_data["Date"] = pd.to_datetime(new_data["Date"]).dt.date

        final_df = pd.concat([existing_df, new_data]).drop_duplicates(subset=["Date"], keep="last")
    else:
        final_df = new_data
    final_df["Price"] = final_df["Price"].round(4)
    final_df.sort_values("Date", ascending=False).to_csv(file_path, index=False)


def update_portfolio_prices() -> None:
    """Updated controller using the waterfall mapping logic."""

    # 1. Load Data and Mappings
    ticker_map = load_ticker_map()
    all_isins = ticker_map.keys()

    print(f"📋 Processing {len(all_isins)} assets via waterfall mapping...")

    for isin in all_isins:
        # Skip if ISIN not in our config
        asset_config: dict[str, str | list[str]] = ticker_map[isin]
        ticker: str = asset_config.get("ticker")
        waterfall: list[str] = asset_config.get("waterfall", [])

        last_date = get_last_update_date(isin)
        now = datetime.now()

        new_data = None
        success = False
        skip_sleep = False

        # 2. Iterate through the waterfall
        for source in waterfall:
            try:
                if source == "Yahoo" and ticker:
                    print(f"🔍 {isin}: Trying Yahoo Finance...")
                    new_data = fetch_history_single_stock_yahoo(
                        isin=isin, ticker=ticker, days_back=HISTORY
                    )
                    if (new_data is not None) and (not new_data.empty):
                        skip_sleep = True

                elif source == "FT":
                    # Applying your specific logic: Check for freshness gap
                    if last_date and (now - last_date).days < 30:
                        print(f"🔄 {isin}: Recent data exists. Using FT.")
                        new_data = fetch_history_single_stock_ft(isin)
                    else:
                        print(f"⏩ {isin}: Data gap too large for FT. Skipping to next source.")
                        continue

                elif source == "Morningstar":
                    print(f"🚀 {isin}: Fetching from Morningstar...")
                    new_data = fetch_history_single_stock_morningstar(isin=isin, days_back=HISTORY)

                elif source == "Evi":
                    print(f"🏦 {isin}: Fetching from Evi...")
                    print("Not integrated yet.")
                    continue

                # 3. If we got data, save and break the waterfall loop
                if (new_data is not None) and (not new_data.empty):
                    save_and_merge(isin=isin, new_data=new_data)
                    success = True
                    print("")
                    break

            except Exception as e:
                print(f"❌ Error fetching {isin} from {source}: {e}")
                continue  # Try the next source in the waterfall

        if not success:
            print(f"🛑 Failed to update {isin} after exhausting all sources: {waterfall}")

        # Polite delay to prevent IP blocking
        if not skip_sleep:
            time.sleep(random.uniform(2, 5))

    print("\n✨ Portfolio Update Complete.")


if __name__ == "__main__":
    update_portfolio_prices()
