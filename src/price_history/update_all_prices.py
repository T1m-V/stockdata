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
from price_history.utils.constants import (
    CURRENCY_METADATA_PATH,
    PRICE_DATA_PATH,
    STOCK_METADATA_PATH,
)

HISTORY = 15


def load_stock_metadata() -> dict[str, str]:
    with open(STOCK_METADATA_PATH, "r") as f:
        return json.load(f)


def load_currency_metadata() -> dict[str, str]:
    with open(CURRENCY_METADATA_PATH, "r") as f:
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
    # 1. Load Data
    stock_metadata = load_stock_metadata()
    currency_metadata = load_currency_metadata()
    all_assets = currency_metadata | stock_metadata

    print(f"📋 Processing {len(all_assets)} total assets...")

    for identifier, asset_config in all_assets.items():
        ticker = asset_config.get("ticker")
        waterfall = asset_config.get("waterfall", [])

        last_date = get_last_update_date(identifier)
        now = datetime.now()

        new_data = None
        success = False
        skip_sleep = False

        # 2. Iterate through the waterfall
        for source in waterfall:
            try:
                if source == "Yahoo" and ticker:
                    print(f"🔍 {identifier}: Trying Yahoo Finance...")
                    new_data = fetch_history_single_stock_yahoo(
                        isin=identifier, ticker=ticker, days_back=HISTORY
                    )

                    # Yahoo is fast/official, so we skip sleep if it works
                    if (new_data is not None) and (not new_data.empty):
                        skip_sleep = True

                elif source == "FT":
                    if last_date and (now - last_date).days < 30:
                        print(f"🔄 {identifier}: Using FT.")
                        new_data = fetch_history_single_stock_ft(identifier)
                    else:
                        print(f"⏩ {identifier}: Data gap too large for FT.")
                        continue

                elif source == "Morningstar":
                    print(f"🚀 {identifier}: Fetching from Morningstar...")
                    new_data = fetch_history_single_stock_morningstar(
                        isin=identifier, days_back=HISTORY
                    )

                if (new_data is not None) and (not new_data.empty):
                    save_and_merge(isin=identifier, new_data=new_data)
                    success = True
                    break

            except Exception as e:
                print(f"❌ Error fetching {identifier} from {source}: {e}")
                continue

        if not success:
            print(f"🛑 Failed to update {identifier} after exhausting: {waterfall}")

        # Polite delay for non-Yahoo sources or failed attempts
        if not skip_sleep:
            time.sleep(random.uniform(2, 4))

    print("\n✨ Portfolio Update Complete.")


if __name__ == "__main__":
    update_portfolio_prices()
