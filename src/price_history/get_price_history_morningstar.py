from datetime import datetime, timedelta

import mstarpy
import pandas as pd


def fetch_history_single_stock_morningstar(isin: str, days_back: int) -> pd.DataFrame:
    """
    Fetches historical NAV data from Morningstar and returns a streamlined DataFrame.

    Args:
        isin: ISIN of the fund.
        days_back: Days of history requested.

    Returns:
        Pandas Dataframe with schema: Date, ISIN, Price, Name
    """
    print(f"🌟 Searching for {isin}...")

    try:
        # 1. Get Metadata (Name)
        results = mstarpy.screener_universe(term=isin, field=["isin", "name"])
        if not results:
            print(f"❌ No results found for ISIN: {isin}")
            return None

        # Extract name from the nested structure
        found_name = results[0]["fields"]["name"]
        if isinstance(found_name, dict):
            found_name = found_name.get("value", "Unknown Fund")

        print(f"✅ Found: {found_name}")

        # 2. Fetch History
        fund_instance = mstarpy.Funds(term=isin)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        history = fund_instance.nav(start_date=start_date, end_date=end_date)

        if not history:
            print(f"⚠️ No price history found for {isin}")
            return None

        # 3. Process to Streamlined Schema: Date, ISIN, Price, Name
        df = pd.DataFrame(history)
        df = df.rename(columns={"nav": "Price", "date": "Date"})

        # Ensure correct types and selection
        df["Date"] = pd.to_datetime(df["Date"]).dt.date  # Keep as date for CSV cleanliness

        # Final selection and sorting
        df = df.dropna(subset=["Price"])[["Date", "Price"]]
        return df.sort_values("Date", ascending=False)

    except Exception as e:
        print(f"❌ Error fetching {isin}: {str(e)}")
        return None
