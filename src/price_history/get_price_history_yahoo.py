from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


def fetch_history_single_stock_yahoo(isin: str, ticker: str, days_back: int) -> pd.DataFrame | None:
    """
    Fetches historical data from Yahoo Finance and returns a streamlined DataFrame.

    Args:
        isin: ISIN of the fund.
        ticker: Ticker of the fund.
        days_back: Days of history requested.

    Returns:
        Pandas Dataframe with schema: Date, ISIN, Price, Name
    """
    print(f"🚀 Fetching Yahoo history for {isin} ({ticker})...")

    try:
        ticker_obj = yf.Ticker(ticker)
        # Fetching until current time
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        hist = ticker_obj.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
        )

        if hist.empty:
            print(f"⚠️ No data found for {ticker}")
            return None

        # Clean up the dataframe to streamlined schema
        hist = hist[["Close"]].reset_index()
        hist["Date"] = hist["Date"].dt.tz_localize(None).dt.date
        hist = hist.rename(columns={"Close": "Price"})
        df = hist[["Date", "Price"]].copy()

        # Currency logging for user info (internal check)
        currency = ticker_obj.fast_info.get("currency", "Unknown")
        print(f"-> {isin} | Currency: {currency} | Rows: {len(df)}")

        return df.sort_values("Date", ascending=False)

    except Exception as e:
        print(f"❌ Error fetching {isin} via Yahoo: {e}")
        return None
