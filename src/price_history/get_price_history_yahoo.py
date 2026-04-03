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
        Pandas Dataframe with schema: Date, Price
    """
    print(f"[yahoo] Fetching history for {isin} ({ticker})...")

    try:
        ticker_obj = yf.Ticker(ticker)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        hist = ticker_obj.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
        )

        if hist.empty:
            print(f"[yahoo] No data found for {ticker}")
            return None

        frame = hist[["Close"]].reset_index()
        frame["Date"] = frame["Date"].dt.tz_localize(None).dt.date
        frame = frame.rename(columns={"Close": "Price"})
        frame = frame[["Date", "Price"]].copy()

        currency = ticker_obj.fast_info.get("currency", "Unknown")
        print(f"[yahoo] {isin} | Currency: {currency} | Rows: {len(frame)}")

        return frame.sort_values("Date", ascending=False)

    except Exception as e:
        print(f"[yahoo] Error fetching {isin} via Yahoo: {e}")
        return None
