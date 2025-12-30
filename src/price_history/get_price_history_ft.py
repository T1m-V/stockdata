import re
from io import StringIO

import pandas as pd
import requests


def clean_ft_date(raw_date_str: str) -> str:
    """Cleans the 'double date' artifact from FT.com scrapes."""
    match = re.search(r"(.*?\d{4})", str(raw_date_str))
    if match:
        return match.group(1)
    return raw_date_str


def fetch_history_single_stock_ft(isin: str) -> pd.DataFrame | None:
    """
    Scrapes historical data from FT.com and returns a streamlined DataFrame.

    Args:
        isin: ISIN of the fund.

    Returns:
        Pandas Dataframe with schema: Date, Price
    """
    print(f"🕵️  Scraping FT.com for {isin}...")

    url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={isin}:EUR"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Could not access FT for {isin}")
            return None

        # 1. Parse Table
        html_buffer = StringIO(response.text)
        tables = pd.read_html(html_buffer)

        if not tables:
            print(f"⚠️ No tables found for {isin}")
            return None

        df = tables[0]

        # 2. Clean and Format to Schema: Date, ISIN, Price, Name
        df["Date"] = df["Date"].apply(clean_ft_date)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
        df["Price"] = df["Close"].replace({",": ""}, regex=True).astype(float)

        # Selection and order
        df = df.dropna(subset=["Date", "Price"])[["Date", "Price"]]
        return df.sort_values("Date", ascending=False)

    except Exception as e:
        print(f"❌ Error scraping {isin}: {e}")
        return None
