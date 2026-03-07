import re
import time
from io import StringIO

import pandas as pd
import requests


def clean_ft_date(raw_date_str: str) -> str:
    """Cleans the double-date artifact from FT.com scrapes."""
    match = re.search(r"(.*?\d{4})", str(raw_date_str))
    if match:
        return match.group(1)
    return raw_date_str


def _get_with_retries(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float = 10.0,
    max_attempts: int = 3,
    backoff_seconds: float = 0.8,
) -> requests.Response | None:
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url=url, headers=headers, timeout=timeout_seconds)
        except Exception as exc:
            if attempt == max_attempts:
                print(f"FT request failed after retries: {exc}")
                return None
            time.sleep(backoff_seconds * attempt)
            continue

        if response.status_code == 200:
            return response

        if attempt == max_attempts:
            print(f"Could not access FT URL: status={response.status_code}")
            return None

        time.sleep(backoff_seconds * attempt)

    return None


def fetch_history_single_stock_ft(isin: str) -> pd.DataFrame | None:
    """
    Scrapes historical data from FT.com.

    args:
        isin: Fund identifier.

    returns:
        Dataframe with Date and Price columns.
    """
    print(f"Fetching FT history for {isin}...")

    url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={isin}:EUR"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = _get_with_retries(url=url, headers=headers)
    if response is None:
        return None

    try:
        html_buffer = StringIO(response.text)
        tables = pd.read_html(html_buffer)
    except Exception as exc:
        print(f"Error parsing FT HTML for {isin}: {exc}")
        return None

    if not tables:
        print(f"No FT tables found for {isin}")
        return None

    try:
        frame = tables[0]
        frame["Date"] = frame["Date"].apply(clean_ft_date)
        frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.date
        frame["Price"] = frame["Close"].replace({",": ""}, regex=True).astype(float)
        frame = frame.dropna(subset=["Date", "Price"])[["Date", "Price"]]
    except Exception as exc:
        print(f"Error cleaning FT data for {isin}: {exc}")
        return None

    if frame.empty:
        return None

    return frame.sort_values("Date", ascending=False)
