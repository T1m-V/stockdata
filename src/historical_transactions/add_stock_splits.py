import json
from pathlib import Path

import pandas as pd
import requests

from file_paths import (
    GETQUIN_URL,
    SPLIT_QUERY_PATH,
    STOCK_SPLIT_JSON_PATH,
    TRANSACTIONS_FILE_PATH,
    get_token,
)

HEADERS = {
    "authorization": get_token(),
    "content-type": "application/json",
    "accept": "*/*",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def get_dynamic_parameters(transaction_file: Path) -> tuple[list[str], str, str]:
    print(f"Analyzing {transaction_file} to determine date range and ISINs...")

    with open(transaction_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = data["data"]["transactions"]["results"]
    df = pd.json_normalize(results)

    # Get time range
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")
    start_date = df["timestamp"].min().strftime("%Y-%m-%d")
    end_date = df["timestamp"].max().strftime("%Y-%m-%d")

    # Get isins
    isins = df["isin"].unique().tolist()
    print(f"Found {len(isins)} unique assets. Date range: {start_date} to {end_date}")

    return isins, start_date, end_date


def download_splits(transaction_file: Path, output_file: Path) -> None:
    isins, start_date, end_date = get_dynamic_parameters(transaction_file=transaction_file)

    payload = {
        "operationName": "getSplits",
        "variables": {
            "include_future": True,
            "isin__in": isins,
            "start_date_from": start_date,
            "start_date_to": end_date,
        },
        "query": SPLIT_QUERY_PATH.read_text(encoding="utf-8"),
    }

    print("Requesting splits from API...")
    try:
        response = requests.post(GETQUIN_URL, headers=HEADERS, json=payload)
        response.raise_for_status()

        data = response.json()
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        count = len(data.get("data", {}).get("splits", []))
        print(f"✅ Success! Found {count} splits. Saved to {output_file}")

    except Exception as e:
        print(f"API Error: {e}")


if __name__ == "__main__":
    download_splits(transaction_file=TRANSACTIONS_FILE_PATH, output_file=STOCK_SPLIT_JSON_PATH)
