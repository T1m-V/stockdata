import json
import os
from pathlib import Path


def get_token():
    token_path = Path(__file__).parent / "token.txt"
    if not os.path.exists(token_path):
        raise FileNotFoundError(
            "token.txt not found! Please create it and paste your getquin token inside."
        )
    with open(token_path, "r") as f:
        return f.read().strip()


GETQUIN_URL = "https://api-gql-v2.getquin.com/"

# Main paths
BASE_FOLDER = Path(__file__).parents[2]
DATA_FOLDER = BASE_FOLDER / "data"
PRICES_FOLDER = DATA_FOLDER / "prices"
QUERY_FOLDER = BASE_FOLDER / "queries"

PRICE_DATA_FOLDER = DATA_FOLDER / "prices"
TRANSACTION_DATA_FOLDER = DATA_FOLDER / "transactions"

# Metadata Files
STOCK_METADATA_PATH = DATA_FOLDER / "stock_metadata.json"
CURRENCY_METADATA_PATH = DATA_FOLDER / "currency_metadata.json"

# Main transaction file
TRANSACTION_JSON_PATH = TRANSACTION_DATA_FOLDER / "transactions_export.json"
STOCK_SPLIT_JSON_PATH = TRANSACTION_DATA_FOLDER / "splits_export.json"
TRANSACTIONS_FILE_PATH = TRANSACTION_DATA_FOLDER / "getquin_data.csv"
SNAPSHOT_FILE_PATH = TRANSACTION_DATA_FOLDER / "portfolio_snapshot.csv"
SUMMARY_FILE_PATH = DATA_FOLDER / "latest_prices.csv"

# Queries
SPLIT_QUERY_PATH = QUERY_FOLDER / "stock_split.txt"
TRANSACTION_QUERY_PATH = QUERY_FOLDER / "transactions.txt"

# Pre-load metadata
with open(STOCK_METADATA_PATH, "r") as f:
    STOCK_METADATA: dict[str, dict[str, str]] = json.load(f)

with open(CURRENCY_METADATA_PATH, "r") as f:
    CURRENCY_METADATA: dict[str, dict[str, str]] = json.load(f)
