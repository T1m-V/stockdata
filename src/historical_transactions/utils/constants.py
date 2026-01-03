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
TOKEN = get_token()

BASE_PATH = Path(__file__).parents[3]
DATA_PATH = BASE_PATH / "data"
STOCK_METADATA_PATH = DATA_PATH / "stock_metadata.json"
TRANSACTIONS_DATA_PATH = DATA_PATH / "transactions"
PRICES_PATH = DATA_PATH / "prices"
TRANSACTIONS_FILE = TRANSACTIONS_DATA_PATH / "getquin_data.csv"
QUERY_PATH = BASE_PATH / "queries"
