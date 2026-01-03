from pathlib import Path

BASE_PATH = Path(__file__).parents[3]
DATA_PATH = BASE_PATH / "data"
STOCK_METADATA_PATH = DATA_PATH / "stock_metadata.json"
CURRENCY_METADATA_PATH = DATA_PATH / "currency_metadata.json"
PRICE_DATA_PATH = DATA_PATH / "prices"
TRANSACTION_DATA_PATH = DATA_PATH / "transactions"
