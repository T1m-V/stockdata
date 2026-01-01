from pathlib import Path

BASE_PATH = Path(__file__).parents[3]
DATA_PATH = BASE_PATH / "data"
MAPPING_FILE_PATH = DATA_PATH / "ticker_map.json"
PRICE_DATA_PATH = DATA_PATH / "prices"
TRANSACTION_DATA_PATH = DATA_PATH / "transactions"
