from pathlib import Path

MAPPING_FILE_PATH = Path(__file__).parent / "ticker_map.json"
BASE_PATH = Path(__file__).parents[3]
PRICE_DATA_PATH = BASE_PATH / "data" / "prices"
TRANSACTION_DATA_PATH = BASE_PATH / "data" / "transactions"
