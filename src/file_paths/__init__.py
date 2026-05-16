import json
import os
import subprocess
from pathlib import Path
from typing import Mapping


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

PRIVATE_DATA_FOLDERS = ("transactions", "real_estate", "blockchain")


def _has_private_dashboard_data(data_folder: Path) -> bool:
    return all((data_folder / folder).exists() for folder in PRIVATE_DATA_FOLDERS)


def _git_common_data_folder(base_folder: Path, git_common_dir: Path | None = None) -> Path | None:
    if git_common_dir is None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--git-common-dir"],
                cwd=base_folder,
                capture_output=True,
                check=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError):
            return None

        common_dir_text = result.stdout.strip()
        if not common_dir_text:
            return None
        git_common_dir = Path(common_dir_text)

    if not git_common_dir.is_absolute():
        git_common_dir = base_folder / git_common_dir

    checkout_root = git_common_dir.resolve().parent
    data_folder = checkout_root / "data"
    if data_folder.exists():
        return data_folder
    return None


def _resolve_data_folder(
    *,
    base_folder: Path = BASE_FOLDER,
    environ: Mapping[str, str] | None = None,
    git_common_dir: Path | None = None,
) -> Path:
    env = os.environ if environ is None else environ
    override = env.get("STOCKDATA_DATA_DIR")
    if override:
        data_folder = Path(override).expanduser()
        if not data_folder.exists():
            raise FileNotFoundError(f"STOCKDATA_DATA_DIR does not exist: {data_folder}")
        return data_folder.resolve()

    local_data_folder = base_folder / "data"
    if _has_private_dashboard_data(local_data_folder):
        return local_data_folder

    main_data_folder = _git_common_data_folder(
        base_folder=base_folder,
        git_common_dir=git_common_dir,
    )
    if main_data_folder and _has_private_dashboard_data(main_data_folder):
        return main_data_folder

    return local_data_folder


DATA_FOLDER = _resolve_data_folder()
PRICES_FOLDER = DATA_FOLDER / "prices"
LP_PRICES_FOLDER = PRICES_FOLDER / "lp_prices"
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


def get_direct_price_file_path(symbol: str, prices_folder: Path | None = None) -> Path:
    root = prices_folder or PRICES_FOLDER
    return root / f"{symbol}.csv"


def get_lp_price_file_path(
    *,
    chain: str,
    symbol: str,
    prices_folder: Path | None = None,
) -> Path:
    root = prices_folder or PRICES_FOLDER
    return root / "lp_prices" / chain / f"{symbol}.csv"


# Queries
SPLIT_QUERY_PATH = QUERY_FOLDER / "stock_split.txt"
TRANSACTION_QUERY_PATH = QUERY_FOLDER / "transactions.txt"

# Pre-load metadata
with open(STOCK_METADATA_PATH, "r") as f:
    STOCK_METADATA: dict[str, dict[str, str]] = json.load(f)

with open(CURRENCY_METADATA_PATH, "r") as f:
    CURRENCY_METADATA: dict[str, dict[str, str]] = json.load(f)

# Blockchain related paths
BLOCKCHAIN_FOLDER = DATA_FOLDER / "blockchain"
CHAIN_INFO_PATH = BLOCKCHAIN_FOLDER / "chain_info.json"
TOKENS_FOLDER = BLOCKCHAIN_FOLDER / "tokens"
BLOCKCHAIN_TRANSACTIONS_FOLDER = BLOCKCHAIN_FOLDER / "transactions"
BLOCKCHAIN_SNAPSHOT_FOLDER = BLOCKCHAIN_FOLDER / "snapshots"
BLOCKCHAIN_BLOCK_MAP_FOLDER = BLOCKCHAIN_FOLDER / "block_map"
PROTOCOL_UNDERLYING_TOKEN_FOLDER = BLOCKCHAIN_FOLDER / "protocol_underlying_tokens"

# Real estate related paths
REAL_ESTATE_FOLDER = DATA_FOLDER / "real_estate"
REAL_ESTATE_COSTS_FILE_NAME = "costs.csv"
REAL_ESTATE_INFLOWS_FILE_NAME = "inflows.csv"
REAL_ESTATE_VALUES_FILE_NAME = "values.csv"
REAL_ESTATE_OWNERSHIP_FILE_NAME = "ownership.csv"
REAL_ESTATE_MORTGAGE_GLOB = "*mortgage*.csv"
