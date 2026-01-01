import json
from pathlib import Path

import pandas as pd

from historical_transactions.utils.constants import TRANSACTIONS_DATA_PATH, TRANSACTIONS_FILE

TRANSACTIONS_DATA = TRANSACTIONS_DATA_PATH / "transactions_export.json"
SPLIT_DATA = TRANSACTIONS_DATA_PATH / "splits_export.json"


def convert_json_to_csv(tx_file: Path, split_file: Path, output_file: Path) -> None:
    # 1. Load the data
    with open(tx_file, "r") as f:
        tx_data = json.load(f)

    with open(split_file, "r") as f:
        split_data = json.load(f)

    # 2. Process Transactions
    transactions = tx_data["data"]["transactions"]["results"]
    df_tx = pd.json_normalize(transactions)

    # 3. Process Splits
    name_map = (
        df_tx.set_index("isin")[["instrument.name", "instrument.ticker"]]
        .drop_duplicates()
        .to_dict("index")
    )

    splits = split_data["data"]["splits"]
    split_rows = []
    for s in splits:
        isin = s["isin"]
        info = name_map.get(isin, {"instrument.name": isin, "instrument.ticker": "N/A"})
        ratio = s["numerator"] / s["denominator"]

        split_rows.append(
            {
                "timestamp": s["start_date"],
                "transaction_type": "STOCK_SPLIT",
                "instrument.name": info["instrument.name"],
                "instrument.ticker": info["instrument.ticker"],
                "instrument.symbol": isin,
                "instrument.category": "split",
                "units": ratio,
                "price": 0,
                "price_currency": "",
                "costs": 0,
                "taxes": 0,
                "security_name": "Corporate Action",
                "id": f"split_{isin}_{s['start_date']}",
            }
        )
    df_splits = pd.DataFrame(split_rows)

    # 4. Combine Datasets
    df_combined = pd.concat([df_tx, df_splits], ignore_index=True)

    # 5. Define clean column names
    column_mapping = {
        "timestamp": "Date",
        "transaction_type": "Type",
        "instrument.name": "Asset Name",
        "instrument.symbol": "ISIN",
        "units": "Quantity",
        "price": "Price",
        "price_currency": "Currency",
        "costs": "Fees",
        "taxes": "Taxes",
    }

    # 6. Filter and rename
    df_clean = df_combined.rename(columns=column_mapping)
    final_cols = [col for col in column_mapping.values() if col in df_clean.columns]
    df_final = df_clean[final_cols].copy()

    # 7. FIX: Unified Timestamp Conversion
    # 'utc=True' makes everything tz-aware first, then '.dt.tz_localize(None)' strips it away
    df_final["Date"] = pd.to_datetime(df_final["Date"], format="ISO8601", utc=True).dt.tz_localize(
        None
    )

    # 8. Sort (Now safe to compare)
    df_final = df_final.sort_values(by="Date", ascending=False)
    df_final["Date"] = df_final["Date"].dt.strftime("%Y-%m-%d")

    # 9. Export
    df_final.to_csv(output_file, index=False)
    print(f"Successfully converted data to {output_file}")


if __name__ == "__main__":
    convert_json_to_csv(
        tx_file=TRANSACTIONS_DATA, split_file=SPLIT_DATA, output_file=TRANSACTIONS_FILE
    )
