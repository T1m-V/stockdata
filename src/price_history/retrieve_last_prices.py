from pathlib import Path

import pandas as pd

from file_paths import PRICE_DATA_FOLDER, SUMMARY_FILE_PATH

SUMMARY_COLUMNS = ["date", "isin", "price"]


def _list_price_files(price_folder: Path) -> list[Path]:
    return sorted(price_folder.glob("*.csv"))


def _read_latest_row(file_path: Path) -> dict[str, str | float] | None:
    try:
        frame = pd.read_csv(file_path, nrows=1)
    except Exception as exc:
        print(f"Skipping {file_path.name}: {exc}")
        return None

    if frame.empty:
        return None

    return {
        "date": frame.iloc[0]["Date"],
        "isin": file_path.stem,
        "price": frame.iloc[0]["Price"],
    }


def generate_latest_prices_summary() -> pd.DataFrame:
    """
    Reads all local price CSV files and writes latest_prices.csv.

    returns:
        Generated summary frame.
    """
    if not PRICE_DATA_FOLDER.exists():
        print("Price data directory does not exist.")
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    csv_files = _list_price_files(price_folder=PRICE_DATA_FOLDER)
    if not csv_files:
        print("No price files found to summarize.")
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    print(f"Generating latest summary for {len(csv_files)} assets...")

    summary_rows = []
    for file_path in csv_files:
        row = _read_latest_row(file_path=file_path)
        if row is not None:
            summary_rows.append(row)

    summary_frame = pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS)
    if summary_frame.empty:
        summary_frame = pd.DataFrame(columns=SUMMARY_COLUMNS)
    else:
        summary_frame = summary_frame.sort_values(by="isin", ascending=True)

    summary_frame.to_csv(SUMMARY_FILE_PATH, index=False)
    print(f"Summary saved to: {SUMMARY_FILE_PATH}")
    return summary_frame


if __name__ == "__main__":
    generate_latest_prices_summary()
