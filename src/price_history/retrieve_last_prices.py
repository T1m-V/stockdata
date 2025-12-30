import pandas as pd

from price_history.utils.constants import (
    PRICE_DATA_PATH,
)

SUMMARY_FILENAME = "latest_prices.csv"


def generate_latest_prices_summary() -> None:
    """
    Reads all ISIN CSV files and creates a single 'latest_prices.csv'.
    Excludes the summary file itself if it exists in the same directory.
    """
    summary_data = []

    if not PRICE_DATA_PATH.exists():
        print("❌ Price data directory does not exist.")
        return

    # Filter: Only grab .csv files and exclude the summary file itself
    csv_files = [f for f in PRICE_DATA_PATH.glob("*.csv") if f.name != SUMMARY_FILENAME]

    if not csv_files:
        print("⚠️ No price files found to summarize.")
        return

    print(f"📊 Generating summary for {len(csv_files)} assets...")

    for file_path in csv_files:
        try:
            # nrows=1 is efficient; it only reads the header and first data row
            df = pd.read_csv(file_path, nrows=1)

            if not df.empty:
                isin = file_path.stem
                latest_date = df.iloc[0]["Date"]
                latest_price = df.iloc[0]["Price"]

                summary_data.append({"date": latest_date, "isin": isin, "price": latest_price})
        except Exception as e:
            print(f"⚠️ Skipping {file_path.name}: {e}")

    summary_df = pd.DataFrame(summary_data)

    # Save it back to the same folder (now safe because of the filter above)
    output_path = PRICE_DATA_PATH / SUMMARY_FILENAME
    summary_df.to_csv(output_path, index=False)

    print(f"✅ Summary saved to: {output_path}")


if __name__ == "__main__":
    generate_latest_prices_summary()
