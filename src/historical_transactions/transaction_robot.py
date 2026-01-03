from file_paths import (
    SNAPSHOT_FILE_PATH,
    STOCK_SPLIT_JSON_PATH,
    TRANSACTION_JSON_PATH,
    TRANSACTIONS_FILE_PATH,
)
from historical_transactions.add_stock_splits import download_splits
from historical_transactions.get_getquin_transactions import download_transactions
from historical_transactions.portfolio_snapshots import generate_portfolio_snapshots
from historical_transactions.transform_data import convert_transaction_json_to_csv


def main():
    print("🚀 Starting Transaction Robot...")

    # Step 1: Update all transactions and splits.
    print("\nStep 1: Updating historical transaction data...")
    download_transactions(output_file=TRANSACTION_JSON_PATH)
    download_splits(transaction_file=TRANSACTION_JSON_PATH, output_file=STOCK_SPLIT_JSON_PATH)

    # Step 2: Generate the summary 'latest_prices.csv'
    print("\nStep 2: Create transaction .csv file...")
    convert_transaction_json_to_csv(
        tx_file=TRANSACTION_JSON_PATH,
        split_file=STOCK_SPLIT_JSON_PATH,
        output_file=TRANSACTIONS_FILE_PATH,
    )

    print("\nStep 3: Make portfolio snapshots for all dates...")
    generate_portfolio_snapshots(input_csv=TRANSACTIONS_FILE_PATH, output_csv=SNAPSHOT_FILE_PATH)

    print("\n✨ Transaction Robot finished successfully.")


if __name__ == "__main__":
    main()
