from price_history.retrieve_last_prices import generate_latest_prices_summary
from price_history.update_all_prices import update_portfolio_prices


def main():
    print("🚀 Starting Price Robot...")

    # Step 1: Update all historical CSV files
    print("Step 1: Updating historical price data...")
    update_portfolio_prices()

    # Step 2: Generate the summary 'latest_prices.csv'
    print("Step 2: Generating latest prices summary...")
    generate_latest_prices_summary()

    print("✨ Price Robot finished successfully.")


if __name__ == "__main__":
    main()
