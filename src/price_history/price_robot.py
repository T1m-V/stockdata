from price_history.retrieve_last_prices import generate_latest_prices_summary
from price_history.update_all_prices import update_portfolio_prices


def main() -> int:
    """
    Runs the daily price update robot.

    returns:
        Process exit code.
    """
    print("Starting price robot...")

    try:
        print("Step 1: Updating historical price data...")
        update_results = update_portfolio_prices()

        print("Step 2: Generating latest prices summary...")
        summary_frame = generate_latest_prices_summary()
    except Exception as exc:
        print(f"Price robot failed: {exc}")
        return 1

    success_count = len([result for result in update_results if result.success])
    skipped_count = len([result for result in update_results if result.skipped])
    failed_count = len(update_results) - success_count - skipped_count

    print(
        "Price robot finished: "
        f"updated={success_count}, skipped={skipped_count}, failed={failed_count}, "
        f"summary_rows={len(summary_frame)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
