import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd

from file_paths import CURRENCY_METADATA, PRICES_FOLDER, TRANSACTION_DATA_FOLDER
from historical_transactions.portfolio_snapshots import get_forex_rate


@functools.lru_cache(maxsize=None)
def get_price_history(coin: str) -> pd.DataFrame:
    """Loads the history of prices for a specific coin.

    Args:
        coin: The coin you want the history for.

    Returns:
        Price history of requested coin.
    """
    file_path = PRICES_FOLDER / f"{coin}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"⚠️ Warning: No data for {coin}.")

    df = pd.read_csv(file_path)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df.sort_values("Date", ascending=True)


def get_crypto_price(coin: str, date: str) -> float:
    """Retrieves exchange rate of a specific coin on a date.

    Args:
        coin: The coin you want the price for.
        date: On which date you want the price.

    Returns:
        Crypto price on the requested date.
    """
    coin_prices = get_price_history(coin)
    target_date = pd.to_datetime(date).date()

    # Find nearest date on or before target
    rate_row = coin_prices[coin_prices["Date"] <= target_date]

    if rate_row.empty:
        # Fallback: Warning or use the oldest available date
        print(f"⚠️ No price found for {coin} on/before {date}. Using oldest known price.")
        price = coin_prices.iloc[0]["Price"]
    else:
        # Get the last row (closest date)
        price = rate_row.iloc[-1]["Price"]

    currency_type = CURRENCY_METADATA[coin]["currency"]
    conversion = get_forex_rate(currency=currency_type, date=date)

    return price * conversion


@dataclass
class CryptoPosition:
    """Tracks the running state and calculations of a single crypto position."""

    coin: str
    quantity: float = 0.0
    principal: float = 0.0

    def buy(self, qty_out: float, qty_in: float, currency: str, date: str):
        self.quantity += qty_out
        self.principal += qty_in * get_forex_rate(currency=currency, date=date)

    def sell(self, qty_out: float, qty_in: float, currency: str, date: str):
        self.quantity -= qty_in
        self.principal -= qty_out * get_forex_rate(currency=currency, date=date)

    def swap(self, qty_out: float, coin_in: "CryptoPosition", qty_coin_in: float, date: str):
        self.quantity += qty_out
        coin_in.quantity -= qty_coin_in
        invested = qty_out * get_crypto_price(self.coin, date)
        self.principal += invested
        coin_in.principal -= invested

    def reward(self, qty: float, coin: "CryptoPosition", date: str):
        self.swap(qty_out=qty, coin_in=coin, qty_coin_in=0, date=date)

    def to_snapshot(self, date) -> dict:
        return {
            "Date": date,
            "Coin": self.coin,
            "Quantity": round(self.quantity, 6),
            "Principal Invested": round(self.principal, 2),
        }


class CryptoTracker:
    def __init__(self):
        self.assets: Dict[str, CryptoPosition] = {}
        self.history: List[dict] = []

    def fetch_asset(self, coin: str) -> CryptoPosition:
        if coin not in self.assets:
            self.assets[coin] = CryptoPosition(coin=coin)
        return self.assets[coin]

    def process_transaction(self, row: pd.Series):
        tx_type: str = row["Type"]
        qty_in = row["Qty in"]
        coin_in = row["Token in"]
        qty_out = row["Qty out"]
        coin_out = row["Token out"]
        date = row["Date"]

        if tx_type == "buy":
            asset_out = self.fetch_asset(coin_out)
            asset_out.buy(qty_out=qty_out, qty_in=qty_in, currency=coin_in, date=date)
            new_snapshots = [asset_out.to_snapshot(date)]

        elif tx_type == "sell":
            asset_in = self.fetch_asset(coin_in)
            asset_in.sell(qty_out=qty_out, qty_in=qty_in, currency=coin_out, date=date)
            new_snapshots = [asset_in.to_snapshot(date)]

        elif tx_type == "swap":
            asset_in = self.fetch_asset(coin_in)
            asset_out = self.fetch_asset(coin_out)
            asset_out.swap(qty_out=qty_out, coin_in=asset_in, qty_coin_in=qty_in, date=date)
            new_snapshots = [asset_in.to_snapshot(date), asset_out.to_snapshot(date)]

        elif tx_type.startswith("reward"):
            new_snapshots = []
            asset_out = self.fetch_asset(coin_out)
            if "|" in tx_type:
                reward_coins = tx_type.split("|")[-1].split(",")
                n_coins = len(reward_coins)
                for reward_coin in reward_coins:
                    asset_in = self.fetch_asset(reward_coin)
                    asset_out.reward(qty=qty_out / n_coins, coin=asset_in, date=date)
                    new_snapshots.append(asset_in.to_snapshot(date))
            else:
                asset_out.reward(qty=qty_out, coin=asset_out, date=date)
            new_snapshots.append(asset_out.to_snapshot(date))

        else:
            error_msg = (
                f"{tx_type}: {qty_in} {coin_in} -> {qty_out} {coin_out} on {date} not found."
            )
            print(error_msg)
            return

        # First transaction of the day for this asset, so append
        self.history.extend(new_snapshots)

    def save_to_csv(self, output_path: Path):
        df = pd.DataFrame(self.history)
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df.to_csv(output_path, index=False)
        print(f"🚀 Portfolio snapshots successfully saved to {output_path}")


def generate_portfolio_snapshots(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(by=["Date"], ascending=True)

    tracker = CryptoTracker()
    for _, row in df.iterrows():
        tracker.process_transaction(row)

    tracker.save_to_csv(output_csv)


if __name__ == "__main__":
    generate_portfolio_snapshots(
        input_csv=TRANSACTION_DATA_FOLDER / "arbitrum transacties.csv",
        output_csv=TRANSACTION_DATA_FOLDER / "arbitrum_snapshots.csv",
    )
