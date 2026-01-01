from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd

from historical_transactions.utils.constants import TRANSACTIONS_DATA_PATH, TRANSACTIONS_FILE

SNAPSHOT_FILE = TRANSACTIONS_DATA_PATH / "portfolio_snapshot.csv"


@dataclass
class AssetPosition:
    """Tracks the running state and calculations of a single ISIN."""

    isin: str
    quantity: float = 0.0
    principal: float = 0.0
    fees: float = 0.0
    taxes: float = 0.0
    dividends: float = 0.0

    def buy(self, qty: float, price: float, fees: float, taxes: float):
        self.quantity += qty
        self.principal += qty * price
        self.fees += fees
        self.taxes += taxes

    def sell(self, qty: float, price: float, fees: float, taxes: float):
        self.quantity -= qty
        self.principal -= qty * price
        self.fees += fees
        self.taxes += taxes

    def split(self, ratio: float):
        self.quantity *= ratio

    def dividend(self, amount: float, taxes: float):
        self.dividends += amount
        self.taxes += taxes

    def to_snapshot(self, date) -> dict:
        return {
            "Date": date,
            "ISIN": self.isin,
            "Quantity": round(self.quantity, 6),
            "Principal Invested": round(self.principal, 2),
            "Cumulative Fees": round(self.fees, 2),
            "Cumulative Taxes": round(self.taxes, 2),
            "Gross Dividends": round(self.dividends, 2),
        }


class PortfolioTracker:
    def __init__(self):
        self.assets: Dict[str, AssetPosition] = {}
        self.history: List[dict] = []

    def fetch_asset(self, isin: str) -> AssetPosition:
        if isin not in self.assets:
            self.assets[isin] = AssetPosition(isin=isin)
        return self.assets[isin]

    def process_transaction(self, row: pd.Series):
        isin = row["ISIN"]
        tx_type = row["Type"]
        val = row["Quantity"]
        price = row["Price"]
        fees = row.get("Fees", 0) or 0
        taxes = row.get("Taxes", 0) or 0
        date = row["Date"]

        asset = self.fetch_asset(isin)

        if tx_type == "BUYING":
            asset.buy(qty=val, price=price, fees=fees, taxes=taxes)

        elif tx_type == "SELLING":
            asset.sell(qty=val, price=price, fees=fees, taxes=taxes)

        elif tx_type == "STOCK_SPLIT":
            asset.split(ratio=val)
            for record in self.history:
                if record["ISIN"] == isin:
                    record["Quantity"] *= val

        elif tx_type == "DIVIDEND":
            asset.dividend(amount=val * price, taxes=taxes)

        self.history.append(asset.to_snapshot(date))

    def save_to_csv(self, output_path: Path):
        df = pd.DataFrame(self.history)
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df.to_csv(output_path, index=False)
        print(f"🚀 Portfolio snapshots successfully saved to {output_path}")


def generate_portfolio_snapshots(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(by="Date", ascending=True)

    tracker = PortfolioTracker()
    for _, row in df.iterrows():
        tracker.process_transaction(row)

    tracker.save_to_csv(output_csv)


if __name__ == "__main__":
    generate_portfolio_snapshots(input_csv=TRANSACTIONS_FILE, output_csv=SNAPSHOT_FILE)
