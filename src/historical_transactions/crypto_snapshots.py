import functools
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pandas as pd

from file_paths import (
    BLOCKCHAIN_TRANSACTIONS_FOLDER,
    BLOCKHAIN_SNAPSHOT_FOLDER,
    CURRENCY_METADATA,
    PRICES_FOLDER,
    TOKENS_FOLDER,
)
from historical_transactions.portfolio_snapshots import get_forex_rate

STABLE_LIST = ["USDC", "USDT"]

TOKEN_METADATA = {}
token_file = TOKENS_FOLDER / "arbitrum_tokens.json"
if token_file.exists():
    with open(token_file, "r") as f:
        TOKEN_METADATA = json.load(f)


@functools.lru_cache(maxsize=None)
def get_price_history(coin: str) -> pd.DataFrame:
    """Loads the history of prices for a specific coin.

    Args:
        coin: The coin you want the history for.

    Returns:
        Price history of requested coin.
    """
    if coin in STABLE_LIST:
        return pd.DataFrame({"Date": [pd.to_datetime("2000-01-01").date()], "Price": [1.0]})

    file_path = PRICES_FOLDER / f"{coin}.csv"
    if not file_path.exists():
        print(f"⚠️ Warning: No data for {coin}. Assuming value is 0.")
        return pd.DataFrame({"Date": [pd.to_datetime("2000-01-01").date()], "Price": [0.0]})

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
        if coin not in STABLE_LIST:
            print(f"⚠️ No price found for {coin} on/before {date}. Using oldest known price.")
        price = coin_prices.iloc[0]["Price"]
    else:
        # Get the last row (closest date)
        price = rate_row.iloc[-1]["Price"]

    currency_type = CURRENCY_METADATA.get(coin, {}).get("currency", "USD")
    conversion = get_forex_rate(currency=currency_type, date=date)

    return price * conversion


@dataclass
class CryptoPosition:
    """Tracks the running state and calculations of a single crypto position."""

    coin: str
    quantity: Decimal = Decimal(0)
    principal: float = 0.0
    family_proxy: "CryptoPosition | None" = None
    price_source: str = ""

    def __post_init__(self):
        if not self.price_source:
            self.price_source = self.coin

    def adjust_principal(self, amount: float):
        if self.family_proxy:
            self.family_proxy.adjust_principal(amount)
        else:
            self.principal += amount

    def buy(self, amount_bought: Decimal, fiat_spent: Decimal, currency: str, date: str):
        self.quantity += amount_bought
        rate = get_forex_rate(currency=currency, date=date)
        self.adjust_principal(float(fiat_spent) * rate)

    def sell(self, amount_sold: Decimal, fiat_received: Decimal, currency: str, date: str):
        self.quantity -= amount_sold
        rate = get_forex_rate(currency=currency, date=date)
        self.adjust_principal(-(float(fiat_received) * rate))

    def receive(self, amount_received: Decimal, date: str):
        self.quantity += amount_received
        price = get_crypto_price(self.price_source, date)
        self.adjust_principal(float(amount_received) * price)

    def send(self, amount_sent: Decimal, date: str):
        self.quantity -= amount_sent
        price = get_crypto_price(self.price_source, date)
        self.adjust_principal(-(float(amount_sent) * price))

    def reward(self, amount_received: Decimal, source_asset: "CryptoPosition", date: str):
        self.quantity += amount_received
        price = get_crypto_price(self.price_source, date)
        invested = float(amount_received) * price
        self.adjust_principal(invested)
        source_asset.adjust_principal(-invested)

    def to_snapshot(self, date) -> dict:
        return {
            "Date": date,
            "Coin": self.coin,
            "Quantity": self.quantity,
            "Principal Invested": round(self.principal, 2),
        }


@dataclass
class TxEntry:
    token: str
    quantity: Decimal
    val: float | None = None


class CryptoTracker:
    def __init__(self):
        self.assets: dict[str, CryptoPosition] = {}
        self.history: list[dict] = []
        self.daily_coin_cache: dict[str, int] = {}
        self.current_date: str | None = None

    def fetch_asset(self, coin: str) -> CryptoPosition:
        if coin not in self.assets:
            meta = None
            for m in TOKEN_METADATA.values():
                if m.get("symbol") == coin:
                    meta = m
                    break

            price_source = meta.get("price_source", coin) if meta else coin
            family_coin = meta.get("family", coin) if meta else coin

            self.assets[coin] = CryptoPosition(coin=coin, price_source=price_source)
            if family_coin != coin:
                self.assets[coin].family_proxy = self.fetch_asset(family_coin)

        return self.assets[coin]

    def _collect_snapshots(self, asset: CryptoPosition, date: str) -> list[dict]:
        snaps = [asset.to_snapshot(date)]
        if asset.family_proxy:
            snaps.append(asset.family_proxy.to_snapshot(date))
        return snaps

    def _process_swap(
        self, ins: list[TxEntry], outs: list[TxEntry], date: str, touched_coins: set[str]
    ) -> None:
        # 1. Calculate Value of all Ins
        total_in_value_eur = 0.0

        for entry in ins:
            asset = self.fetch_asset(entry.token)
            price = get_crypto_price(asset.price_source, date)
            val = price * float(entry.quantity)
            total_in_value_eur += val
            entry.val = val

        # 2. Calculate Value of all Outs (for weighting)
        total_out_value_eur = 0.0
        for entry in outs:
            asset = self.fetch_asset(entry.token)
            price = get_crypto_price(asset.price_source, date)
            val = price * float(entry.quantity)
            total_out_value_eur += val
            entry.val = val

        if total_out_value_eur == 0:
            total_out_value_eur = 1
            equal_share = 1.0 / len(outs)
            for entry in outs:
                entry.val = equal_share

        # 3. Process Ins (Increase Quantity, Increase Principal)
        for entry in ins:
            asset_in = self.fetch_asset(entry.token)
            asset_in.quantity += entry.quantity
            if entry.val is not None:
                asset_in.adjust_principal(entry.val)
            touched_coins.add(asset_in.coin)

        # 4. Process Outs (Decrease Quantity, Decrease Principal)
        for entry in outs:
            asset_out = self.fetch_asset(entry.token)
            val_out = entry.val if entry.val is not None else 0.0
            share_of_out = val_out / total_out_value_eur
            principal_reduction = total_in_value_eur * share_of_out
            asset_out.quantity -= entry.quantity
            asset_out.adjust_principal(-principal_reduction)
            touched_coins.add(asset_out.coin)

    def _process_reward(
        self,
        rewards: list[TxEntry],
        allocate_reward_to: list[str],
        date: str,
        touched_coins: set[str],
    ) -> None:
        if not rewards:
            return

        for entry_in in rewards:
            asset_in = self.fetch_asset(entry_in.token)
            price = get_crypto_price(asset_in.price_source, date)
            invested = float(entry_in.quantity) * price

            asset_in.quantity += entry_in.quantity
            asset_in.adjust_principal(invested)
            touched_coins.add(asset_in.coin)

            if allocate_reward_to:
                share = invested / len(allocate_reward_to)
                for source_coin in allocate_reward_to:
                    source_asset = self.fetch_asset(source_coin.upper())
                    source_asset.adjust_principal(-share)
                    touched_coins.add(source_asset.coin)
            else:
                asset_in.adjust_principal(-invested)

    def handle_fees(
        self,
        row: pd.Series,
        date: str,
        ins: list[TxEntry],
        outs: list[TxEntry],
        tx_type_lower: str,
        touched_coins: set[str],
    ) -> None:
        fee_str = row.get("Fee")
        fee_token = row.get("Fee Token")

        if pd.notna(fee_str) and pd.notna(fee_token):
            fee_qty = Decimal(str(fee_str))
            if fee_qty > 0:
                fee_asset = self.fetch_asset(str(fee_token))

                fee_price = get_crypto_price(coin=fee_asset.price_source, date=date)
                fee_val_eur = float(fee_qty) * fee_price

                # 1. Deduct from Fee Asset (Qty decreases, Principal decreases)
                fee_asset.quantity -= fee_qty
                touched_coins.add(fee_asset.coin)

                # 2. Map Cost to Target Asset (Principal increases)
                target_entries = []
                if tx_type_lower in ["swap", "buy", "receive"] and ins:
                    target_entries = ins

                elif tx_type_lower in ["sell", "send"] and outs:
                    target_entries = outs

                if target_entries:
                    fee_asset.adjust_principal(-fee_val_eur)
                    share_val_eur = fee_val_eur / len(target_entries)
                    for entry in target_entries:
                        t_asset = self.fetch_asset(entry.token)
                        t_asset.adjust_principal(share_val_eur)
                        touched_coins.add(t_asset.coin)

    def _update_snapshots(self, touched_coins: set[str], date: str) -> None:
        new_snapshots = []
        for coin in touched_coins:
            asset = self.assets[coin]
            new_snapshots.extend(self._collect_snapshots(asset=asset, date=date))

        # 1. Deduplicate: Keep only the last snapshot per coin for this transaction
        unique_snapshots = {s["Coin"]: s for s in new_snapshots}

        # 2. Update History: Overwrite if exists for same Date+Coin, else Append
        for snapshot in unique_snapshots.values():
            snap_date = snapshot["Date"].date()
            coin = snapshot["Coin"]

            if self.current_date != snap_date:
                self.daily_coin_cache = {}
                self.current_date = snap_date

            if coin in self.daily_coin_cache:
                idx = self.daily_coin_cache[coin]
                self.history[idx] = snapshot
            else:
                self.history.append(snapshot)
                self.daily_coin_cache[coin] = len(self.history) - 1

    def process_transaction(self, row: pd.Series):
        tx_type: str = row["Type"]
        tx_type_lower = tx_type.lower()
        date = row["Date"]

        def _parse_entries(qty_val: str, token_val: str) -> list[TxEntry]:
            if pd.isna(qty_val) or str(qty_val).strip() == "":
                return []
            qty_str = str(qty_val)
            token_str = str(token_val) if pd.notna(token_val) else ""
            quantities = [Decimal(x.strip()) for x in qty_str.split(",") if x.strip()]
            tokens = [x.strip() for x in token_str.split(",") if x.strip()]
            return [TxEntry(token=t, quantity=q) for t, q in zip(tokens, quantities)]

        ins = _parse_entries(qty_val=row.get("Qty in"), token_val=row.get("Token in"))
        outs = _parse_entries(qty_val=row.get("Qty out"), token_val=row.get("Token out"))
        touched_coins = set()

        if tx_type_lower == "buy":
            entry_in = ins[0]
            entry_out = outs[0]

            asset_in = self.fetch_asset(entry_in.token)
            asset_in.buy(
                amount_bought=entry_in.quantity,
                fiat_spent=entry_out.quantity,
                currency=entry_out.token,
                date=date,
            )
            touched_coins.add(asset_in.coin)

        elif tx_type_lower == "receive":
            for entry in ins:
                asset_in = self.fetch_asset(entry.token)
                asset_in.receive(amount_received=entry.quantity, date=date)
                touched_coins.add(asset_in.coin)

        elif tx_type_lower == "sell":
            entry_in = ins[0]
            entry_out = outs[0]

            asset_out = self.fetch_asset(entry_out.token)
            asset_out.sell(
                amount_sold=entry_out.quantity,
                fiat_received=entry_in.quantity,
                currency=entry_in.token,
                date=date,
            )
            touched_coins.add(asset_out.coin)

        elif tx_type_lower == "send":
            for entry in outs:
                asset_out = self.fetch_asset(entry.token)
                asset_out.send(amount_sent=entry.quantity, date=date)
                touched_coins.add(asset_out.coin)

        elif tx_type_lower == "swap":
            self._process_swap(ins=ins, outs=outs, date=date, touched_coins=touched_coins)

        elif tx_type_lower.startswith("reward"):
            allocate_reward_to = tx_type_lower.split("|")[-1].split(",")
            self._process_reward(
                rewards=ins,
                allocate_reward_to=allocate_reward_to,
                date=date,
                touched_coins=touched_coins,
            )

        elif tx_type_lower.startswith("approve"):
            return

        elif tx_type_lower == "interaction":
            pass
        else:
            error_msg = f"{tx_type}: {ins} -> {outs} on {date} not found."
            print(error_msg)
            return

        # Process Gas Fee (if applicable)
        self.handle_fees(
            row=row,
            date=date,
            ins=ins,
            outs=outs,
            tx_type_lower=tx_type_lower,
            touched_coins=touched_coins,
        )

        self._update_snapshots(touched_coins=touched_coins, date=date)

    def save_to_csv(self, output_path: Path):
        df = pd.DataFrame(self.history)
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df.to_csv(output_path, index=False)
        print(f"🚀 Portfolio snapshots successfully saved to {output_path}")


def generate_portfolio_snapshots(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv, dtype=str)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(by=["Date"], ascending=True)

    tracker = CryptoTracker()
    for _, row in df.iterrows():
        tracker.process_transaction(row)

    tracker.save_to_csv(output_csv)


if __name__ == "__main__":
    generate_portfolio_snapshots(
        input_csv=BLOCKCHAIN_TRANSACTIONS_FOLDER / "arbitrum_transactions.csv",
        output_csv=BLOCKHAIN_SNAPSHOT_FOLDER / "arbitrum_snapshots.csv",
    )
