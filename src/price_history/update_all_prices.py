import random
import time
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from file_paths import CURRENCY_METADATA, PRICE_DATA_FOLDER, STOCK_METADATA
from price_history import (
    fetch_history_defillama,
    fetch_history_single_stock_ft,
    fetch_history_single_stock_morningstar,
    fetch_history_single_stock_yahoo,
)
from price_history.price_data_utils import load_price_csv, merge_price_frames, normalize_price_frame

HISTORY_DAYS = 10
MIN_FT_HISTORY_GAP_DAYS = 30
SLEEP_RANGE_SECONDS = (2.0, 4.0)


@dataclass(slots=True)
class AssetUpdateResult:
    identifier: str
    success: bool
    source_used: str | None
    skipped: bool
    rows_written: int
    reason: str | None


@lru_cache(maxsize=1)
def load_all_metadata() -> dict[str, dict[str, Any]]:
    """
    Caches and returns merged metadata.

    returns:
        Asset metadata map keyed by identifier.
    """
    return CURRENCY_METADATA.copy() | STOCK_METADATA.copy()


def get_last_update_date(identifier: str) -> datetime | None:
    """
    Reads the newest known date for an asset.

    args:
        identifier: Asset id used as file stem.

    returns:
        Parsed max date or None when unavailable.
    """
    file_path = _price_file_path(identifier=identifier)
    frame = load_price_csv(file_path=file_path)
    if frame.empty:
        return None
    return pd.to_datetime(frame["Date"]).max()


def _price_file_path(identifier: str) -> Path:
    return PRICE_DATA_FOLDER / f"{identifier}.csv"


def _can_use_ft(last_date: datetime | None, now: datetime) -> bool:
    if last_date is None:
        return False
    return (now - last_date).days < MIN_FT_HISTORY_GAP_DAYS


def _fetch_from_source(
    source: str,
    identifier: str,
    asset_config: dict[str, Any],
    days_back: int,
) -> pd.DataFrame:
    ticker = asset_config.get("ticker")
    if source == "Yahoo":
        if not ticker:
            return pd.DataFrame(columns=["Date", "Price"])
        return fetch_history_single_stock_yahoo(isin=identifier, ticker=ticker, days_back=days_back)

    if source == "Llama":
        if not ticker:
            return pd.DataFrame(columns=["Date", "Price"])
        return fetch_history_defillama(ticker=ticker, days_back=days_back)

    if source == "FT":
        return fetch_history_single_stock_ft(
            isin=identifier,
            ft_symbol=asset_config.get("ft_symbol"),
            ft_asset_type=asset_config.get("ft_asset_type", "funds"),
        )

    if source == "Morningstar":
        return fetch_history_single_stock_morningstar(isin=identifier, days_back=days_back)

    return pd.DataFrame(columns=["Date", "Price"])


def _save_and_merge(
    identifier: str,
    incoming: pd.DataFrame,
    history_start: str | None = None,
) -> int:
    file_path = _price_file_path(identifier=identifier)
    existing = load_price_csv(file_path=file_path)
    merged = merge_price_frames(existing=existing, incoming=incoming)
    if history_start:
        merged = merged[pd.to_datetime(merged["Date"]) >= pd.Timestamp(history_start)]
    merged.to_csv(file_path, index=False)
    return len(merged)


def _should_sleep_after_source(source: str) -> bool:
    # Yahoo has stricter anti-bot heuristics, so we skip the global extra pause when it succeeds.
    return source != "Yahoo"


def update_single_asset(
    identifier: str,
    asset_config: dict[str, Any],
    now: datetime,
) -> AssetUpdateResult:
    """
    Updates one asset by trying configured source waterfall.

    args:
        identifier: Asset identifier (file stem).
        asset_config: Metadata settings for this asset.
        now: Reference timestamp for gap checks.

    returns:
        Outcome record for this asset.
    """
    if not asset_config.get("active", True):
        return AssetUpdateResult(
            identifier=identifier,
            success=False,
            source_used=None,
            skipped=True,
            rows_written=0,
            reason="inactive",
        )

    waterfall: list[str] = asset_config.get("waterfall", [])
    if not waterfall:
        return AssetUpdateResult(
            identifier=identifier,
            success=False,
            source_used=None,
            skipped=False,
            rows_written=0,
            reason="no_sources_configured",
        )

    last_date = get_last_update_date(identifier=identifier)
    for source in waterfall:
        if source == "FT" and not _can_use_ft(last_date=last_date, now=now):
            print(f"[{identifier}] skipping FT: data gap too large")
            continue

        print(f"[{identifier}] trying source={source}")

        try:
            fetched = _fetch_from_source(
                source=source,
                identifier=identifier,
                asset_config=asset_config,
                days_back=HISTORY_DAYS,
            )
        except Exception as exc:
            print(f"[{identifier}] source={source} error: {exc}")
            continue

        normalized = normalize_price_frame(frame=fetched)
        if normalized.empty:
            print(f"[{identifier}] source={source} returned no usable data")
            continue

        rows_written = _save_and_merge(
            identifier=identifier,
            incoming=normalized,
            history_start=asset_config.get("history_start"),
        )
        if _should_sleep_after_source(source=source):
            time.sleep(random.uniform(*SLEEP_RANGE_SECONDS))

        return AssetUpdateResult(
            identifier=identifier,
            success=True,
            source_used=source,
            skipped=False,
            rows_written=rows_written,
            reason=None,
        )

    time.sleep(random.uniform(*SLEEP_RANGE_SECONDS))
    return AssetUpdateResult(
        identifier=identifier,
        success=False,
        source_used=None,
        skipped=False,
        rows_written=0,
        reason="waterfall_exhausted",
    )


def update_portfolio_prices() -> list[AssetUpdateResult]:
    """
    Updates all active assets in metadata.

    returns:
        List of per-asset results.
    """
    all_assets = load_all_metadata()
    now = datetime.now()

    print(f"Processing {len(all_assets)} total assets...")
    results: list[AssetUpdateResult] = []

    for identifier, asset_config in all_assets.items():
        result = update_single_asset(identifier=identifier, asset_config=asset_config, now=now)
        results.append(result)

        if result.success:
            print(
                f"[{identifier}] updated via {result.source_used}; "
                f"rows_in_file={result.rows_written}"
            )
        elif result.skipped:
            print(f"[{identifier}] skipped: {result.reason}")
        else:
            print(f"[{identifier}] failed: {result.reason}")

    success_count = len([result for result in results if result.success])
    skipped_count = len([result for result in results if result.skipped])
    failed_count = len(results) - success_count - skipped_count
    print(
        "Portfolio update complete: "
        f"success={success_count}, skipped={skipped_count}, failed={failed_count}"
    )
    return results


if __name__ == "__main__":
    update_portfolio_prices()
