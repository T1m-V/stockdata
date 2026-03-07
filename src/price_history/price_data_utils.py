from __future__ import annotations

from pathlib import Path

import pandas as pd


def normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes a price frame to the canonical Date/Price schema.

    args:
        frame: Raw input data frame.

    returns:
        Normalized frame sorted by Date descending.
    """
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Date", "Price"])

    normalized = frame.copy()

    if "Date" not in normalized.columns or "Price" not in normalized.columns:
        return pd.DataFrame(columns=["Date", "Price"])

    normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce").dt.date
    normalized["Price"] = pd.to_numeric(normalized["Price"], errors="coerce")

    normalized = normalized.dropna(subset=["Date", "Price"])[["Date", "Price"]]
    if normalized.empty:
        return normalized

    normalized["Price"] = normalized["Price"].round(4)
    return normalized.sort_values(by="Date", ascending=False)


def merge_price_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """
    Merges existing and incoming price data by date.

    args:
        existing: Existing local history.
        incoming: New fetched history.

    returns:
        Deduplicated merged frame.
    """
    existing_normalized = normalize_price_frame(frame=existing)
    incoming_normalized = normalize_price_frame(frame=incoming)

    if existing_normalized.empty:
        return incoming_normalized
    if incoming_normalized.empty:
        return existing_normalized

    merged = pd.concat([existing_normalized, incoming_normalized], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Date"], keep="last")
    return merged.sort_values(by="Date", ascending=False)


def load_price_csv(file_path: Path) -> pd.DataFrame:
    """
    Loads a price CSV file in canonical schema.

    args:
        file_path: CSV file path.

    returns:
        Parsed frame or empty frame when file is missing/invalid.
    """
    if not file_path.exists():
        return pd.DataFrame(columns=["Date", "Price"])

    try:
        loaded = pd.read_csv(file_path)
    except Exception:
        return pd.DataFrame(columns=["Date", "Price"])

    return normalize_price_frame(frame=loaded)


def save_price_csv(file_path: Path, frame: pd.DataFrame) -> None:
    """
    Persists canonical price frame.

    args:
        file_path: Output CSV path.
        frame: Canonical frame.
    """
    canonical = normalize_price_frame(frame=frame)
    canonical.to_csv(file_path, index=False)
