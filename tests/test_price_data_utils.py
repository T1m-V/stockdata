import pandas as pd

from price_history.price_data_utils import merge_price_frames, normalize_price_frame


def test_normalize_price_frame_keeps_schema_and_sort() -> None:
    frame = pd.DataFrame(
        {
            "Date": ["2026-02-01", "invalid", "2026-02-03"],
            "Price": ["1.23456", "2.0", 3.33333],
        }
    )

    normalized = normalize_price_frame(frame=frame)
    assert list(normalized.columns) == ["Date", "Price"]
    assert len(normalized) == 2
    assert str(normalized.iloc[0]["Date"]) == "2026-02-03"
    assert normalized.iloc[0]["Price"] == 3.3333


def test_merge_price_frames_deduplicates_date() -> None:
    existing = pd.DataFrame({"Date": ["2026-02-02", "2026-02-01"], "Price": [1.0, 2.0]})
    incoming = pd.DataFrame({"Date": ["2026-02-02", "2026-02-03"], "Price": [1.5, 3.0]})

    merged = merge_price_frames(existing=existing, incoming=incoming)
    assert len(merged) == 3

    replaced_date_mask = merged["Date"].astype(str) == "2026-02-02"
    price_on_replaced_date = merged.loc[replaced_date_mask, "Price"].iloc[0]
    assert price_on_replaced_date == 1.5
