import unittest

import pandas as pd

from price_history.price_data_utils import merge_price_frames, normalize_price_frame


class PriceDataUtilsTests(unittest.TestCase):
    def test_normalize_price_frame_keeps_schema_and_sort(self) -> None:
        frame = pd.DataFrame(
            {
                "Date": ["2026-02-01", "invalid", "2026-02-03"],
                "Price": ["1.23456", "2.0", 3.33333],
            }
        )

        normalized = normalize_price_frame(frame=frame)
        self.assertEqual(list(normalized.columns), ["Date", "Price"])
        self.assertEqual(len(normalized), 2)
        self.assertEqual(str(normalized.iloc[0]["Date"]), "2026-02-03")
        self.assertEqual(normalized.iloc[0]["Price"], 3.3333)

    def test_merge_price_frames_deduplicates_date(self) -> None:
        existing = pd.DataFrame({"Date": ["2026-02-02", "2026-02-01"], "Price": [1.0, 2.0]})
        incoming = pd.DataFrame({"Date": ["2026-02-02", "2026-02-03"], "Price": [1.5, 3.0]})

        merged = merge_price_frames(existing=existing, incoming=incoming)
        self.assertEqual(len(merged), 3)

        replaced_date_mask = merged["Date"].astype(str) == "2026-02-02"
        price_on_replaced_date = merged.loc[replaced_date_mask, "Price"].iloc[0]
        self.assertEqual(price_on_replaced_date, 1.5)


if __name__ == "__main__":
    unittest.main()
