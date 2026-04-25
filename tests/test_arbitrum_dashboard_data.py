import pandas as pd

import dashboard.data_handling.arbitrum_health_data as arbitrum_health_data


def _build_bundle(
    *,
    transactions: pd.DataFrame | None = None,
    raw_snapshots: pd.DataFrame | None = None,
    snapshots: pd.DataFrame | None = None,
    base_ingredients: pd.DataFrame | None = None,
    exceptions: pd.DataFrame | None = None,
    block_map: pd.DataFrame | None = None,
    errors: list[str] | None = None,
) -> arbitrum_health_data.ArbitrumHealthBundle:
    return arbitrum_health_data.ArbitrumHealthBundle(
        transactions=transactions if transactions is not None else pd.DataFrame(columns=["Date"]),
        raw_snapshots=(
            raw_snapshots if raw_snapshots is not None else pd.DataFrame(columns=["Date"])
        ),
        snapshots=(
            snapshots
            if snapshots is not None
            else pd.DataFrame(columns=["Date", "Coin", "Quantity", "Principal Invested"])
        ),
        base_ingredients=(
            base_ingredients if base_ingredients is not None else pd.DataFrame(columns=["Date"])
        ),
        exceptions=(
            exceptions if exceptions is not None else pd.DataFrame(columns=["Date", "Reason"])
        ),
        block_map=block_map if block_map is not None else pd.DataFrame(columns=["date"]),
        errors=errors if errors is not None else [],
    )


def test_freshness_status_thresholds() -> None:
    bundle = _build_bundle(
        transactions=pd.DataFrame({"Date": ["2025-01-18"]}),
        raw_snapshots=pd.DataFrame({"Date": ["2025-01-10"]}),
        snapshots=pd.DataFrame({"Date": ["2024-12-25"], "Coin": ["AAA"], "Quantity": [1.0]}),
        base_ingredients=pd.DataFrame({"Date": ["2025-01-20"]}),
        exceptions=pd.DataFrame({"Date": ["2025-01-08"], "Reason": ["x"]}),
        block_map=pd.DataFrame({"date": ["2025-01-01"]}),
    )

    freshness = arbitrum_health_data.build_dataset_freshness_frame(
        bundle=bundle,
        today="2025-01-20",
    )

    status_map = dict(zip(freshness["Dataset"], freshness["Status"], strict=True))
    assert status_map == {
        "Transactions": "OK",
        "Raw Snapshots": "WARN",
        "Snapshots": "CRIT",
        "Base Ingredients": "OK",
        "Composition Exceptions": "WARN",
        "Block Map": "CRIT",
    }


def test_pipeline_sync_spread_uses_mismatched_max_dates(monkeypatch) -> None:
    bundle = _build_bundle(
        transactions=pd.DataFrame({"Date": ["2025-01-10"]}),
        raw_snapshots=pd.DataFrame({"Date": ["2025-01-09"]}),
        snapshots=pd.DataFrame(
            {
                "Date": ["2025-01-08"],
                "Coin": ["AAA"],
                "Quantity": [1.0],
                "Principal Invested": [1.0],
            }
        ),
        base_ingredients=pd.DataFrame({"Date": ["2025-01-20"]}),
        exceptions=pd.DataFrame({"Date": ["2025-01-08"], "Reason": ["x"]}),
    )

    monkeypatch.setattr(
        arbitrum_health_data,
        "build_snapshot_valuation_frame",
        lambda bundle, chain="arbitrum": pd.DataFrame(
            columns=[
                "Date",
                "Coin",
                "Quantity",
                "Principal Invested",
                "Price EUR",
                "Market Value EUR",
                "Valuation Route",
                "Is Material",
                "Missing Price",
            ]
        ),
    )

    metrics = arbitrum_health_data.build_latest_health_metrics(bundle=bundle, today="2025-01-20")
    assert metrics["pipeline_sync_spread_days"] == 2
    assert metrics["pipeline_sync_status"] == "CRIT"


def test_holdings_valuation_uses_base_ingredients_market_values() -> None:
    bundle = _build_bundle(
        base_ingredients=pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-01"],
                "Coin": ["AAA", "DUST"],
                "Quantity": [2.0, 1e-12],
                "PriceEUR": [2.5, None],
                "EstimatedValueEUR": [5.0, None],
                "ValuationRoute": ["DIRECT", "DIRECT"],
            }
        )
    )

    valuation = arbitrum_health_data.build_holdings_valuation_frame(bundle=bundle)

    aaa = valuation[valuation["Coin"] == "AAA"].iloc[0]
    assert aaa["Price EUR"] == 2.5
    assert aaa["Market Value EUR"] == 5.0
    assert bool(aaa["Is Material"]) is True
    assert bool(aaa["Missing Price"]) is False

    dust = valuation[valuation["Coin"] == "DUST"].iloc[0]
    assert bool(dust["Is Material"]) is False
    assert bool(dust["Missing Price"]) is False


def test_holdings_value_daily_frame_forward_fills_principal_after_last_snapshot() -> None:
    bundle = _build_bundle(
        snapshots=pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-02"],
                "Coin": ["AAA", "AAA"],
                "Quantity": [1.0, 1.0],
                "Principal Invested": [10.0, 12.0],
            }
        ),
        base_ingredients=pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
                "Coin": ["AAA", "AAA", "AAA", "AAA"],
                "Quantity": [1.0, 1.0, 1.0, 1.0],
                "PriceEUR": [2.0, 3.0, 4.0, 5.0],
                "EstimatedValueEUR": [2.0, 3.0, 4.0, 5.0],
                "ValuationRoute": ["DIRECT", "DIRECT", "DIRECT", "DIRECT"],
            }
        ),
    )

    daily = arbitrum_health_data.build_holdings_value_daily_frame(bundle=bundle)

    assert daily.to_dict("records") == [
        {
            "Date": pd.Timestamp("2025-01-01"),
            "Market Value EUR": 2.0,
            "Principal Invested": 10.0,
        },
        {
            "Date": pd.Timestamp("2025-01-02"),
            "Market Value EUR": 3.0,
            "Principal Invested": 12.0,
        },
        {
            "Date": pd.Timestamp("2025-01-03"),
            "Market Value EUR": 4.0,
            "Principal Invested": 12.0,
        },
        {
            "Date": pd.Timestamp("2025-01-04"),
            "Market Value EUR": 5.0,
            "Principal Invested": 12.0,
        },
    ]


def test_snapshot_valuation_missing_prices_are_deterministic(monkeypatch) -> None:
    bundle = _build_bundle(
        snapshots=pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-01", "2025-01-01"],
                "Coin": ["AAA", "MISSING", "DUST"],
                "Quantity": [2.0, 1.0, 1e-12],
                "Principal Invested": [3.0, 2.0, 0.0],
            }
        )
    )

    monkeypatch.setattr(arbitrum_health_data, "load_token_metadata", lambda **kwargs: {})
    monkeypatch.setattr(
        arbitrum_health_data, "build_symbol_protocol_map", lambda token_metadata: {}
    )

    def fake_price(symbol: str, **kwargs):
        if symbol == "AAA":
            return 2.5
        return None

    monkeypatch.setattr(arbitrum_health_data, "get_price_eur_on_or_before", fake_price)

    valuation = arbitrum_health_data.build_snapshot_valuation_frame(bundle=bundle, chain="arbitrum")

    aaa = valuation[valuation["Coin"] == "AAA"].iloc[0]
    assert aaa["Price EUR"] == 2.5
    assert aaa["Market Value EUR"] == 5.0
    assert bool(aaa["Missing Price"]) is False

    missing_symbols = set(valuation[valuation["Missing Price"]]["Coin"].tolist())
    assert missing_symbols == {"MISSING"}


def test_exception_daily_aggregation_splits_by_reason() -> None:
    bundle = _build_bundle(
        exceptions=pd.DataFrame(
            {
                "Date": [
                    "2025-01-01",
                    "2025-01-01",
                    "2025-01-02",
                    "2025-01-02",
                    "2025-01-02",
                ],
                "Reason": ["a", "a", "a", "b", "b"],
            }
        )
    )

    aggregated = arbitrum_health_data.build_exception_daily_frame(bundle=bundle)
    counts = {
        (row["Date"].strftime("%Y-%m-%d"), row["Reason"]): int(row["Count"])
        for _, row in aggregated.iterrows()
    }
    assert counts == {
        ("2025-01-01", "a"): 2,
        ("2025-01-02", "a"): 1,
        ("2025-01-02", "b"): 2,
    }


def test_load_bundle_with_missing_files_returns_empty_frames_and_errors(
    monkeypatch, tmp_path
) -> None:
    missing_paths = {
        name: tmp_path / f"{name}.csv" for name in arbitrum_health_data.DATASET_FILES.keys()
    }
    monkeypatch.setattr(arbitrum_health_data, "DATASET_FILES", missing_paths)

    bundle = arbitrum_health_data.load_arbitrum_health_bundle()
    assert len(bundle.errors) == 6
    assert bundle.transactions.empty
    assert bundle.raw_snapshots.empty
    assert bundle.snapshots.empty
    assert bundle.base_ingredients.empty
    assert bundle.exceptions.empty
    assert bundle.block_map.empty


def test_health_metrics_use_overall_counts_and_asset_filter(monkeypatch) -> None:
    bundle = _build_bundle(
        exceptions=pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-02", "2025-01-03"],
                "Coin": ["AAA", "BBB", "AAA"],
                "Reason": ["x", "x", "y"],
            }
        )
    )

    valuation = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp("2025-01-01"),
                "Coin": "AAA",
                "Missing Price": True,
                "Is Material": True,
                "Market Value EUR": None,
            },
            {
                "Date": pd.Timestamp("2025-01-02"),
                "Coin": "BBB",
                "Missing Price": True,
                "Is Material": True,
                "Market Value EUR": None,
            },
            {
                "Date": pd.Timestamp("2025-01-03"),
                "Coin": "AAA",
                "Missing Price": False,
                "Is Material": True,
                "Market Value EUR": 10.0,
            },
        ]
    )

    monkeypatch.setattr(
        arbitrum_health_data,
        "build_snapshot_valuation_frame",
        lambda bundle, chain="arbitrum": valuation,
    )

    all_metrics = arbitrum_health_data.build_latest_health_metrics(
        bundle=bundle,
        today="2025-01-20",
        selected_asset="ALL",
    )
    aaa_metrics = arbitrum_health_data.build_latest_health_metrics(
        bundle=bundle,
        today="2025-01-20",
        selected_asset="AAA",
    )

    assert all_metrics["missing_prices_overall_count"] == 2
    assert all_metrics["exceptions_overall_count"] == 3
    assert aaa_metrics["missing_prices_overall_count"] == 1
    assert aaa_metrics["exceptions_overall_count"] == 2


def test_health_metrics_use_latest_holdings_date_and_value() -> None:
    bundle = _build_bundle(
        transactions=pd.DataFrame({"Date": ["2025-01-10"]}),
        raw_snapshots=pd.DataFrame({"Date": ["2025-01-09"]}),
        snapshots=pd.DataFrame(
            {
                "Date": ["2025-01-08"],
                "Coin": ["AAA"],
                "Quantity": [1.0],
                "Principal Invested": [8.0],
            }
        ),
        base_ingredients=pd.DataFrame(
            {
                "Date": ["2025-01-08", "2025-01-20"],
                "Coin": ["AAA", "AAA"],
                "Quantity": [1.0, 1.0],
                "PriceEUR": [8.0, 10.0],
                "EstimatedValueEUR": [8.0, 10.0],
                "ValuationRoute": ["DIRECT", "DIRECT"],
            }
        ),
    )

    metrics = arbitrum_health_data.build_latest_health_metrics(bundle=bundle, today="2025-01-20")

    assert metrics["latest_snapshot_date"] == "2025-01-08"
    assert metrics["latest_holdings_date"] == "2025-01-20"
    assert metrics["estimated_portfolio_value_eur"] == 10.0
    assert metrics["pipeline_sync_spread_days"] == 2
    assert metrics["pipeline_sync_status"] == "CRIT"


def test_latest_transactions_frame_filters_asset_and_sorts_descending() -> None:
    bundle = _build_bundle(
        transactions=pd.DataFrame(
            {
                "Date": [
                    "2025-01-01 10:00:00",
                    "2025-01-02 10:00:00",
                    "2025-01-03 10:00:00",
                ],
                "Type": ["Swap", "Receive", "Send"],
                "Token in": ["aaa", "BBB", ""],
                "Qty in": ["1", "2", ""],
                "Token out": ["USDC", "", "AAA"],
                "Qty out": ["10", "", "1"],
                "Fee": ["0.1", "0.2", "0.3"],
                "Fee Token": ["ETH", "AAA", "ETH"],
                "TX Hash": ["h1", "h2", "h3"],
            }
        )
    )

    latest = arbitrum_health_data.build_latest_transactions_frame(
        bundle=bundle,
        selected_asset="AAA",
        max_rows=100,
    )

    assert latest["TX Hash"].tolist() == ["h3", "h2", "h1"]
