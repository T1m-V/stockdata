import json
from pathlib import Path

import pandas as pd

from historical_transactions.transform_data import convert_transaction_json_to_csv


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _transaction(
    tx_id: str,
    timestamp: str,
    *,
    isin: str = "NL000000001",
    name: str = "Example Fund",
) -> dict:
    return {
        "id": tx_id,
        "timestamp": timestamp,
        "transaction_type": "BUYING",
        "instrument": {
            "name": name,
            "symbol": isin,
            "ticker": "EXF",
        },
        "isin": isin,
        "units": 2,
        "price": 10,
        "price_currency": "EUR",
        "costs": 1,
        "taxes": 0,
    }


def _transaction_export(transactions: list[dict]) -> dict:
    return {"data": {"transactions": {"results": transactions}}}


def _split_export() -> dict:
    return {
        "data": {
            "splits": [
                {
                    "isin": "NL000000001",
                    "start_date": "2026-05-10",
                    "numerator": 2,
                    "denominator": 1,
                }
            ]
        }
    }


def test_full_reload_creates_csv_with_transaction_ids(tmp_path: Path) -> None:
    tx_file = tmp_path / "transactions_export.json"
    split_file = tmp_path / "splits_export.json"
    output_file = tmp_path / "getquin_data.csv"

    _write_json(
        tx_file,
        _transaction_export(
            [
                _transaction("tx_older", "2026-05-14T10:00:00Z"),
                _transaction("tx_newer", "2026-05-16T10:00:00Z"),
            ]
        ),
    )
    _write_json(split_file, {"data": {"splits": []}})

    convert_transaction_json_to_csv(tx_file=tx_file, split_file=split_file, output_file=output_file)

    result = pd.read_csv(output_file)

    assert "Transaction ID" in result.columns
    assert result["Transaction ID"].tolist() == ["tx_newer", "tx_older"]


def test_existing_csv_rows_are_preserved_and_overlapping_ids_are_not_duplicated(
    tmp_path: Path,
) -> None:
    tx_file = tmp_path / "transactions_export.json"
    split_file = tmp_path / "splits_export.json"
    output_file = tmp_path / "getquin_data.csv"

    pd.DataFrame(
        [
            {
                "Transaction ID": "tx_existing",
                "Date": "2026-05-01",
                "Type": "BUYING",
                "Asset Name": "Existing Fund",
                "ISIN": "NL000000002",
                "Quantity": 1,
                "Price": 5,
                "Currency": "EUR",
                "Fees": 0,
                "Taxes": 0,
            },
            {
                "Transaction ID": "tx_overlap",
                "Date": "2026-05-14",
                "Type": "BUYING",
                "Asset Name": "Example Fund",
                "ISIN": "NL000000001",
                "Quantity": 2,
                "Price": 10,
                "Currency": "EUR",
                "Fees": 1,
                "Taxes": 0,
            },
        ]
    ).to_csv(output_file, index=False)
    _write_json(
        tx_file,
        _transaction_export(
            [
                _transaction("tx_overlap", "2026-05-14T10:00:00Z"),
                _transaction("tx_new", "2026-05-16T10:00:00Z"),
            ]
        ),
    )
    _write_json(split_file, {"data": {"splits": []}})

    convert_transaction_json_to_csv(tx_file=tx_file, split_file=split_file, output_file=output_file)

    result = pd.read_csv(output_file)

    assert result["Transaction ID"].tolist() == ["tx_new", "tx_overlap", "tx_existing"]


def test_split_rows_use_stable_ids_and_are_not_duplicated(tmp_path: Path) -> None:
    tx_file = tmp_path / "transactions_export.json"
    split_file = tmp_path / "splits_export.json"
    output_file = tmp_path / "getquin_data.csv"

    _write_json(tx_file, _transaction_export([_transaction("tx_1", "2026-05-16T10:00:00Z")]))
    _write_json(split_file, _split_export())

    convert_transaction_json_to_csv(tx_file=tx_file, split_file=split_file, output_file=output_file)
    convert_transaction_json_to_csv(tx_file=tx_file, split_file=split_file, output_file=output_file)

    result = pd.read_csv(output_file)

    assert result["Transaction ID"].tolist() == [
        "tx_1",
        "split_NL000000001_2026-05-10_2_1",
    ]
