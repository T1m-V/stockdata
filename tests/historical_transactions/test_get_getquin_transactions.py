import json
from pathlib import Path

import pytest

from historical_transactions import get_getquin_transactions


class _Response:
    def __init__(self, data: dict) -> None:
        self._data = data

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._data


@pytest.fixture(autouse=True)
def _token(monkeypatch) -> None:
    monkeypatch.setattr(get_getquin_transactions, "get_token", lambda: "test-token")


def test_download_transactions_rejects_graphql_errors(monkeypatch, tmp_path: Path) -> None:
    output_file = tmp_path / "transactions_export.json"

    monkeypatch.setattr(
        get_getquin_transactions.requests,
        "post",
        lambda *args, **kwargs: _Response(
            {
                "data": {"transactions": None},
                "errors": [{"message": "401: Unauthorized"}],
            }
        ),
    )

    with pytest.raises(RuntimeError, match="401: Unauthorized"):
        get_getquin_transactions.download_transactions(output_file=output_file)

    assert not output_file.exists()


def test_download_transactions_writes_valid_response(monkeypatch, tmp_path: Path) -> None:
    output_file = tmp_path / "transactions_export.json"
    data = {"data": {"transactions": {"results": [{"id": "tx_1"}]}}}

    monkeypatch.setattr(
        get_getquin_transactions.requests,
        "post",
        lambda *args, **kwargs: _Response(data=data),
    )

    get_getquin_transactions.download_transactions(output_file=output_file)

    assert json.loads(output_file.read_text(encoding="utf-8")) == data


def test_download_transactions_defaults_to_twenty_recent_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "transactions_export.json"
    data = {"data": {"transactions": {"results": [{"id": "tx_1"}]}}}
    payloads = []

    def post(*args, **kwargs):
        payloads.append(kwargs["json"])
        return _Response(data=data)

    monkeypatch.setattr(get_getquin_transactions.requests, "post", post)

    get_getquin_transactions.download_transactions(output_file=output_file)

    assert payloads[0]["variables"]["limit"] == 20


def test_download_transactions_uses_requested_limit(monkeypatch, tmp_path: Path) -> None:
    output_file = tmp_path / "transactions_export.json"
    data = {"data": {"transactions": {"results": [{"id": "tx_1"}]}}}
    payloads = []

    def post(*args, **kwargs):
        payloads.append(kwargs["json"])
        return _Response(data=data)

    monkeypatch.setattr(get_getquin_transactions.requests, "post", post)

    get_getquin_transactions.download_transactions(output_file=output_file, limit=500)

    assert payloads[0]["variables"]["limit"] == 500
