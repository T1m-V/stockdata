import json
from pathlib import Path

import requests

from file_paths import GETQUIN_URL, TRANSACTION_JSON_PATH, TRANSACTION_QUERY_PATH, get_token

MAX_TRANSACTIONS = 500


def _headers() -> dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "en",
        "apollographql-client-name": "web",
        "apollographql-client-version": "2.213.2",
        "authorization": get_token(),
        "content-type": "application/json",
        "priority": "u=1, i",
        "sec-ch-ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "referrer": "https://app.getquin.com/",
    }


# The GraphQL query and variables
PAYLOAD = {
    "operationName": "getDashboardAggregatedTransactions",
    "variables": {
        "isin__in": [],
        "limit": MAX_TRANSACTIONS,
        "offset": 0,
        "transaction_type__in": [],
    },
    "query": TRANSACTION_QUERY_PATH.read_text(encoding="utf-8"),
}


def _extract_transactions(data: dict) -> list[dict]:
    errors = data.get("errors")
    if errors:
        messages = ", ".join(error.get("message", "Unknown GraphQL error") for error in errors)
        raise RuntimeError(f"getquin API returned GraphQL errors: {messages}")

    transactions = data.get("data", {}).get("transactions")
    if not isinstance(transactions, dict) or not isinstance(transactions.get("results"), list):
        raise RuntimeError("getquin API response did not include transaction results.")

    return transactions["results"]


def download_transactions(output_file: Path) -> None:
    print("Sending request to getquin API...")
    response = requests.post(GETQUIN_URL, headers=_headers(), json=PAYLOAD)
    response.raise_for_status()

    data = response.json()
    transactions = _extract_transactions(data=data)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Successfully downloaded {len(transactions)} transactions.")
    print(f"Data saved to {output_file}")


if __name__ == "__main__":
    download_transactions(output_file=TRANSACTION_JSON_PATH)
