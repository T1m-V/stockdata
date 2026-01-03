import json
from pathlib import Path

import requests

from file_paths import GETQUIN_URL, TOKEN, TRANSACTION_JSON_PATH, TRANSACTION_QUERY_PATH

MAX_TRANSACTIONS = 500

HEADERS = {
    "accept": "*/*",
    "accept-language": "en",
    "apollographql-client-name": "web",
    "apollographql-client-version": "2.213.2",
    "authorization": TOKEN,
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


def download_transactions(output_file: Path) -> None:
    try:
        print("Sending request to getquin API...")
        response = requests.post(GETQUIN_URL, headers=HEADERS, json=PAYLOAD)
        response.raise_for_status()

        data = response.json()

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        transactions = len(data["data"]["transactions"]["results"])

        print(f"Successfully downloaded {transactions} transactions.")
        print(f"Data saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    download_transactions(output_file=TRANSACTION_JSON_PATH)
