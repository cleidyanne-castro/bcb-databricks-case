from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER = logging.getLogger(__name__)

BASE_URL = (
    "https://api.bcb.gov.br/dados/serie/"
    "bcdata.sgs.{series_id}/dados"
)

SERIES = {
    "selic": 11,
    "ipca": 433,
}

START_DATE = "01/01/2020"
END_DATE = "31/12/2024"

OUTPUT_DIRECTORY = Path("data/raw")


def create_session() -> requests.Session:
    # Retry setup
    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)

    return session


def fetch_series(
    session: requests.Session,
    series_id: int,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    url = BASE_URL.format(series_id=series_id)

    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date,
    }

    LOGGER.info("Requesting BCB series %s", series_id)

    # API request
    try:
        response = session.get(
            url,
            params=params,
            timeout=(10, 60),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to retrieve BCB series {series_id}."
        ) from exc

    # Payload validation
    try:
        payload = response.json()
    except requests.JSONDecodeError as exc:
        raise RuntimeError(
            f"BCB series {series_id} returned invalid JSON."
        ) from exc

    if not isinstance(payload, list):
        raise RuntimeError(
            f"BCB series {series_id} returned an invalid payload type."
        )

    if not payload:
        raise RuntimeError(
            f"BCB series {series_id} returned an empty payload."
        )

    required_fields = {"data", "valor"}

    invalid_records = [
        record
        for record in payload
        if not isinstance(record, dict)
        or not required_fields.issubset(record)
    ]

    if invalid_records:
        raise RuntimeError(
            f"BCB series {series_id} returned records "
            "with an unexpected schema."
        )

    LOGGER.info(
        "BCB series %s returned %s records",
        series_id,
        len(payload),
    )

    return payload


def save_json(
    records: list[dict[str, Any]],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = output_path.with_suffix(".tmp")

    # Atomic write
    with temporary_path.open(
        mode="w",
        encoding="utf-8",
    ) as file:
        json.dump(
            records,
            file,
            ensure_ascii=False,
            indent=2,
        )

    temporary_path.replace(output_path)

    LOGGER.info(
        "Saved %s records to %s",
        len(records),
        output_path,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    session = create_session()

    for series_name, series_id in SERIES.items():
        records = fetch_series(
            session=session,
            series_id=series_id,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        output_path = OUTPUT_DIRECTORY / f"{series_name}.json"

        save_json(
            records=records,
            output_path=output_path,
        )

    LOGGER.info("All series were extracted successfully.")


if __name__ == "__main__":
    main()
