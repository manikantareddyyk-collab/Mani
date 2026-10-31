import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


def _validate_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Source data must contain an integer 'count'")
    if value < 0:
        raise ValueError("count must be non-negative")
    return value


def _count_from_json(payload: object) -> int:
    if isinstance(payload, dict):
        if "count" not in payload:
            raise ValueError("JSON source must contain a 'count' field")
        payload = payload["count"]
    return _validate_count(payload)


def ingest_count(source_type: str, source_path: str, api_url: str, fallback_count: int) -> int:
    """Load a Fibonacci count from a configured file, JSON API, or DAG parameter."""
    if source_type == "manual":
        return _validate_count(fallback_count)
    if source_type == "csv":
        if not source_path:
            raise ValueError("source_path is required for CSV ingestion")
        with Path(source_path).open(newline="", encoding="utf-8") as source_file:
            rows = csv.DictReader(source_file)
            row = next(rows, None)
            if row is None or "count" not in row:
                raise ValueError("CSV source must have a 'count' column and at least one row")
            try:
                return _validate_count(int(row["count"]))
            except (TypeError, ValueError) as error:
                raise ValueError("CSV 'count' value must be a non-negative integer") from error
    if source_type == "json":
        if not source_path:
            raise ValueError("source_path is required for JSON ingestion")
        with Path(source_path).open(encoding="utf-8") as source_file:
            return _count_from_json(json.load(source_file))
    if source_type == "http_json":
        parsed_url = urlparse(api_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("api_url must be a valid HTTP or HTTPS URL")
        with urlopen(api_url, timeout=15) as response:
            payload = response.read(1_000_001)
        if len(payload) > 1_000_000:
            raise ValueError("HTTP JSON response exceeds the 1 MB limit")
        return _count_from_json(json.loads(payload))
    raise ValueError(f"Unsupported source_type: {source_type}")


def fibonacci_series(count: int) -> list[int]:
    """Return the first `count` Fibonacci numbers."""
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError("count must be an integer")
    if count < 0:
        raise ValueError("count must be non-negative")

    numbers = []
    first, second = 0, 1
    for _ in range(count):
        numbers.append(first)
        first, second = second, first + second
    return numbers


@dag(
    dag_id="fibonacci_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "count": Param(
            10,
            type="integer",
            minimum=0,
            description="Number of Fibonacci values to generate",
        ),
        "source_type": Param(
            "manual",
            type="string",
            enum=["manual", "csv", "json", "http_json"],
            description="Where to load the Fibonacci count from",
        ),
        "source_path": Param(
            "",
            type="string",
            description="Worker-accessible CSV or JSON file path",
        ),
        "api_url": Param(
            "",
            type="string",
            description="HTTP(S) endpoint returning JSON with a count field",
        ),
    },
    tags=["example"],
)
def fibonacci_pipeline():
    @task
    def ingest_source() -> int:
        params = get_current_context()["params"]
        count = ingest_count(
            params["source_type"],
            params["source_path"],
            params["api_url"],
            params["count"],
        )
        logging.getLogger(__name__).info(
            "Ingested Fibonacci count %d from %s", count, params["source_type"]
        )
        return count

    @task
    def calculate_fibonacci(count: int) -> list[int]:
        return fibonacci_series(count)

    @task
    def log_fibonacci(numbers: list[int]) -> None:
        logging.getLogger(__name__).info("Generated Fibonacci sequence: %s", numbers)

    count = ingest_source()
    numbers = calculate_fibonacci(count)
    log_fibonacci(numbers)


fibonacci_dag = fibonacci_pipeline()
