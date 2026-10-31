import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


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
        )
    },
    tags=["example"],
)
def fibonacci_pipeline():
    @task
    def get_count() -> int:
        count = get_current_context()["params"]["count"]
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("DAG parameter 'count' must be an integer")
        if count < 0:
            raise ValueError("DAG parameter 'count' must be non-negative")
        return count

    @task
    def calculate_fibonacci(count: int) -> list[int]:
        return fibonacci_series(count)

    @task
    def log_fibonacci(numbers: list[int]) -> None:
        logging.getLogger(__name__).info("Generated Fibonacci sequence: %s", numbers)

    count = get_count()
    numbers = calculate_fibonacci(count)
    log_fibonacci(numbers)


fibonacci_dag = fibonacci_pipeline()
