from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

from customer_reporting_demo.reports.common.database_connections import get_postgres_engine


def test_postgres_connection():
    """Test connectivity to the PostgreSQL database."""

    engine = get_postgres_engine()

    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT random()"))
            print(f"SELECT 1 result: {result.scalar()}")

            result = connection.execute(text("SELECT version()"))
            print(f"PostgreSQL version: {result.scalar()}")

    finally:
        engine.dispose()


with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "database"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )
