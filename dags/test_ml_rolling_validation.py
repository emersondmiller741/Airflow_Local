from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from census_ml_demo.ml.ml_population_rolling_validation import (
    run_population_rolling_validation,
)


with DAG(
    dag_id="test_ml_rolling_validation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "ml", "validation", "population"],
) as dag:

    run_rolling_validation = PythonOperator(
        task_id="run_rolling_validation",
        python_callable=run_population_rolling_validation,
    )