from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from census_ml_demo.ml.ml_population_linear_regression import (
    run_population_linear_regression,
)

with DAG(
    dag_id="test_ml_linear_regression",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "ml", "linear-regression", "population"],
) as dag:

    run_linear_regression = PythonOperator(
        task_id="run_linear_regression",
        python_callable=run_population_linear_regression,
    )