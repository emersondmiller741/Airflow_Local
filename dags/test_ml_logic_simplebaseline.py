from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from census_ml_demo.ml.ml_population_growth_simplebaseline import (
    run_population_growth_baseline,
)


with DAG(
    dag_id="test_ml_logic_simplebaseline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "ml", "population"],
) as dag:

    run_ml_model = PythonOperator(
        task_id="test_ml_logic_simplebaseline",
        python_callable=run_population_growth_baseline,
    )