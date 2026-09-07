from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="basic_example",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def hello():
        print("Hello from Airflow 3!")

    hello()

