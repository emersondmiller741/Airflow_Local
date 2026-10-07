import os
from datetime import datetime

import pandas as pd

from airflow.sdk import DAG, task

from census_ml_demo.census.census_data import CensusData


from customer_reporting_demo.reports.common.business_calendar import (
    BusinessCalendar
)

from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
    create_table_from_dataframe,
    insert_dataframe,
)

from sqlalchemy import text

with (DAG(
    dag_id="census_ml_demo",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["reports"],
) as dag):

    @task
    def check_business_day():

        report_date = datetime.now().date()
        calendar = BusinessCalendar()

        if not calendar.is_business_day(report_date):
            print(
                f"{report_date} is not a business day. "
                "Census ML Pipeline will not run."
            )
            return False

        print(
            f"{report_date} is a business day. "
            "Census ML Pipeline will run."
        )

        return True

    @task
    def run_census_data():

        engine = get_postgres_engine()

        try:
            census_years = range(2012, 2025)

            for year in census_years:

                print()
                print("=" * 70)
                print(f"PROCESSING CENSUS YEAR: {year}")
                print("=" * 70)

                report = CensusData(
                    report_date=datetime.now().date()
                )

                df = report.generate(
                    year=year
                )

                print(
                    f"Census {year} extraction returned "
                    f"{len(df)} county records."
                )

                create_table_from_dataframe(
                    df=df,
                    table_name="census_county_demographics",
                    engine=engine,
                    unique_columns=[
                        "year",
                        "state_fips",
                        "county_fips",
                    ],
                )

                insert_dataframe(
                    df=df,
                    table_name="census_county_demographics",
                    engine=engine,
                    unique_columns=[
                        "year",
                        "state_fips",
                        "county_fips",
                    ],
                )

                print(
                    f"Census {year} successfully processed."
                )

            print()
            print("=" * 70)
            print("CENSUS HISTORICAL BACKFILL COMPLETE")
            print("=" * 70)

        finally:
            engine.dispose()



    business_day = check_business_day()

    # Census historical load currently disabled.
    census_data = run_census_data()

    business_day >> census_data



