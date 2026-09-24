import os
from datetime import datetime

import pandas as pd

from airflow.sdk import DAG, task

from census_weather_ml_demo.census.census_data import CensusData
from census_weather_ml_demo.census.census_geographics import CensusGeographics
from census_weather_ml_demo.weather.weather_data import WeatherData

from customer_reporting_demo.reports.common.business_calendar import (
    BusinessCalendar
)

from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
    create_table_from_dataframe,
    insert_dataframe,
)

from sqlalchemy import text

with DAG(
    dag_id="census_weather_ml_demo",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["reports"],
) as dag:

    @task
    def check_business_day():

        report_date = datetime.now().date()
        calendar = BusinessCalendar()

        if not calendar.is_business_day(report_date):
            print(
                f"{report_date} is not a business day. "
                "Census Weather ML Pipeline will not run."
            )
            return False

        print(
            f"{report_date} is a business day. "
            "Census Weather ML Pipeline will run."
        )

        return True

    @task
    def run_census_data():

        engine = get_postgres_engine()

        try:
            census_years = range(2010, 2025)

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


    @task
    def run_weather_data():

        engine = get_postgres_engine()

        try:

            print(
                f"WEATHER_START_DATE: "
                f"{os.environ.get('WEATHER_START_DATE')}"
            )

            # ------------------------------------------------------------
            # Make sure the weather table exists before checking
            # for the latest loaded date.
            # ------------------------------------------------------------

            create_weather_table_sql = """
                CREATE TABLE IF NOT EXISTS weather_daily (
                    location_id TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    local_date TEXT,
                    timestamp_utc TIMESTAMP,
                    temperature_max DOUBLE PRECISION,
                    temperature_min DOUBLE PRECISION,
                    temperature_mean DOUBLE PRECISION,
                    precipitation DOUBLE PRECISION,
                    snowfall DOUBLE PRECISION,
                    wind_speed_max DOUBLE PRECISION,
                    weather_code DOUBLE PRECISION,
                    UNIQUE (location_id, local_date)
                );
            """

            with engine.begin() as connection:
                connection.execute(
                    text(create_weather_table_sql)
                )

            # ------------------------------------------------------------
            # Get the most recent Census location for each
            # South Carolina county.
            # ------------------------------------------------------------

            query = """
                SELECT DISTINCT ON (county_fips)
                    state_fips,
                    county_fips,
                    county_name,
                    latitude,
                    longitude,
                    year
                FROM census_county_demographics
                WHERE state_fips = '45'
                ORDER BY county_fips, year DESC;
            """

            counties = pd.read_sql(
                query,
                engine,
            )

            print()
            print("=" * 70)
            print("SOUTH CAROLINA COUNTY LOCATIONS FROM CENSUS DATA")
            print("=" * 70)

            print(
                counties.to_string(index=False)
            )

            print()
            print(
                f"South Carolina county locations found: "
                f"{len(counties)}"
            )

            # ------------------------------------------------------------
            # Historical starting date used only when a county has
            # no weather data yet.
            # ------------------------------------------------------------

            configured_start_date = pd.to_datetime(
                os.environ["WEATHER_START_DATE"]
            ).date()

            weather_end_date = datetime.now().date()

            total_rows = 0

            # ------------------------------------------------------------
            # Process each county independently.
            # ------------------------------------------------------------

            for _, county in counties.iterrows():

                location_id = (
                    f"SC{county['state_fips']}"
                    f"{county['county_fips']}"
                )

                print()
                print("=" * 70)
                print(
                    f"PROCESSING WEATHER: "
                    f"{county['county_name']}"
                )
                print(
                    f"Location ID: {location_id}"
                )
                print("=" * 70)

                # --------------------------------------------------------
                # Find the latest weather date already loaded for
                # this county.
                # --------------------------------------------------------

                latest_date_query = """
                    SELECT MAX(local_date::date) AS latest_date
                    FROM weather_daily
                    WHERE location_id = :location_id;
                """

                latest_date = pd.read_sql(
                    text(latest_date_query),
                    engine,
                    params={
                        "location_id": location_id
                    },
                ).iloc[0]["latest_date"]

                # --------------------------------------------------------
                # Determine where this county's next API request
                # should begin.
                # --------------------------------------------------------

                if pd.isna(latest_date):

                    weather_start_date = configured_start_date

                    print(
                        "No existing weather data found."
                    )

                    print(
                        f"Starting historical load from: "
                        f"{weather_start_date}"
                    )

                else:

                    weather_start_date = (
                            pd.to_datetime(latest_date).date()
                            + pd.Timedelta(days=1)
                    )

                    print(
                        f"Latest weather date already loaded: "
                        f"{latest_date}"
                    )

                    print(
                        f"Next weather date to load: "
                        f"{weather_start_date}"
                    )

                # --------------------------------------------------------
                # If the county already has today's data, skip it.
                # --------------------------------------------------------

                if weather_start_date > weather_end_date:
                    print()
                    print(
                        f"{county['county_name']} is already "
                        f"up to date through {weather_end_date}."
                    )

                    continue

                # --------------------------------------------------------
                # Create the weather API client.
                # --------------------------------------------------------

                weather = WeatherData(
                    report_date=datetime.now().date()
                )

                print()
                print(
                    f"Loading weather: "
                    f"{weather_start_date} through "
                    f"{weather_end_date}"
                )

                # --------------------------------------------------------
                # Request only the missing weather dates.
                # --------------------------------------------------------

                df = weather.generate(
                    location_id=location_id,
                    latitude=county["latitude"],
                    longitude=county["longitude"],
                    start_date=weather_start_date.isoformat(),
                    end_date=weather_end_date.isoformat(),
                )

                # --------------------------------------------------------
                # Insert new rows.
                #
                # ON CONFLICT DO NOTHING protects us if a date somehow
                # already exists.
                # --------------------------------------------------------

                insert_dataframe(
                    df=df,
                    table_name="weather_daily",
                    engine=engine,
                    unique_columns=[
                        "location_id",
                        "local_date",
                    ],
                )

                total_rows += len(df)

                print()
                print(
                    f"{county['county_name']} successfully processed."
                )

                print(
                    f"Rows returned: {len(df)}"
                )

            print()
            print("=" * 70)
            print("SOUTH CAROLINA WEATHER INCREMENTAL LOAD COMPLETE")
            print("=" * 70)

            print(
                f"Counties processed: {len(counties)}"
            )

            print(
                f"Total rows returned: {total_rows}"
            )

        finally:

            engine.dispose()

    @task
    def build_weather_annual_features():

        engine = get_postgres_engine()

        try:

            query = """
                DROP TABLE IF EXISTS weather_annual_features;

                CREATE TABLE weather_annual_features AS

                SELECT
                    location_id,
                    EXTRACT(
                        YEAR FROM local_date::date
                    )::integer AS year,

                    -- Temperature: Celsius → Fahrenheit
                    ROUND(
                        AVG(
                            temperature_mean * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_temperature_f,

                    ROUND(
                        AVG(
                            temperature_max * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_high_temperature_f,

                    ROUND(
                        AVG(
                            temperature_min * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_low_temperature_f,

                    -- Precipitation: millimeters → inches
                    ROUND(
                        SUM(precipitation / 25.4)::numeric,
                        3
                    ) AS total_precipitation_in,

                    -- Wind: km/h → mph
                    ROUND(
                        AVG(
                            wind_speed_max * 0.621371
                        )::numeric,
                        2
                    ) AS avg_wind_speed_mph,

                    -- Weather event counts
                    COUNT(*) FILTER (
                        WHERE precipitation > 0
                    ) AS days_with_precipitation,

                    COUNT(*) FILTER (
                        WHERE snowfall > 0
                    ) AS days_with_snow

                FROM weather_daily

                WHERE location_id LIKE 'SC45%'
                  AND EXTRACT(
                      YEAR FROM local_date::date
                  ) BETWEEN 2010 AND 2025

                GROUP BY
                    location_id,
                    EXTRACT(
                        YEAR FROM local_date::date
                    )

                ORDER BY
                    location_id,
                    year;
            """

            with engine.begin() as connection:
                connection.execute(text(query))

            validation_query = """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT location_id) AS counties,
                    MIN(year) AS first_year,
                    MAX(year) AS last_year
                FROM weather_annual_features;
            """

            validation = pd.read_sql(
                validation_query,
                engine,
            )

            print()
            print("=" * 70)
            print("WEATHER ANNUAL FEATURES COMPLETE")
            print("=" * 70)

            print(validation.to_string(index=False))

        finally:
            engine.dispose()


    @task
    def build_weather_annual_extremes():

        engine = get_postgres_engine()

        try:

            query = """
                DROP TABLE IF EXISTS weather_annual_extremes;

                CREATE TABLE weather_annual_extremes AS

                SELECT
                    location_id,

                    EXTRACT(
                        YEAR FROM local_date::date
                    )::integer AS year,

                    ROUND(
                        MAX(
                            temperature_max * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS max_temperature_f,

                    ROUND(
                        MIN(
                            temperature_min * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS min_temperature_f,

                    COUNT(*) FILTER (
                        WHERE temperature_max * 9.0 / 5.0 + 32 >= 95
                    ) AS hot_days_95f,

                    COUNT(*) FILTER (
                        WHERE temperature_min * 9.0 / 5.0 + 32 <= 32
                    ) AS freezing_days,

                    COUNT(*) FILTER (
                        WHERE precipitation / 25.4 >= 1
                    ) AS heavy_precipitation_days,

                    ROUND(
                        MAX(
                            precipitation / 25.4
                        )::numeric,
                        3
                    ) AS max_daily_precipitation_in

                FROM weather_daily

                WHERE location_id LIKE 'SC45%'

                  AND EXTRACT(
                      YEAR FROM local_date::date
                  ) BETWEEN 2010 AND 2025

                GROUP BY
                    location_id,
                    EXTRACT(
                        YEAR FROM local_date::date
                    )

                ORDER BY
                    location_id,
                    year;
            """

            with engine.begin() as connection:
                connection.execute(text(query))

            validation_query = """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT location_id) AS counties,
                    MIN(year) AS first_year,
                    MAX(year) AS last_year
                FROM weather_annual_extremes;
            """

            validation = pd.read_sql(
                validation_query,
                engine,
            )

            print()
            print("=" * 70)
            print("WEATHER ANNUAL EXTREMES COMPLETE")
            print("=" * 70)

            print(validation.to_string(index=False))

        finally:

            engine.dispose()


    @task
    def build_weather_seasonal_features():

        engine = get_postgres_engine()

        try:

            query = """
                DROP TABLE IF EXISTS weather_seasonal_features;

                CREATE TABLE weather_seasonal_features AS

                SELECT
                    location_id,

                    EXTRACT(
                        YEAR FROM local_date::date
                    )::integer AS year,

                    CASE
                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (12, 1, 2)
                            THEN 'Winter'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (3, 4, 5)
                            THEN 'Spring'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (6, 7, 8)
                            THEN 'Summer'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (9, 10, 11)
                            THEN 'Fall'
                    END AS season,

                    ROUND(
                        AVG(
                            temperature_mean * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_temperature_f,

                    ROUND(
                        AVG(
                            temperature_max * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_high_temperature_f,

                    ROUND(
                        AVG(
                            temperature_min * 9.0 / 5.0 + 32
                        )::numeric,
                        2
                    ) AS avg_low_temperature_f,

                    ROUND(
                        SUM(
                            precipitation / 25.4
                        )::numeric,
                        3
                    ) AS total_precipitation_in,

                    ROUND(
                        AVG(
                            wind_speed_max * 0.621371
                        )::numeric,
                        2
                    ) AS avg_wind_speed_mph,

                    COUNT(*) FILTER (
                        WHERE precipitation > 0
                    ) AS days_with_precipitation

                FROM weather_daily

                WHERE location_id LIKE 'SC45%'

                  AND EXTRACT(
                      YEAR FROM local_date::date
                  ) BETWEEN 2010 AND 2025

                GROUP BY
                    location_id,

                    EXTRACT(
                        YEAR FROM local_date::date
                    ),

                    CASE
                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (12, 1, 2)
                            THEN 'Winter'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (3, 4, 5)
                            THEN 'Spring'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (6, 7, 8)
                            THEN 'Summer'

                        WHEN EXTRACT(
                            MONTH FROM local_date::date
                        ) IN (9, 10, 11)
                            THEN 'Fall'
                    END

                ORDER BY
                    location_id,
                    year,
                    season;
            """

            with engine.begin() as connection:
                connection.execute(text(query))

            validation_query = """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT location_id) AS counties,
                    COUNT(DISTINCT year) AS years,
                    COUNT(DISTINCT season) AS seasons,
                    MIN(year) AS first_year,
                    MAX(year) AS last_year
                FROM weather_seasonal_features;
            """

            validation = pd.read_sql(
                validation_query,
                engine,
            )

            print()
            print("=" * 70)
            print("WEATHER SEASONAL FEATURES COMPLETE")
            print("=" * 70)

            print(validation.to_string(index=False))

        finally:

            engine.dispose()


    @task
    def build_weather_severe_events():

        engine = get_postgres_engine()

        try:

            query = """
                DROP TABLE IF EXISTS weather_severe_events;

                CREATE TABLE weather_severe_events AS

                SELECT
                    location_id,

                    EXTRACT(
                        YEAR FROM local_date::date
                    )::integer AS year,

                    COUNT(*) FILTER (
                        WHERE temperature_max * 9.0 / 5.0 + 32 >= 95
                    ) AS hot_days_95f,

                    COUNT(*) FILTER (
                        WHERE temperature_max * 9.0 / 5.0 + 32 >= 100
                    ) AS very_hot_days_100f,

                    COUNT(*) FILTER (
                        WHERE temperature_min * 9.0 / 5.0 + 32 <= 32
                    ) AS freezing_days,

                    COUNT(*) FILTER (
                        WHERE precipitation / 25.4 >= 1
                    ) AS heavy_precipitation_days,

                    COUNT(*) FILTER (
                        WHERE precipitation / 25.4 >= 2
                    ) AS extreme_precipitation_days,

                    COUNT(*) FILTER (
                        WHERE snowfall > 0
                    ) AS snow_days,

                    COUNT(*) FILTER (
                        WHERE wind_speed_max * 0.621371 >= 30
                    ) AS high_wind_days,

                    COUNT(*) FILTER (
                        WHERE wind_speed_max * 0.621371 >= 40
                    ) AS very_high_wind_days

                FROM weather_daily

                WHERE location_id LIKE 'SC45%'

                  AND EXTRACT(
                      YEAR FROM local_date::date
                  ) BETWEEN 2010 AND 2025

                GROUP BY
                    location_id,

                    EXTRACT(
                        YEAR FROM local_date::date
                    )

                ORDER BY
                    location_id,
                    year;
            """

            with engine.begin() as connection:
                connection.execute(text(query))

            validation_query = """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT location_id) AS counties,
                    COUNT(DISTINCT year) AS years,
                    MIN(year) AS first_year,
                    MAX(year) AS last_year
                FROM weather_severe_events;
            """

            validation = pd.read_sql(
                validation_query,
                engine,
            )

            print()
            print("=" * 70)
            print("WEATHER SEVERE EVENTS COMPLETE")
            print("=" * 70)

            print(validation.to_string(index=False))

        finally:

            engine.dispose()


    @task
    def build_weather_variability():

        engine = get_postgres_engine()

        try:

            query = """
                DROP TABLE IF EXISTS weather_variability;

                CREATE TABLE weather_variability AS

                WITH annual_weather AS (

                    SELECT
                        location_id,

                        EXTRACT(
                            YEAR FROM local_date::date
                        )::integer AS year,

                        -- Temperature variability within the year
                        ROUND(
                            STDDEV(
                                temperature_mean * 9.0 / 5.0 + 32
                            )::numeric,
                            2
                        ) AS temperature_stddev_f,

                        -- Precipitation variability within the year
                        ROUND(
                            STDDEV(
                                precipitation / 25.4
                            )::numeric,
                            3
                        ) AS precipitation_stddev_in,

                        -- Difference between annual maximum and minimum temperature
                        ROUND(
                            (
                                MAX(
                                    temperature_max * 9.0 / 5.0 + 32
                                )
                                -
                                MIN(
                                    temperature_min * 9.0 / 5.0 + 32
                                )
                            )::numeric,
                            2
                        ) AS annual_temperature_range_f,

                        -- Average daily temperature range
                        ROUND(
                            AVG(
                                (
                                    temperature_max * 9.0 / 5.0 + 32
                                )
                                -
                                (
                                    temperature_min * 9.0 / 5.0 + 32
                                )
                            )::numeric,
                            2
                        ) AS avg_daily_temperature_range_f,

                        -- Annual average temperature
                        ROUND(
                            AVG(
                                temperature_mean * 9.0 / 5.0 + 32
                            )::numeric,
                            2
                        ) AS avg_temperature_f,

                        -- Annual precipitation
                        ROUND(
                            SUM(
                                precipitation / 25.4
                            )::numeric,
                            3
                        ) AS total_precipitation_in

                    FROM weather_daily

                    WHERE location_id LIKE 'SC45%'

                      AND EXTRACT(
                          YEAR FROM local_date::date
                      ) BETWEEN 2010 AND 2025

                    GROUP BY
                        location_id,

                        EXTRACT(
                            YEAR FROM local_date::date
                        )
                ),

                weather_with_previous_year AS (

                    SELECT
                        *,

                        LAG(avg_temperature_f) OVER (
                            PARTITION BY location_id
                            ORDER BY year
                        ) AS previous_avg_temperature_f,

                        LAG(total_precipitation_in) OVER (
                            PARTITION BY location_id
                            ORDER BY year
                        ) AS previous_total_precipitation_in

                    FROM annual_weather
                )

                SELECT
                    location_id,
                    year,

                    temperature_stddev_f,
                    precipitation_stddev_in,
                    annual_temperature_range_f,
                    avg_daily_temperature_range_f,
                    avg_temperature_f,
                    total_precipitation_in,

                    ROUND(
                        (
                            avg_temperature_f
                            - previous_avg_temperature_f
                        )::numeric,
                        2
                    ) AS temperature_change_from_previous_year_f,

                    ROUND(
                        (
                            total_precipitation_in
                            - previous_total_precipitation_in
                        )::numeric,
                        3
                    ) AS precipitation_change_from_previous_year_in

                FROM weather_with_previous_year

                ORDER BY
                    location_id,
                    year;
            """

            with engine.begin() as connection:
                connection.execute(text(query))

            validation_query = """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT location_id) AS counties,
                    COUNT(DISTINCT year) AS years,
                    MIN(year) AS first_year,
                    MAX(year) AS last_year
                FROM weather_variability;
            """

            validation = pd.read_sql(
                validation_query,
                engine,
            )

            print()
            print("=" * 70)
            print("WEATHER VARIABILITY COMPLETE")
            print("=" * 70)

            print(validation.to_string(index=False))

        finally:

            engine.dispose()

    @task
    def run_census_geographics():

        engine = get_postgres_engine()

        try:

            census_geographics = CensusGeographics(
                report_date=datetime.now().date()
            )

            dataframe = census_geographics.generate()

            if dataframe.empty:
                raise ValueError(
                    "Census geographic data returned no rows."
                )

            create_table_from_dataframe(
                dataframe,
                "county_geographic_features",
                engine,
                unique_columns=["location_id"],
            )

            insert_dataframe(
                dataframe,
                "county_geographic_features",
                engine,
                unique_columns=["location_id"],
            )

            print()
            print("=" * 70)
            print("CENSUS GEOGRAPHIC LOAD COMPLETE")
            print("=" * 70)
            print(f"Rows loaded: {len(dataframe)}")

        finally:

            engine.dispose()






    business_day = check_business_day()

    # Census historical load currently disabled.
    census_data = run_census_data()

    census_geographics = run_census_geographics()

    # weather_data = run_weather_data()
    # weather_features = build_weather_annual_features()
    # weather_extremes = build_weather_annual_extremes()
    # weather_seasonal = build_weather_seasonal_features()
    # weather_severe_events = build_weather_severe_events()
    # weather_variability = build_weather_variability()

    business_day >> census_data >>  census_geographics
        # weather_data,


    # weather_data >> [
    #    weather_features,
    #    weather_extremes,
    #    weather_seasonal,
    #    weather_severe_events,
    #    weather_variability,
    #]

    # When Census is enabled again:
    # business_day >> [census_data, weather_data]