from datetime import datetime

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
)

from sqlalchemy import text

FEATURE_COLUMNS = [
    "population",
    "historical_growth_1yr",
    "median_age",
    "pct_under_18",
    "pct_over_65",
    "pct_white",
    "pct_black",
    "pct_hispanic",
    "median_household_income",
    "pct_below_poverty",
    "labor_force_participation_rate",
    "unemployment_rate",
    "total_housing_units",
    "homeownership_rate",
    "vacancy_rate",
    "average_household_size",
    "pct_foreign_born",
    "pct_moved_last_year",
]

TARGET_COLUMN = "future_growth_1yr"

FIRST_TEST_YEAR = 2018
LAST_TEST_YEAR = 2023


def run_population_growth_baseline():

    engine = get_postgres_engine()

    query = """
        SELECT
            location_id,
            county_name,
            feature_year,
            population,
            historical_growth_1yr,
            future_growth_1yr
        FROM county_ml_dataset
        WHERE future_growth_1yr IS NOT NULL
        ORDER BY feature_year, county_name;
    """

    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df)} rows.")

    # ---------------------------------------------------------
    # Create model run record
    # ---------------------------------------------------------

    run_record = pd.DataFrame(
        [
            {
                "model_name": "persistence_baseline",
                "feature_start_year": FIRST_TEST_YEAR - 5,
                "feature_end_year": LAST_TEST_YEAR - 1,
                "test_start_year": FIRST_TEST_YEAR,
                "test_end_year": LAST_TEST_YEAR,
                "training_rows": None,
                "test_rows": None,
                "notes": (
                    "Prediction uses the most recent historical "
                    "one-year population growth rate."
                ),
            }
        ]
    )

    with engine.begin() as connection:
        run_record.to_sql(
            "ml_model_runs",
            connection,
            if_exists="append",
            index=False,
        )

        run_id = connection.execute(
            text(
                """
                SELECT MAX(run_id)
                FROM ml_model_runs
                WHERE model_name = 'persistence_baseline'
                """
            )
        ).scalar()

    print(f"Created model run: {run_id}")

    results = []
    prediction_records = []

    total_training_rows = 0
    total_test_rows = 0
    ```python
    import pandas as pd
    from sqlalchemy import text
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    from customer_reporting_demo.reports.common.database_connections import (
        get_postgres_engine,
    )

    FEATURE_COLUMNS = [
        "population",
        "historical_growth_1yr",
        "median_age",
        "pct_under_18",
        "pct_over_65",
        "pct_white",
        "pct_black",
        "pct_hispanic",
        "median_household_income",
        "pct_below_poverty",
        "labor_force_participation_rate",
        "unemployment_rate",
        "total_housing_units",
        "homeownership_rate",
        "vacancy_rate",
        "average_household_size",
        "pct_foreign_born",
        "pct_moved_last_year",
    ]

    TARGET_COLUMN = "future_growth_1yr"

    FIRST_TEST_YEAR = 2018
    LAST_TEST_YEAR = 2023

    def run_population_growth_baseline():

        engine = get_postgres_engine()

        query = """
            SELECT
                location_id,
                county_name,
                feature_year,
                population,
                historical_growth_1yr,
                future_growth_1yr
            FROM county_ml_dataset
            WHERE future_growth_1yr IS NOT NULL
            ORDER BY feature_year, county_name;
        """

        df = pd.read_sql(query, engine)

        print(f"Loaded {len(df)} rows.")

        # ---------------------------------------------------------
        # Create model run record
        # ---------------------------------------------------------

        run_record = pd.DataFrame(
            [
                {
                    "model_name": "persistence_baseline",
                    "feature_start_year": FIRST_TEST_YEAR - 5,
                    "feature_end_year": LAST_TEST_YEAR - 1,
                    "test_start_year": FIRST_TEST_YEAR,
                    "test_end_year": LAST_TEST_YEAR,
                    "training_rows": None,
                    "test_rows": None,
                    "notes": (
                        "Prediction uses the most recent historical "
                        "one-year population growth rate."
                    ),
                }
            ]
        )

        with engine.begin() as connection:

            run_record.to_sql(
                "ml_model_runs",
                connection,
                if_exists="append",
                index=False,
            )

            run_id = connection.execute(
                text(
                    """
                    SELECT MAX(run_id)
                    FROM ml_model_runs
                    WHERE model_name = 'persistence_baseline'
                    """
                )
            ).scalar()

        print(f"Created model run: {run_id}")

        results = []
        prediction_records = []

        total_training_rows = 0
        total_test_rows = 0

        # ---------------------------------------------------------
        # Rolling validation
        # ---------------------------------------------------------

        for test_year in range(FIRST_TEST_YEAR, LAST_TEST_YEAR + 1):

            train_df = df[
                (df["feature_year"] < test_year)
                & df[["historical_growth_1yr", TARGET_COLUMN]]
                .notna()
                .all(axis=1)
                ].copy()

            test_df = df[
                (df["feature_year"] == test_year)
                & df[["historical_growth_1yr", TARGET_COLUMN]]
                .notna()
                .all(axis=1)
                ].copy()

            print("")
            print("=" * 60)
            print(f"Test year: {test_year}")
            print("=" * 60)

            print(f"Training rows: {len(train_df)}")
            print(f"Test rows: {len(test_df)}")

            if train_df.empty:
                print("Skipping year because no training data is available.")
                continue

            if test_df.empty:
                print("Skipping year because no test data is available.")
                continue

            print(
                f"Training years: "
                f"{train_df['feature_year'].min()}-"
                f"{train_df['feature_year'].max()}"
            )

            total_training_rows += len(train_df)
            total_test_rows += len(test_df)

            # -----------------------------------------------------
            # Persistence prediction
            # -----------------------------------------------------

            test_df["predicted_growth"] = (
                test_df["historical_growth_1yr"]
            )

            y_test = test_df[TARGET_COLUMN]

            mae = mean_absolute_error(
                y_test,
                test_df["predicted_growth"],
            )

            rmse = mean_squared_error(
                y_test,
                test_df["predicted_growth"],
            ) ** 0.5

            r2 = r2_score(
                y_test,
                test_df["predicted_growth"],
            )

            print("")
            print("Growth prediction performance:")
            print(f"MAE: {mae:.6f}")
            print(f"RMSE: {rmse:.6f}")
            print(f"R²: {r2:.6f}")

            # -----------------------------------------------------
            # Population predictions
            # -----------------------------------------------------

            test_df["actual_population"] = (
                    test_df["population"]
                    * (1 + test_df[TARGET_COLUMN])
            )

            test_df["predicted_population"] = (
                    test_df["population"]
                    * (1 + test_df["predicted_growth"])
            )

            test_df["population_error"] = (
                    test_df["predicted_population"]
                    - test_df["actual_population"]
            )

            test_df["absolute_population_error"] = (
                test_df["population_error"].abs()
            )

            test_df["absolute_percentage_error"] = (
                    test_df["absolute_population_error"]
                    / test_df["actual_population"].abs()
                    * 100
            )

            population_mae = (
                test_df["absolute_population_error"].mean()
            )

            population_mape = (
                test_df["absolute_percentage_error"].mean()
            )

            print("")
            print("Population prediction performance:")
            print(f"MAE: {population_mae:.0f} people")
            print(f"MAPE: {population_mape:.2f}%")

            # -----------------------------------------------------
            # Store yearly metrics
            # -----------------------------------------------------

            results.append(
                {
                    "run_id": run_id,
                    "model_name": "persistence_baseline",
                    "test_year": test_year,
                    "training_rows": len(train_df),
                    "test_rows": len(test_df),
                    "growth_mae": mae,
                    "growth_rmse": rmse,
                    "growth_r2": r2,
                    "population_mae": population_mae,
                    "population_mape": population_mape,
                }
            )

            # -----------------------------------------------------
            # Store county predictions
            # -----------------------------------------------------

            for _, row in test_df.iterrows():
                prediction_records.append(
                    {
                        "run_id": run_id,
                        "model_name": "persistence_baseline",
                        "test_year": test_year,
                        "location_id": row["location_id"],
                        "county_name": row["county_name"],
                        "feature_year": row["feature_year"],
                        "actual_population": row["actual_population"],
                        "predicted_population": row["predicted_population"],
                        "population_error": row["population_error"],
                        "absolute_population_error": (
                            row["absolute_population_error"]
                        ),
                        "absolute_percentage_error": (
                            row["absolute_percentage_error"]
                        ),
                        "actual_growth": row[TARGET_COLUMN],
                        "predicted_growth": row["predicted_growth"],
                    }
                )

        results_df = pd.DataFrame(results)
        predictions_df = pd.DataFrame(prediction_records)

        if results_df.empty:
            raise ValueError(
                "No rolling-validation results were generated."
            )

        # ---------------------------------------------------------
        # Store results in PostgreSQL
        # ---------------------------------------------------------

        with engine.begin() as connection:

            results_df.to_sql(
                "ml_model_metrics",
                connection,
                if_exists="append",
                index=False,
            )

            predictions_df.to_sql(
                "ml_predictions",
                connection,
                if_exists="append",
                index=False,
            )

            connection.execute(
                text(
                    """
                    UPDATE ml_model_runs
                    SET
                        training_rows = :training_rows,
                        test_rows = :test_rows
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "training_rows": total_training_rows,
                    "test_rows": total_test_rows,
                    "run_id": run_id,
                },
            )

        # ---------------------------------------------------------
        # Summary
        # ---------------------------------------------------------

        print("")
        print("=" * 60)
        print("PERSISTENCE BASELINE ROLLING VALIDATION SUMMARY")
        print("=" * 60)

        print(
            f"Test years: "
            f"{results_df['test_year'].min()}-"
            f"{results_df['test_year'].max()}"
        )

        print(
            f"Years evaluated: "
            f"{len(results_df)}"
        )

        print("")
        print("Average growth prediction metrics:")

        print(
            f"Average MAE: "
            f"{results_df['growth_mae'].mean():.6f}"
        )

        print(
            f"Average RMSE: "
            f"{results_df['growth_rmse'].mean():.6f}"
        )

        print(
            f"Average R²: "
            f"{results_df['growth_r2'].mean():.6f}"
        )

        print("")
        print("Average population prediction metrics:")

        print(
            f"Average population MAE: "
            f"{results_df['population_mae'].mean():.0f} people"
        )

        print(
            f"Average MAPE: "
            f"{results_df['population_mape'].mean():.2f}%"
        )

        print("")
        print("Year-by-year results:")

        for _, row in results_df.iterrows():
            print(
                f"{int(row['test_year'])}: "
                f"MAE {row['growth_mae']:.6f} | "
                f"RMSE {row['growth_rmse']:.6f} | "
                f"R² {row['growth_r2']:.6f} | "
                f"Population MAPE "
                f"{row['population_mape']:.2f}%"
            )

        print("")
        print(f"Stored model run: {run_id}")
        print(f"Stored metrics rows: {len(results_df)}")
        print(f"Stored prediction rows: {len(predictions_df)}")



    # ---------------------------------------------------------
    # Rolling validation
    # ---------------------------------------------------------

    for test_year in range(FIRST_TEST_YEAR, LAST_TEST_YEAR + 1):

        train_df = df[
            (df["feature_year"] < test_year)
            & df[FEATURE_COLUMNS + [TARGET_COLUMN]]
            .notna()
            .all(axis=1)
        ].copy()

        test_df = df[
            (df["feature_year"] == test_year)
            & df[FEATURE_COLUMNS + [TARGET_COLUMN]]
            .notna()
            .all(axis=1)
        ].copy()

        print("")
        print("=" * 60)
        print(f"Test year: {test_year}")
        print("=" * 60)

        print(f"Training rows: {len(train_df)}")
        print(f"Test rows: {len(test_df)}")

        if train_df.empty:
            print("Skipping year because no training data is available.")
            continue

        if test_df.empty:
            print("Skipping year because no test data is available.")
            continue

        print(
            f"Training years: "
            f"{train_df['feature_year'].min()}-"
            f"{train_df['feature_year'].max()}"
        )

        total_training_rows += len(train_df)
        total_test_rows += len(test_df)

        # -----------------------------------------------------
        # Persistence prediction
        # -----------------------------------------------------

        test_df["predicted_growth"] = (
            test_df["historical_growth_1yr"]
        )

        y_test = test_df[TARGET_COLUMN]

        mae = mean_absolute_error(
            y_test,
            test_df["predicted_growth"],
        )

        rmse = mean_squared_error(
            y_test,
            test_df["predicted_growth"],
        ) ** 0.5

        r2 = r2_score(
            y_test,
            test_df["predicted_growth"],
        )

        print("")
        print("Growth prediction performance:")
        print(f"MAE: {mae:.6f}")
        print(f"RMSE: {rmse:.6f}")
        print(f"R²: {r2:.6f}")

        # -----------------------------------------------------
        # Population predictions
        # -----------------------------------------------------

        test_df["actual_population"] = (
            test_df["population"]
            * (1 + test_df[TARGET_COLUMN])
        )

        test_df["predicted_population"] = (
            test_df["population"]
            * (1 + test_df["predicted_growth"])
        )

        test_df["population_error"] = (
            test_df["predicted_population"]
            - test_df["actual_population"]
        )

        test_df["absolute_population_error"] = (
            test_df["population_error"].abs()
        )

        test_df["absolute_percentage_error"] = (
            test_df["absolute_population_error"]
            / test_df["actual_population"].abs()
            * 100
        )

        population_mae = (
            test_df["absolute_population_error"].mean()
        )

        population_mape = (
            test_df["absolute_percentage_error"].mean()
        )

        print("")
        print("Population prediction performance:")
        print(f"MAE: {population_mae:.0f} people")
        print(f"MAPE: {population_mape:.2f}%")

        # -----------------------------------------------------
        # Store yearly metrics
        # -----------------------------------------------------

        results.append(
            {
                "run_id": run_id,
                "model_name": "persistence_baseline",
                "test_year": test_year,
                "training_rows": len(train_df),
                "test_rows": len(test_df),
                "growth_mae": mae,
                "growth_rmse": rmse,
                "growth_r2": r2,
                "population_mae": population_mae,
                "population_mape": population_mape,
            }
        )

        # -----------------------------------------------------
        # Store county predictions
        # -----------------------------------------------------

        for _, row in test_df.iterrows():

            prediction_records.append(
                {
                    "run_id": run_id,
                    "model_name": "persistence_baseline",
                    "test_year": test_year,
                    "location_id": row["location_id"],
                    "county_name": row["county_name"],
                    "feature_year": row["feature_year"],
                    "actual_population": row["actual_population"],
                    "predicted_population": row["predicted_population"],
                    "population_error": row["population_error"],
                    "absolute_population_error": (
                        row["absolute_population_error"]
                    ),
                    "absolute_percentage_error": (
                        row["absolute_percentage_error"]
                    ),
                    "actual_growth": row[TARGET_COLUMN],
                    "predicted_growth": row["predicted_growth"],
                }
            )

    results_df = pd.DataFrame(results)
    predictions_df = pd.DataFrame(prediction_records)

    if results_df.empty:
        raise ValueError(
            "No rolling-validation results were generated."
        )

    # ---------------------------------------------------------
    # Store results in PostgreSQL
    # ---------------------------------------------------------

    with engine.begin() as connection:

        results_df.to_sql(
            "ml_model_metrics",
            connection,
            if_exists="append",
            index=False,
        )

        predictions_df.to_sql(
            "ml_predictions",
            connection,
            if_exists="append",
            index=False,
        )

        connection.execute(
            text(
                """
                UPDATE ml_model_runs
                SET
                    training_rows = :training_rows,
                    test_rows = :test_rows
                WHERE run_id = :run_id
                """
            ),
            {
                "training_rows": total_training_rows,
                "test_rows": total_test_rows,
                "run_id": run_id,
            },
        )

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------

    print("")
    print("=" * 60)
    print("PERSISTENCE BASELINE ROLLING VALIDATION SUMMARY")
    print("=" * 60)

    print(
        f"Test years: "
        f"{results_df['test_year'].min()}-"
        f"{results_df['test_year'].max()}"
    )

    print(
        f"Years evaluated: "
        f"{len(results_df)}"
    )

    print("")
    print("Average growth prediction metrics:")

    print(
        f"Average MAE: "
        f"{results_df['growth_mae'].mean():.6f}"
    )

    print(
        f"Average RMSE: "
        f"{results_df['growth_rmse'].mean():.6f}"
    )

    print(
        f"Average R²: "
        f"{results_df['growth_r2'].mean():.6f}"
    )

    print("")
    print("Average population prediction metrics:")

    print(
        f"Average population MAE: "
        f"{results_df['population_mae'].mean():.0f} people"
    )

    print(
        f"Average MAPE: "
        f"{results_df['population_mape'].mean():.2f}%"
    )

    print("")
    print("Year-by-year results:")

    for _, row in results_df.iterrows():

        print(
            f"{int(row['test_year'])}: "
            f"MAE {row['growth_mae']:.6f} | "
            f"RMSE {row['growth_rmse']:.6f} | "
            f"R² {row['growth_r2']:.6f} | "
            f"Population MAPE "
            f"{row['population_mape']:.2f}%"
        )

    print("")
    print(f"Stored model run: {run_id}")
    print(f"Stored metrics rows: {len(results_df)}")
    print(f"Stored prediction rows: {len(predictions_df)}")