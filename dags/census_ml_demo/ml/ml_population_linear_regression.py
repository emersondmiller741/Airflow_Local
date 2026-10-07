from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
)

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


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


def run_population_linear_regression():

    engine = get_postgres_engine()

    query = """
        SELECT
            location_id,
            county_name,
            feature_year,
            population,
            historical_growth_1yr,
            median_age,
            pct_under_18,
            pct_over_65,
            pct_white,
            pct_black,
            pct_hispanic,
            median_household_income,
            pct_below_poverty,
            labor_force_participation_rate,
            unemployment_rate,
            total_housing_units,
            homeownership_rate,
            vacancy_rate,
            average_household_size,
            pct_foreign_born,
            pct_moved_last_year,
            future_growth_1yr
        FROM county_ml_dataset
        WHERE future_growth_1yr IS NOT NULL
        ORDER BY feature_year, county_name;
    """

    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df)} rows.")

    results = []

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

        # -----------------------------------------------------
        # Train linear regression model
        # -----------------------------------------------------

        X_train = train_df[FEATURE_COLUMNS]
        y_train = train_df[TARGET_COLUMN]

        X_test = test_df[FEATURE_COLUMNS]
        y_test = test_df[TARGET_COLUMN]

        print("Training Linear Regression model.")

        model = LinearRegression()

        model.fit(X_train, y_train)

        test_df["predicted_growth"] = model.predict(X_test)

        # -----------------------------------------------------
        # Growth prediction metrics
        # -----------------------------------------------------

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
        print(
            f"MAE: {population_mae:.0f} people"
        )
        print(
            f"MAPE: {population_mape:.2f}%"
        )

        results.append(
            {
                "test_year": test_year,
                "training_rows": len(train_df),
                "test_rows": len(test_df),
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
                "population_mae": population_mae,
                "mape": population_mape,
            }
        )

    # ---------------------------------------------------------
    # Overall rolling-validation summary
    # ---------------------------------------------------------

    results_df = pd.DataFrame(results)

    if results_df.empty:
        raise ValueError(
            "No rolling-validation results were generated."
        )

    print("")
    print("=" * 60)
    print("LINEAR REGRESSION ROLLING VALIDATION SUMMARY")
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
        f"{results_df['mae'].mean():.6f}"
    )

    print(
        f"Average RMSE: "
        f"{results_df['rmse'].mean():.6f}"
    )

    print(
        f"Average R²: "
        f"{results_df['r2'].mean():.6f}"
    )

    print("")
    print("Average population prediction metrics:")

    print(
        f"Average population MAE: "
        f"{results_df['population_mae'].mean():.0f} people"
    )

    print(
        f"Average MAPE: "
        f"{results_df['mape'].mean():.2f}%"
    )

    print("")
    print("Year-by-year results:")

    for _, row in results_df.iterrows():

        print(
            f"{int(row['test_year'])}: "
            f"MAE {row['mae']:.6f} | "
            f"RMSE {row['rmse']:.6f} | "
            f"R² {row['r2']:.6f} | "
            f"Population MAPE {row['mape']:.2f}%"
        )