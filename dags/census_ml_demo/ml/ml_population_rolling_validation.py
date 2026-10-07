from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
)

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
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


def run_population_rolling_validation():

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

    # ---------------------------------------------------------
    # Run one complete out-of-sample test for each year.
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

        # -----------------------------------------------------
        # Persistence baseline
        # -----------------------------------------------------

        test_df["baseline_predicted_growth"] = (
            test_df["historical_growth_1yr"]
        )

        baseline_actual = test_df[TARGET_COLUMN]
        baseline_predicted = test_df["baseline_predicted_growth"]

        baseline_mae = mean_absolute_error(
            baseline_actual,
            baseline_predicted,
        )

        baseline_rmse = mean_squared_error(
            baseline_actual,
            baseline_predicted,
        ) ** 0.5

        baseline_r2 = r2_score(
            baseline_actual,
            baseline_predicted,
        )

        # -----------------------------------------------------
        # Random Forest
        # -----------------------------------------------------

        X_train = train_df[FEATURE_COLUMNS]
        y_train = train_df[TARGET_COLUMN]

        X_test = test_df[FEATURE_COLUMNS]
        y_test = test_df[TARGET_COLUMN]

        print("Training Random Forest model.")

        model = RandomForestRegressor(
            n_estimators=300,
            random_state=42,
            n_jobs=1,
        )

        model.fit(X_train, y_train)

        test_df["rf_predicted_growth"] = model.predict(X_test)

        rf_actual = test_df[TARGET_COLUMN]
        rf_predicted = test_df["rf_predicted_growth"]

        rf_mae = mean_absolute_error(
            rf_actual,
            rf_predicted,
        )

        rf_rmse = mean_squared_error(
            rf_actual,
            rf_predicted,
        ) ** 0.5

        rf_r2 = r2_score(
            rf_actual,
            rf_predicted,
        )

        # -----------------------------------------------------
        # Population predictions
        # -----------------------------------------------------

        test_df["actual_population"] = (
            test_df["population"]
            * (1 + test_df[TARGET_COLUMN])
        )

        test_df["baseline_predicted_population"] = (
            test_df["population"]
            * (1 + test_df["baseline_predicted_growth"])
        )

        test_df["rf_predicted_population"] = (
            test_df["population"]
            * (1 + test_df["rf_predicted_growth"])
        )

        test_df["baseline_population_error"] = (
            test_df["baseline_predicted_population"]
            - test_df["actual_population"]
        )

        test_df["rf_population_error"] = (
            test_df["rf_predicted_population"]
            - test_df["actual_population"]
        )

        test_df["baseline_ape"] = (
            test_df["baseline_population_error"].abs()
            / test_df["actual_population"].abs()
            * 100
        )

        test_df["rf_ape"] = (
            test_df["rf_population_error"].abs()
            / test_df["actual_population"].abs()
            * 100
        )

        baseline_population_mae = (
            test_df["baseline_population_error"].abs().mean()
        )

        rf_population_mae = (
            test_df["rf_population_error"].abs().mean()
        )

        baseline_population_mape = (
            test_df["baseline_ape"].mean()
        )

        rf_population_mape = (
            test_df["rf_ape"].mean()
        )

        # -----------------------------------------------------
        # Log yearly results
        # -----------------------------------------------------

        print("")
        print("Growth prediction performance:")
        print(
            f"Baseline - MAE: {baseline_mae:.6f}, "
            f"RMSE: {baseline_rmse:.6f}, "
            f"R²: {baseline_r2:.6f}"
        )
        print(
            f"Random Forest - MAE: {rf_mae:.6f}, "
            f"RMSE: {rf_rmse:.6f}, "
            f"R²: {rf_r2:.6f}"
        )

        print("")
        print("Population prediction performance:")
        print(
            f"Baseline - MAE: "
            f"{baseline_population_mae:.0f} people, "
            f"MAPE: {baseline_population_mape:.2f}%"
        )
        print(
            f"Random Forest - MAE: "
            f"{rf_population_mae:.0f} people, "
            f"MAPE: {rf_population_mape:.2f}%"
        )

        results.append(
            {
                "test_year": test_year,
                "training_rows": len(train_df),
                "test_rows": len(test_df),
                "baseline_mae": baseline_mae,
                "baseline_rmse": baseline_rmse,
                "baseline_r2": baseline_r2,
                "rf_mae": rf_mae,
                "rf_rmse": rf_rmse,
                "rf_r2": rf_r2,
                "baseline_population_mae": baseline_population_mae,
                "rf_population_mae": rf_population_mae,
                "baseline_mape": baseline_population_mape,
                "rf_mape": rf_population_mape,
            }
        )

    # ---------------------------------------------------------
    # Overall rolling-validation summary
    # ---------------------------------------------------------

    results_df = pd.DataFrame(results)

    if results_df.empty:
        raise ValueError("No rolling-validation results were generated.")

    print("")
    print("=" * 60)
    print("ROLLING VALIDATION SUMMARY")
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
        f"Baseline average MAE: "
        f"{results_df['baseline_mae'].mean():.6f}"
    )

    print(
        f"Random Forest average MAE: "
        f"{results_df['rf_mae'].mean():.6f}"
    )

    print(
        f"Baseline average RMSE: "
        f"{results_df['baseline_rmse'].mean():.6f}"
    )

    print(
        f"Random Forest average RMSE: "
        f"{results_df['rf_rmse'].mean():.6f}"
    )

    print(
        f"Baseline average R²: "
        f"{results_df['baseline_r2'].mean():.6f}"
    )

    print(
        f"Random Forest average R²: "
        f"{results_df['rf_r2'].mean():.6f}"
    )

    print("")
    print("Average population prediction metrics:")

    print(
        f"Baseline average population MAE: "
        f"{results_df['baseline_population_mae'].mean():.0f} people"
    )

    print(
        f"Random Forest average population MAE: "
        f"{results_df['rf_population_mae'].mean():.0f} people"
    )

    print(
        f"Baseline average MAPE: "
        f"{results_df['baseline_mape'].mean():.2f}%"
    )

    print(
        f"Random Forest average MAPE: "
        f"{results_df['rf_mape'].mean():.2f}%"
    )

    # ---------------------------------------------------------
    # Year-by-year comparison
    # ---------------------------------------------------------

    print("")
    print("Year-by-year comparison:")

    for _, row in results_df.iterrows():

        print(
            f"{int(row['test_year'])}: "
            f"Baseline MAPE {row['baseline_mape']:.2f}% | "
            f"Random Forest MAPE {row['rf_mape']:.2f}%"
        )