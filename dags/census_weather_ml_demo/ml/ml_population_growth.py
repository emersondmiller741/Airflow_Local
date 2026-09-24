import logging

import pandas as pd


from customer_reporting_demo.reports.common.database_connections import (
    get_postgres_engine,
    create_table_from_dataframe,
    insert_dataframe,
)

logger = logging.getLogger(__name__)


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
    "land_area_sq_miles",
    "water_area_sq_miles",
    "total_area_sq_miles",
    "latitude",
    "longitude",
    "avg_temperature_f",
    "total_precipitation_in",
    "days_with_precipitation",
    "hot_days_95f",
    "freezing_days",
    "heavy_precipitation_days",
    "max_daily_precipitation_in",
    "annual_temperature_range_f",
]

TARGET_COLUMN = "future_growth_1yr"


def run_population_growth_model():
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    """
    Train a Random Forest model to predict one-year future
    county population growth using the county_ml_dataset view.
    """

    engine = get_postgres_engine()

    try:
        query = """
            SELECT *
            FROM county_ml_dataset
            ORDER BY county_name, feature_year;
        """

        logger.info("Loading ML dataset from county_ml_dataset view.")

        df = pd.read_sql(query, engine)

        logger.info(
            "Loaded %s rows and %s columns.",
            len(df),
            len(df.columns),
        )

        if df.empty:
            raise ValueError(
                "county_ml_dataset returned no rows."
            )

        # ---------------------------------------------------------
        # Validate required columns
        # ---------------------------------------------------------

        required_columns = FEATURE_COLUMNS + [
            TARGET_COLUMN,
            "location_id",
            "county_name",
            "feature_year",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns from county_ml_dataset: "
                f"{missing_columns}"
            )

        # ---------------------------------------------------------
        # Verify target values look correct
        # ---------------------------------------------------------

        logger.info(
            "Future 1-year growth range before filtering: "
            "%s to %s",
            df[TARGET_COLUMN].min(),
            df[TARGET_COLUMN].max(),
        )

        # ---------------------------------------------------------
        # Keep only rows with complete feature/target data
        # ---------------------------------------------------------

        model_columns = FEATURE_COLUMNS + [TARGET_COLUMN]

        model_df = df.dropna(
            subset=model_columns
        ).copy()

        logger.info(
            "Rows available after removing NULL feature/target values: %s",
            len(model_df),
        )

        if model_df.empty:
            raise ValueError(
                "No rows remain after removing NULL feature/target values."
            )

        # ---------------------------------------------------------
        # Time-based train/test split
        #
        # Train: 2020-2023
        # Test:  2024
        # ---------------------------------------------------------

        train_df = model_df[
            model_df["feature_year"] <= 2022
        ].copy()

        test_df = model_df[
            model_df["feature_year"] == 2023
        ].copy()

        if train_df.empty:
            raise ValueError("Training dataset is empty.")

        if test_df.empty:
            raise ValueError("Test dataset is empty.")

        logger.info(
            "Training rows: %s",
            len(train_df),
        )

        logger.info(
            "Test rows: %s",
            len(test_df),
        )

        logger.info(
            "Training years: %s-%s",
            train_df["feature_year"].min(),
            train_df["feature_year"].max(),
        )

        logger.info(
            "Test year: %s",
            test_df["feature_year"].min(),
        )

        # ---------------------------------------------------------
        # Build X / y
        # ---------------------------------------------------------

        X_train = train_df[FEATURE_COLUMNS]
        y_train = train_df[TARGET_COLUMN]

        X_test = test_df[FEATURE_COLUMNS]
        y_test = test_df[TARGET_COLUMN]

        # ---------------------------------------------------------
        # Train Random Forest
        # ---------------------------------------------------------

        logger.info("Training Random Forest model.")

        model = RandomForestRegressor(
            n_estimators=300,
            random_state=42,
            n_jobs=-1,
        )

        model.fit(
            X_train,
            y_train,
        )

        logger.info("Random Forest training complete.")

        # ---------------------------------------------------------
        # Predictions
        # ---------------------------------------------------------

        predictions = model.predict(X_test)

        # ---------------------------------------------------------
        # Evaluation
        # ---------------------------------------------------------

        mae = mean_absolute_error(
            y_test,
            predictions,
        )

        rmse = mean_squared_error(
            y_test,
            predictions,
        ) ** 0.5

        r2 = r2_score(
            y_test,
            predictions,
        )

        logger.info(
            "Model performance:"
        )

        logger.info(
            "MAE: %.6f",
            mae,
        )

        logger.info(
            "RMSE: %.6f",
            rmse,
        )

        logger.info(
            "R²: %.6f",
            r2,
        )

        # ---------------------------------------------------------
        # Feature importance
        # ---------------------------------------------------------

        feature_importance = (
            pd.DataFrame(
                {
                    "feature": FEATURE_COLUMNS,
                    "importance": model.feature_importances_,
                }
            )
            .sort_values(
                "importance",
                ascending=False,
            )
        )

        logger.info(
            "Top 15 feature importances:"
        )

        for _, row in feature_importance.head(15).iterrows():
            logger.info(
                "%s: %.6f",
                row["feature"],
                row["importance"],
            )

        # ---------------------------------------------------------
        # Actual vs predicted
        # ---------------------------------------------------------

        results = test_df[
            [
                "location_id",
                "county_name",
                "feature_year",
                "population",
                TARGET_COLUMN,
            ]
        ].copy()

        results["predicted_growth_1yr"] = predictions

        # Calculate the population the model is predicting
        # based on the predicted growth rate.
        results["predicted_population"] = (
            results["population"]
            * (1 + results["predicted_growth_1yr"])
        )

        # Actual population is the starting population grown
        # by the actual observed growth rate.
        results["actual_population"] = (
            results["population"]
            * (1 + results[TARGET_COLUMN])
        )

        results["population_error"] = (
            results["predicted_population"]
            - results["actual_population"]
        )

        results["population_error_pct"] = (
            results["population_error"]
            / results["actual_population"]
        ) * 100

        results["prediction_error"] = (
            results["predicted_growth_1yr"]
            - results[TARGET_COLUMN]
        )

        logger.info(
            "Actual vs predicted population results:"
        )

        # ---------------------------------------------------------
        # Overall population prediction summary
        # ---------------------------------------------------------

        mean_absolute_error_population = (
            results["population_error"]
            .abs()
            .mean()
        )

        mean_absolute_percentage_error_population = (
            results["population_error_pct"]
            .abs()
            .mean()
        )

        median_absolute_percentage_error_population = (
            results["population_error_pct"]
            .abs()
            .median()
        )

        max_absolute_error_population = (
            results["population_error"]
            .abs()
            .max()
        )

        largest_overprediction = results.loc[
            results["population_error"].idxmax()
        ]

        largest_underprediction = results.loc[
            results["population_error"].idxmin()
        ]

        logger.info(
            "Population prediction summary:"
        )

        logger.info(
            "Counties evaluated: %s",
            len(results),
        )

        logger.info(
            "Mean absolute population error: %.0f people",
            mean_absolute_error_population,
        )

        logger.info(
            "Mean absolute percentage error: %.2f%%",
            mean_absolute_percentage_error_population,
        )

        logger.info(
            "Median absolute percentage error: %.2f%%",
            median_absolute_percentage_error_population,
        )

        logger.info(
            "Maximum absolute population error: %.0f people",
            max_absolute_error_population,
        )

        logger.info(
            "Largest overprediction: %s by %.0f people (%.2f%%)",
            largest_overprediction["county_name"],
            largest_overprediction["population_error"],
            largest_overprediction["population_error_pct"],
        )

        logger.info(
            "Largest underprediction: %s by %.0f people (%.2f%%)",
            largest_underprediction["county_name"],
            largest_underprediction["population_error"],
            largest_underprediction["population_error_pct"],
        )

        for _, row in results.iterrows():
            logger.info(
                "%s (%s): "
                "actual_population=%.0f, "
                "predicted_population=%.0f, "
                "population_error=%.0f "
                "(%.2f%%)",
                row["county_name"],
                row["feature_year"],
                row["actual_population"],
                row["predicted_population"],
                row["population_error"],
                row["population_error_pct"],
            )

    finally:
        engine.dispose()