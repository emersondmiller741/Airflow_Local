import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from census_weather_ml_demo.base_report import BaseReport
import pandas as pd



class CensusData(BaseReport):

    def generate(self, year: int) -> pd.DataFrame:

        def get_census_api_url(year: int) -> str:
            return f"https://api.census.gov/data/{year}/acs/acs5"

        def get_census_geo_api_url(year: int) -> str:
            return f"https://api.census.gov/data/{year}/geoinfo"

        def fetch_census_data(year: int) -> pd.DataFrame:
            """Fetch county-level Census demographic, economic,
            housing, geographic mobility, and geographic data.
            """

            CENSUS_API_URL = get_census_api_url(year)
            CENSUS_GEO_API_URL = get_census_geo_api_url(year)

            api_key = os.environ.get("CENSUS_API_KEY")

            if not api_key:
                raise ValueError(
                    "CENSUS_API_KEY environment variable is not set."
                )

            STATES = {
                "SC": "45",
                # "FL": "12",
            }

            records = []

            for state, state_fips in STATES.items():

                # ---------------------------------------------------------
                # Census demographic, economic, and housing data
                # ---------------------------------------------------------

                params = {
                    "get": ",".join([
                        "NAME",

                        # Population
                        "B01001_001E",

                        # Age
                        "B01002_001E",

                        # Age groups
                        "B01001_003E",
                        "B01001_004E",
                        "B01001_005E",
                        "B01001_006E",
                        "B01001_020E",
                        "B01001_021E",
                        "B01001_022E",
                        "B01001_023E",
                        "B01001_024E",
                        "B01001_025E",
                        "B01001_027E",
                        "B01001_028E",
                        "B01001_029E",
                        "B01001_030E",
                        "B01001_044E",
                        "B01001_045E",
                        "B01001_046E",
                        "B01001_047E",
                        "B01001_048E",
                        "B01001_049E",

                        # Race
                        "B02001_002E",
                        "B02001_003E",

                        # Hispanic origin
                        "B03003_003E",

                        # Education
                        "B15003_001E",
                        "B15003_022E",
                        "B15003_023E",
                        "B15003_024E",
                        "B15003_025E",

                        # Income
                        "B19013_001E",

                        # Poverty
                        "B17001_001E",
                        "B17001_002E",

                        # Employment
                        "B23025_001E",
                        "B23025_003E",
                        "B23025_005E",

                        # Housing
                        "B25001_001E",
                        "B25002_001E",
                        "B25002_003E",
                        "B25003_001E",
                        "B25003_002E",
                        "B25010_001E",
                        "B25064_001E",
                        "B25077_001E",

                        # Foreign born
                        "B05001_001E",
                        "B05001_002E",

                    ]),
                    "for": "county:*",
                    "in": f"state:{state_fips}",
                    "key": api_key,
                }

                response = requests.get(
                    CENSUS_API_URL,
                    params=params,
                    timeout=30,
                )

                if not response.ok:
                    print("Census API request failed")
                    print("Status code:", response.status_code)
                    print("Response:", response.text)

                response.raise_for_status()

                data = response.json()

                headers = data[0]
                rows = data[1:]

                census_counties = [
                    dict(zip(headers, row))
                    for row in rows
                ]

                print(
                    f"Retrieved {len(census_counties)} counties "
                    f"for {state}"
                )

                # ---------------------------------------------------------
                # Geographic mobility data
                # ---------------------------------------------------------

                mobility_params = {
                    "get": ",".join([
                        "NAME",
                        "B07001_001E",
                        "B07001_033E",
                        "B07001_049E",
                        "B07001_065E",
                        "B07001_081E",
                    ]),
                    "for": "county:*",
                    "in": f"state:{state_fips}",
                    "key": api_key,
                }

                mobility_response = requests.get(
                    CENSUS_API_URL,
                    params=mobility_params,
                    timeout=30,
                )

                if not mobility_response.ok:
                    print("Census mobility API request failed")
                    print(
                        "Status code:",
                        mobility_response.status_code
                    )
                    print(
                        "Response:",
                        mobility_response.text
                    )

                mobility_response.raise_for_status()

                mobility_data = mobility_response.json()

                mobility_headers = mobility_data[0]
                mobility_rows = mobility_data[1:]

                mobility_counties = [
                    dict(zip(mobility_headers, row))
                    for row in mobility_rows
                ]

                mobility_by_county = {
                    county["county"]: county
                    for county in mobility_counties
                }

                print(
                    f"Retrieved {len(mobility_counties)} mobility "
                    f"records for {state}"
                )

                # ---------------------------------------------------------
                # County latitude and longitude
                # ---------------------------------------------------------

                geo_params = {
                    "get": "NAME,INTPTLAT,INTPTLON",
                    "for": "county:*",
                    "in": f"state:{state_fips}",
                    "key": api_key,
                }

                geo_response = requests.get(
                    CENSUS_GEO_API_URL,
                    params=geo_params,
                    timeout=30,
                )

                if not geo_response.ok:
                    print("Census geographic API request failed")
                    print(
                        "Status code:",
                        geo_response.status_code
                    )
                    print(
                        "Response:",
                        geo_response.text
                    )

                geo_response.raise_for_status()

                geo_data = geo_response.json()

                geo_headers = geo_data[0]
                geo_rows = geo_data[1:]

                county_coordinates = [
                    dict(zip(geo_headers, row))
                    for row in geo_rows
                ]

                coordinates_by_county = {
                    county["county"]: county
                    for county in county_coordinates
                }

                # ---------------------------------------------------------
                # Build records
                # ---------------------------------------------------------

                for county in census_counties:

                    county_fips = county["county"]

                    coordinates = coordinates_by_county.get(
                        county_fips
                    )

                    if not coordinates:
                        print(
                            f"WARNING: No coordinates found for "
                            f"{county['NAME']}"
                        )
                        continue

                    mobility = mobility_by_county.get(
                        county_fips
                    )

                    if not mobility:
                        print(
                            f"WARNING: No mobility data found for "
                            f"{county['NAME']}"
                        )
                        continue

                    records.append(
                        {
                            "year": year,
                            "state": state,
                            "state_fips": county["state"],
                            "county_fips": county["county"],
                            "county_name": county["NAME"],

                            # Population
                            "population": county["B01001_001E"],

                            # Demographics
                            "median_age": county["B01002_001E"],

                            "under_18": (
                                int(county["B01001_003E"])
                                + int(county["B01001_004E"])
                                + int(county["B01001_005E"])
                                + int(county["B01001_006E"])
                                + int(county["B01001_027E"])
                                + int(county["B01001_028E"])
                                + int(county["B01001_029E"])
                                + int(county["B01001_030E"])
                            ),

                            "over_65": (
                                int(county["B01001_020E"])
                                + int(county["B01001_021E"])
                                + int(county["B01001_022E"])
                                + int(county["B01001_023E"])
                                + int(county["B01001_024E"])
                                + int(county["B01001_025E"])
                                + int(county["B01001_044E"])
                                + int(county["B01001_045E"])
                                + int(county["B01001_046E"])
                                + int(county["B01001_047E"])
                                + int(county["B01001_048E"])
                                + int(county["B01001_049E"])
                            ),

                            "white_population": county["B02001_002E"],
                            "black_population": county["B02001_003E"],
                            "hispanic_population": county["B03003_003E"],

                            # Education
                            "education_25_plus": county["B15003_001E"],
                            "bachelors_degree": (
                                int(county["B15003_022E"])
                                + int(county["B15003_023E"])
                                + int(county["B15003_024E"])
                                + int(county["B15003_025E"])
                            ),

                            # Economic
                            "median_household_income": (
                                county["B19013_001E"]
                            ),

                            "poverty_population": county["B17001_001E"],
                            "below_poverty": county["B17001_002E"],

                            "population_16_plus": county["B23025_001E"],
                            "civilian_labor_force": county["B23025_003E"],
                            "unemployed": county["B23025_005E"],

                            # Housing
                            "housing_units": county["B25001_001E"],
                            "total_housing_units": county["B25002_001E"],
                            "vacant_housing_units": county["B25002_003E"],
                            "occupied_housing_units": county["B25003_001E"],
                            "owner_occupied_units": county["B25003_002E"],
                            "average_household_size": county["B25010_001E"],
                            "median_gross_rent": county["B25064_001E"],
                            "median_home_value": county["B25077_001E"],

                            # Foreign born
                            "foreign_born": (
                                int(county["B05001_001E"])
                                - int(county["B05001_002E"])
                            ),

                            # Geographic mobility
                            "population_1_plus": (
                                mobility["B07001_001E"]
                            ),
                            "moved_within_county": (
                                mobility["B07001_033E"]
                            ),
                            "moved_from_other_county_same_state": (
                                mobility["B07001_049E"]
                            ),
                            "moved_from_other_state": (
                                mobility["B07001_065E"]
                            ),
                            "moved_from_abroad": (
                                mobility["B07001_081E"]
                            ),

                            # Geography
                            "latitude": coordinates["INTPTLAT"],
                            "longitude": coordinates["INTPTLON"],
                        }
                    )

            return pd.DataFrame(records)

        print(f"Generating census extraction for {year}")

        df = fetch_census_data(year)

        # -------------------------------------------------------------
        # Convert numeric fields
        # -------------------------------------------------------------

        numeric_columns = [
            "population",
            "median_age",
            "under_18",
            "over_65",
            "white_population",
            "black_population",
            "hispanic_population",
            "education_25_plus",
            "bachelors_degree",
            "median_household_income",
            "poverty_population",
            "below_poverty",
            "population_16_plus",
            "civilian_labor_force",
            "unemployed",
            "housing_units",
            "total_housing_units",
            "vacant_housing_units",
            "occupied_housing_units",
            "owner_occupied_units",
            "average_household_size",
            "median_gross_rent",
            "median_home_value",
            "foreign_born",
            "population_1_plus",
            "moved_within_county",
            "moved_from_other_county_same_state",
            "moved_from_other_state",
            "moved_from_abroad",
            "latitude",
            "longitude",
        ]

        for column in numeric_columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

        # -------------------------------------------------------------
        # Calculate model features as percentages
        # -------------------------------------------------------------

        df["pct_under_18"] = (
            df["under_18"] / df["population"] * 100
        )

        df["pct_over_65"] = (
            df["over_65"] / df["population"] * 100
        )

        df["pct_white"] = (
            df["white_population"] / df["population"] * 100
        )

        df["pct_black"] = (
            df["black_population"] / df["population"] * 100
        )

        df["pct_hispanic"] = (
            df["hispanic_population"] / df["population"] * 100
        )

        df["pct_bachelors_or_higher"] = (
            df["bachelors_degree"]
            / df["education_25_plus"]
            * 100
        )

        df["pct_below_poverty"] = (
            df["below_poverty"]
            / df["poverty_population"]
            * 100
        )

        df["labor_force_participation_rate"] = (
            df["civilian_labor_force"]
            / df["population_16_plus"]
            * 100
        )

        df["unemployment_rate"] = (
            df["unemployed"]
            / df["civilian_labor_force"]
            * 100
        )

        df["homeownership_rate"] = (
            df["owner_occupied_units"]
            / df["occupied_housing_units"]
            * 100
        )

        df["vacancy_rate"] = (
            df["vacant_housing_units"]
            / df["total_housing_units"]
            * 100
        )

        df["pct_foreign_born"] = (
            df["foreign_born"]
            / df["population"]
            * 100
        )

        # -------------------------------------------------------------
        # Geographic mobility feature
        # -------------------------------------------------------------

        df["moved_last_year"] = (
            df["moved_within_county"]
            + df["moved_from_other_county_same_state"]
            + df["moved_from_other_state"]
            + df["moved_from_abroad"]
        )

        df["pct_moved_last_year"] = (
            df["moved_last_year"]
            / df["population_1_plus"]
            * 100
        )

        # -------------------------------------------------------------
        # Geographic mobility validation
        # -------------------------------------------------------------

        print()
        print("Geographic mobility:")
        print(
            df[
                [
                    "county_name",
                    "population_1_plus",
                    "moved_last_year",
                    "pct_moved_last_year",
                ]
            ].sort_values(
                "pct_moved_last_year",
                ascending=False,
            )
        )

        # -------------------------------------------------------------
        # DataFrame output
        # -------------------------------------------------------------

        print(df)
        print()
        print("DataFrame shape:", df.shape)
        print()
        print("Data types:")
        print(df.dtypes)

        # -------------------------------------------------------------
        # Data quality validation
        # -------------------------------------------------------------

        print()
        print("=" * 70)
        print("DATA QUALITY VALIDATION")
        print("=" * 70)

        validation_errors = []

        # -------------------------------------------------------------
        # Row count
        # -------------------------------------------------------------

        expected_counties = 46

        if len(df) != expected_counties:
            validation_errors.append(
                f"Expected {expected_counties} counties, "
                f"found {len(df)}"
            )
        else:
            print(
                f"PASS: County count = {len(df)}"
            )

        # -------------------------------------------------------------
        # One record per county/year
        # -------------------------------------------------------------

        duplicate_count = df.duplicated(
            subset=[
                "state",
                "county_fips",
                "year",
            ]
        ).sum()

        if duplicate_count > 0:
            validation_errors.append(
                f"Found {duplicate_count} duplicate "
                "county/year records"
            )
        else:
            print(
                "PASS: No duplicate county/year records"
            )

        # -------------------------------------------------------------
        # Required fields
        # -------------------------------------------------------------

        required_columns = [
            "year",
            "state",
            "state_fips",
            "county_fips",
            "county_name",
            "population",
            "population_1_plus",
            "latitude",
            "longitude",
        ]

        for column in required_columns:

            missing_count = df[column].isna().sum()

            if missing_count > 0:
                validation_errors.append(
                    f"{column}: {missing_count} missing values"
                )
            else:
                print(
                    f"PASS: {column} has no missing values"
                )

        # -------------------------------------------------------------
        # Population sanity checks
        # -------------------------------------------------------------

        if (df["population"] <= 0).any():
            validation_errors.append(
                "Population contains zero or negative values"
            )
        else:
            print(
                "PASS: Population values are positive"
            )

        if (df["population_1_plus"] <= 0).any():
            validation_errors.append(
                "population_1_plus contains zero or negative values"
            )
        else:
            print(
                "PASS: population_1_plus values are positive"
            )

        # -------------------------------------------------------------
        # Percentage sanity checks
        # -------------------------------------------------------------

        percentage_columns = [
            "pct_under_18",
            "pct_over_65",
            "pct_white",
            "pct_black",
            "pct_hispanic",
            "pct_bachelors_or_higher",
            "pct_below_poverty",
            "labor_force_participation_rate",
            "unemployment_rate",
            "homeownership_rate",
            "vacancy_rate",
            "pct_foreign_born",
            "pct_moved_last_year",
        ]

        for column in percentage_columns:

            invalid_count = (
                (df[column] < 0)
                | (df[column] > 100)
                | df[column].isna()
            ).sum()

            if invalid_count > 0:
                validation_errors.append(
                    f"{column}: {invalid_count} invalid "
                    "values outside 0-100 or missing"
                )
            else:
                print(
                    f"PASS: {column} is within 0-100"
                )

        # -------------------------------------------------------------
        # Age consistency
        # -------------------------------------------------------------

        invalid_age_count = (
            (df["under_18"] > df["population"])
            | (df["over_65"] > df["population"])
        ).sum()

        if invalid_age_count > 0:
            validation_errors.append(
                f"Age totals exceed population in "
                f"{invalid_age_count} counties"
            )
        else:
            print(
                "PASS: Age totals do not exceed population"
            )

        # -------------------------------------------------------------
        # Housing consistency
        # -------------------------------------------------------------

        housing_mismatch = (
            df["vacant_housing_units"]
            + df["occupied_housing_units"]
            != df["total_housing_units"]
        ).sum()

        if housing_mismatch > 0:
            validation_errors.append(
                f"Housing totals do not reconcile in "
                f"{housing_mismatch} counties"
            )
        else:
            print(
                "PASS: Occupied + vacant housing = total housing"
            )

        # -------------------------------------------------------------
        # Labor force consistency
        # -------------------------------------------------------------

        invalid_labor_count = (
            (df["civilian_labor_force"] < 0)
            | (
                df["civilian_labor_force"]
                > df["population_16_plus"]
            )
            | (df["unemployed"] < 0)
            | (
                df["unemployed"]
                > df["civilian_labor_force"]
            )
        ).sum()

        if invalid_labor_count > 0:
            validation_errors.append(
                f"Labor force values are inconsistent in "
                f"{invalid_labor_count} counties"
            )
        else:
            print(
                "PASS: Labor force values are internally consistent"
            )

        # -------------------------------------------------------------
        # Geographic mobility consistency
        # -------------------------------------------------------------

        calculated_moved = (
            df["moved_within_county"]
            + df["moved_from_other_county_same_state"]
            + df["moved_from_other_state"]
            + df["moved_from_abroad"]
        )

        mobility_mismatch = (
            calculated_moved
            != df["moved_last_year"]
        ).sum()

        if mobility_mismatch > 0:
            validation_errors.append(
                f"Mobility totals do not reconcile in "
                f"{mobility_mismatch} counties"
            )
        else:
            print(
                "PASS: Mobility components reconcile"
            )

        invalid_mobility_count = (
            (df["moved_last_year"] < 0)
            | (
                df["moved_last_year"]
                > df["population_1_plus"]
            )
        ).sum()

        if invalid_mobility_count > 0:
            validation_errors.append(
                f"Moved population exceeds mobility universe "
                f"in {invalid_mobility_count} counties"
            )
        else:
            print(
                "PASS: Moved population does not exceed "
                "mobility universe"
            )

        # -------------------------------------------------------------
        # Geographic coordinate validation
        # -------------------------------------------------------------

        invalid_latitude = (
            (df["latitude"] < -90)
            | (df["latitude"] > 90)
            | df["latitude"].isna()
        ).sum()

        invalid_longitude = (
            (df["longitude"] < -180)
            | (df["longitude"] > 180)
            | df["longitude"].isna()
        ).sum()

        if invalid_latitude > 0:
            validation_errors.append(
                f"Found {invalid_latitude} invalid latitude values"
            )
        else:
            print(
                "PASS: Latitude values are valid"
            )

        if invalid_longitude > 0:
            validation_errors.append(
                f"Found {invalid_longitude} invalid longitude values"
            )
        else:
            print(
                "PASS: Longitude values are valid"
            )

        # -------------------------------------------------------------
        # Foreign-born calculation validation
        # -------------------------------------------------------------

        foreign_born_recalculated = (
            df["population"]
            - (
                df["population"]
                - df["foreign_born"]
            )
        )

        foreign_born_mismatch = (
            foreign_born_recalculated
            != df["foreign_born"]
        ).sum()

        if foreign_born_mismatch > 0:
            validation_errors.append(
                "Foreign-born calculation does not reconcile"
            )
        else:
            print(
                "PASS: Foreign-born calculation reconciles"
            )

        # -------------------------------------------------------------
        # Validation summary
        # -------------------------------------------------------------

        print()
        print("-" * 70)

        if validation_errors:

            print(
                f"VALIDATION FAILED: "
                f"{len(validation_errors)} issue(s) found"
            )

            for error in validation_errors:
                print(f"ERROR: {error}")

            raise ValueError(
                "Census data quality validation failed"
            )

        else:

            print(
                "VALIDATION PASSED: "
                "No data-quality issues detected."
            )

        print("=" * 70)

        return df