import os

import pandas as pd
import requests

from census_weather_ml_demo.base_report import BaseReport


class CensusGeographics(BaseReport):

    def generate(self) -> pd.DataFrame:

        # Census API configuration
        api_key = os.environ.get("CENSUS_API_KEY")

        if not api_key:
            raise ValueError(
                "CENSUS_API_KEY environment variable is not set."
            )

        # Get South Carolina county geographic information
        county_url = (
            "https://api.census.gov/data/2023/geoinfo"
        )

        county_params = {
            "get": (
                "NAME,"
                "AREALAND_SQMI,"
                "AREAWATR_SQMI,"
                "INTPTLAT,"
                "INTPTLON"
            ),
            "for": "county:*",
            "in": "state:45",
            "key": api_key,
        }

        county_response = requests.get(
            county_url,
            params=county_params,
            timeout=60,
        )

        county_response.raise_for_status()

        county_data = county_response.json()

        county_dataframe = pd.DataFrame(
            county_data[1:],
            columns=county_data[0],
        )

        county_dataframe = county_dataframe.rename(
            columns={
                "NAME": "county_name",
                "AREALAND_SQMI": "land_area_sq_miles",
                "AREAWATR_SQMI": "water_area_sq_miles",
                "INTPTLAT": "latitude",
                "INTPTLON": "longitude",
                "state": "state_fips",
                "county": "county_fips",
            }
        )

        county_dataframe["land_area_sq_miles"] = pd.to_numeric(
            county_dataframe["land_area_sq_miles"]
        )

        county_dataframe["water_area_sq_miles"] = pd.to_numeric(
            county_dataframe["water_area_sq_miles"]
        )

        county_dataframe["latitude"] = pd.to_numeric(
            county_dataframe["latitude"]
        )

        county_dataframe["longitude"] = pd.to_numeric(
            county_dataframe["longitude"]
        )

        county_dataframe["total_area_sq_miles"] = (
            county_dataframe["land_area_sq_miles"]
            + county_dataframe["water_area_sq_miles"]
        )

        # Create standardized project location ID.
        # Format matches the existing weather tables:
        # SC + state FIPS + county FIPS
        # Example: Abbeville County = SC45001
        county_dataframe["location_id"] = (
            "SC"
            + county_dataframe["state_fips"]
            + county_dataframe["county_fips"]
        )

        # Get South Carolina CBSA list
        cbsa_list_params = {
            "get": "NAME",
            "for": (
                "metropolitan statistical area/"
                "micropolitan statistical area (or part):*"
            ),
            "in": "state:45",
            "key": api_key,
        }

        cbsa_list_response = requests.get(
            county_url,
            params=cbsa_list_params,
            timeout=60,
        )

        cbsa_list_response.raise_for_status()

        cbsa_list_data = cbsa_list_response.json()

        cbsa_list_dataframe = pd.DataFrame(
            cbsa_list_data[1:],
            columns=cbsa_list_data[0],
        )

        cbsa_list_dataframe = cbsa_list_dataframe.rename(
            columns={
                "NAME": "cbsa_name",
                (
                    "metropolitan statistical area/"
                    "micropolitan statistical area (or part)"
                ): "cbsa_code",
            }
        )

        # Get county membership for each CBSA
        cbsa_county_rows = []

        for _, cbsa_row in cbsa_list_dataframe.iterrows():

            cbsa_code = cbsa_row["cbsa_code"]
            cbsa_name = cbsa_row["cbsa_name"]

            county_params = {
                "get": "NAME",
                "for": "county:*",
                "in": (
                    "state:45 "
                    "metropolitan statistical area/"
                    "micropolitan statistical area (or part):"
                    f"{cbsa_code}"
                ),
                "key": api_key,
            }

            response = requests.get(
                county_url,
                params=county_params,
                timeout=60,
            )

            response.raise_for_status()

            data = response.json()

            if len(data) <= 1:
                continue

            dataframe = pd.DataFrame(
                data[1:],
                columns=data[0],
            )

            dataframe["cbsa_code"] = cbsa_code
            dataframe["cbsa_name"] = cbsa_name

            cbsa_county_rows.append(dataframe)

        if cbsa_county_rows:

            cbsa_county_dataframe = pd.concat(
                cbsa_county_rows,
                ignore_index=True,
            )

            cbsa_county_dataframe = (
                cbsa_county_dataframe.rename(
                    columns={
                        "county": "county_fips",
                    }
                )
            )

            cbsa_county_dataframe = (
                cbsa_county_dataframe[
                    [
                        "county_fips",
                        "cbsa_code",
                        "cbsa_name",
                    ]
                ]
            )

        else:

            cbsa_county_dataframe = pd.DataFrame(
                columns=[
                    "county_fips",
                    "cbsa_code",
                    "cbsa_name",
                ]
            )

        geographic_features = county_dataframe.merge(
            cbsa_county_dataframe,
            on="county_fips",
            how="left",
        )

        geographic_features = geographic_features[
            [
                "location_id",
                "state_fips",
                "county_fips",
                "county_name",
                "land_area_sq_miles",
                "water_area_sq_miles",
                "total_area_sq_miles",
                "latitude",
                "longitude",
                "cbsa_code",
                "cbsa_name",
            ]
        ]

        print()
        print("=" * 70)
        print("CENSUS GEOGRAPHIC DATA")
        print("=" * 70)

        print(
            f"Rows returned: "
            f"{len(geographic_features)}"
        )

        print(
            f"Unique counties: "
            f"{geographic_features['county_fips'].nunique()}"
        )

        print(
            f"Unique CBSAs: "
            f"{geographic_features['cbsa_code'].nunique()}"
        )

        print()
        print(
            geographic_features.to_string(
                index=False
            )
        )

        return geographic_features