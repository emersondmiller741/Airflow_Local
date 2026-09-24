import openmeteo_requests
import time
import pandas as pd
import requests_cache
from retry_requests import retry

from census_weather_ml_demo.base_report import BaseReport


class WeatherData(BaseReport):

    def generate(
            self,
            location_id: str,
            latitude: float,
            longitude: float,
            start_date: str,
            end_date: str,
    ) -> pd.DataFrame:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(
            ".cache",
            expire_after=3600,
        )

        retry_session = retry(
            cache_session,
            retries=5,
            backoff_factor=0.2,
        )

        openmeteo = openmeteo_requests.Client(
            session=retry_session
        )

        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "snowfall_sum",
                "wind_speed_10m_max",
                "weather_code",
            ],
            "timezone": "auto",
        }

        max_attempts = 5

        for attempt in range(1, max_attempts + 1):

            try:

                responses = openmeteo.weather_api(
                    url,
                    params=params,
                )

                time.sleep(2)

                break

            except Exception as exc:

                error_message = str(exc)

                if "Minutely API request limit exceeded" not in error_message:
                    raise

                if attempt == max_attempts:
                    raise

                wait_seconds = 65

                print()
                print("=" * 70)
                print("OPEN-METEO RATE LIMIT REACHED")
                print("=" * 70)
                print(
                    f"Attempt {attempt} of {max_attempts}."
                )
                print(
                    f"Waiting {wait_seconds} seconds before retrying..."
                )

                time.sleep(wait_seconds)

        response = responses[0]

        print(
            f"Coordinates: "
            f"{response.Latitude()}°N "
            f"{response.Longitude()}°E"
        )

        print(
            f"Elevation: "
            f"{response.Elevation()} m asl"
        )

        print(
            f"Timezone difference to GMT+0: "
            f"{response.UtcOffsetSeconds()}s"
        )

        # Process daily data.
        daily = response.Daily()

        # Open-Meteo daily timestamps represent the start of the
        # local calendar day. Convert that timestamp to UTC for
        # standardized storage, while also preserving the local date.
        timestamp_utc = pd.date_range(
            start=pd.to_datetime(
                daily.Time(),
                unit="s",
                utc=True,
            ),
            end=pd.to_datetime(
                daily.TimeEnd(),
                unit="s",
                utc=True,
            ),
            freq=pd.Timedelta(
                seconds=daily.Interval()
            ),
            inclusive="left",
        )

        daily_dataframe = pd.DataFrame(
            {
                "location_id": location_id,
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "local_date": timestamp_utc.date,
                "timestamp_utc": timestamp_utc,
                "temperature_max": daily.Variables(0).ValuesAsNumpy(),
                "temperature_min": daily.Variables(1).ValuesAsNumpy(),
                "temperature_mean": daily.Variables(2).ValuesAsNumpy(),
                "precipitation": daily.Variables(3).ValuesAsNumpy(),
                "snowfall": daily.Variables(4).ValuesAsNumpy(),
                "wind_speed_max": daily.Variables(5).ValuesAsNumpy(),
                "weather_code": daily.Variables(6).ValuesAsNumpy(),
            }
        )

        print("\nDaily weather data\n")
        print(daily_dataframe)

        print(
            f"\nRows returned: "
            f"{len(daily_dataframe)}"
        )

        return daily_dataframe