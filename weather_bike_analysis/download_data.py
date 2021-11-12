from weather_bike_analysis.file_helpers import (
    get_bucket_data_path,
    get_weather_data_path,
    get_temp_data_path,
    mkdir,
    unzip,
)
from weather_bike_analysis.file_helpers import get_weather_data_path
from weather_bike_analysis.constants import (
    new_york_latitude,
    new_york_longitude,
    base_weather_query,
)
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
import wget, os, glob, csv


def prepare_system(cfg):
    mkdir(cfg.base_path)
    mkdir(get_bucket_data_path(cfg))
    mkdir(get_weather_data_path(cfg))
    mkdir(get_temp_data_path(cfg))


def download_ride_data(cfg):
    bucket_url = "https://s3.amazonaws.com/tripdata"
    filenames = cfg.ride_file_names
    for filename in filenames:
        url_complete = f"{bucket_url}/{filename}"

        bucket_data_path = get_bucket_data_path(cfg)

        dest = os.path.join(bucket_data_path, filename)
        if cfg.fresh or not os.path.exists(dest):
            _ = wget.download(url_complete, out=dest)
            unzip(bucket_data_path, filename, delete=False)


def download_weather_data(cfg):
    weather_data_path = get_weather_data_path(cfg)
    credentials = service_account.Credentials.from_service_account_file(
        cfg.google_cloud_key_path
    )
    stations_path = os.path.join(weather_data_path, "stations.csv")
    if cfg.fresh or not os.path.exists(stations_path):
        stations = pandas_gbq.read_gbq(
            "SELECT * FROM bigquery-public-data.ghcn_d.ghcnd_stations",
            credentials=credentials,
        )
        stations.to_csv(stations_path, index=False)
    else:
        stations = pd.read_csv(stations_path)
    latitude_boolean_index = (
        stations.latitude - new_york_latitude
    ) ** 2 < cfg.roi_size ** 2
    longitude_boolean_index = (
        stations.longitude - new_york_longitude
    ) ** 2 < cfg.roi_size ** 2
    stations_in_roi = stations.loc[latitude_boolean_index & longitude_boolean_index, :]
    stations_in_roi.to_csv(
        os.path.join(weather_data_path, "stations_roi.csv"), index=False
    )
    for year in cfg.years:
        weather_time_series_path = os.path.join(
            weather_data_path, f"weather_{year}.csv"
        )
        if cfg.fresh or not os.path.exists(weather_time_series_path):
            query = base_weather_query.format(
                **{"year": year, "stations": tuple(list(stations_in_roi.id))}
            )
            weather_time_series = pandas_gbq.read_gbq(query, credentials=credentials)
            weather_time_series.to_csv(weather_time_series_path, index=False)
