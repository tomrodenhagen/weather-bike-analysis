import pyspark.sql.functions as F
from scipy import spatial
import pandas as pd
import logging


def process_ride_datetime_columns(df_ride):
    df_ride = df_ride.withColumn(
        "starttime_ts", F.unix_timestamp(df_ride.starttime, "yyyy-MM-dd HH:mm:ss")
    )
    df_ride = df_ride.withColumn(
        "stoptime_ts", F.unix_timestamp(df_ride.stoptime, "yyyy-MM-dd HH:mm:ss")
    )
    df_ride = df_ride.withColumn(
        "date", F.from_unixtime(df_ride.starttime_ts, format="yyyy-MM-dd")
    )
    return df_ride


def process_weather_datetime_columns(df_weather):
    df_weather = df_weather.withColumn("date", F.to_date(df_weather.date, "yyyy-MM-dd"))
    return df_weather


def get_ride_stations(df_ride):
    res = df_ride.drop_duplicates(subset=["ride_station_id"])
    res = res.select(
        "ride_station_id", "start station latitude", "start station longitude"
    )
    return res


def add_close_weather_stations_to_ride_stations(
    df_ride_stations, df_weather_stations, n_stations, max_dist
):
    log = logging.getLogger("merge_stations_logger")
    # Its a valid assumption that the number of weather stations and ride stations fits into memory
    log.info("Load into pandas")
    df_ride_stations = df_ride_stations.toPandas()
    df_weather_stations = df_weather_stations.toPandas()
    log.info(
        f"Process {df_ride_stations.shape[0]} ride stations and {df_weather_stations.shape[0]} weather stations"
    )
    ride_locations = df_ride_stations.loc[
        :, ["start station latitude", "start station longitude"]
    ].values
    weather_locations = df_weather_stations.loc[:, ["latitude", "longitude"]].values
    log.info(f"Building KD-Tree")
    tree = spatial.KDTree(weather_locations)
    log.info(f"Query points")
    # This is a bit fishy, because latitude and longitude have different lengths
    stations_in_radius = tree.query_ball_point(ride_locations, max_dist)
    # This could be better optimized
    df_stations_in_radius = []

    for k, station_list in enumerate(stations_in_radius):
        weather_ids = df_weather_stations.weather_station_id.iloc[
            station_list[:n_stations]
        ]
        ride_id = df_ride_stations["ride_station_id"].iloc[k]
        df_ride_station = pd.DataFrame.from_dict(
            {"ride_station_id": ride_id, "weather_station_id": weather_ids}
        )
        df_stations_in_radius.append(df_ride_station)
    df_stations_in_radius = pd.concat(df_stations_in_radius)
    return df_stations_in_radius


def cross_join_with_dates(df_merged_stations, dates):
    return df_merged_stations.crossJoin(dates.dropDuplicates())


def rename_agg_weather_features(res):
    res = res.withColumnRenamed("avg(prcp)", "prcp")
    res = res.withColumnRenamed("avg(tmin)", "tmin")
    res = res.withColumnRenamed("avg(tmax)", "tmax")
    return res


def aggregate_over_weather_stations(df_merged_stations_with_date_and_weather):
    agg_dic = {"prcp": "avg", "tmin": "avg", "tmax": "avg"}
    res = df_merged_stations_with_date_and_weather.groupby(
        ["date", "ride_station_id"]
    ).agg(agg_dic)
    res = rename_agg_weather_features(res)
    return res


def get_per_station_and_day_data(df_ride_with_weather_data):
    res = df_ride_with_weather_data.groupby(["date", "ride_station_id"]).agg(
        {"gender": "count", "tmin": "avg", "tmax": "avg", "prcp": "avg"}
    )
    res = rename_agg_weather_features(res)
    res = res.withColumnRenamed("count(gender)", "n_rides")
    return res
