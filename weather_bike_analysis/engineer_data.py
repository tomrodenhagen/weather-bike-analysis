from weather_bike_analysis.file_helpers import (
    get_bucket_data_path,
    get_weather_data_path,
    get_temp_data_path,
)
from weather_bike_analysis.load_data import (
    load_weather_data,
    load_ride_data,
    load_weather_stations,
)
from weather_bike_analysis.data_transformations import (
    process_ride_datetime_columns,
    get_ride_stations,
    add_close_weather_stations_to_ride_stations,
    cross_join_with_dates,
    aggregate_over_weather_stations,
    get_per_station_and_day_data,
    process_weather_datetime_columns,
)
from weather_bike_analysis.data_quality_checks import (
    check_weather_data,
    check_ride_data,
)
from pyspark.sql import SparkSession
import os, json
import logging


def engineer_data(cfg):
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(get_temp_data_path(cfg) / "debug.log", mode="w"),
            logging.StreamHandler(),
        ],
    )
    log = logging.getLogger("engineer_data_logger")
    data_quality_logger = {}
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("weather_bike_analysis")
        .getOrCreate()
    )

    bucket_data_path = get_bucket_data_path(cfg)
    weather_data_path = get_weather_data_path(cfg)

    log.info("Load data into spark")
    df_weather = load_weather_data(spark, weather_data_path)
    df_ride = load_ride_data(spark, bucket_data_path).sample(
        fraction=cfg.fraction_ride_data
    )
    df_weather_stations = load_weather_stations(spark, weather_data_path)

    log.info(df_ride.show(10))
    log.info(df_weather.show(10))
    log.info(df_weather_stations.show(10))

    log.info("Do some simple transformations")
    df_ride = process_ride_datetime_columns(df_ride)
    df_weather = process_weather_datetime_columns(df_weather)
    data_quality_logger.update({"weather_data": check_weather_data(df_weather)})
    data_quality_logger.update({"ride_data": check_ride_data(df_ride)})
    df_ride_stations = get_ride_stations(df_ride)

    log.info("Find close weather stations to ride stations")
    # Get a normalized table with ride stations mapped to n weather stations in specified radius
    pd_df_merged_stations = add_close_weather_stations_to_ride_stations(
        df_ride_stations,
        df_weather_stations,
        cfg.number_of_close_stations,
        cfg.max_station_dist,
    )
    # Very ugly way to transform from pandas to spark, but otherwise this fails
    pd_df_merged_stations.to_csv(
        os.path.join(get_temp_data_path(cfg), "ride_weather_station.csv"), index=False
    )
    df_merged_stations = spark.read.csv(
        os.path.join(get_temp_data_path(cfg), "ride_weather_station.csv"), header=True
    )
    log.info(df_merged_stations.show(10))

    log.info("Cross joining with dates")
    # Add all needed dates to merged station table
    df_merged_stations_with_date = cross_join_with_dates(
        df_merged_stations, df_ride.select("date")
    )
    log.info(df_merged_stations_with_date.show(10))

    log.info("Join with weather data")
    # Add weather features now to each weather station and date we need
    df_merged_stations_with_date_and_weather = df_merged_stations_with_date.join(
        df_weather, on=["weather_station_id", "date"], how="left"
    )
    log.info(df_merged_stations_with_date_and_weather.show(10))

    log.info("Aggregate weather data")
    # Aggregate for each date and ride station over the weather data from different weather stations
    df_weather_per_day_and_station = aggregate_over_weather_stations(
        df_merged_stations_with_date_and_weather
    )
    log.info(df_weather_per_day_and_station.show(10))
    data_quality_logger.update(
        {
            "weather_per_station_and_day": check_weather_data(
                df_weather_per_day_and_station
            )
        }
    )

    log.info("Join with ride data")
    df_ride_with_weather_data = df_ride.join(
        df_weather_per_day_and_station, on=["date", "ride_station_id"], how="left"
    )
    log.info(df_ride_with_weather_data.show(10))
    log.info("Get per day data")

    df_per_station_and_day_data = get_per_station_and_day_data(
        df_ride_with_weather_data
    )
    json.dump(
        data_quality_logger,
        open(get_temp_data_path(cfg) / "data_quality.json", "w"),
        indent=3,
    )
    return df_ride_with_weather_data, df_per_station_and_day_data
