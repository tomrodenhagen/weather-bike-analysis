import glob, os


def load_ride_data(spark, bucket_data_path):
    paths = glob.glob(f"{bucket_data_path}/*.csv")
    return spark.read.csv(paths, header=True).withColumnRenamed(
        "start station id", "ride_station_id"
    )


def load_weather_data(spark, weather_data_path):
    paths = glob.glob(f"{weather_data_path}/weather_*.csv")
    return spark.read.csv(paths, header=True).withColumnRenamed(
        "id", "weather_station_id"
    )


def load_weather_stations(spark, weather_data_path):
    res = spark.read.csv(
        os.path.join(weather_data_path, "stations_roi.csv"), header=True
    )
    res = res.withColumnRenamed("id", "weather_station_id")
    return res
