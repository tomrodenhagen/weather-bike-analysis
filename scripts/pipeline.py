from weather_bike_analyis.file_helpers import get_bucket_data_path, get_weather_data_path
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, from_unixtime, unix_timestamp
from omegaconf import DictConfig
import hydra
import os,glob
import pandas as pd

def aggregate_ride_data(spark, bucket_data_path):
    paths = glob.glob(f"{bucket_data_path}/*.csv")
    return spark.read.csv(paths, header=True)



def aggregate_weather_data(spark, weather_data_path, years=[2013,2014]):
    paths = [path for year in years
             for path in glob.glob(f"{weather_data_path}/{year}/*.txt") ]
    for path in paths:
        preprocess_weather_csv(path)
    return spark.read.option("sep",'').csv(paths)


def process_datetime_columns(df_ride):
    df_ride = df_ride.withColumn("starttime_ts", unix_timestamp(df_ride.starttime, 'yyyy-MM-dd HH:mm:ss'))
    df_ride = df_ride.withColumn("stoptime_ts", unix_timestamp(df_ride.stoptime, 'yyyy-MM-dd HH:mm:ss'))
    return df_ride
def check_duration_consistency(df_ride):
    df_consistency_check = df_ride.withColumn("expected_duration", df_ride["stoptime_ts"] - df_ride["starttime_ts"])
    df_diff = df_consistency_check.withColumn("difference", df_consistency_check.expected_duration - df_consistency_check.tripduration)
    df_zeros = df_diff.select(F.count(F.when( F.abs(df_diff.difference) > 1, "Nonzero_count")).alias("Nonzero_count") )
    df_bad_samples = df_diff.filter(F.abs(df_diff.difference) > 1).collect()
    res = {"Number of inconsistent rows": df_zeros.toPandas().iloc[0,0],
           "Inconsistent samples": df_bad_samples[:3]
           }
    return res


@hydra.main(config_path=".", config_name="config")
def run_pipeline(cfg: DictConfig) -> None:
    spark = SparkSession.builder.master("local[1]").appName("weather_bike_analysis").getOrCreate()
    data_quality_logger = {}
    bucket_data_path = get_bucket_data_path(cfg)
    weather_data_path = get_weather_data_path(cfg)
    df_weather = aggregate_weather_data(spark, weather_data_path)
    print(df_weather.show())
    df_ride = aggregate_ride_data(spark, bucket_data_path)
    df_ride = process_datetime_columns(df_ride)

    res = check_duration_consistency( df_ride.sample( fraction = cfg.fraction_to_check ) )
    data_quality_logger.update({"duration_consistency": res})


if __name__ == "__main__":
    run_pipeline()