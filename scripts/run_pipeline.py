import os,sys
#This is a bit ugly
sys.path.append(os.getcwd())
from weather_bike_analysis.file_helpers import get_temp_data_path
from weather_bike_analysis.engineer_data import engineer_data
from weather_bike_analysis.analyse_data import analyse_per_station_and_day_data
from weather_bike_analysis.download_data import (
    download_weather_data,
    download_ride_data,
)
from weather_bike_analysis.file_helpers import prepare_system
import pandas as pd
import hydra
import logging


@hydra.main(config_path=".", config_name="config")
def main(cfg):
    log = logging.getLogger("main_logger")
    prepare_system(cfg)
    log.info("Download data")
    download_weather_data(cfg)
    download_ride_data(cfg)

    per_ride_path = os.path.join(get_temp_data_path(cfg), "per_ride.csv")
    per_day_path = os.path.join(get_temp_data_path(cfg), "per_day_and_station.csv")
    if (
        cfg.fresh
        or cfg.fresh_engineering
        or not os.path.exists(per_ride_path)
        or not os.path.exists(per_day_path)
    ):
        log.info("Engineer data")
        per_ride_data, per_day_data = engineer_data(cfg)
        # This might not work if the files are too big
        per_ride_data = per_ride_data.sample(0.05).toPandas()
        per_ride_data.to_csv(per_ride_path)
        per_day_data = per_day_data.toPandas()
        per_day_data.to_csv(per_day_path)
    else:
        log.info("Load cached data")
        per_ride_data = pd.read_csv(per_ride_path)
        per_day_data = pd.read_csv(per_day_path)
    log.info("Analyse data")
    analyse_per_station_and_day_data(per_day_data)


if __name__ == "__main__":
    main()
