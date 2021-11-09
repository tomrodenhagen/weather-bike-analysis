from weather_bike_analyis.file_helpers import get_bucket_data_path, get_weather_data_path, mkdir, \
    get_processed_weather_data_path
from omegaconf import DictConfig, OmegaConf
import hydra
import os



@hydra.main(config_path=".", config_name="config")
def prepare_system(cfg: DictConfig) -> None:

    mkdir(cfg.base_path)
    mkdir( get_bucket_data_path(cfg) )
    mkdir( get_weather_data_path(cfg))
    mkdir( get_processed_weather_data_path(cfg))

if __name__ == "__main__":
    prepare_system()