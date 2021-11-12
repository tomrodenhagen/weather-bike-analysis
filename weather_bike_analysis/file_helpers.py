from omegaconf import DictConfig
import os, zipfile
from pathlib import Path


def unzip(directory, filename, delete=False):
    path = os.path.join(directory, filename)
    with zipfile.ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(directory)
        # Delete zip file
    if delete:
        os.remove(path)


def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def get_weather_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "weather_data")


def get_bucket_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "bucket_data")


def get_temp_data_path(cfg: DictConfig) -> Path:
    return Path(os.path.join(cfg.base_path, "temp_data"))


def prepare_system(cfg):

    mkdir(cfg.base_path)
    mkdir(get_bucket_data_path(cfg))
    mkdir(get_weather_data_path(cfg))
    mkdir(get_temp_data_path(cfg))
