from omegaconf import DictConfig
import os, zipfile
from pathlib import Path

def unzip_and_delete(directory, filename):
    path = os.path.join(directory,filename)
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(directory)
        # Delete zip file
    os.remove(path)

def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)

def get_weather_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "weather_data")
def get_processed_weather_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "processed_weather_data")

def get_bucket_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "bucket_data")