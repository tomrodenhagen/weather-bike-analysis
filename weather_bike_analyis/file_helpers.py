from omegaconf import DictConfig
import os
from pathlib import Path

def get_bucket_data_path(cfg: DictConfig) -> Path:
    return os.path.join(cfg.base_path, "bucket_data")