from weather_bike_analyis.file_helpers import get_bucket_data_path
from omegaconf import DictConfig, OmegaConf
import hydra
import os,glob
import pandas as pd

def aggregate_csv_files(bucket_data_path):
    dfs = []
    for csv_path in glob.glob(f"{bucket_data_path}/*.csv"):
        df = pd.read_csv(csv_path)
        dfs.append(df)
    df_aggregated = pd.concat(dfs)
    return df_aggregated


@hydra.main(config_path=".", config_name="config")
def run_pipeline(cfg: DictConfig) -> None:
    bucket_data_path = get_bucket_data_path(cfg)
    data = aggregate_csv_files(bucket_data_path)
    print(data.shape)

if __name__ == "__main__":
    run_pipeline()