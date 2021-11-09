from weather_bike_analyis.file_helpers import get_weather_data_path, unzip_and_delete, get_processed_weather_data_path
from omegaconf import DictConfig
import hydra
import pandas as pd
import wget, os, glob

def read_header(weather_data_path):
    file = open(os.path.join(weather_data_path, "HEADERS.txt"))
    return file.readlines()[1].split(" ")


def preprocess_weather_data(source, dest, columns):

    df = pd.read_csv(source, sep='\s{1,}',header=None)
    df.set_axis(columns, axis=1)

@hydra.main(config_path=".", config_name="config")
def download_and_preprocess_data(cfg):
    snapshot_url = "https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/snapshots/CRNH0203-202111080550.zip"
    directory = get_weather_data_path(cfg)
    filename = "snapshot.zip"
    dest = os.path.join(directory, filename )

    #Semi nice criteria to check if we already unzipped
    if cfg.fresh or not os.path.exists(os.path.join(directory, "README.txt")):
        wget.download(snapshot_url,
                      out=dest
                      )
        unzip_and_delete(directory, filename)
    columns = read_header(directory)
    for filename in glob.glob(directory):
        source = os.path.join(directory, filename)
        dest = os.path.join(get_processed_weather_data_path(cfg), filename)
        preprocess_weather_data(source, dest, columns)

if __name__ == "__main__":
    download_and_preprocess_data()
