from weather_bike_analyis.file_helpers import get_bucket_data_path, unzip_and_delete
from omegaconf import DictConfig
import hydra
import os
import wget

@hydra.main(config_path=".", config_name="config")
def download_data(cfg: DictConfig) -> None:
    bucket_url = "https://s3.amazonaws.com/tripdata"
    #This should be autoextracted
    filenames = ["201307-citibike-tripdata.zip", "201308-citibike-tripdata.zip"]
    for filename in filenames:
        url_complete = f"{bucket_url}/{filename}"

        bucket_data_path = get_bucket_data_path(cfg)
        dest = os.path.join(bucket_data_path, filename)
        _ = wget.download(url_complete,
                                    out= dest
                                  )
        unzip_and_delete(bucket_data_path, filename)




if __name__ == "__main__":
    download_data()