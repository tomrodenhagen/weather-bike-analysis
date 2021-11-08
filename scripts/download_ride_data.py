from weather_bike_analyis.file_helpers import get_bucket_data_path
from omegaconf import DictConfig
import hydra
import os
import wget, zipfile

@hydra.main(config_path=".", config_name="config")
def download_data(cfg: DictConfig) -> None:
    bucket_url = "https://s3.amazonaws.com/tripdata"
    #This should be autoextracted
    data_files = ["201307-citibike-tripdata.zip", "201308-citibike-tripdata.zip"]
    for data_file in data_files:
        url_complete = f"{bucket_url}/{data_file}"

        bucket_data_path = get_bucket_data_path(cfg)
        dest = os.path.join(bucket_data_path, data_file)
        _ = wget.download(url_complete,
                                    out= dest
                                  )
        with zipfile.ZipFile(dest, 'r') as zip_ref:
            zip_ref.extractall(bucket_data_path)
        #Delete zip file
        os.remove(dest)



if __name__ == "__main__":
    download_data()