How to run: 
1. Install requirements.txt to Python (3.9) 
2. Set in scripts/config.yaml 
    1. `base_path` to an existing directory
    2. `google_cloud_key_path` to an Big-Query enabled GCP-Service-Account Credential-Json 
    3. cd to repo and run `python scripts/run_pipeline.py`

Look at results for some insights about the merged data, if you dont want to run yourself.
For merging the data the main steps are:
1. Extract weather stations and ride station locations
2. Find for each ride station close enough weather stations
3. Add for every date and for every close weather station the weather features
4. For every ride station, aggregate (average) over the weather features of all matched stations
5. Now we have a weather lookup for every day and every station and can easily join

The reason for using multiple weather stations is, that there is  a lot of missing data
for single stations (you can see in the data_logger.json in results folder)