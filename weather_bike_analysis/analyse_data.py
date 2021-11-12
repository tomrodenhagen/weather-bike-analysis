import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import OneHotEncoder


def analyse_per_station_and_day_data(df):
    # Hydra does some strange things with the cwd
    save_path = "../../../../results"
    # Plots
    tmax_per_day = df.groupby("date").mean()["tmax"]
    rides_per_day = df.groupby("date").sum()["n_rides"]
    plt.scatter(tmax_per_day, rides_per_day)
    plt.xlabel("Maximum temperature in celsius")
    plt.ylabel("Number of rides")
    plt.savefig(os.path.join(save_path, "n_rides_vs_tmax.png"))
    plt.clf()
    prcp_per_day = df.groupby("date").mean()["prcp"]
    plt.scatter(prcp_per_day, rides_per_day)
    plt.xlabel("precipitation in mm")
    plt.ylabel("Number of rides")
    plt.savefig(os.path.join(save_path, "n_rides_vs_precipitation.png"))
    plt.clf()
    # Build some features
    df["is_weekday"] = pd.to_datetime(df["date"]).dt.dayofweek < 5
    df["precipitation"] = df["prcp"] != 0.0
    # This should be implemented sparse with more ride stations
    stations_one_hot = OneHotEncoder(sparse=False).fit_transform(
        df.ride_station_id.values.reshape(-1, 1)
    )
    features = pd.concat(
        [df.loc[:, ["is_weekday", "precipitation"]], pd.DataFrame(stations_one_hot)],
        axis=1,
    )
    # Some regression
    model = PoissonRegressor()
    model.fit(features, df["n_rides"].values)
    named_coefs = pd.DataFrame.from_dict(
        dict(zip(features.columns, model.coef_)), orient="index", columns=["coef"]
    ).loc[["precipitation", "is_weekday"]]
    named_coefs["exp"] = np.exp(named_coefs.loc[:, "coef"])

    named_coefs.round(2).to_csv(os.path.join(save_path, "reg_coefs.csv"))
