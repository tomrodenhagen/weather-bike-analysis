import pyspark.sql.functions as F


def count_missings(spark_df):
    """
    Counts number of nulls and nans in each column
    """
    df = spark_df.select(
        [
            F.count(F.when(F.isnan(c) | F.isnull(c), c)).alias(c + "_missing_values")
            for (c, c_type) in spark_df.dtypes
            if c_type != "date"
        ]
    ).toPandas()
    return df / spark_df.count()


def check_date_format(df):
    n_rows = df.count()
    invalid_months = (
        df.select(
            F.count(F.when(F.month(df.date) > 12, 0)).alias("invalid_month")
        ).toPandas()
        / n_rows
    )
    invalid_days = (
        df.select(
            F.count(F.when(F.dayofmonth(df.date) > 31, 0)).alias("invalid_days")
        ).toPandas()
        / n_rows
    )
    res = invalid_months.iloc[0].to_dict()
    res.update(invalid_days.iloc[0].to_dict())
    return res


def check_weather_data(weather_data):
    n_rows = weather_data.count()
    res = count_missings(weather_data).iloc[0].to_dict()
    prcp_neg = (
        weather_data.select(
            F.count(F.when(weather_data.prcp.cast("float") < 0, 0)).alias("neg_prec")
        ).toPandas()
        / n_rows
    )
    max_smaller_than_min = (
        weather_data.select(
            F.count(
                F.when(
                    weather_data.tmin.cast("float") > weather_data.tmax.cast("float"), 0
                )
            ).alias("tmax<tmin")
        ).toPandas()
        / n_rows
    )

    res.update(prcp_neg.iloc[0].to_dict())
    res.update(max_smaller_than_min.iloc[0].to_dict())
    res.update(check_date_format(weather_data))
    return res


def check_ride_data(ride_data):
    boolean_col = F.when(
        ride_data.tripduration != (ride_data.stoptime_ts - ride_data.starttime_ts), 0
    )
    duration_consistency = ride_data.select(
        F.avg(boolean_col).alias("Inconsistent tripduration")
    ).toPandas()
    res = duration_consistency.iloc[0].to_dict()
    res.update(check_date_format(ride_data))

    return res
