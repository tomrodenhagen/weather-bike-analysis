new_york_latitude = 40.730610
new_york_longitude = -73.935242

base_weather_query = """SELECT
              date,
              id,
              MAX(prcp) AS prcp,
              MAX(tmin) AS tmin,
              MAX(tmax) AS tmax
            FROM (
              SELECT
                wx.date,
                wx.id,
                IF (wx.element = 'PRCP', wx.value/10, NULL) AS prcp,
                IF (wx.element = 'TMIN', wx.value/10, NULL) AS tmin,
                IF (wx.element = 'TMAX', wx.value/10, NULL) AS tmax,
                IF (SUBSTR(wx.element, 0, 2) = 'WT', 'True', NULL) AS haswx
              FROM
                bigquery-public-data.ghcn_d.ghcnd_{year} AS wx
              WHERE
                id IN {stations}
                AND qflag IS NULL )
            GROUP BY
              date,
              id
            ORDER BY
              date"""
