# -*- coding: utf-8 -*-
"""
Created on Sat May 18 10:56:13 2019

@author: GAS01409
"""

import os
import pandas as pd
# from datetime import date
from timer import timer

# from datawarehouse.prod import db as dw_prod

FILEPATH = os.path.join('static', 'daily_weather.csv')


# sql = ('select min(observationdate) '#top 10 heatingdegreedays '
#       'from F_DailyWeatherHistory '
#       'where '#zip=? '
#       'observationdate between ? and ? '
#       )
#
# df=dw_prod.query(sql, date(2019, 1, 1), date(2019, 1, 1))

@timer
def read_weather_csv():
    df = pd.read_csv(FILEPATH, index_col=0)
    df.index = pd.to_datetime(df.index, format='%m/%d/%Y')
    return df
