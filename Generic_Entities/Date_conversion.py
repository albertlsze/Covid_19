import datetime
import numpy as np

def convert_covid_date(data):
    dates = []
    for year in data:
        day = year % 100
        year = year // 100
        month = year % 100
        year = year // 100

        dates.append(datetime.datetime(year, month, day).date())
    return dates