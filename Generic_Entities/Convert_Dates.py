import pandas as pd

def convert_date(data, date):
    data['Date'] = pd.to_datetime(data[date]).dt.date
    data = data.sort_values(by = date)
    data = data.reset_index(drop=True)
    return data