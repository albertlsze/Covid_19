import pandas as pd
import numpy as np
import os

from SQL_Connector.Data_Manager import data_manager

import Generic_Entities.read_write_txt_file as rwtxt

'''-------------------------------------------------User input-------------------------------------------------------'''
#input_file = 'C:\\Users\\Albert Sze\\Documents\\Github\\albertlsze\\Data_sets\\mount-rainier-weather-and-climbing-data'
#climbing_data = 'climbing_statistics.csv'
#weather = 'Rainier_Weather.csv'

'''-------------------------------------------------logistics--------------------------------------------------------'''
#climbing_data = pd.read_csv(os.path.join(input_file,climbing_data))
#weather = pd.read_csv(os.path.join(input_file,weather))
#climbing_data = convert_date(climbing_data, 'Date')
#weather = convert_date(weather, 'Date')

'''--------------------------------------------Connect to Database---------------------------------------------------'''

login_info = rwtxt.read_file('MySQL_Desktop_Login.txt')
for i in login_info:
    exec(i)

database_manager = data_manager(host,port = port,username=username,password=password)

#database_manager = login()

'''-------------------------------------------------Add routes-------------------------------------------------------'''
#route_table.add_routes(climbing_data['Route'], database_manager)

'''-----------------------------------------------Add Weather log----------------------------------------------------'''
#weather_table.add_weather(weather, database_manager)

'''-----------------------------------------------Add Climbing log---------------------------------------------------'''
#climbing_table.add_climbing_log(climbing_data,database_manager)