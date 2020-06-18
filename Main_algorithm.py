import pandas as pd
import numpy as np
import os

import Generic_Entities.read_write_txt_file as rwtxt
import Generic_Entities.Excel_read_write as excel_rw

from SQL_Connector.Data_Manager import data_manager
import SQL_Connector.census_us_national as census_us
import SQL_Connector.census_us_county as census_county
import SQL_Connector.HILFD_hospital as hospital_sql

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
data_folder = 'C:\\Users\\Albert Sze\\Google Drive\\Coding_Projects\\Covid_19\\Data'
login_info = rwtxt.read_file('MySQL_Desktop_Login.txt')
for i in login_info:
    exec(i)

database_manager = data_manager(host,port = port,username=username,password=password)

'''-----------------------------------------Add to National Census---------------------------------------------------'''
# National Census data
import_data = os.path.join(data_folder, 'US_Census_Data\\nst-est2019-alldata.xlsx')
import_data = excel_rw.Open_Excel(import_data)
import_data = import_data['nst-est2019-alldata']
import_data = import_data.where(pd.notnull(import_data), None)

#census_us.AddState(database_manager,import_data)
'''------------------------------------------Add to County Census----------------------------------------------------'''
# County Census data
import_data = os.path.join(data_folder, 'US_Census_Data\\co-est2019-alldata.xlsx')
import_data = excel_rw.Open_Excel(import_data)
import_data = import_data['co-est2019-alldata']
import_data = import_data.where(pd.notnull(import_data), None)

import_data['COUNTY_ID'] = import_data['COUNTY_ID'].astype(str)
for index,value in enumerate(import_data['COUNTY_ID'].values):
    dec_loc = value.find('.')
    after_dec = len(value[dec_loc+1:])
    if after_dec > 3:
        import_data['COUNTY_ID'][index] = value[:dec_loc + 4]
#census_county.AddCounty(database_manager, import_data)

'''------------------------------------------Add to Hospital Data----------------------------------------------------'''
import_data = os.path.join(data_folder, 'HIFLD\\Hospitals.csv')
import_data = pd.read_csv(import_data)
import_data = import_data.where(pd.notnull(import_data), None)

database_manager.cursor.execute('SELECT * FROM us_hospital WHERE id = (%s)',(5793230,))

#hospital_sql.AddHospital(database_manager, import_data)
#hospital_sql.check(database_manager,hospital)


#database_manager = login()

