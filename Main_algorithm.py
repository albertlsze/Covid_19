import pandas as pd
import numpy as np
import os

import Generic_Entities.read_write_txt_file as rwtxt
import Generic_Entities.Excel_read_write as excel_rw
import Generic_Entities.Date_conversion as date_converter

from SQL_Connector.Data_Manager import data_manager
import SQL_Connector.census_us_national as census_us
import SQL_Connector.census_us_county as census_county
import SQL_Connector.HILFD_hospital as hospital_sql
import SQL_Connector.us_covid as us_covid
import SQL_Connector.us_state_covid as us_state_covid
import SQL_Connector.us_county_covid as us_county_covid
import datetime

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
'''# National Census data
print('National Census')
import_data = os.path.join(data_folder, 'US_Census_Data\\nst-est2019-alldata.xlsx')
import_data = excel_rw.Open_Excel(import_data)
import_data = import_data['nst-est2019-alldata']
import_data = import_data.where(pd.notnull(import_data), None)

census_us.AddState(database_manager,import_data)
'''
'''------------------------------------------Add to County Census----------------------------------------------------'''
'''
# County Census data
print('County Census')
import_data = os.path.join(data_folder, 'US_Census_Data\\co-est2019-alldata_update.xlsx')
import_data = excel_rw.Open_Excel(import_data)
import_data = import_data['co-est2019-alldata']
import_data = import_data.where(pd.notnull(import_data), None)

COUNTY_ID = []
for index,row in import_data.iterrows():
    COUNTY_ID.append(str(row['STATE'])+'.'+str(row['COUNTY']))
import_data['COUNTY_ID'] = COUNTY_ID

census_county.AddCounty(database_manager, import_data)
'''
'''------------------------------------------Add to Hospital Data----------------------------------------------------'''
'''
print('Hospital')
import_data = os.path.join(data_folder, 'HIFLD\\Hospitals.csv')
import_data = pd.read_csv(import_data)
import_data = import_data.where(pd.notnull(import_data), None)
import_data = import_data.replace('NOT AVAILABLE','')
import_data = import_data[(import_data['COUNTRY']=='USA') | (import_data['COUNTRY']=='PRI')]

import_data['SOURCEDATE'] = pd.to_datetime(import_data['SOURCEDATE']).dt.date
import_data['VAL_DATE'] = pd.to_datetime(import_data['VAL_DATE']).dt.date

import_data = import_data.replace(np.datetime64('NaT'), None)

hospital_sql.AddHospital(database_manager, import_data)
'''

'''------------------------------------------Add to us_covid19_daily Data----------------------------------------------------'''
'''
#set timer up
print('us covid')
import_data = os.path.join(data_folder, 'us_covid19_daily.csv')
import_data = pd.read_csv(import_data)
import_data = import_data.where(pd.notnull(import_data), None)

import_data['date'] = date_converter.convert_covid_date(import_data['date'].values)
import_data['dateChecked'] = pd.to_datetime(import_data['dateChecked'])
import_data['dateChecked'] = import_data['dateChecked'].dt.date
import_data = import_data.replace(np.datetime64('NaT'), None)

us_covid.AddLog(database_manager, import_data)
'''
'''------------------------------------------Add to us_state_covid19 Data----------------------------------------------------'''
'''
#set timer up
print('us state covid')
import_data = os.path.join(data_folder, 'us_states_covid19_daily.csv')
import_data = pd.read_csv(import_data)
import_data = import_data.where(pd.notnull(import_data), None)

import_data['date'] = date_converter.convert_covid_date(import_data['date'].values)
import_data['dateChecked'] = pd.to_datetime(import_data['dateChecked'])
import_data['dateChecked'] = import_data['dateChecked'].dt.date
import_data['lastUpdateEt'] = pd.to_datetime(import_data['lastUpdateEt'])
import_data['lastUpdateEt'] = import_data['lastUpdateEt'].dt.date
import_data = import_data.replace(np.datetime64('NaT'), None)

us_state_covid.AddLog(database_manager, import_data)
'''
'''------------------------------------------Add to us_state_covid19 Data----------------------------------------------------'''
#set timer up
print('us county covid')
import_data = os.path.join(data_folder, 'us_counties_covid19_daily.csv')
import_data = pd.read_csv(import_data)
import_data = import_data.where(pd.notnull(import_data), None)

import_data['date'] = pd.to_datetime(import_data['date'])
import_data['date'] = import_data['date'].dt.date
import_data = import_data.replace(np.datetime64('NaT'), None)
import_data = import_data.replace('New York City', 'New York')
import_data = import_data.replace('Kansas City', 'Jackson')
import_data = import_data[import_data['county']!='Unknown']

counties = import_data['county'].values
states =  import_data['state'].values
for i in range(0,len(counties)):

    if 'City and Borough' in counties[i]:
        counties[i] = counties[i].replace(' City and Borough', '')
    elif 'Borough' in counties[i]:
        counties[i] = counties[i].replace(' Borough','')
    elif 'Census Area' in counties[i]:
        counties[i] = counties[i].replace(' Census Area', '')
    elif 'LaSalle' in counties[i] and 'Louis' in states[i]:
        counties[i] = 'La Salle'
import_data['county']=counties


us_county_covid.AddLog(database_manager, import_data)




print('\nCOMPLETED')
database_manager.cursor.close()
#database_manager = login()

