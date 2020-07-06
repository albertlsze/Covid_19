import pandas as pd
import numpy as np
import os

import Generic_Entities.read_write_txt_file as rwtxt
import Generic_Entities.Excel_read_write as excel_rw
import Generic_Entities.Date_conversion as date_converter
import Generic_Entities.Pickle_Functions as pickle

from SQL_Connector_Entities.Data_Manager import data_manager
import SQL_Connector_Entities.census_us_national as census_us
import SQL_Connector_Entities.census_us_county as census_county
import SQL_Connector_Entities.HILFD_hospital as hospital_sql
import SQL_Connector_Entities.us_covid as us_covid
import SQL_Connector_Entities.us_state_covid as us_state_covid
import SQL_Connector_Entities.us_county_covid as us_county_covid
import datetime

def Update_database(data_folder, database_manager, national_census=False, county_census = False, hospital = False, us_covid_update = False, us_state_update = False, us_county_update = False):
    '''------------------------------------------------ Logistics -------------------------------------------------------'''
    log_date = pickle.load('log_date.pkl')
    log_date['update_log'] = datetime.datetime.today().strftime('%Y-%m-%d')
    database_structure = os.path.join(data_folder,'Database_table_set.xlsx')
    database_structure = excel_rw.Open_Excel(database_structure)

    '''-----------------------------------------Add to National Census---------------------------------------------------'''
    if national_census:
        print('updating: National Census')
        import_data = os.path.join(data_folder, 'US_Census_Data\\nst-est2019-alldata.xlsx')
        import_data = excel_rw.Open_Excel(import_data)
        import_data = import_data['nst-est2019-alldata']
        import_data = import_data.where(pd.notnull(import_data), None)
        log_date['census_national'] = datetime.datetime.today().strftime('%Y-%m-%d')

        census_us.AddState(database_manager, import_data)
        pickle.save(log_date, 'log_date.pkl')
    '''------------------------------------------Add to County Census----------------------------------------------------'''
    if county_census:
        print('updating: County Census')
        import_data = os.path.join(data_folder, 'US_Census_Data\\co-est2019-alldata.xlsx')
        import_data = excel_rw.Open_Excel(import_data)
        import_data = import_data['co-est2019-alldata']
        import_data = import_data.where(pd.notnull(import_data), None)

        COUNTY_ID = []
        for index, row in import_data.iterrows():
            COUNTY_ID.append(str(row['STATE']) + '.' + str(row['COUNTY']))
        import_data['COUNTY_ID'] = COUNTY_ID
        log_date['census_county'] = datetime.datetime.today().strftime('%Y-%m-%d')

        census_county.AddCounty(database_manager, import_data)
        pickle.save(log_date, 'log_date.pkl')
    '''------------------------------------------Add to Hospital Data----------------------------------------------------'''
    if hospital:
        print('updating: Hospital')
        import_data = os.path.join(data_folder, 'HIFLD\\Hospitals.csv')
        import_data = pd.read_csv(import_data)
        import_data = import_data.where(pd.notnull(import_data), None)
        import_data = import_data.replace('NOT AVAILABLE', '')
        import_data = import_data[(import_data['COUNTRY'] == 'USA') | (import_data['COUNTRY'] == 'PRI')]

        import_data['SOURCEDATE'] = pd.to_datetime(import_data['SOURCEDATE']).dt.date
        import_data['VAL_DATE'] = pd.to_datetime(import_data['VAL_DATE']).dt.date

        import_data = import_data.replace(np.datetime64('NaT'), None)
        log_date['hospital'] = datetime.datetime.today().strftime('%Y-%m-%d')

        hospital_sql.AddHospital(database_manager, import_data)
        pickle.save(log_date, 'log_date.pkl')
    '''------------------------------------------Add to us_covid19_daily Data----------------------------------------------------'''
    if us_covid_update:
        print('updating: us covid')
        import_data = os.path.join(data_folder, 'us_covid19_daily.csv')
        import_data = pd.read_csv(import_data)
        import_data = import_data.where(pd.notnull(import_data), None)

        import_data['date'] = date_converter.convert_covid_date(import_data['date'].values)
        import_data['dateChecked'] = pd.to_datetime(import_data['dateChecked'])
        import_data['dateChecked'] = import_data['dateChecked'].dt.date
        import_data = import_data.replace(np.datetime64('NaT'), None)

        import_data = import_data[import_data['date']>log_date['us_covid']]

        log_date['us_covid'] = import_data['date'].max()

        us_covid.AddLog(database_manager, import_data,database_structure['us_covid'])
        pickle.save(log_date, 'log_date.pkl')
    '''------------------------------------------Add to us_state_covid19 Data----------------------------------------------------'''
    if us_state_update:
        print('updating: us state covid')
        import_data = os.path.join(data_folder, 'us_states_covid19_daily.csv')
        import_data = pd.read_csv(import_data)
        import_data = import_data.where(pd.notnull(import_data), None)

        import_data['date'] = date_converter.convert_covid_date(import_data['date'].values)
        import_data['dateChecked'] = pd.to_datetime(import_data['dateChecked'])
        import_data['dateChecked'] = import_data['dateChecked'].dt.date
        import_data['lastUpdateEt'] = pd.to_datetime(import_data['lastUpdateEt'])
        import_data['lastUpdateEt'] = import_data['lastUpdateEt'].dt.date
        import_data = import_data.replace(np.datetime64('NaT'), None)

        import_data = import_data[import_data['date'] > log_date['us_state_covid']]
        log_date['us_state_covid'] = import_data['date'].max()

        us_state_covid.AddLog(database_manager, import_data,database_structure['us_state_covid'])
        pickle.save(log_date, 'log_date.pkl')
    '''------------------------------------------Add to us_state_covid19 Data----------------------------------------------------'''
    if us_county_update:
        print('updating: us county covid')
        import_data = os.path.join(data_folder, 'us_counties_covid19_daily.csv')
        import_data = pd.read_csv(import_data)
        import_data = import_data.where(pd.notnull(import_data), None)

        import_data['date'] = pd.to_datetime(import_data['date'])
        import_data['date'] = import_data['date'].dt.date
        import_data = import_data.replace(np.datetime64('NaT'), None)
        import_data = import_data.replace('New York City', 'New York')
        import_data = import_data.replace('Kansas City', 'Jackson')
        import_data = import_data.replace('Lake and Peninsula Borough','Lake Peninsula')
        import_data = import_data[import_data['county'] != 'Unknown']

        counties = import_data['county'].values
        states = import_data['state'].values
        for i in range(0, len(counties)):

            if 'City and Borough' in counties[i]:
                counties[i] = counties[i].replace(' City and Borough', '')
            elif 'Borough' in counties[i]:
                counties[i] = counties[i].replace(' Borough', '')
            elif 'Census Area' in counties[i]:
                counties[i] = counties[i].replace(' Census Area', '')
            elif 'LaSalle' in counties[i] and 'Louis' in states[i]:
                counties[i] = 'La Salle'
        import_data['county'] = counties

        import_data = import_data[import_data['date'] > log_date['us_county_covid']]
        log_date['us_county_covid'] = import_data['date'].max()

        us_county_covid.AddLog(database_manager, import_data,database_structure['us_county_covid'])
        pickle.save(log_date,'log_date.pkl')

    print('Updated complete \n')
