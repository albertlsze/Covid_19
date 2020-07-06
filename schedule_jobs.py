import os
import schedule
import time

from Update_Database_Entities import Update_database

import Generic_Entities.read_write_txt_file as rwtxt

from SQL_Connector_Entities.Data_Manager import data_manager
import Generic_Entities.Pickle_Functions as pickle

def schedule_jobs():
    '''-----------------------------------------------User input-----------------------------------------------------'''
    data_folder = 'C:\\Users\\Albert Sze\\Google Drive\\Coding_Projects\\Covid_19\\Data'
    login_info = pickle.load('mysql_login.pkl')

    '''------------------------------------------Connect to Database-------------------------------------------------'''
    database_manager = data_manager(login_info['host'], port=login_info['port'], username=login_info['username'], password=login_info['password'])

    '''------------------------------------------Schedules-------------------------------------------------'''
    # update covid database
    schedule.every().day.at("9:07").do(Update_database.Update_database,data_folder, database_manager, us_covid_update = True, us_state_update = True, us_county_update = True)

if __name__ == '__main__':
    '''-----------------------------------------------run schedule---------------------------------------------------'''
    schedule_jobs()
    while True:
        schedule.run_pending()
        time.sleep(1)