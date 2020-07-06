from datetime import datetime, timedelta
from threading import Timer
from Update_Database_Entities import Update_database

import Generic_Entities.read_write_txt_file as rwtxt

from SQL_Connector_Entities.Data_Manager import data_manager

'''-------------------------------------------------User input-------------------------------------------------------'''
data_folder = 'C:\\Users\\Albert Sze\\Google Drive\\Coding_Projects\\Covid_19\\Data'
login_info = rwtxt.read_file('MySQL_Desktop_Login.txt')

'''-------------------------------------------------logistics--------------------------------------------------------'''
x=datetime.today()
y = x.replace(day=x.day, hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
#y = x.replace(day=x.day, hour=15, minute=0, second=0, microsecond=0) + timedelta(days=1)
delta_t=y-x

secs=delta_t.seconds+1

'''--------------------------------------------Connect to Database---------------------------------------------------'''
for i in login_info:
    exec(i)

database_manager = data_manager(host,port = port,username=username,password=password)

'''-----------------------------------------Add to National Census---------------------------------------------------'''
Update_database.Update_database(data_folder, database_manager, national_census=True, county_census = True, hospital = True, us_covid_update = True, us_state_update = True, us_county_update = True)

print('\nCOMPLETED')
database_manager.cursor.close()
#database_manager = login()

