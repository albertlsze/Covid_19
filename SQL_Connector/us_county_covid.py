import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

def AddLog(connection, data):
    primary_key = set(['date','county'])
    col_list = data.columns
    sql_table = 'us_county_covid'

    for index,row in data.iterrows():
        state = row['state']
        county = row['county']

        connection.Query_Replace(row, 'census_us_county', ['STNAME','CTYNAME'], ['state','county'], 'county_id',cap=[0,1])
        row['state'] = None

        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)
        if not row['county']:
            print(county, state)
            #continue

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ', row['date'],'\n\tState: ',state,'\n\tCounty: ',county,)

            connection.repeatcommand(col_list, sql_table, primary_key, row)

            '''
            if connection.continue_prev_command[1].upper() == 'U':
                connection.SQLUpdateEntry(col_list, sql_table, primary_key, row)
            elif connection.continue_prev_command[1].upper() == 'R':
                connection.SQLQueryDeleteEntry(sql_table, primary_key, row,command = 1)
                connection.SQLInsertEntry(col_list, sql_table, row)
            '''

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False