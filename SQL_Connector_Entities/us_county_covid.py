import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error
import Update_Database_Entities.Filter_list as filter_list

def AddLog(connection, data,structure):
    primary_key = set(['date','county'])
    sql_table = 'us_county_covid'

    col_list = filter_list.filter_list(structure, data.columns.values)
    col_list.append('state')
    data = data.filter(col_list)

    for index,row in data.iterrows():
        state = row['state']
        county = row['county']

        connection.Query_Replace(row, 'census_us_county', ['STNAME','CTYNAME'], ['state','county'], 'county_id',cap=[0,1])
        row['state'] = None

        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)
        if not row['county']:
            print('skip: ',county,' ',state)
            continue

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ', row['date'],'\n\tState: ',state,'\n\tCounty: ',county,)

            connection.repeatcommand(col_list, sql_table, primary_key, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False