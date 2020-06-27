import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

'''
def check(connection,data,sql_table,entry,col_name,search):
    temp = {}
    for i in range(0,len(entry)):
        if type(data[col_name[i]]) is str:
            temp[entry[i]] = '"' + data[col_name[i]][0]+data[col_name[i]][1:].lower() + '"'
        else:
            temp[entry[i]] = data[col_name[i]]
    connection.SQLQueryDeleteEntry(sql_table, entry,temp, command=0, col=search)
    val = connection.cursor.fetchone()
    if val:
        data[col_name[i]] = val[0]
    else:
        print('Missing Country: ',data[col_name[i]])
        data[col_name[i]] = None
'''

def AddHospital(connection, data):
    primary_key = set(['ID'])
    col_list = data.columns
    sql_table = 'us_hospital'

    for index,row in data.iterrows():
        connection.Query_Replace(row, 'census_us_national', ['ABBREVIATION'], ['STATE'], 'state')
        connection.Query_Replace(row, 'census_us_county', ['STATE','CTYNAME'], ['STATE','COUNTY'], 'county_id',cap=[0,1])

        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:

                print('Log already exists: ' + row['NAME'] + ' in ' + str(row['STATE']) +', ' + row['CITY'])

            connection.repeatcommand(col_list, sql_table, primary_key, row)
            '''
            connection.repeatcommand()

            if connection.continue_prev_command[1].upper() == 'U':
                connection.SQLUpdateEntry(col_list, sql_table, primary_key, row)
            elif connection.continue_prev_command[1].upper() == 'R':
                connection.SQLQueryDeleteEntry(sql_table, primary_key, row,command = 1)
                connection.SQLInsertEntry(col_list, sql_table, row)
            '''
        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False