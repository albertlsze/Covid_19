import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

def AddLog(connection, data):
    primary_key = set(['date','state'])
    col_list = data.columns
    sql_table = 'us_state_covid'

    for index,row in data.iterrows():
        state = row['state']
        connection.Query_Replace(row, 'census_us_national', ['ABBREVIATION'], ['state'], 'state')

        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ', row['date'],' - ', state)

            connection.repeatcommand(col_list, sql_table, primary_key, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False