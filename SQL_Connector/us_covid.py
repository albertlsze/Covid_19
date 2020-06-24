import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

def AddLog(connection, data):
    primary_key = set(['date'])
    col_list = data.columns
    sql_table = 'us_covid'

    for index,row in data.iterrows():
        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ', row['date'])

            connection.repeatcommand()

            if connection.continue_prev_command[1].upper() == 'U':
                connection.SQLUpdateEntry(col_list, sql_table, primary_key, row)
            elif connection.continue_prev_command[1].upper() == 'R':
                connection.SQLQueryDeleteEntry(sql_table, primary_key, row,command = 1)
                connection.SQLInsertEntry(col_list, sql_table, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False