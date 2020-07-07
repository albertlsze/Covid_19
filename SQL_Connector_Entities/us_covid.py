import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error
import Update_Database_Entities.Filter_list as filter_list

def AddLog(connection, data,structure):
    primary_key = set(['date'])
    sql_table = 'us_covid'

    col_list = filter_list.filter_list(structure,data.columns.values)
    data = data.filter(col_list)

    for index,row in data.iterrows():
        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ', row['date'])

            connection.repeatcommand(col_list, sql_table, primary_key, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False