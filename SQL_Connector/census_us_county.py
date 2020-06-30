import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

def AddCounty(connection, data):
    primary_key = set(['COUNTY_ID'])
    col_list = data.columns
    sql_table = 'census_us_county'

    for index,row in data.iterrows():
        connection.SQLQueryDeleteEntry(sql_table, primary_key, row, command=0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ' + row['STNAME'] + ', ' + row['CTYNAME'])

            connection.repeatcommand(col_list, sql_table, primary_key, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False
