import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error


def check(connection,data,sql_table,entry,col_name,search):
    temp = {entry: '"' + data[col_name] + '"'}
    connection.SQLQueryDeleteEntry(sql_table, [entry],temp, command=0, col=search)
    val = connection.cursor.fetchone()
    if val:
        data[col_name] = val[0]

def AddHospital(connection, data):
    primary_key = set(['ID'])
    col_list = data.columns
    sql_table = 'us_hospital'

    for index,row in data.iterrows():
        check(connection, row, 'census_us_national', 'ABBREVIATION', 'STATE', 'state')
        check(connection, row, 'census_us_county', 'CTYNAME', 'COUNTY', 'county_id')

        connection.SQLQueryDeleteEntry(sql_table,primary_key, row,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ' + row['NAME'] + ' in ' + row['STATE'] +', ' + row['CITY'])

            connection.repeatcommand()

            if connection.continue_prev_command[1].upper() == 'U':
                connection.SQLUpdateEntry(col_list, sql_table, primary_key, row)
            elif connection.continue_prev_command[1].upper() == 'R':
                connection.SQLQueryDeleteEntry(sql_table, primary_key, row,command = 1)
                connection.SQLInsertEntry(col_list, sql_table, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False