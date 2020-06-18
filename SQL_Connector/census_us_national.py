import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

def StateID(state):
    state = str(state)
    if 2 > len(state):
        state = '"00'+state+'"'
    elif 3 > len(state):
        state = '"0'+state+'"'
    else:
        state = '"' + state + '"'
    return state

def AddState(connection, data):
    primary_key = set(['STATE'])
    col_list = data.columns
    sql_table = 'census_us_national'

    for index,row in data.iterrows():
        state_id = {'STATE':StateID(row['STATE'])}
        connection.SQLQueryDeleteEntry(sql_table,primary_key, state_id,command = 0)

        if connection.cursor.fetchone():
            if not connection.continue_prev_command[0]:
                print('Log already exists: ' + row['NAME'])

            connection.repeatcommand()

            if connection.continue_prev_command[1].upper() == 'U':
                connection.SQLUpdateEntry(col_list, sql_table, primary_key, row)
            elif connection.continue_prev_command[1].upper() == 'R':
                connection.SQLQueryDeleteEntry(sql_table, primary_key, state_id,command = 1)
                connection.SQLInsertEntry(col_list, sql_table, row)

        else:
            connection.SQLInsertEntry(col_list, sql_table, row)

    connection.continue_prev_command[0] = False

def Query(stat_key):
    None