import mysql.connector
import mysql
from termcolor import colored
from getpass import getpass
from datetime import datetime
from mysql.connector import Error

class data_manager():
    '''-------------------------------------------Initiate Database Access-------------------------------------------'''
    def __init__(self,host,port=3306,username=None,password = None):
        self.connection = None
        self.continue_prev_command = [False,None]

        if not username:
            username = input("Username: ")

        if not password:
            password = input("Password: ")

        #password = getpass("Password: ")
        database = "covid_usa"

        try:
            self.connection = mysql.connector.connect(host = host,
                                                      port = port,
                                                      user = username,
                                                      passwd = password,
                                                      use_pure = True,
                                                      database = database
                                                      )

            self.cursor = self.connection.cursor(buffered=True)
            #self.connection.cursor(bu)
            #self.buffer = self.cursor(buffered=True)

            if self.connection.is_connected():
                print("Connected to "+database+" database")

        except Error as e:
            print(e)

    '''---------------------------------------------Execute SQL code-------------------------------------------------'''
    def AddUpdateRecord(self, command, value = None,commit=True):
        self.cursor.execute(command,value)
        if commit:
            self.connection.commit()

        '''--------------------------------------Query/Delete Entry Table--------------------------------------------'''

    def SQLQueryDeleteEntry(self,sql_table, entry, data,command = False,col = None):
        # 0 is Query and 1 = Delete
        values = "("
        if command:
            sql_code = "DELETE"
        else:
            if not col:
                sql_code = "SELECT *"
            else:
                sql_code = "SELECT " + col
        sql_code += " FROM " + sql_table + " WHERE "

        for col_name in entry:
            if sql_code[-1] != " ":
                sql_code += " AND "

            sql_code += col_name.lower() + " = (%s)"
            values += "data['" + col_name + "'],"
        values += ")"

        values = eval(values)

        if command:
            commit = True
        else:
            commit = False

        self.AddUpdateRecord(sql_code, value=values,commit=commit)
        return None

    def SQLDeleteEntry2(self, sql_table, entry, base_table, base_entry, not_in=True):
        sql_code = "DELETE FROM " + sql_table + " WHERE ("
        for col_name in entry:
            if sql_code[-1] != "(":
                sql_code += ", "
            sql_code += col_name

        if not_in:
            sql_code += ") NOT IN (SELECT "
        else:
            sql_code += ") IN (SELECT "

        for col_name in base_entry:
            if sql_code[-1] != " ":
                sql_code += ", "
            sql_code += col_name
        sql_code += " FROM " + base_table + ")"
        self.AddUpdateRecord(sql_code)

        return None

    '''-----------------------------------------Insert Entry Table---------------------------------------------------'''
    def SQLInsertEntry(self, col_list, sql_table, data):
        sql_command = "INSERT INTO " + sql_table + " ("
        sql_command_2 = "VALUES ("
        values = "("

        for col_name in col_list:
            if data[col_name] != None:
                if sql_command[-1] != "(":
                    sql_command += ", "
                    sql_command_2 += ", "
                sql_command += col_name
                sql_command_2 += "%s"

                values += "data['" + col_name + "'],"

        if "," in sql_command:
            sql_command = sql_command + ") " + sql_command_2 + ")"
            values += ")"
            values = eval(values)
            self.AddUpdateRecord(sql_command, value=values)

        return None

    '''------------------------------------------Update Entry Table--------------------------------------------------'''

    def SQLUpdateEntry(self, col_list, sql_table, primary_key, data):
        sql_primary_key = " WHERE "
        primary_values = ""

        for pk in primary_key:

            if len(sql_primary_key) > 7:
                sql_primary_key += " AND "

            sql_primary_key += pk + " = %s"
            if type(data[pk]) is str:
                primary_values += ',"' + data[pk] + '"'
            else:
                primary_values += "," + str(data[pk])
        primary_values += ")"

        for col_name in col_list:
            if col_name not in primary_key:
                sql_code = "UPDATE " + sql_table + " SET " + col_name + " = %s"
                values = "(data['" + col_name + "']" + primary_values

                sql_code += sql_primary_key
                values = eval(values)
                self.AddUpdateRecord(sql_code, value=values)
        return None

    '''------------------------------------------Repeat command------------------------------------------------------'''
    def repeatcommand(self, col_list, sql_table, primary_key, data):
        # Ask for command
        answer = None
        while not answer and not self.continue_prev_command[0]:
            answer = input("\t Would you like to Update, or Nothing to the record (R/U/N): ")
            if answer.upper() != "R" and answer.upper() != "U" and answer.upper() != "N":
                answer = None
            else:
                self.continue_prev_command[1] = answer

        while not self.continue_prev_command[0]:
            answer = input("\t Would you like to do same for the rest (y/n): ")
            if answer.lower() == "y":
                self.continue_prev_command[0] = True
            else:
                self.continue_prev_command[0] = False
                if answer.lower() == "n":
                    break

        if self.continue_prev_command[1].upper() == 'U':
            self.SQLUpdateEntry(col_list, sql_table, primary_key, data)
        elif self.continue_prev_command[1].upper() == 'R':
            self.SQLQueryDeleteEntry(sql_table, primary_key, data, command=1)
            self.SQLInsertEntry(col_list, sql_table, data)


    '''-------------------------------Query and Replace command------------------------------------------------------'''
    def Query_Replace(self, data, sql_table, entry, col_name, search, cap = None):
        temp = {}
        if not cap:
            cap = [0]*len(entry)

        for i in range(0, len(entry)):
            if type(data[col_name[i]]) is str:
                if cap == 1:
                    str_list = data[col_name[i]].lower()
                    str_list = str_list.split(' ')

                    county = ''
                    for word in str_list:
                        if county:
                            county += ' '
                        county += word[0].upper() + word[1:]

                    temp[entry[i]] = county
                elif cap == 2:
                    temp[entry[i]] = data[col_name[i]].lower()
                else:
                    temp[entry[i]] = data[col_name[i]]
            else:
                temp[entry[i]] = data[col_name[i]]
        self.SQLQueryDeleteEntry(sql_table, entry, temp, command=0, col=search)
        val = self.cursor.fetchone()
        if val:
            data[col_name[i]] = val[0]
        else:
            print('Missing ' + col_name[i] + ': ', data[col_name[i]])
            data[col_name[i]] = None