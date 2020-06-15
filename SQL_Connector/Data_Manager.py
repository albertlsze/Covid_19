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

            self.cursor = self.connection.cursor()

            if self.connection.is_connected():
                print('Connected to '+database+' database')

        except Error as e:
            print(e)

    '''---------------------------------------------Execute SQL code-------------------------------------------------'''
    def AddUpdateRecord(self, command, value = None):
        self.cursor.execute(command,value)
        self.connection.commit()

    '''------------------------------------------------Route Table---------------------------------------------------'''
    def AddRoute(self,route):
        sql_code = "SELECT * FROM routes WHERE route = %s"
        self.cursor.execute(sql_code,(route,))
        if self.cursor.fetchone():
            print(route + ' Already exist')
        else:
            sql_code = "INSERT INTO routes (route) VALUES (%s)"
            self.AddUpdateRecord(sql_code, value = (route,))

    def QueryRoute(self,route):
        sql_code = "SELECT * FROM routes WHERE route = %s"
        self.cursor.execute(sql_code, (route,))
        val = self.cursor.fetchone()

        if val:
            return val
        else:
            return None

    '''-----------------------------------------------Weather Table--------------------------------------------------'''
    def AddWeatherLog(self,date,battery,temp,humid,wind,wind_dir,solar):
        sql_code = "SELECT * FROM weather WHERE date = %s"
        self.cursor.execute(sql_code, (date,))
        if self.cursor.fetchone():
            print('data already exists for ' + str(date))
        else:
            sql_code = "INSERT INTO weather (date,battery_volt_avg,temp_avg,rela_humid_avg,wind_speed_daily_avg,wind_direct_avg,solare_rad_avg) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            self.AddUpdateRecord(sql_code, value=(date, battery, temp, humid, wind, wind_dir, solar))

    def QueryWeather(self,date):
        sql_code = "SELECT * FROM weather WHERE date = %s"
        self.cursor.execute(sql_code, (date,))
        val = self.cursor.fetchone()

        if val:
            return val
        else:
            return None

    '''------------------------------------------------Climbing Table------------------------------------------------'''
    def AddClimbLog(self, date,route,weather,attempted,succeeded,success_pct):
        sql_code = "SELECT * FROM climbing_stat_log WHERE date = %s AND route = %s"
        self.cursor.execute(sql_code, (date,route))

        if self.cursor.fetchone():
            sql_code = "SELECT * FROM routes WHERE id = %s"
            self.cursor.execute(sql_code, (route,))
            val = self.cursor.fetchone()
            if not self.continue_prev_command[0]:
                print('Log already exists: ' + str(date)+ ', ' + val[1])

            # Ask for command
            answer = None
            while not answer and not self.continue_prev_command[0]:
                answer = input("\t Would you like to Replace, Update, or Nothing to the record (R/U/N): ")
                if answer.upper() != 'R' and answer.upper() != 'U' and answer.upper() != 'N':
                    answer = None
                else:
                    self.continue_prev_command[1] = answer

            while not self.continue_prev_command[0]:
                self.continue_prev_command[0] = input("\t Would you like to do same for the rest (y/n): ")
                if self.continue_prev_command[0].lower() == 'y':
                    self.continue_prev_command[0] = True
                else:
                    self.continue_prev_command[0] = False
                    if answer.lower != 'n':
                        break

            if self.continue_prev_command[1].upper() != 'N':
                self.UpdateClimblog(date,route,weather=weather,attempted=attempted,succeeded=succeeded,command=self.continue_prev_command[1].upper())
        else:
            sql_code = "INSERT INTO climbing_stat_log (date, route, weather, attempted, succeeded, success_pct) VALUES (%s, %s, %s, %s, %s, %s)"
            self.AddUpdateRecord(sql_code, value=(date, route, weather, attempted, succeeded, success_pct))

    def UpdateClimblog(self,date,route,weather=None,attempted=None,succeeded=None,command = 'R'):
        sql_code = "SELECT * FROM climbing_stat_log WHERE date = %s AND route = %s"
        self.cursor.execute(sql_code, (date,route))
        val = self.cursor.fetchone()

        if weather:
            sql_code = "UPDATE climbing_stat_log SET weather = %s WHERE date = %s AND route = %s"
            self.AddUpdateRecord(sql_code, value = (weather,date,route))

        if attempted:
            if command == 'U':
                attempted += val[4]
            sql_code = "UPDATE climbing_stat_log SET attempted = %s WHERE date = %s AND route = %s"
            self.AddUpdateRecord(sql_code, value=(attempted, date, route))

        if succeeded:
            if command == 'U':
                succeeded += val[5]
            sql_code = "UPDATE climbing_stat_log SET succeeded = %s WHERE date = %s AND route = %s"
            self.AddUpdateRecord(sql_code, value=(succeeded, date, route))

        if attempted or succeeded:
            if not attempted:
                attempted = val[4]

            if not succeeded:
                succeeded = val[5]

            sql_code = "UPDATE climbing_stat_log SET success_pct = %s WHERE date = %s AND route = %s"
            self.AddUpdateRecord(sql_code, value=(succeeded/attempted, date, route))

        self.connection.commit()