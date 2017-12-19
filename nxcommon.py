# 05 Oct 2017 | Common Functionality
# -| Returns API Key(s)
# -| Oracle Functions

import json
import cx_Oracle
import os
from time import gmtime, strftime


class NXKey:

    def key_twitter(self):
        # Open KEY files
        with open('e:\\GitHub\\python\\keys\\twitter.key') as key_file:
            key = json.load(key_file)
        return key

    def key_zomato(self):
        # Open KEY files
        with open('e:\\GitHub\\python\\keys\\zomato.key') as key_file:
            key = json.load(key_file)
        return key

    def key_instagram(self):
        # Open KEY files
        with open('e:\\GitHub\\python\\keys\\instagram.key') as key_file:
            key = json.load(key_file)
        return key

    def key_facebook(self):
        # Open KEY files
        with open('e:\\GitHub\\python\\keys\\facebook.key') as key_file:
            key = json.load(key_file)
        return key


class NXOracle:

    def db_dos_start(self):
        os.system("C:\Windows\SysWOW64\cmd.exe  /k C:\oraclexe\app\oracle\product\11.2.0\server\bin\StartDB.bat")


    def db_start(self):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Startup Initiated")
        with open('e:\\GitHub\\python\\keys\\oracle_sysdba.key') as key_file_oracle:
            key_oracle = json.load(key_file_oracle)

        connection = cx_Oracle.connect(key_oracle[0]['USER'],
                                       key_oracle[0]['PASSWORD'],
                                       key_oracle[0]['CONNECT_STRING'],
                                       cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH)

        connection.startup()
        connection = cx_Oracle.connect(mode=cx_Oracle.SYSDBA)
        cursor = connection.cursor()
        cursor.execute("alter database mount")
        cursor.execute("alter database open")

        '''try:
        con_main = cx_Oracle.connect(key_oracle[0]['USER'],
                                       key_oracle[0]['PASSWORD'],
                                       key_oracle[0]['CONNECT_STRING'],
                                       cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH)
        except cx_Oracle.DatabaseError:
        sys.exit()
        try:
            con_main.startup()
        except cx_Oracle.DatabaseError:
            sys.exit()

        try:
            con_main = cx_Oracle.connect(username, password, '%s/%s' % (hostname, service), cx_Oracle.SYSDBA)
        except cx_Oracle.DatabaseError:
            sys.exit()

        cur_main = con_main.cursor()

        try:
            sql = "alter database mount"
            cur_main.execute(sql)
        except cx_Oracle.DatabaseError:
            sys.exit()

        try:
            sql = "alter database open"
            cur_main.execute(sql)
        except cx_Oracle.DatabaseError:
            sys.exit()'''

        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Startup Completed")

    def db_stop(self):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Shutdown Initiated")
        with open('e:\\GitHub\\python\\keys\\oracle_sysdba.key') as key_file_oracle:
            key_oracle = json.load(key_file_oracle)

        con_main = cx_Oracle.connect(key_oracle[0]['USER'],
                                       key_oracle[0]['PASSWORD'],
                                       key_oracle[0]['CONNECT_STRING'],
                                       cx_Oracle.SYSDBA)

        #connection = cx_Oracle.Connection(mode = cx_Oracle.SYSDBA)
        con_main.shutdown(mode=cx_Oracle.DBSHUTDOWN_IMMEDIATE)
        cursor = con_main.cursor()
        cursor.execute("alter database close normal")
        cursor.execute("alter database dismount")
        con_main.shutdown(mode=cx_Oracle.DBSHUTDOWN_FINAL)

        '''try:
            con_main.shutdown(cx_Oracle.DBSHUTDOWN_IMMEDIATE)
        except cx_Oracle.DatabaseError:
            pass

        cur_main = con_main.cursor()

        sqls = ["alter database close normal", "alter database dismount"]
        for sql in sqls:
            try:
                cur_main.execute(sql)
            except cx_Oracle.DatabaseError:
                sys.exit()

        try:
            con_main.shutdown(mode=cx_Oracle.DBSHUTDOWN_FINAL)
        except cx_Oracle.DatabaseError:
            sys.exit()'''

        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Shutdown Completed")

    def db_login(self):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Logon Initiated")

        with open('e:\\GitHub\\python\\keys\\oracle.key') as key_file_oracle:
            key_oracle = json.load(key_file_oracle)

        connection = cx_Oracle.connect(key_oracle[0]['USER'],
                                       key_oracle[0]['PASSWORD'],
                                       key_oracle[0]['CONNECT_STRING'])

        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Logon Completed")
        return connection
