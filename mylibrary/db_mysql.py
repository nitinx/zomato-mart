# 11 Mar 2018 | MySQL Client

import json
import logging
import MySQLdb
import os
from time import gmtime, strftime

base_dir = 'e:\\GitHub\\python\\keys\\'
log = logging.getLogger(__name__)

class MySQLClient:

    def db_dos_start(self):
        os.system("C:\Windows\SysWOW64\cmd.exe  /k C:\oraclexe\app\oracle\product\11.2.0\server\bin\StartDB.bat")


    def db_start(self):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Startup Initiated")
        with open(base_dir + 'oracle_sysdba.key') as key_file_oracle:
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

        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Startup Completed")

    def db_stop(self):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Shutdown Initiated")
        with open(base_dir + 'oracle_sysdba.key') as key_file_oracle:
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

        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [Oracle DB] Shutdown Completed")

    def db_login(self):
        #print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [MySQL DB] Logon Initiated")
        log.info("[Oracle DB] Logon Initiated")

        with open(base_dir + 'mysql_aws.key') as key_file_mysql:
            key_mysql = json.load(key_file_mysql)

        connection = MySQLdb.connect(key_mysql[0]['CONNECT_STRING'],
                                    key_mysql[0]['USER'],
                                    key_mysql[0]['PASSWORD'],
                                    db='mysqldb')

        #print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [MySQL DB] Logon Completed")
        log.info("[Oracle DB] Logon Completed")
        return connection
