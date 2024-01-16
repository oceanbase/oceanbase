#!/usr/bin/python
import sys
import re
import os
import getopt
import mysql.connector
import argparse
from mysql.connector import errorcode

class SrsDataImporter:
    def get_args(self):
        parser = argparse.ArgumentParser(conflict_handler='resolve')
        parser.add_argument("-h", "--host", help="Connect to host", required=True)
        parser.add_argument("-P", "--port", help="Port number to use for connection", required=True)
        parser.add_argument("-p", "--password", default = '', help="Password of sys tenant")
        parser.add_argument("-f", "--file", help="The sql script for loading default spatial reference system data", required=True)
        parser.add_argument("-t", "--tenant", default='sys', help="Tenant for import data if not sys")
        args = parser.parse_args()
        self.host=args.host
        self.port=args.port
        self.pwd=args.password
        self.file_name=args.file
        self.tenant=args.tenant
        self.old_result_cnt = [0, 0]
        self.new_result_cnt = [0, 0]
        self.print_srs_sql = False
        print("host:{0} port:{1} pwd:{2} file:{3}".format(self.host, self.port, self.pwd, self.file_name))

    def generate_sql(self):
        self.sql_list = []
        with open(self.file_name) as f_read:
            for line in f_read:
                if not line.isspace():
                    self.sql_list.append(line)

    def connect_server(self):
        try:
            self.conn = mysql.connector.connect(user='root', password=self.pwd, host=self.host, port=self.port, database='mysql', connection_timeout=10)
        except mysql.connector.Error as err:
            print(err)
            exit("ERROR: failed to connect host")
        self.cur = self.conn.cursor(buffered=True)
        print("INFO: sucess to connect server {0}:{1}".format(self.host, self.port))
        try:
            sql = "select value from oceanbase.__all_sys_parameter where name = 'enable_upgrade_mode';"
            self.cur.execute(sql)
            print("INFO: execute sql -- {0}".format(sql))
            result = self.cur.fetchall()
            if 1 == len(result) and 1 == result[0][0]:
                self.upgrade_mode = True
            else:
                self.upgrade_mode = False
                sql = "select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(str(self.tenant))
                print("INFO: execute sql -- {0}".format(sql))
                self.cur.execute(sql)
                result = self.cur.fetchall()
                if 1 == len(result):
                    print("tenant_id = {0}".format(str(result[0][0])))
                    self.tenant_id = result[0][0]
                else:
                    print("multiple tenants with the same name, tenant_name: '{0}'".format(self.tenant))
            if False == self.upgrade_mode and self.tenant_id != 1:
                sql = "commit"
                self.cur.execute(sql)
                sql = "alter system change tenant '{0}'".format(str(self.tenant))
                print("INFO: execute sql -- {0}".format(sql))
                self.cur.execute(sql)
        except mysql.connector.Error as err:
            print("ERROR: " + sql)
            print(err)
            exit("ERROR: failed to import srs data")
    def execute_sql(self):
        try:
            for sql in self.sql_list:
                if self.print_srs_sql == True:
                    print("INFO: execute sql -- {0}".format(sql))
                self.cur.execute(sql)
        except mysql.connector.Error as err:
            print("ERROR: " + sql)
            print(err)
            self.conn.rollback()
            print("ERROR: failed to import srs data")
        else:
            print("INFO: succeed to import srs data")


    def execute_prepare_execute(self, table_name, idx):
        try:
            self.cur.execute("select count(*) from {0}".format(table_name))
            result = self.cur.fetchone()
            self.old_result_cnt[idx] = result[0]
            print("INFO: {0} old result rows -- {1}".format(table_name, self.old_result_cnt[idx]))
        except mysql.connector.Error as err:
            print(err)
            exit("ERROR: failed to import srs data")

    def prepare_execute(self):
        self.execute_prepare_execute("oceanbase.__all_spatial_reference_systems", 0)

    def execute_check_sql(self, table_name, idx):
        self.cur.execute("select count(*) from {0}".format(table_name))
        result = self.cur.fetchone()
        self.new_result_cnt[idx] = result[0]
        print("INFO: {0} old result rows -- {1}".format(table_name, self.old_result_cnt[idx]))
        print("INFO: {0} new result rows -- {1}".format(table_name, self.new_result_cnt[idx]))

    def check_result(self):
        self.execute_check_sql("oceanbase.__all_spatial_reference_systems", 0)

def main():
    srs_data_importer = SrsDataImporter()
    srs_data_importer.get_args()
    srs_data_importer.connect_server()
    if False == srs_data_importer.upgrade_mode:
        srs_data_importer.generate_sql()
        srs_data_importer.prepare_execute()
        srs_data_importer.execute_sql()
        srs_data_importer.check_result()
    else:
        print("ERROR: cannot import srs data when upgrade_mode is true")

if __name__ == "__main__":
    main()
