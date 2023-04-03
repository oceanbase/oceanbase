#!/usr/bin/python
import sys
import re
import os
import getopt
import mysql.connector
import argparse
from mysql.connector import errorcode

class TimeZoneInfoImporter:
    def get_args(self):
        parser = argparse.ArgumentParser(conflict_handler='resolve')
        parser.add_argument("-h", "--host", help="Connect to host", required=True)
        parser.add_argument("-P", "--port", help="Port number to use for connection", required=True)
        parser.add_argument("-p", "--password", help="Password of sys tenant")
        parser.add_argument("-f", "--file", help="The script generate from MySQL mysql_tzinfo_to_sql", required=True)
        parser.add_argument("-t", "--tenant", help="Tenant for import data if not sys")
        args = parser.parse_args()
        self.host=args.host
        self.port=args.port
        self.pwd=args.password
        self.file_name=args.file
        self.tenant=args.tenant
        ##print "host:{0} port:{1} pwd:{2} file:{3}".format(host, port, pwd, file_name)

    def generate_sql(self):
        self.sql_list = []
        self.tz_version_sql_list = []
        self.expect_count = [0, 0, 0, 0]
        replace_str1 = 'TRUNCATE TABLE time_zone'
        replace_str2 = 'INTO time_zone'
        replace_str3 = 'ALTER TABLE time_zone_transition'
        replace_count_str0 = 'time_zone count:'
        replace_count_str1 = 'time_zone_name count:'
        replace_count_str2 = 'time_zone_transition count:'
        replace_count_str3 = 'time_zone_transition_type count:'
        with open(self.file_name) as f_read:
            sql = ""
            for line in f_read:
                if re.search('__all_sys_stat', line, re.IGNORECASE):
                    self.tz_version_sql_list.append(line)
                elif re.search('count:', line, re.IGNORECASE):
                    if re.search(replace_count_str3, line, re.IGNORECASE):
                        self.expect_count[3] = int(line.replace(replace_count_str3, ''))
                    elif re.search(replace_count_str2, line, re.IGNORECASE):
                        self.expect_count[2] = int(line.replace(replace_count_str2, ''))
                    elif re.search(replace_count_str1, line, re.IGNORECASE):
                        self.expect_count[1] = int(line.replace(replace_count_str1, ''))
                    elif re.search(replace_count_str0, line, re.IGNORECASE):
                        self.expect_count[0] = int(line.replace(replace_count_str0, ''))
                else:
                    if re.search(replace_str1, line, re.IGNORECASE):#replace truncate to delete from
                        new_line = line.replace(replace_str1, 'DELETE FROM oceanbase.__all_tenant_time_zone')
                    elif re.search(replace_str2, line, re.IGNORECASE):# replace mysql table name to ob table name
                        new_line = line.replace(replace_str2, 'INTO  oceanbase.__all_tenant_time_zone')
                    elif re.search(replace_str3, line, re.IGNORECASE):# delete alter table...order by
                        new_line = ''
                    else:
                        new_line = line
                    new_line = new_line.replace('tid', "0")
                    sql += new_line
                    if ";" in new_line:
                        self.sql_list.append(sql)
                        sql = ""

    def connect_server(self):
        self.conn = mysql.connector.connect(user='root', password=self.pwd, host=self.host, port=self.port, database='mysql')
        self.cur = self.conn.cursor(buffered=True)
        print "INFO : sucess to connect server {0}:{1}".format(self.host, self.port)
        try:
            sql = "select value from oceanbase.__all_sys_parameter where name = 'enable_upgrade_mode';"
            self.cur.execute(sql)
            print "INFO : execute sql -- {0}".format(sql)
            result = self.cur.fetchall()
            if 1 == len(result) and 1 == result[0][0]:
                self.upgrade_mode = True
            else:
                self.upgrade_mode = False
                sql = "select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(str(self.tenant))
                print "INFO : execute sql -- {0}".format(sql)
                self.cur.execute(sql)
                result = self.cur.fetchall()
                if 1 == len(result):
                    print "tenant_id = {0}".format(str(result[0][0]))
                    self.tenant_id = result[0][0]
                else:
                    self.tenant_id = 0
            if False == self.upgrade_mode and self.tenant_id != 1:
                sql = "commit"
                self.cur.execute(sql)
                sql = "alter system change tenant " + str(self.tenant)
                self.cur.execute(sql)
        except mysql.connector.Error as err:
            print("ERROR : " + sql)
            print(err)
	    raise

    def execute_sql(self):
        try:
            for sql in self.sql_list:
                self.cur.execute(sql);
                print "INFO : execute sql -- {0}".format(sql)
        except mysql.connector.Error as err:
            print("ERROR : " + sql)
            print(err)
            print("ERROR : fail to import time zone info")
	    raise
        else:
            print("INFO : success to import time zone info")

    def execute_check_sql(self, table_name, idx):
        self.cur.execute("select count(*) from {0}".format(table_name))
        result = self.cur.fetchone()
        self.result_count[idx] = result[0]
        print "INFO : {0} record count -- {1}, expect count -- {2}".format(table_name, result[0], self.expect_count[idx])

    def check_result(self):
        self.result_count = [0, 0, 0, 0]
        self.execute_check_sql("oceanbase.__all_tenant_time_zone", 0)
        self.execute_check_sql("oceanbase.__all_tenant_time_zone_name", 1)
        self.execute_check_sql("oceanbase.__all_tenant_time_zone_transition", 2)
        self.execute_check_sql("oceanbase.__all_tenant_time_zone_transition_type", 3)
        if self.expect_count[0] == self.result_count[0] \
            and self.expect_count[1] == self.result_count[1] \
            and self.expect_count[2] == self.result_count[2] \
            and self.expect_count[3] == self.result_count[3]:
            try:
                for sql in self.tz_version_sql_list:
                    self.cur.execute(sql)
                    print "INFO : execute sql -- {0}".format(sql)
            except mysql.connector.Error as err:
                print("ERROR : " + sql)
                print(err)
                print("ERROR : fail to insert time zone version")
		raise
            else:
                print("INFO : success to insert time zone version")

def main():
    tz_info_importer = TimeZoneInfoImporter()
    tz_info_importer.get_args()
    try:
	tz_info_importer.connect_server()
	if False == tz_info_importer.upgrade_mode:
	    tz_info_importer.generate_sql()
	    tz_info_importer.execute_sql()
	    tz_info_importer.check_result()
    except:
	print("except error in main")

if __name__ == "__main__":
    main()
