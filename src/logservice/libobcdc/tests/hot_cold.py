#!/usr/bin/python

# to create a case
# hot insert a record every one second,
# cold insert a record every 30 seconds,

import MySQLdb
import time

N=100

conn = MySQLdb.connect(host="100.81.152.41",
                       port=59703,
                       user="root@sys",
                       passwd="",
                       db="oceanbase")
conn.autocommit = True
cursor = conn.cursor()
cursor.execute("drop table if exists cold;")
cursor.execute("drop table if exists hot;")
cursor.execute("create table cold (c1 int primary key);")
cursor.execute("create table hot (c1 int primary key);")

for r in range(1,N):
  cursor.execute("insert into hot values (" + `r` + ");")
  if r % 30 == 0:
    cursor.execute("insert into cold values (" + `r` + ");")
  conn.commit()
  time.sleep(1)
  print "insert " + `r`

cursor.execute("select * from hot;")
data = cursor.fetchall();
print data

cursor.close()
conn.close()
