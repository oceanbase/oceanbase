'''
example
python sqladuit.py -h 100.69.198.71 -P 31903 -uroot -Uroot
'''

import mysql.connector
from optparse import OptionParser

parser = OptionParser(add_help_option=False)
parser.add_option("-h", "--host", dest="host", help="host")
parser.add_option("-P", "--port", dest="port", help="port")
parser.add_option("-u", "--user", dest="user", help="user")
parser.add_option("-p", "--password", dest="password", help="password")
parser.add_option("-U", "--username", dest="username", help="username")
(options, args) = parser.parse_args()

user_name = options.username
if options.password == None:
  passwd = ''
else:
  passwd = options.password
conn = mysql.connector.connect(host=options.host, port = int(options.port), user = options.user, password = passwd);
cur = conn.cursor(dictionary = True);
last_request_time = 0
while True:
  cur.execute("select request_time, query_sql from oceanbase.V$OB_SQL_AUDIT where request_time > %d and user_name = '%s' limit 1000" % (last_request_time, user_name))
  rs = cur.fetchall()
  for line in rs:
    sql = line['query_sql'].strip()
    if sql[:len("select")].lower() == 'select':
      print sql
    last_request_time = line['request_time']
