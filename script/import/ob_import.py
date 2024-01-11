#!/usr/bin/env python

import os
import sys
import MySQLdb
import Queue
import threading 
import multiprocessing
import traceback
import random

param = {}
param['data_file'] = sys.argv[1]
param['delima'] = '\1'
param['nop'] = '\2'
param['null'] = '\3'

param['table_name'] = 't1'
param['column_count'] = 4
param['host'] = "127.0.0.1"
param['port'] = 35999
param['user'] = "admin"
param['passwd'] = "admin"
param['batch_count'] = 1000
param['concurrency'] = 20

id_column_map = [
  (0,'c1'), 
  (1,'c2'),
  (2,'c3'),
  (3,'c4'),
  ]

param['id_column_map'] = id_column_map

for k,v in param.items():
  print k, v

class MyExcept(BaseException):
  pass

def get_column_type(cursor, table_name):
  column_type_map = {}
  if cursor.execute("select table_id from __first_tablet_entry where table_name = '%s'" % table_name) != 1:
    raise MyExcept
  table_id = cursor.fetchone()[0]
  if cursor.execute("select column_name, data_type from __all_all_column where table_id = %d" % table_id) <= 0:
    raise MyExcept
  for item in cursor.fetchall():
    column_type_map[item[0]] = item[1]
  return column_type_map


def add_value(count, line_num, tokens, set_values, execute_values):
  for id_column in id_column_map:
    id = id_column[0]
    if tokens[id] == param['nop']:
      execute_values.append('@nop')
    elif tokens[id] == param['null']:
      execute_values.append('@null')
    else:
      var_name = '@a%d' % count
      execute_values.append(var_name)
      set_values.append("%s='%s'" % (var_name, tokens[id]))
      count += 1
  return count

def gen_replace_sql(**param):
  values = "(%s)" % ','.join([ '?' for i in xrange(0,len(id_column_map)) ])
  values = ",".join([ values for i in xrange(0, param["batch_count"]) ])
  column_def = ','.join([ i[1] for i in id_column_map ])
  param["values"] = values
  param["column_def"] = column_def
  return "prepare p1 from replace into %(table_name)s(%(column_def)s) values%(values)s" % param

def worker():
  conn = MySQLdb.connect(host=param["host"], port=param["port"], user=param["user"], passwd = param["passwd"])
  conn.autocommit(True)
  cursor = conn.cursor()
  replace_sql = gen_replace_sql(**param)
  cursor.execute(replace_sql)

  print "work", os.getpid()

  while True:
    lines = q.get()
    if lines == None:
      break
    if len(lines) != param['batch_count']:
      raise MyExcept
    set_values = []
    execute_values = []
    count = 0
    for i in xrange(0, len(lines)):
      line = lines[i]
      tokens = line.split(param['delima'])
      if len(tokens) != param['column_count']:
        print tokens
        raise MyExcept
      count = add_value(count, i, tokens, set_values, execute_values);
    set_sql = "set " + ",".join(set_values)
    execute_sql = "execute p1 using " + ",".join(execute_values)

    try:
      cursor.execute(set_sql)
      cursor.execute(execute_sql)
    except:
      print set_sql
      print execute_sql
      print "".join(traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback))

  cursor.close()
  conn.close()

def import_lines(lines):
  conn = MySQLdb.connect(host=param["host"], port=param["port"], user=param["user"], passwd = param["passwd"])
  conn.autocommit(True)
  cursor = conn.cursor()
  param['batch_count'] = len(lines)
  replace_sql = gen_replace_sql(**param)
  cursor.execute(replace_sql)

  if len(lines) != param['batch_count']:
    raise MyExcept
  set_values = []
  execute_values = []
  count = 0
  for i in xrange(0, len(lines)):
    line = lines[i]
    tokens = line.split(param['delima'])
    if len(tokens) != param['column_count']:
      print tokens
      raise MyExcept
    count = add_value(count, i, tokens, set_values, execute_values);
  set_sql = "set " + ",".join(set_values)
  execute_sql = "execute p1 using " + ",".join(execute_values)

  try:
    cursor.execute(set_sql)
    cursor.execute(execute_sql)
  except:
    print set_sql
    print execute_sql
    print "".join(traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback))

  cursor.close()
  conn.close()



f = open(param['data_file'])
line_count = 0
q = multiprocessing.Queue(param['concurrency'] * 2)

threads = []
for i in xrange(param['concurrency']):
  t = multiprocessing.Process(target=worker)
  threads.append(t)
  t.daemon = True
  t.start()

print "main", os.getpid()

while True:
  flag = False
  lines = []
  for i in xrange(0, param['batch_count']):
    line = f.readline()
    if 0 == len(line):
      flag = True
      break
    line = line.strip()
    lines.append(line)
  if flag:
    if len(lines) != 0:
      import_lines(lines)
    break
  line_count += len(lines)
  if line_count % 10000 == 0:
    print line_count
  if len(lines) == param['batch_count']:
    q.put(lines, timeout = 3)

for i in xrange(0, param['concurrency']):
  q.put(None)

f.close()

for t in threads:
  t.join()


