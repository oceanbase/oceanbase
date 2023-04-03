#!/usr/bin/python
# coding=utf8
import os
import argparse
import mysql.connector
import sys

class Config:
  pass

g_config = Config()

class Stat:
  def reset(self):
    self.db_count = 0
    self.outline_count = 0

  def __init__(self):
    self.reset()


g_stat = Stat()

class ImportStat:
  def reset(self):
    self.succ_db_count = 0
    self.fail_db_count = 0
    self.succ_outline_count = 0
    self.fail_outline_count = 0

  def __init__(self):
    self.reset()


g_import_stat = ImportStat()

#参数
#-h -P -u -p -t -i -d
#如果-t没指定就是所有租户
#
#-d 表示dump
#-i 表示import

#查询__all_server表。获取有多少台server, 设置并发度
#设置查询超时时间
#查询GV$OB_PLAN_CACHE_PLAN_STAT表获取outline_data
#
#select sql_id, sql_text, outline_data from GV$OB_PLAN_CACHE_PLAN_STAT。检查sql_id是否有冲突，outline_data是否有冲突//获取对应的database。
#
#dump成outline语句, 生成对应的database。
#生成outline语句

def get_connect():
  return mysql.connector.connect(host = g_config.host, port = g_config.port, user = g_config.username, password = g_config.password)

def get_real_db_id(tenant_id, db_id):
  if db_id == 1:
    return tenant_id << 40 | db_id
  else:
    return db_id

def get_args(args):
  parser = argparse.ArgumentParser(add_help = False)
  parser.add_argument('-h', '--host', dest='host', type=str)
  parser.add_argument('-P', '--port', dest='port', type=int)
  parser.add_argument('-u', '--username', dest='username', type=str)
  parser.add_argument('-p', '--password', dest='password', type=str)
  parser.add_argument('-t', '--tenant', dest='tenant', type=int)
  parser.add_argument('-d', '--dump', dest='dump', action='store_true')
  parser.add_argument('-i', '--import', dest='import1', action='store_true')
  ret = parser.parse_args(args)

  if ret.host == None:
    print >> sys.stderr, 'please give hostname: -h'
    return -1
  else:
    g_config.host = ret.host

  if ret.port == None:
    print >> sys.stderr, 'please give port: -P'
    return -1
  else:
    g_config.port = ret.port

  if ret.username == None:
    print >> sys.stderr, 'please give username: -u'
    return -1
  else:
    g_config.username = ret.username

  if ret.tenant == None:
    print >> sys.stderr, 'please give tenant_id: -t'
    return -1
  else:
    g_config.tenant = ret.tenant

  g_config.password = ret.password

  if ret.dump == False and ret.import1 == False:
    print >> sys.stderr, 'please give dump or import: -d/-i'
    return -1
  elif ret.dump == True and ret.import1 == True:
    print >> sys.stderr, 'only dump or import: -d/-i'
    return -1
  else:
    g_config.dump = ret.dump

  return 0


def output(name, sql_id, outline_data):
  g_stat.outline_count += 1
  print "create outline auto_gen_%s on '%s' using hint %s;" % (name, sql_id, outline_data)

def check(lines):
  for i in range(1,len(lines)):
    if lines[i][1] != lines[0][1]:
      return False
    elif lines[i][2] != lines[0][2]:
      return False
  return True

def get_db_name(db_id):
  conn = get_connect()
  cur = conn.cursor()
  sql = "select database_name from oceanbase.__all_database where database_id = %d and tenant_id = %d" % (db_id, g_config.tenant)
  cur.execute(sql)
  rs = cur.fetchone()
  return rs[0]


def dump_db_outline(db_id, items):
  if db_id == 18446744073709551615:
    for item in items:
      print >> sys.stderr, "sql_id = %s | sql_text = %s : no use database" % (item[0], item[1])
    return
  db_name = get_db_name(db_id)
  g_stat.db_count += 1
  print 
  print "use %s;" % db_name
  map = {}
  for line in items:
    sql_id = line[0]
    sql_text = line[1]
    outline_data = line[2]

    if sql_id in map:
      map[sql_id].append((sql_id, sql_text, outline_data))
    else:
      map[sql_id] = [(sql_id, sql_text, outline_data)]

  count = 0
  for k, v in map.items():
    name = '%d_%s%d' % (g_config.tenant, db_name, count)
    if len(v) == 1:
      output(name, v[0][0], v[0][2])
    else:
      if check(v):
        output(name, v[0][0], v[0][2])
      else:
        print >> sys.stderr, "sql_id = %s has conflict" % (v[0][0])
    count += 1


def dump_outline():
  conn = get_connect()
  cur = conn.cursor()
  cur.execute('select count(1) from oceanbase.__all_server')
  rs = cur.fetchone()
  server_count = rs[0]
  cur.execute('select db_id, sql_id, statement, outline_data from oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT where tenant_id = %d order by db_id' % g_config.tenant)
  rs = cur.fetchall()
  last_db_id = 0
  items = []
  for i in range(0, len(rs)):
    db_id = get_real_db_id(1, rs[i][0])
    if db_id == last_db_id:
      items.append(rs[i][1:])
    else:
      if len(items) != 0:
        dump_db_outline(last_db_id, items)
        items = []
      last_db_id = db_id
      items.append(rs[i][1:])
  if len(items) != 0:
    dump_db_outline(last_db_id, items)
    items = []
  print >> sys.stderr, "%d database and %d outline dumped" % (g_stat.db_count, g_stat.outline_count)

#导入outline。判断outline是否存在。如果存在，那么就不导入了。
#
#查询__all_outline表。判断是否全部正确导入。
#
#输出导入信息。
#1. 总共导入了多少outline, 之前已经存在多少outline
#2. 中间有冲突的信息输出 

def import_outline():
  conn = get_connect()
  cur = conn.cursor()
  cur.execute("select effective_tenant_id()")
  rs = cur.fetchone()
  if rs[0] != g_config.tenant:
    print >> sys.stderr, 'tenant id not equal %d <> %d' % (rs[0], g_config.tenant)
    sys.exit(-1)

  state = 0
  for line in sys.stdin:
    line = line.strip()
    if len(line) != 0 and line[0] != '#':
      if len(line) >= 3 and line[:3] == 'use':
        print >> sys.stderr, 'change database: %s' % line
        try:
          cur.execute(line)
          state = 0
          g_import_stat.succ_db_count += 1
        except:
          g_import_stat.fail_db_count += 1
          print >> sys.stderr, 'fail to execute: %s' % line
          state = 1
      else:
        if state == 0:
          try:
            cur.execute(line)
            g_import_stat.succ_outline_count += 1
          except:
            print >> sys.stderr, 'fail to execute: %s' % line
            g_import_stat.fail_outline_count += 1
        else:
          g_import_stat.fail_outline_count += 1
          print >> sys.stderr, 'skip to execute: %s' % line
  print >> sys.stderr, "db succ %d | db fail %d | outline succ %d | outline fail %d" % (g_import_stat.succ_db_count, g_import_stat.fail_db_count, g_import_stat.succ_outline_count, g_import_stat.fail_outline_count)


if __name__ == '__main__':
  if -1 == get_args(sys.argv[1:]):
    sys.exit(-1)

  if g_config.dump:
    conn = get_connect()
    cur = conn.cursor()
    cur.execute("select effective_tenant_id()")
    rs = cur.fetchone()
    if rs[0] != 1:
      print >> sys.stderr, 'please use sys tenant to dump'
      sys.exit(-1)

    cur.execute("select * from oceanbase.__all_tenant where tenant_id = %d" % g_config.tenant)
    rs = cur.fetchall()
    if 1 != len(rs):
      print >> sys.stderr, 'no such tenant_id %d ' % g_config.tenant
      sys.exit(-1)

    dump_outline()
  else:
    import_outline()


