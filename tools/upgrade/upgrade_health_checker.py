#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import re

class UpgradeParams:
  log_filename = 'upgrade_cluster_health_checker.log'

class PasswordMaskingFormatter(logging.Formatter):
  def format(self, record):
    s = super(PasswordMaskingFormatter, self).format(record)
    return re.sub(r'password="(?:[^"\\]|\\.)*"', 'password="******"', s)

#### --------------start : my_error.py --------------
class MyError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

#### --------------start : actions.py 只允许执行查询语句--------------
class QueryCursor:
  __cursor = None
  def __init__(self, cursor):
    self.__cursor = cursor
  def exec_sql(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
      return rowcount
    except mysql.connector.Error as e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise
    except Exception as e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise
  def exec_query(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      results = self.__cursor.fetchall()
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
      return (self.__cursor.description, results)
    except mysql.connector.Error as e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise
    except Exception as e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise
#### ---------------end----------------------

#### --------------start :  opt.py --------------
help_str = \
"""
Help:
""" +\
sys.argv[0] + """ [OPTIONS]""" +\
'\n\n' +\
'-I, --help          Display this help and exit.\n' +\
'-V, --version       Output version information and exit.\n' +\
'-h, --host=name     Connect to host.\n' +\
'-P, --port=name     Port number to use for connection.\n' +\
'-u, --user=name     User for login.\n' +\
'-p, --password=name Password to use when connecting to server. If password is\n' +\
'                    not given it\'s empty string "".\n' +\
'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
'                    system_variable_dml, special_action, all. "all" represents\n' +\
'                    that all modules should be run. They are splitted by ",".\n' +\
'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
'-t, --timeout=name  check timeout.\n' + \
'-z, --zone=name     If zone is not specified, check all servers status in cluster. \n' +\
'                    Otherwise, only check servers status in specified zone. \n' + \
'\n\n' +\
'Maybe you want to run cmd like that:\n' +\
sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'

version_str = """version 1.0.0"""

class Option:
  __g_short_name_set = set([])
  __g_long_name_set = set([])
  __short_name = None
  __long_name = None
  __is_with_param = None
  __is_local_opt = None
  __has_value = None
  __value = None
  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
    if short_name in Option.__g_short_name_set:
      raise MyError('duplicate option short name: {0}'.format(short_name))
    elif long_name in Option.__g_long_name_set:
      raise MyError('duplicate option long name: {0}'.format(long_name))
    Option.__g_short_name_set.add(short_name)
    Option.__g_long_name_set.add(long_name)
    self.__short_name = short_name
    self.__long_name = long_name
    self.__is_with_param = is_with_param
    self.__is_local_opt = is_local_opt
    self.__has_value = False
    if None != default_value:
      self.set_value(default_value)
  def is_with_param(self):
    return self.__is_with_param
  def get_short_name(self):
    return self.__short_name
  def get_long_name(self):
    return self.__long_name
  def has_value(self):
    return self.__has_value
  def get_value(self):
    return self.__value
  def set_value(self, value):
    self.__value = value
    self.__has_value = True
  def is_local_opt(self):
    return self.__is_local_opt
  def is_valid(self):
    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value

g_opts =\
[\
Option('I', 'help', False, True),\
Option('V', 'version', False, True),\
Option('h', 'host', True, False),\
Option('P', 'port', True, False),\
Option('u', 'user', True, False),\
Option('p', 'password', True, False, ''),\
# 要跑哪个模块，默认全跑
Option('m', 'module', True, False, 'all'),\
# 日志文件路径，不同脚本的main函数中中会改成不同的默认值
Option('l', 'log-file', True, False),\
Option('t', 'timeout', True, False, 0),\
Option('z', 'zone', True, False, ''),\
]\

def change_opt_defult_value(opt_long_name, opt_default_val):
  global g_opts
  for opt in g_opts:
    if opt.get_long_name() == opt_long_name:
      opt.set_value(opt_default_val)
      return

def has_no_local_opts():
  global g_opts
  no_local_opts = True
  for opt in g_opts:
    if opt.is_local_opt() and opt.has_value():
      no_local_opts = False
  return no_local_opts

def check_db_client_opts():
  global g_opts
  for opt in g_opts:
    if not opt.is_local_opt() and not opt.has_value():
      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
          .format(opt.get_short_name(), sys.argv[0]))

def parse_option(opt_name, opt_val):
  global g_opts
  for opt in g_opts:
    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
      opt.set_value(opt_val)

def parse_options(argv):
  global g_opts
  short_opt_str = ''
  long_opt_list = []
  for opt in g_opts:
    if opt.is_with_param():
      short_opt_str += opt.get_short_name() + ':'
    else:
      short_opt_str += opt.get_short_name()
  for opt in g_opts:
    if opt.is_with_param():
      long_opt_list.append(opt.get_long_name() + '=')
    else:
      long_opt_list.append(opt.get_long_name())
  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
  for (opt_name, opt_val) in opts:
    parse_option(opt_name, opt_val)
  if has_no_local_opts():
    check_db_client_opts()

def deal_with_local_opt(opt):
  if 'help' == opt.get_long_name():
    global help_str
    print(help_str)
  elif 'version' == opt.get_long_name():
    global version_str
    print(version_str)

def deal_with_local_opts():
  global g_opts
  if has_no_local_opts():
    raise MyError('no local options, can not deal with local options')
  else:
    for opt in g_opts:
      if opt.is_local_opt() and opt.has_value():
        deal_with_local_opt(opt)
        # 只处理一个
        return

def get_opt_host():
  global g_opts
  for opt in g_opts:
    if 'host' == opt.get_long_name():
      return opt.get_value()

def get_opt_port():
  global g_opts
  for opt in g_opts:
    if 'port' == opt.get_long_name():
      return opt.get_value()

def get_opt_user():
  global g_opts
  for opt in g_opts:
    if 'user' == opt.get_long_name():
      return opt.get_value()

def get_opt_password():
  global g_opts
  for opt in g_opts:
    if 'password' == opt.get_long_name():
      return opt.get_value()

def get_opt_module():
  global g_opts
  for opt in g_opts:
    if 'module' == opt.get_long_name():
      return opt.get_value()

def get_opt_log_file():
  global g_opts
  for opt in g_opts:
    if 'log-file' == opt.get_long_name():
      return opt.get_value()

def get_opt_timeout():
  global g_opts
  for opt in g_opts:
    if 'timeout' == opt.get_long_name():
      return opt.get_value()

def get_opt_zone():
  global g_opts
  for opt in g_opts:
    if 'zone' == opt.get_long_name():
      return opt.get_value()
#### ---------------end----------------------

#### --------------start :  do_upgrade_pre.py--------------
def config_logging_module(log_filenamme):
  logger = logging.getLogger('')
  logger.setLevel(logging.INFO)
  # 定义日志打印格式
  formatter = PasswordMaskingFormatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
  #######################################
  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.setLevel(logging.INFO)
  stdout_handler.setFormatter(formatter)
  # 定义一个Handler处理文件输出
  file_handler = logging.FileHandler(log_filenamme, mode='w')
  file_handler.setLevel(logging.INFO)
  file_handler.setFormatter(formatter)
  logging.getLogger('').addHandler(stdout_handler)
  logging.getLogger('').addHandler(file_handler)
#### ---------------end----------------------

def check_zone_valid(query_cur, zone):
  if zone != '':
    sql = """select count(*) from oceanbase.DBA_OB_ZONES where zone = '{0}'""".format(zone)
    (desc, results) = query_cur.exec_query(sql);
    if len(results) != 1 or len(results[0]) != 1:
      raise MyError("unmatched row/column cnt")
    elif results[0][0] == 0:
      raise MyError("zone:{0} doesn't exist".format(zone))
    else:
      logging.info("zone:{0} is valid".format(zone))
  else:
    logging.info("zone is empty, check all servers in cluster")

def fetch_tenant_ids(query_cur):
  try:
    tenant_id_list = []
    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
    for r in results:
      tenant_id_list.append(r[0])
    return tenant_id_list
  except Exception as e:
    logging.exception('fail to fetch distinct tenant ids')
    raise

def set_default_timeout_by_tenant(query_cur, timeout, timeout_per_tenant, min_timeout):
  if timeout > 0:
    logging.info("use timeout from opt, timeout(s):{0}".format(timeout))
  else:
    tenant_id_list = fetch_tenant_ids(query_cur)
    cal_timeout = len(tenant_id_list) * timeout_per_tenant
    timeout = (cal_timeout if cal_timeout > min_timeout else min_timeout)
    logging.info("use default timeout caculated by tenants, "
                 "timeout(s):{0}, tenant_count:{1}, "
                 "timeout_per_tenant(s):{2}, min_timeout(s):{3}"
                 .format(timeout, len(tenant_id_list), timeout_per_tenant, min_timeout))

  return timeout

#### START ####
# 0. 检查server版本是否严格一致
def check_server_version_by_zone(query_cur, zone):
  if zone == '':
    logging.info("skip check server version by cluster")
  else:
    sql = """select distinct(substring_index(build_version, '_', 1)) from oceanbase.__all_server where zone = '{0}'""".format(zone);
    (desc, results) = query_cur.exec_query(sql);
    if len(results) != 1:
      raise MyError("servers build_version not match")
    else:
      logging.info("check server version success")

# 1. 检查paxos副本是否同步, paxos副本是否缺失
def check_paxos_replica(query_cur, timeout):
  # 1.1 检查paxos副本是否同步
  sql = """select count(*) from oceanbase.GV$OB_LOG_STAT where in_sync = 'NO'"""
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 10, 600)
  check_until_timeout(query_cur, sql, 0, wait_timeout)

  # 1.2 检查paxos副本是否有缺失 TODO
  logging.info('check paxos replica success')

# 2. 检查observer是否可服务
def check_observer_status(query_cur, zone, timeout):
  sql = """select count(*) from oceanbase.__all_server where (start_service_time <= 0 or status='inactive')"""
  if zone != '':
    sql += """ and zone = '{0}'""".format(zone)
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 10, 600)
  check_until_timeout(query_cur, sql, 0, wait_timeout)

# 3. 检查schema是否刷新成功
def check_schema_status(query_cur, timeout):
  sql = """select if (a.cnt = b.cnt, 1, 0) as passed from (select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version > 1 and refreshed_schema_version % 8 = 0) as a join (select count(*) as cnt from oceanbase.__all_server join oceanbase.__all_tenant) as b"""
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 30, 600)
  check_until_timeout(query_cur, sql, 1, wait_timeout)

# 4. check major finish
def check_major_merge(query_cur, timeout):
  need_check = 0
  (desc, results) = query_cur.exec_query("""select distinct value from oceanbase.GV$OB_PARAMETERS where name = 'enable_major_freeze';""")
  if len(results) != 1:
    need_check = 1
  elif results[0][0] != 'True':
    need_check = 1
  if need_check == 1:
    wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 30, 600)
    sql = """select count(1) from oceanbase.CDB_OB_MAJOR_COMPACTION where (GLOBAL_BROADCAST_SCN > LAST_SCN or STATUS != 'IDLE')"""
    check_until_timeout(query_cur, sql, 0, wait_timeout)
    sql2 = """select /*+ query_timeout(1000000000) */ count(1) from oceanbase.__all_virtual_tablet_compaction_info where max_received_scn > finished_scn and max_received_scn > 0"""
    check_until_timeout(query_cur, sql2, 0, wait_timeout)

def check_until_timeout(query_cur, sql, value, timeout):
  times = timeout / 10
  while times >= 0:
    (desc, results) = query_cur.exec_query(sql)

    if len(results) != 1 or len(results[0]) != 1:
      raise MyError("unmatched row/column cnt")
    elif results[0][0] == value:
      logging.info("check value is {0} success".format(value))
      break
    else:
      logging.info("value is {0}, expected value is {1}, not matched".format(results[0][0], value))

    times -= 1
    if times == -1:
      logging.warn("""result not expected, sql: '{0}', expected: '{1}', current: '{2}'""".format(sql, value, results[0][0]))
      raise MyError("""result not expected, sql: '{0}', expected: '{1}', current: '{2}'""".format(sql, value, results[0][0]))
    time.sleep(10)

# 开始健康检查
def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout, need_check_major_status, zone = ''):
  try:
    conn = mysql.connector.connect(user = my_user,
                                   password = my_passwd,
                                   host = my_host,
                                   port = my_port,
                                   database = 'oceanbase',
                                   raise_on_warnings = True)
    conn.autocommit = True
    cur = conn.cursor(buffered=True)
    try:
      query_cur = QueryCursor(cur)
      check_zone_valid(query_cur, zone)
      check_observer_status(query_cur, zone, timeout)
      check_paxos_replica(query_cur, timeout)
      check_schema_status(query_cur, timeout)
      check_server_version_by_zone(query_cur, zone)
      if True == need_check_major_status:
        check_major_merge(query_cur, timeout)
    except Exception as e:
      logging.exception('run error')
      raise
    finally:
      cur.close()
      conn.close()
  except mysql.connector.Error as e:
    logging.exception('connection error')
    raise
  except Exception as e:
    logging.exception('normal error')
    raise

if __name__ == '__main__':
  upgrade_params = UpgradeParams()
  change_opt_defult_value('log-file', upgrade_params.log_filename)
  parse_options(sys.argv[1:])
  if not has_no_local_opts():
    deal_with_local_opts()
  else:
    check_db_client_opts()
    log_filename = get_opt_log_file()
    upgrade_params.log_filename = log_filename
    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
    config_logging_module(upgrade_params.log_filename)
    try:
      host = get_opt_host()
      port = int(get_opt_port())
      user = get_opt_user()
      password = get_opt_password()
      timeout = int(get_opt_timeout())
      zone = get_opt_zone()
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s, zone=\"%s\"', \
          host, port, user, password.replace('"', '\\"'), log_filename, timeout, zone)
      do_check(host, port, user, password, upgrade_params, timeout, False, zone) # need_check_major_status = False
    except mysql.connector.Error as e:
      logging.exception('mysql connctor error')
      raise
    except Exception as e:
      logging.exception('normal error')
      raise

