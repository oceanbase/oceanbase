#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt

class UpgradeParams:
  log_filename = 'upgrade_cluster_health_checker.log'

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
    except mysql.connector.Error, e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise e
    except Exception, e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise e
  def exec_query(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      results = self.__cursor.fetchall()
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
      return (self.__cursor.description, results)
    except mysql.connector.Error, e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise e
    except Exception, e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise e
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
'-t, --timeout=name  check timeout, default: 600(s).\n' + \
'\n\n' +\
'Maybe you want to run cmd like that:\n' +\
sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'

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
# 一些检查的超时时间，默认是600s
Option('t', 'timeout', True, False, '600')
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
    print help_str
  elif 'version' == opt.get_long_name():
    global version_str
    print version_str

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
#### ---------------end----------------------

#### --------------start :  do_upgrade_pre.py--------------
def config_logging_module(log_filenamme):
  logging.basicConfig(level=logging.INFO,\
      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
      datefmt='%Y-%m-%d %H:%M:%S',\
      filename=log_filenamme,\
      filemode='w')
  # 定义日志打印格式
  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
  #######################################
  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.setLevel(logging.INFO)
  # 设置日志打印格式
  stdout_handler.setFormatter(formatter)
  # 将定义好的stdout_handler日志handler添加到root logger
  logging.getLogger('').addHandler(stdout_handler)
#### ---------------end----------------------

#### START ####
# 1. 检查paxos副本是否同步, paxos副本是否缺失
def check_paxos_replica(query_cur):
  # 2.1 检查paxos副本是否同步
  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
  if results[0][0] > 0 :
    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
  # 2.2 检查paxos副本是否有缺失 TODO
  logging.info('check paxos replica success')

# 2. 检查是否有做balance, locality变更
def check_rebalance_task(query_cur):
  # 3.1 检查是否有做locality变更
  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
  if results[0][0] > 0 :
    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
  # 3.2 检查是否有做balance
  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
  if results[0][0] > 0 :
    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
  logging.info('check rebalance task success')

# 3. 检查集群状态
def check_cluster_status(query_cur):
  # 4.1 检查是否非合并状态
  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
  if cmp(results[0][0], 'IDLE')  != 0 :
    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
  logging.info('check cluster status success')
  # 4.2 检查合并版本是否>=3
  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
  if results[0][0] < 2:
      raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
  logging.info('check global last_merged_version success')

# 4. 检查租户分区数是否超出内存限制
def check_tenant_part_num(query_cur):
  # 统计每个租户在各个server上的分区数量
  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
  # 计算每个租户在每个server上的max_memory
  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
  # 查询每个server的memstore_limit_percentage
  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
  part_static_cost = 128 * 1024
  part_dynamic_cost = 400 * 1024
  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
  part_num_reserved = 512
  for line in res_part_num:
    svr_ip = line[0]
    svr_port = line[1]
    tenant_id = line[2]
    part_num = line[3]
    for uline in res_unit_memory:
      uip = uline[0]
      uport = uline[1]
      utid = uline[2]
      umem = uline[3]
      utype = uline[4]
      if svr_ip == uip and svr_port == uport and tenant_id == utid:
        for mpline in res_svr_memstore_percent:
          mpip = mpline[0]
          mpport = mpline[1]
          if mpip == uip and mpport == uport:
            mspercent = int(mpline[3])
            mem_limit = umem
            if 0 == utype:
              # full类型的unit需要为memstore预留内存
              mem_limit = umem * (100 - mspercent) / 100
            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
            if part_num_limit <= 1000:
              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
            if part_num >= (part_num_limit - part_num_reserved):
              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
            break
  logging.info('check tenant partition num success')

# 5. 检查存在租户partition，但是不存在unit的observer
def check_tenant_resource(query_cur):
  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
  for line in res_unit:
    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
  logging.info("check tenant resource success")

# 6. 检查progressive_merge_round都升到1
def check_progressive_merge_round(query_cur):
  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id = 0""")
  if results[0][0] != 0:
    raise MyError("""progressive_merge_round of main table should all be 1""")
  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id > 0 and data_table_id in (select table_id from __all_virtual_table where table_type not in (1,2,4) and data_table_id = 0)""")
  if results[0][0] != 0:
    raise MyError("""progressive_merge_round of index should all be 1""")
  logging.info("""check progressive_merge_round status success""")

# 主库状态检查
def check_primary_cluster_sync_status(query_cur, timeout):
  (desc, res) = query_cur.exec_query("""select  current_scn  from oceanbase.v$ob_cluster where cluster_role='PRIMARY' and cluster_status='VALID'""")
  if len(res) != 1:
      raise MyError('query results count is not 1')
  query_sql = "select count(*) from oceanbase.v$ob_standby_status where cluster_role != 'PHYSICAL STANDBY' or cluster_status != 'VALID' or current_scn < {0}".format(res[0][0]);  
  times = timeout
  print times
  while times > 0 :
    (desc, res1) = query_cur.exec_query(query_sql)
    if len(res1) == 1 and res1[0][0] == 0:
      break;
    time.sleep(1)
    times -=1
  if times == 0:
    raise MyError("there exists standby cluster not synchronizing, checking primary cluster status failed!!!")
  else:
    logging.info("check primary cluster sync status success")

# 备库状态检查
def check_standby_cluster_sync_status(query_cur, timeout):
  (desc, res) = query_cur.exec_query("""select time_to_usec(now(6)) from dual""")
  query_sql = "select count(*) from oceanbase.v$ob_cluster where (cluster_role != 'PHYSICAL STANDBY') or (cluster_status != 'VALID') or (current_scn  < {0}) or (switchover_status != 'NOT ALLOWED')".format(res[0][0]);
  times = timeout
  while times > 0 :
    (desc, res2) = query_cur.exec_query(query_sql)
    if len(res2) == 1 and res2[0][0] == 0:
      break
    time.sleep(1)
    times -= 1
  if times == 0:
    raise MyError('current standby cluster not synchronizing, please check!!!')
  else:
    logging.info("check standby cluster sync status success")

# 判断是主库还是备库
def check_cluster_sync_status(query_cur, timeout):
  (desc, res) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
  if res[0][0] == 'PRIMARY':
    check_primary_cluster_sync_status(query_cur, timeout)
  else:
    check_standby_cluster_sync_status(query_cur, timeout)
  

# 开始升级前的检查
def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout):
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
      check_paxos_replica(query_cur)
      check_rebalance_task(query_cur)
      check_cluster_status(query_cur)
      check_tenant_part_num(query_cur)
      check_tenant_resource(query_cur)
      check_cluster_sync_status(query_cur, timeout)
    except Exception, e:
      logging.exception('run error')
      raise e
    finally:
      cur.close()
      conn.close()
  except mysql.connector.Error, e:
    logging.exception('connection error')
    raise e
  except Exception, e:
    logging.exception('normal error')
    raise e

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
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s', \
          host, port, user, password, log_filename, timeout)
      do_check(host, port, user, password, upgrade_params, timeout)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      raise e
    except Exception, e:
      logging.exception('normal error')
      raise e

