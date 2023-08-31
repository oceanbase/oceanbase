#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import time

class UpgradeParams:
  log_filename = 'upgrade_checker.log'
  old_version = '4.0.0.0'
#### --------------start : my_error.py --------------
class MyError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)
#### --------------start : actions.py------------
class Cursor:
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

def set_parameter(cur, parameter, value):
  sql = """alter system set {0} = '{1}'""".format(parameter, value)
  logging.info(sql)
  cur.execute(sql)
  wait_parameter_sync(cur, parameter, value)

def wait_parameter_sync(cur, key, value):
  sql = """select count(*) as cnt from oceanbase.__all_virtual_sys_parameter_stat
           where name = '{0}' and value != '{1}'""".format(key, value)
  times = 10
  while times > 0:
    logging.info(sql)
    cur.execute(sql)
    result = cur.fetchall()
    if len(result) != 1 or len(result[0]) != 1:
      logging.exception('result cnt not match')
      raise e
    elif result[0][0] == 0:
      logging.info("""{0} is sync, value is {1}""".format(key, value))
      break
    else:
      logging.info("""{0} is not sync, value should be {1}""".format(key, value))

    times -= 1
    if times == 0:
      logging.exception("""check {0}:{1} sync timeout""".format(key, value))
      raise e
    time.sleep(5)

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
'-t, --timeout=name  Cmd/Query/Inspection execute timeout(s).\n' +\
'-p, --password=name Password to use when connecting to server. If password is\n' +\
'                    not given it\'s empty string "".\n' +\
'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
'                    system_variable_dml, special_action, all. "all" represents\n' +\
'                    that all modules should be run. They are splitted by ",".\n' +\
'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
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
Option('t', 'timeout', True, False, 0),\
Option('p', 'password', True, False, ''),\
# 要跑哪个模块，默认全跑
Option('m', 'module', True, False, 'all'),\
# 日志文件路径，不同脚本的main函数中中会改成不同的默认值
Option('l', 'log-file', True, False)
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

def get_opt_timeout():
  global g_opts
  for opt in g_opts:
    if 'timeout' == opt.get_long_name():
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


fail_list=[]

def get_version(version_str):
  versions = version_str.split(".")

  if len(versions) != 4:
    logging.exception("""version:{0} is invalid""".format(version_str))
    raise e

  major = int(versions[0])
  minor = int(versions[1])
  major_patch = int(versions[2])
  minor_patch = int(versions[3])

  if major > 0xffffffff or minor > 0xffff or major_patch > 0xff or minor_patch > 0xff:
    logging.exception("""version:{0} is invalid""".format(version_str))
    raise e

  version = (major << 32) | (minor << 16) | (major_patch << 8) | (minor_patch)
  return version

#### START ####
# 1. 检查前置版本
def check_observer_version(query_cur, upgrade_params):
  (desc, results) = query_cur.exec_query("""select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'""")
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif cmp(results[0][0], upgrade_params.old_version) < 0 :
    fail_list.append('old observer version is expected equal or higher then: {0}, actual version:{1}'.format(upgrade_params.old_version, results[0][0]))
  logging.info('check observer version success, version = {0}'.format(results[0][0]))

def check_data_version(query_cur):
  min_cluster_version = 0
  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif len(results[0]) != 1:
    fail_list.append('column cnt not match')
  else:
    min_cluster_version = get_version(results[0][0])

    # check data version
    if min_cluster_version < get_version("4.1.0.0"):
      # last barrier cluster version should be 4.1.0.0
      fail_list.append('last barrier cluster version is 4.1.0.0. prohibit cluster upgrade from cluster version less than 4.1.0.0')
    else:
      data_version_str = ''
      data_version = 0
      # check compatible is same
      sql = """select distinct value from oceanbase.__all_virtual_tenant_parameter_info where name='compatible'"""
      (desc, results) = query_cur.exec_query(sql)
      if len(results) != 1:
        fail_list.append('compatible is not sync')
      elif len(results[0]) != 1:
        fail_list.append('column cnt not match')
      else:
        data_version_str = results[0][0]
        data_version = get_version(results[0][0])

        if data_version < get_version("4.1.0.0"):
          # last barrier data version should be 4.1.0.0
          fail_list.append('last barrier data version is 4.1.0.0. prohibit cluster upgrade from data version less than 4.1.0.0')
        else:
          # check target_data_version/current_data_version
          sql = "select count(*) from oceanbase.__all_tenant"
          (desc, results) = query_cur.exec_query(sql)
          if len(results) != 1 or len(results[0]) != 1:
            fail_list.append('result cnt not match')
          else:
            tenant_count = results[0][0]

            sql = "select count(*) from __all_virtual_core_table where column_name in ('target_data_version', 'current_data_version') and column_value = {0}".format(data_version)
            (desc, results) = query_cur.exec_query(sql)
            if len(results) != 1 or len(results[0]) != 1:
              fail_list.append('result cnt not match')
            elif 2 * tenant_count != results[0][0]:
              fail_list.append('target_data_version/current_data_version not match with {0}, tenant_cnt:{1}, result_cnt:{2}'.format(data_version_str, tenant_count, results[0][0]))
            else:
              logging.info("check data version success, all tenant's compatible/target_data_version/current_data_version is {0}".format(data_version_str))

# 2. 检查paxos副本是否同步, paxos副本是否缺失
def check_paxos_replica(query_cur):
  # 2.1 检查paxos副本是否同步
  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from GV$OB_LOG_STAT where in_sync = 'NO'""")
  if results[0][0] > 0 :
    fail_list.append('{0} replicas unsync, please check'.format(results[0][0]))
  # 2.2 检查paxos副本是否有缺失 TODO
  logging.info('check paxos replica success')

# 3. 检查是否有做balance, locality变更
def check_rebalance_task(query_cur):
  # 3.1 检查是否有做locality变更
  (desc, results) = query_cur.exec_query("""select count(1) as cnt from DBA_OB_TENANT_JOBS where job_status='INPROGRESS' and result_code is null""")
  if results[0][0] > 0 :
    fail_list.append('{0} locality tasks is doing, please check'.format(results[0][0]))
  # 3.2 检查是否有做balance
  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from CDB_OB_LS_REPLICA_TASKS""")
  if results[0][0] > 0 :
    fail_list.append('{0} rebalance tasks is doing, please check'.format(results[0][0]))
  logging.info('check rebalance task success')

# 4. 检查集群状态
def check_cluster_status(query_cur):
  # 4.1 检查是否非合并状态
  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_MAJOR_COMPACTION where (GLOBAL_BROADCAST_SCN > LAST_SCN or STATUS != 'IDLE')""")
  if results[0][0] > 0 :
    fail_list.append('{0} tenant is merging, please check'.format(results[0][0]))
  (desc, results) = query_cur.exec_query("""select /*+ query_timeout(1000000000) */ count(1) from __all_virtual_tablet_compaction_info where max_received_scn > finished_scn and max_received_scn > 0""")
  if results[0][0] > 0 :
    fail_list.append('{0} tablet is merging, please check'.format(results[0][0]))
  logging.info('check cluster status success')

# 5. 检查是否有异常租户(creating，延迟删除，恢复中)
def check_tenant_status(query_cur):

  # check tenant schema
  (desc, results) = query_cur.exec_query("""select count(*) as count from DBA_OB_TENANTS where status != 'NORMAL'""")
  if len(results) != 1 or len(results[0]) != 1:
    fail_list.append('results len not match')
  elif 0 != results[0][0]:
    fail_list.append('has abnormal tenant, should stop')
  else:
    logging.info('check tenant status success')

  # check tenant info
  # don't support restore tenant upgrade
  (desc, results) = query_cur.exec_query("""select count(*) as count from oceanbase.__all_virtual_tenant_info where tenant_role != 'PRIMARY' and tenant_role != 'STANDBY'""")
  if len(results) != 1 or len(results[0]) != 1:
    fail_list.append('results len not match')
  elif 0 != results[0][0]:
    fail_list.append('has abnormal tenant info, should stop')
  else:
    logging.info('check tenant info success')

# 6. 检查无恢复任务
def check_restore_job_exist(query_cur):
  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_RESTORE_PROGRESS""")
  if len(results) != 1 or len(results[0]) != 1:
    fail_list.append('failed to restore job cnt')
  elif results[0][0] != 0:
      fail_list.append("""still has restore job, upgrade is not allowed temporarily""")
  logging.info('check restore job success')

def check_is_primary_zone_distributed(primary_zone_str):
  semicolon_pos = len(primary_zone_str)
  for i in range(len(primary_zone_str)):
    if primary_zone_str[i] == ';':
      semicolon_pos = i
      break
  comma_pos = len(primary_zone_str)
  for j in range(len(primary_zone_str)):
    if primary_zone_str[j] == ',':
      comma_pos = j
      break
  if comma_pos < semicolon_pos:
    return True
  else:
    return False

# 7. 升级前需要primary zone只有一个
def check_tenant_primary_zone(query_cur):
  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif len(results[0]) != 1:
    fail_list.append('column cnt not match')
  else:
    min_cluster_version = get_version(results[0][0])
    if min_cluster_version < get_version("4.1.0.0"):
      (desc, results) = query_cur.exec_query("""select tenant_name,primary_zone from DBA_OB_TENANTS where  tenant_id != 1""");
      for item in results:
        if cmp(item[1], "RANDOM") == 0:
          fail_list.append('{0} tenant primary zone random before update not allowed'.format(item[0]))
        elif check_is_primary_zone_distributed(item[1]):
          fail_list.append('{0} tenant primary zone distributed before update not allowed'.format(item[0]))
      logging.info('check tenant primary zone success')

# 8. 修改永久下线的时间，避免升级过程中缺副本
def modify_server_permanent_offline_time(cur):
  set_parameter(cur, 'server_permanent_offline_time', '72h')

# 9. 检查是否有DDL任务在执行
def check_ddl_task_execute(query_cur):
  (desc, results) = query_cur.exec_query("""select count(1) from __all_virtual_ddl_task_status""")
  if 0 != results[0][0]:
    fail_list.append("There are DDL task in progress")
  logging.info('check ddl task execut status success')

# 10. 检查无备份任务
def check_backup_job_exist(query_cur):
  # Backup jobs cannot be in-progress during upgrade.
  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_BACKUP_JOBS""")
  if len(results) != 1 or len(results[0]) != 1:
    fail_list.append('failed to backup job cnt')
  elif results[0][0] != 0:
    fail_list.append("""still has backup job, upgrade is not allowed temporarily""")
  else:
    logging.info('check backup job success')

# 11. 检查无归档任务
def check_archive_job_exist(query_cur):
  min_cluster_version = 0
  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif len(results[0]) != 1:
    fail_list.append('column cnt not match')
  else:
    min_cluster_version = get_version(results[0][0])

    # Archive jobs cannot be in-progress before upgrade from 4.0.
    if min_cluster_version < get_version("4.1.0.0"):
      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_ARCHIVELOG where status!='STOP'""")
      if len(results) != 1 or len(results[0]) != 1:
        fail_list.append('failed to archive job cnt')
      elif results[0][0] != 0:
        fail_list.append("""still has archive job, upgrade is not allowed temporarily""")
      else:
        logging.info('check archive job success')

# 12. 检查归档路径是否清空
def check_archive_dest_exist(query_cur):
  min_cluster_version = 0
  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif len(results[0]) != 1:
    fail_list.append('column cnt not match')
  else:
    min_cluster_version = get_version(results[0][0])
    # archive dest need to be cleaned before upgrade from 4.0.
    if min_cluster_version < get_version("4.1.0.0"):
      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_ARCHIVE_DEST""")
      if len(results) != 1 or len(results[0]) != 1:
        fail_list.append('failed to archive dest cnt')
      elif results[0][0] != 0:
        fail_list.append("""still has archive destination, upgrade is not allowed temporarily""")
      else:
        logging.info('check archive destination success')

# 13. 检查备份路径是否清空
def check_backup_dest_exist(query_cur):
  min_cluster_version = 0
  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1:
    fail_list.append('min_observer_version is not sync')
  elif len(results[0]) != 1:
    fail_list.append('column cnt not match')
  else:
    min_cluster_version = get_version(results[0][0])
    # backup dest need to be cleaned before upgrade from 4.0.
    if min_cluster_version < get_version("4.1.0.0"):
      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_BACKUP_PARAMETER where name='data_backup_dest' and (value!=NULL or value!='')""")
      if len(results) != 1 or len(results[0]) != 1:
        fail_list.append('failed to data backup dest cnt')
      elif results[0][0] != 0:
        fail_list.append("""still has backup destination, upgrade is not allowed temporarily""")
      else:
        logging.info('check backup destination success')

def check_server_version(query_cur):
    sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server""";
    (desc, results) = query_cur.exec_query(sql);
    if len(results) != 1:
      fail_list.append("servers build_version not match")
    else:
      logging.info("check server version success")

# 14. 检查server是否可服务
def check_observer_status(query_cur):
  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_server where (start_service_time <= 0 or status != "active")""")
  if results[0][0] > 0 :
    fail_list.append('{0} observer not available , please check'.format(results[0][0]))
  logging.info('check observer status success')

# 15  检查schema是否刷新成功
def check_schema_status(query_cur):
  (desc, results) = query_cur.exec_query("""select if (a.cnt = b.cnt, 1, 0) as passed from (select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version > 1 and refreshed_schema_version % 8 = 0) as a join (select count(*) as cnt from oceanbase.__all_server join oceanbase.__all_tenant) as b""")
  if results[0][0] != 1 :
    fail_list.append('{0} schema not available, please check'.format(results[0][0]))
  logging.info('check schema status success')

# 16. 检查是否存在名为all/all_user/all_meta的租户
def check_not_supported_tenant_name(query_cur):
  names = ["all", "all_user", "all_meta"]
  (desc, results) = query_cur.exec_query("""select tenant_name from oceanbase.DBA_OB_TENANTS""")
  for i in range(len(results)):
    if results[i][0].lower() in names:
      fail_list.append('a tenant named all/all_user/all_meta (case insensitive) cannot exist in the cluster, please rename the tenant')
      break
  logging.info('check special tenant name success')

# last check of do_check, make sure no function execute after check_fail_list
def check_fail_list():
  if len(fail_list) != 0 :
     error_msg ="upgrade checker failed with " + str(len(fail_list)) + " reasons: " + ", ".join(['['+x+"] " for x in fail_list])
     raise MyError(error_msg)

def set_query_timeout(query_cur, timeout):
  if timeout != 0:
    sql = """set @@session.ob_query_timeout = {0}""".format(timeout * 1000 * 1000)
    query_cur.exec_sql(sql)

# 开始升级前的检查
def do_check(my_host, my_port, my_user, my_passwd, timeout, upgrade_params):
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
      query_cur = Cursor(cur)
      set_query_timeout(query_cur, timeout)
      check_observer_version(query_cur, upgrade_params)
      check_data_version(query_cur)
      check_paxos_replica(query_cur)
      check_rebalance_task(query_cur)
      check_cluster_status(query_cur)
      check_tenant_status(query_cur)
      check_restore_job_exist(query_cur)
      check_tenant_primary_zone(query_cur)
      check_ddl_task_execute(query_cur)
      check_backup_job_exist(query_cur)
      check_archive_job_exist(query_cur)
      check_archive_dest_exist(query_cur)
      check_backup_dest_exist(query_cur)
      check_observer_status(query_cur)
      check_schema_status(query_cur)
      check_server_version(query_cur)
      check_not_supported_tenant_name(query_cur)
      # all check func should execute before check_fail_list
      check_fail_list()
      modify_server_permanent_offline_time(cur)
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
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", timeout=\"%s\", log-file=\"%s\"',\
          host, port, user, password, timeout, log_filename)
      do_check(host, port, user, password, timeout, upgrade_params)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      raise e
    except Exception, e:
      logging.exception('normal error')
      raise e
