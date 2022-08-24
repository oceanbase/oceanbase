#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import time

class UpgradeParams:
  log_filename = 'upgrade_post_checker.log'
  new_version = '3.1.5'
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

#### STAR
# 1 检查版本号
def check_cluster_version(query_cur):
  # 一方面配置项生效是个异步生效任务,另一方面是2.2.0之后新增租户级配置项刷新，和系统级配置项刷新复用同一个timer，这里暂且等一下。
  times = 30
  sql="select distinct value = '{0}' from oceanbase.__all_virtual_sys_parameter_stat where name='min_observer_version'".format(upgrade_params.new_version)
  while times > 0 :
    (desc, results) = query_cur.exec_query(sql)
    if len(results) == 1 and results[0][0] == 1:
      break;
    time.sleep(1)
    times -=1
  if times == 0:
    logging.warn("check cluster version timeout!")
    raise e
  else:
    logging.info("check_cluster_version success")

def check_storage_format_version(query_cur):
  # Specified expected version each time want to upgrade (see OB_STORAGE_FORMAT_VERSION_MAX)
  expect_version = 4;
  sql = "select value from oceanbase.__all_zone where zone = '' and name = 'storage_format_version'"
  times = 180
  while times > 0 :
    (desc, results) = query_cur.exec_query(sql)
    if len(results) == 1 and results[0][0] == expect_version:
      break
    time.sleep(10)
    times -= 1
  if times == 0:
    logging.warn("check storage format version timeout! Expected version {0}".format(expect_version))
    raise e
  else:
    logging.info("check expected storage format version '{0}' success".format(expect_version))

def upgrade_table_schema_version(conn, cur):
  try:
    sql = """SELECT * FROM v$ob_cluster
             WHERE cluster_role = "PRIMARY"
             AND cluster_status = "VALID"
             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
    (desc, results) = cur.exec_query(sql)
    is_primary = len(results) > 0
    if is_primary:
      sql = "set @@session.ob_query_timeout = 60000000;"
      logging.info(sql)
      cur.exec_sql(sql)

      sql = "alter system run job 'UPDATE_TABLE_SCHEMA_VERSION';"
      logging.info(sql)
      cur.exec_sql(sql)
    else:
      logging.info("standby cluster no need to run job update_table_schema_ersion")
  except Exception, e:
    logging.warn("update table schema failed")
    raise MyError("update table schema failed")
  logging.info("update table schema finish")

def upgrade_storage_format_version(conn, cur):
  try:
    # enable_ddl
    set_parameter(cur, 'enable_ddl', 'True')

    # run job
    sql = "alter system run job 'UPGRADE_STORAGE_FORMAT_VERSION';"
    logging.info(sql)
    cur.execute(sql)

  except Exception, e:
    logging.warn("upgrade storage format version failed")
    raise e
  logging.info("upgrade storage format version finish")

# 2 检查内部表自检是否成功
def check_root_inspection(query_cur):
  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where info != 'succeed'"
  times = 180
  while times > 0 :
    (desc, results) = query_cur.exec_query(sql)
    if results[0][0] == 0:
      break
    time.sleep(10)
    times -= 1
  if times == 0:
    logging.warn('check root inspection failed!')
    raise e
  logging.info('check root inspection success')

# 4 开ddl
def enable_ddl(cur):
  set_parameter(cur, 'enable_ddl', 'True')

# 5 打开rebalance
def enable_rebalance(cur):
  set_parameter(cur, 'enable_rebalance', 'True')

# 6 打开rereplication
def enable_rereplication(cur):
  set_parameter(cur, 'enable_rereplication', 'True')

# 7 打开major freeze
def enable_major_freeze(cur):
  set_parameter(cur, 'enable_major_freeze', 'True')
  
# 8 打开sql走px
def enable_px_inner_sql(query_cur, cur):
  cur.execute("alter system set _ob_enable_px_for_inner_sql = True")
  (desc, results) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
  if (len(results) != 1) :
    raise MyError('cluster role results is not valid')
  elif (cmp(results[0][0], "PRIMARY") == 0):
    tenant_id_list = fetch_tenant_ids(query_cur)
    for tenant_id in tenant_id_list:
      cur.execute("alter system change tenant tenant_id = {0}".format(tenant_id))
      sql = """set global _ob_use_parallel_execution = true"""
      logging.info("tenant_id : %d , %s", tenant_id, sql)
      cur.execute(sql)
  elif (cmp(results[0][0], "PHYSICAL STANDBY") == 0):
    sql = """set global _ob_use_parallel_execution = true"""
    cur.execute(sql)
    logging.info("execute sql in standby: %s", sql)

def execute_schema_split_v2(conn, cur, query_cur, user, pwd):
  try:
    # check local cluster finish schema split
    done = check_schema_split_v2_finish(query_cur)
    if not done:
      ###### disable ddl
      set_parameter(cur, 'enable_ddl', 'False')

      # run job
      sql = """alter system run job 'SCHEMA_SPLIT_V2'"""
      logging.info("""run job 'SCHEMA_SPLIT_V2'""")
      query_cur.exec_sql(sql)
      # check schema split v2 finish
      check_schema_split_v2_finish_until_timeout(query_cur, 360)

    # primary cluster should wait standby clusters' schema split results
    is_primary = check_current_cluster_is_primary(query_cur)
    if is_primary:
      standby_cluster_list = fetch_standby_cluster_infos(conn, query_cur, user, pwd)
      for standby_cluster in standby_cluster_list:
        # connect
        logging.info("start to check schema split result by cluster: cluster_id = {0}"
                     .format(standby_cluster['cluster_id']))
        logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
                     .format(standby_cluster['cluster_id'],
                             standby_cluster['ip'],
                             standby_cluster['port']))
        tmp_conn = mysql.connector.connect(user     =  standby_cluster['user'],
                                           password =  standby_cluster['pwd'],
                                           host     =  standby_cluster['ip'],
                                           port     =  standby_cluster['port'],
                                           database =  'oceanbase')
        tmp_cur = tmp_conn.cursor(buffered=True)
        tmp_conn.autocommit = True
        tmp_query_cur = Cursor(tmp_cur)
        # check if stanby cluster
        is_primary = check_current_cluster_is_primary(tmp_query_cur)
        if is_primary:
          logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                            .format(standby_cluster['cluster_id'],
                                    standby_cluster['ip'],
                                    standby_cluster['port']))
          raise e
        # check schema split finish
        check_schema_split_v2_finish_until_timeout(tmp_query_cur, 180)
        # close
        tmp_cur.close()
        tmp_conn.close()
        logging.info("""check schema split result success : cluster_id = {0}, ip = {1}, port = {2}"""
                        .format(standby_cluster['cluster_id'],
                                standby_cluster['ip'],
                                standby_cluster['port']))
  except Exception, e:
    logging.warn("execute schema_split_v2 failed")
    raise e
  logging.info("execute schema_split_v2 success")

def check_schema_split_v2_finish(query_cur):
  done = False;
  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where name = 'SCHEMA_SPLIT_V2' and info = 'succeed'"
  logging.info(sql)
  (desc, results) = query_cur.exec_query(sql)
  if 1 != len(results) or 1 != len(results[0]):
    logging.warn("should has one record")
    raise e
  elif 1 == results[0][0]:
    done = True
  else:
    done = False
  return done

def check_schema_split_v2_finish_until_timeout(query_cur, times):
  while times > 0:
    done = check_schema_split_v2_finish(query_cur)
    if done:
      break;
    else:
      times -= 1
      time.sleep(10)
    if 0 == times:
      logging.warn('check schema split v2 timeout!')
      raise e
  logging.info("check schema split v2 success")

def fetch_tenant_ids(query_cur):
  try:
    tenant_id_list = []
    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
    for r in results:
      tenant_id_list.append(r[0])
    return tenant_id_list
  except Exception, e:
    logging.exception('fail to fetch distinct tenant ids')
    raise e

def check_current_cluster_is_primary(query_cur):
  try:
    sql = """SELECT * FROM v$ob_cluster
             WHERE cluster_role = "PRIMARY"
             AND cluster_status = "VALID"
             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
    (desc, results) = query_cur.exec_query(sql)
    is_primary = len(results) > 0
    return is_primary
  except Exception, e:
    logging.exception("""fail to check current is primary""")
    raise e

def fetch_standby_cluster_infos(conn, query_cur, user, pwd):
  try:
    is_primary = check_current_cluster_is_primary(query_cur)
    if not is_primary:
      logging.exception("""should be primary cluster""")
      raise e

    standby_cluster_infos = []
    sql = """SELECT cluster_id, rootservice_list from v$ob_standby_status"""
    (desc, results) = query_cur.exec_query(sql)

    for r in results:
      standby_cluster_info = {}
      if 2 != len(r):
        logging.exception("length not match")
        raise e
      standby_cluster_info['cluster_id'] = r[0]
      standby_cluster_info['user'] = user
      standby_cluster_info['pwd'] = pwd
      # construct ip/port
      address = r[1].split(";")[0] # choose first address in rs_list
      standby_cluster_info['ip'] = str(address.split(":")[0])
      standby_cluster_info['port'] = address.split(":")[2]
      # append
      standby_cluster_infos.append(standby_cluster_info)
      logging.info("""cluster_info :  cluster_id = {0}, ip = {1}, port = {2}"""
                   .format(standby_cluster_info['cluster_id'],
                           standby_cluster_info['ip'],
                           standby_cluster_info['port']))
    conn.commit()
    # check standby cluster
    for standby_cluster_info in standby_cluster_infos:
      # connect
      logging.info("""create connection : cluster_id = {0}, ip = {1}, port = {2}"""
                   .format(standby_cluster_info['cluster_id'],
                           standby_cluster_info['ip'],
                           standby_cluster_info['port']))

      tmp_conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
                                         password =  standby_cluster_info['pwd'],
                                         host     =  standby_cluster_info['ip'],
                                         port     =  standby_cluster_info['port'],
                                         database =  'oceanbase')

      tmp_cur = tmp_conn.cursor(buffered=True)
      tmp_conn.autocommit = True
      tmp_query_cur = Cursor(tmp_cur)
      is_primary = check_current_cluster_is_primary(tmp_query_cur)
      if is_primary:
        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                          .format(standby_cluster_info['cluster_id'],
                                  standby_cluster_info['ip'],
                                  standby_cluster_info['port']))
        raise e
      # close
      tmp_cur.close()
      tmp_conn.close()

    return standby_cluster_infos
  except Exception, e:
    logging.exception('fail to fetch standby cluster info')
    raise e

def check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_ids):
  try:
    conn.commit()
    # check if need check ddl and dml sync
    is_primary = check_current_cluster_is_primary(query_cur)
    if not is_primary:
      logging.exception("""should be primary cluster""")
      raise e

    # fetch sys stats
    sys_infos = []
    sql = """SELECT tenant_id,
                    refreshed_schema_version,
                    min_sys_table_scn,
                    min_user_table_scn
             FROM oceanbase.v$ob_cluster_stats
             ORDER BY tenant_id desc"""
    (desc, results) = query_cur.exec_query(sql)
    if len(tenant_ids) != len(results):
      logging.exception("result not match")
      raise e
    else:
      for i in range(len(results)):
        if len(results[i]) != 4:
          logging.exception("length not match")
          raise e
        elif results[i][0] != tenant_ids[i]:
          logging.exception("tenant_id not match")
          raise e
        else:
          sys_info = {}
          sys_info['tenant_id'] = results[i][0]
          sys_info['refreshed_schema_version'] = results[i][1]
          sys_info['min_sys_table_scn'] = results[i][2]
          sys_info['min_user_table_scn'] = results[i][3]
          logging.info("sys info : {0}".format(sys_info))
          sys_infos.append(sys_info)
    conn.commit()

    # check ddl and dml by cluster
    for standby_cluster_info in standby_cluster_infos:
      check_ddl_and_dml_sync_by_cluster(standby_cluster_info, sys_infos)

  except Exception, e:
    logging.exception("fail to check ddl and dml sync")
    raise e

def check_ddl_and_dml_sync_by_tenant(query_cur, sys_info):
  try:
    times = 1800 # 30min
    logging.info("start to check ddl and dml sync by tenant : {0}".format(sys_info))
    start_time = time.time()
    sql = ""
    if 1 == sys_info['tenant_id'] :
      # 备库系统租户DML不走物理同步，需要升级脚本负责写入，系统租户仅校验DDL同步
      sql = """SELECT count(*)
               FROM oceanbase.v$ob_cluster_stats
               WHERE tenant_id = {0}
                     AND refreshed_schema_version >= {1}
            """.format(sys_info['tenant_id'],
                       sys_info['refreshed_schema_version'])
    else:
      sql = """SELECT count(*)
               FROM oceanbase.v$ob_cluster_stats
               WHERE tenant_id = {0}
                     AND refreshed_schema_version >= {1}
                     AND min_sys_table_scn >= {2}
                     AND min_user_table_scn >= {3}
            """.format(sys_info['tenant_id'],
                       sys_info['refreshed_schema_version'],
                       sys_info['min_sys_table_scn'],
                       sys_info['min_user_table_scn'])
    while times > 0 :
      (desc, results) = query_cur.exec_query(sql)
      if len(results) == 1 and results[0][0] == 1:
        break;
      time.sleep(1)
      times -= 1
    if times == 0:
      logging.exception("check ddl and dml sync timeout! : {0}, cost = {1}"
                    .format(sys_info, time.time() - start_time))
      raise e
    else:
      logging.info("check ddl and dml sync success! : {0}, cost = {1}"
                   .format(sys_info, time.time() - start_time))

  except Exception, e:
    logging.exception("fail to check ddl and dml sync : {0}".format(sys_info))
    raise e

# 开始升级后的检查
def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
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
      try:
        check_cluster_version(query_cur)
        upgrade_table_schema_version(conn, query_cur)
        check_root_inspection(query_cur)
        enable_ddl(cur)
        enable_rebalance(cur)
        enable_rereplication(cur)
        enable_major_freeze(cur)
        enable_px_inner_sql(query_cur, cur)
      except Exception, e:
        logging.exception('run error')
        raise e
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
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
          host, port, user, password, log_filename)
      do_check(host, port, user, password, upgrade_params)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      raise e
    except Exception, e:
      logging.exception('normal error')
      raise e

