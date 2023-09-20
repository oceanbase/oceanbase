#!/usr/bin/env python
# -*- coding: utf-8 -*-
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:__init__.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:actions.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import time
#import re
#import json
#import traceback
#import sys
#import mysql.connector
#from mysql.connector import errorcode
#from my_error import MyError
#import logging
#
#class SqlItem:
#  action_sql = None
#  rollback_sql = None
#  def __init__(self, action_sql, rollback_sql):
#    self.action_sql = action_sql
#    self.rollback_sql = rollback_sql
#
#current_cluster_version = "4.3.0.0"
#current_data_version = "4.3.0.0"
#g_succ_sql_list = []
#g_commit_sql_list = []
#
#def get_current_cluster_version():
#  return current_cluster_version
#
#def get_current_data_version():
#  return current_data_version
#
#def refresh_commit_sql_list():
#  global g_succ_sql_list
#  global g_commit_sql_list
#  if len(g_commit_sql_list) < len(g_succ_sql_list):
#    for i in range(len(g_commit_sql_list), len(g_succ_sql_list)):
#      g_commit_sql_list.append(g_succ_sql_list[i])
#
#def get_succ_sql_list_str():
#  global g_succ_sql_list
#  ret_str = ''
#  for i in range(0, len(g_succ_sql_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += g_succ_sql_list[i].action_sql + ';'
#  return ret_str
#
#def get_commit_sql_list_str():
#  global g_commit_sql_list
#  ret_str = ''
#  for i in range(0, len(g_commit_sql_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += g_commit_sql_list[i].action_sql + ';'
#  return ret_str
#
#def get_rollback_sql_file_lines_str():
#  global g_commit_sql_list
#  ret_str = ''
#  g_commit_sql_list_len = len(g_commit_sql_list)
#  for i in range(0, g_commit_sql_list_len):
#    if i > 0:
#      ret_str += '\n'
#    idx = g_commit_sql_list_len - 1 - i
#    ret_str += '/*\n' + g_commit_sql_list[idx].action_sql + ';\n*/\n'
#    ret_str += g_commit_sql_list[idx].rollback_sql + ';'
#  return ret_str
#
#def dump_rollback_sql_to_file(rollback_sql_filename):
#  logging.info('===================== begin to dump rollback sql file ============================')
#  rollback_sql_file = open(rollback_sql_filename, 'w')
#  rollback_sql_file.write('# 此文件是回滚用的sql。\n')
#  rollback_sql_file.write('# 注释的sql是已经成功commit的sql，它的下一条没被注释的sql则是对应的回滚sql。回滚的sql的排序跟commit的sql的排序刚好相反。\n')
#  rollback_sql_file.write('# 跑升级脚本失败的时候可以参考本文件来进行回滚。\n')
#  rollback_sql_file.write('\n')
#  rollback_sql_file_lines_str = get_rollback_sql_file_lines_str()
#  rollback_sql_file.write(rollback_sql_file_lines_str)
#  rollback_sql_file.close()
#  logging.info('=========== succeed to dump rollback sql file to: ' + rollback_sql_filename + '===============')
#
#def check_is_ddl_sql(sql):
#  word_list = sql.split()
#  if len(word_list) < 1:
#    raise MyError('sql is empty, sql="{0}"'.format(sql))
#  key_word = word_list[0].lower()
#  if 'create' != key_word and 'alter' != key_word:
#    raise MyError('sql must be ddl, key_word="{0}", sql="{1}"'.format(key_word, sql))
#
#def check_is_query_sql(sql):
#  word_list = sql.split()
#  if len(word_list) < 1:
#    raise MyError('sql is empty, sql="{0}"'.format(sql))
#  key_word = word_list[0].lower()
#  if 'select' != key_word and 'show' != key_word and 'desc' != key_word:
#    raise MyError('sql must be query, key_word="{0}", sql="{1}"'.format(key_word, sql))
#
#def check_is_update_sql(sql):
#  word_list = sql.split()
#  if len(word_list) < 1:
#    raise MyError('sql is empty, sql="{0}"'.format(sql))
#  key_word = word_list[0].lower()
#  if 'insert' != key_word and 'update' != key_word and 'replace' != key_word and 'set' != key_word and 'delete' != key_word:
#    # 还有类似这种：select @current_ts := now()
#    if not (len(word_list) >= 3 and 'select' == word_list[0].lower()\
#        and word_list[1].lower().startswith('@') and ':=' == word_list[2].lower()):
#      raise MyError('sql must be update, key_word="{0}", sql="{1}"'.format(key_word, sql))
#
#def get_min_cluster_version(cur):
#  min_cluster_version = 0
#  sql = """select distinct value from oceanbase.GV$OB_PARAMETERS where name='min_observer_version'"""
#  logging.info(sql)
#  cur.execute(sql)
#  results = cur.fetchall()
#  if len(results) != 1:
#    logging.exception('min_observer_version is not sync')
#    raise e
#  elif len(results[0]) != 1:
#    logging.exception('column cnt not match')
#    raise e
#  else:
#    min_cluster_version = get_version(results[0][0])
#  return min_cluster_version
#
#def set_parameter(cur, parameter, value, timeout = 0):
#  sql = """alter system set {0} = '{1}'""".format(parameter, value)
#  logging.info(sql)
#  cur.execute(sql)
#  wait_parameter_sync(cur, False, parameter, value, timeout)
#
#def set_tenant_parameter(cur, parameter, value, timeout = 0):
#  tenants_list = []
#  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
#    tenants_list = ['all']
#  else:
#    tenants_list = ['sys', 'all_user', 'all_meta']
#  for tenants in tenants_list:
#    sql = """alter system set {0} = '{1}' tenant = '{2}'""".format(parameter, value, tenants)
#    logging.info(sql)
#    cur.execute(sql)
#  wait_parameter_sync(cur, True, parameter, value, timeout)
#
#def get_ori_enable_ddl(cur, timeout):
#  ori_value_str = fetch_ori_enable_ddl(cur)
#  wait_parameter_sync(cur, False, 'enable_ddl', ori_value_str, timeout)
#  ori_value = (ori_value_str == 'True')
#  return ori_value
#
#def fetch_ori_enable_ddl(cur):
#  ori_value = 'True'
#  sql = """select value from oceanbase.__all_sys_parameter where name = 'enable_ddl'"""
#
#  logging.info(sql)
#  cur.execute(sql)
#  result = cur.fetchall()
#
#  if len(result) == 0:
#    # means default value, is True
#    ori_value = 'True'
#  elif len(result) != 1 or len(result[0]) != 1:
#    logging.exception('result cnt not match')
#    raise e
#  elif result[0][0].lower() in ["1", "true", "on", "yes", 't']:
#    ori_value = 'True'
#  elif result[0][0].lower() in ["0", "false", "off", "no", 'f']:
#    ori_value = 'False'
#  else:
#    logging.exception("""result value is invalid, result:{0}""".format(result[0][0]))
#    raise e
#  return ori_value
#
## print version like "x.x.x.x"
#def print_version(version):
#  version = int(version)
#  major = (version >> 32) & 0xffffffff
#  minor = (version >> 16) & 0xffff
#  major_patch = (version >> 8) & 0xff
#  minor_patch = version & 0xff
#  version_str = "{0}.{1}.{2}.{3}".format(major, minor, major_patch, minor_patch)
#
## version str should like "x.x.x.x"
#def get_version(version_str):
#  versions = version_str.split(".")
#
#  if len(versions) != 4:
#    logging.exception("""version:{0} is invalid""".format(version_str))
#    raise e
#
#  major = int(versions[0])
#  minor = int(versions[1])
#  major_patch = int(versions[2])
#  minor_patch = int(versions[3])
#
#  if major > 0xffffffff or minor > 0xffff or major_patch > 0xff or minor_patch > 0xff:
#    logging.exception("""version:{0} is invalid""".format(version_str))
#    raise e
#
#  version = (major << 32) | (minor << 16) | (major_patch << 8) | (minor_patch)
#  return version
#
#def check_server_version_by_cluster(cur):
#  sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server""";
#  logging.info(sql)
#  cur.execute(sql)
#  result = cur.fetchall()
#  if len(result) != 1:
#    raise MyError("servers build_version not match")
#  else:
#    logging.info("check server version success")
#
#def check_parameter(cur, is_tenant_config, key, value):
#  table_name = "GV$OB_PARAMETERS" if not is_tenant_config else "__all_virtual_tenant_parameter_info"
#  sql = """select * from oceanbase.{0}
#           where name = '{1}' and value = '{2}'""".format(table_name, key, value)
#  logging.info(sql)
#  cur.execute(sql)
#  result = cur.fetchall()
#  bret = False
#  if len(result) > 0:
#    bret = True
#  else:
#    bret = False
#  return bret
#
#def wait_parameter_sync(cur, is_tenant_config, key, value, timeout):
#  table_name = "GV$OB_PARAMETERS" if not is_tenant_config else "__all_virtual_tenant_parameter_info"
#  sql = """select count(*) as cnt from oceanbase.{0}
#           where name = '{1}' and value != '{2}'""".format(table_name, key, value)
#  times = (timeout if timeout > 0 else 60) / 5
#  while times >= 0:
#    logging.info(sql)
#    cur.execute(sql)
#    result = cur.fetchall()
#    if len(result) != 1 or len(result[0]) != 1:
#      logging.exception('result cnt not match')
#      raise e
#    elif result[0][0] == 0:
#      logging.info("""{0} is sync, value is {1}""".format(key, value))
#      break
#    else:
#      logging.info("""{0} is not sync, value should be {1}""".format(key, value))
#
#    times -= 1
#    if times == -1:
#      logging.exception("""check {0}:{1} sync timeout""".format(key, value))
#      raise e
#    time.sleep(5)
#
#def do_begin_upgrade(cur, timeout):
#
#  if not check_parameter(cur, False, "enable_upgrade_mode", "True"):
#    action_sql = "alter system begin upgrade"
#    rollback_sql = "alter system end upgrade"
#    logging.info(action_sql)
#
#    cur.execute(action_sql)
#
#    global g_succ_sql_list
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#  wait_parameter_sync(cur, False, "enable_upgrade_mode", "True", timeout)
#
#
#def do_begin_rolling_upgrade(cur, timeout):
#
#  if not check_parameter(cur, False, "_upgrade_stage", "DBUPGRADE"):
#    action_sql = "alter system begin rolling upgrade"
#    rollback_sql = "alter system end upgrade"
#
#    logging.info(action_sql)
#    cur.execute(action_sql)
#
#    global g_succ_sql_list
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#  wait_parameter_sync(cur, False, "_upgrade_stage", "DBUPGRADE", timeout)
#
#
#def do_end_rolling_upgrade(cur, timeout):
#
#  # maybe in upgrade_post_check stage or never run begin upgrade
#  if check_parameter(cur, False, "enable_upgrade_mode", "False"):
#    return
#
#  current_cluster_version = get_current_cluster_version()
#  if not check_parameter(cur, False, "_upgrade_stage", "POSTUPGRADE") or not check_parameter(cur, False, "min_observer_version", current_cluster_version):
#    action_sql = "alter system end rolling upgrade"
#    rollback_sql = "alter system end upgrade"
#
#    logging.info(action_sql)
#    cur.execute(action_sql)
#
#    global g_succ_sql_list
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#  wait_parameter_sync(cur, False, "min_observer_version", current_data_version, timeout)
#  wait_parameter_sync(cur, False, "_upgrade_stage", "POSTUPGRADE", timeout)
#
#
#def do_end_upgrade(cur, timeout):
#
#  if not check_parameter(cur, False, "enable_upgrade_mode", "False"):
#    action_sql = "alter system end upgrade"
#    rollback_sql = ""
#
#    logging.info(action_sql)
#    cur.execute(action_sql)
#
#    global g_succ_sql_list
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#  wait_parameter_sync(cur, False, "enable_upgrade_mode", "False", timeout)
#
#def do_suspend_merge(cur, timeout):
#  tenants_list = []
#  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
#    tenants_list = ['all']
#  else:
#    tenants_list = ['sys', 'all_user', 'all_meta']
#  for tenants in tenants_list:
#    action_sql = "alter system suspend merge tenant = {0}".format(tenants)
#    rollback_sql = "alter system resume merge tenant = {0}".format(tenants)
#    logging.info(action_sql)
#    cur.execute(action_sql)
#
#def do_resume_merge(cur, timeout):
#  tenants_list = []
#  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
#    tenants_list = ['all']
#  else:
#    tenants_list = ['sys', 'all_user', 'all_meta']
#  for tenants in tenants_list:
#    action_sql = "alter system resume merge tenant = {0}".format(tenants)
#    rollback_sql = "alter system suspend merge tenant = {0}".format(tenants)
#    logging.info(action_sql)
#    cur.execute(action_sql)
#
#class Cursor:
#  __cursor = None
#  def __init__(self, cursor):
#    self.__cursor = cursor
#  def exec_sql(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
#      return rowcount
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
#  def exec_query(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      results = self.__cursor.fetchall()
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
#      return (self.__cursor.description, results)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
#
#class DDLCursor:
#  _cursor = None
#  def __init__(self, cursor):
#    self._cursor = Cursor(cursor)
#  def exec_ddl(self, sql, print_when_succ = True):
#    try:
#      # 这里检查是不是ddl，不是ddl就抛错
#      check_is_ddl_sql(sql)
#      return self._cursor.exec_sql(sql, print_when_succ)
#    except Exception, e:
#      logging.exception('fail to execute ddl: %s', sql)
#      raise e
#
#class QueryCursor:
#  _cursor = None
#  def __init__(self, cursor):
#    self._cursor = Cursor(cursor)
#  def exec_query(self, sql, print_when_succ = True):
#    try:
#      # 这里检查是不是query，不是query就抛错
#      check_is_query_sql(sql)
#      return self._cursor.exec_query(sql, print_when_succ)
#    except Exception, e:
#      logging.exception('fail to execute dml query: %s', sql)
#      raise e
#
#class DMLCursor(QueryCursor):
#  def exec_update(self, sql, print_when_succ = True):
#    try:
#      # 这里检查是不是update，不是update就抛错
#      check_is_update_sql(sql)
#      return self._cursor.exec_sql(sql, print_when_succ)
#    except Exception, e:
#      logging.exception('fail to execute dml update: %s', sql)
#      raise e
#
#class BaseDDLAction():
#  __ddl_cursor = None
#  _query_cursor = None
#  def __init__(self, cursor):
#    self.__ddl_cursor = DDLCursor(cursor)
#    self._query_cursor = QueryCursor(cursor)
#  def do_action(self):
#    global g_succ_sql_list
#    action_sql = self.get_action_ddl()
#    rollback_sql = self.get_rollback_sql()
#    self.__ddl_cursor.exec_ddl(action_sql)
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#    # ddl马上就提交了，因此刷新g_commit_sql_list
#    refresh_commit_sql_list()
#
#class BaseDMLAction():
#  __dml_cursor = None
#  _query_cursor = None
#  def __init__(self, cursor):
#    self.__dml_cursor = DMLCursor(cursor)
#    self._query_cursor = QueryCursor(cursor)
#  def do_action(self):
#    global g_succ_sql_list
#    action_sql = self.get_action_dml()
#    rollback_sql = self.get_rollback_sql()
#    self.__dml_cursor.exec_update(action_sql)
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#class BaseEachTenantDMLAction():
#  __dml_cursor = None
#  _query_cursor = None
#  _tenant_id_list = None
#  _cursor = None
#  def __init__(self, cursor, tenant_id_list):
#    self.__dml_cursor = DMLCursor(cursor)
#    self._query_cursor = QueryCursor(cursor)
#    self._tenant_id_list = tenant_id_list
#    self._cursor = Cursor(cursor)
#  def get_tenant_id_list(self):
#    return self._tenant_id_list
#  def do_each_tenant_action(self, tenant_id):
#    global g_succ_sql_list
#    action_sql = self.get_each_tenant_action_dml(tenant_id)
#    rollback_sql = self.get_each_tenant_rollback_sql(tenant_id)
#    self.__dml_cursor.exec_update(action_sql)
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#
#class BaseEachTenantDDLAction():
#  __dml_cursor = None
#  _query_cursor = None
#  _tenant_id_list = None
#  _all_table_name = "__all_table"
#  def __init__(self, cursor, tenant_id_list):
#    self.__ddl_cursor = DDLCursor(cursor)
#    self._query_cursor = QueryCursor(cursor)
#    self._tenant_id_list = tenant_id_list
#  def get_tenant_id_list(self):
#    return self._tenant_id_list
#  def get_all_table_name(self):
#    return self._all_table_name
#  def set_all_table_name(self, table_name):
#    self._all_table_name = table_name
#  def do_each_tenant_action(self, tenant_id):
#    global g_succ_sql_list
#    action_sql = self.get_each_tenant_action_ddl(tenant_id)
#    rollback_sql = self.get_each_tenant_rollback_sql(tenant_id)
#    self.__ddl_cursor.exec_ddl(action_sql)
#    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
#    # ddl马上就提交了，因此刷新g_commit_sql_list
#    refresh_commit_sql_list()
#
#def actions_cls_compare(x, y):
#  diff = x.get_seq_num() - y.get_seq_num()
#  if 0 == diff:
#    raise MyError('seq num is equal')
#  elif diff < 0:
#    return -1
#  else:
#    return 1
#
#def reflect_action_cls_list(action_module, action_name_prefix):
#  action_cls_list = []
#  cls_from_actions = dir(action_module)
#  for cls in cls_from_actions:
#    if cls.startswith(action_name_prefix):
#      action_cls = getattr(action_module, cls)
#      action_cls_list.append(action_cls)
#  action_cls_list.sort(actions_cls_compare)
#  return action_cls_list
#
#def fetch_observer_version(cur):
#  sql = """select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'"""
#  logging.info(sql)
#  cur.execute(sql)
#  result = cur.fetchall()
#  if len(result) != 1:
#    raise MyError('query results count is not 1')
#  else:
#    logging.info('get observer version success, version = {0}'.format(result[0][0]))
#  return result[0][0]
#
#def fetch_tenant_ids(query_cur):
#  try:
#    tenant_id_list = []
#    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
#    for r in results:
#      tenant_id_list.append(r[0])
#    return tenant_id_list
#  except Exception, e:
#    logging.exception('fail to fetch distinct tenant ids')
#    raise e
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:config.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#pre_upgrade_log_filename = 'upgrade_pre.log'
#pre_upgrade_sql_filename = 'upgrade_sql_pre.txt'
#pre_upgrade_rollback_sql_filename = 'rollback_sql_pre.txt'
#
#post_upgrade_log_filename = 'upgrade_post.log'
#post_upgrade_sql_filename = 'upgrade_sql_post.txt'
#post_upgrade_rollback_sql_filename = 'rollback_sql_post.txt'
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:do_upgrade_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import sys
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#import json
#import config
#import opts
#import run_modules
#import actions
#import upgrade_health_checker
#import tenant_upgrade_action
#import upgrade_post_checker
#
## 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户
#
#class UpgradeParams:
#  log_filename = config.post_upgrade_log_filename
#  sql_dump_filename = config.post_upgrade_sql_filename
#  rollback_sql_filename =  config.post_upgrade_rollback_sql_filename
#
#def config_logging_module(log_filenamme):
#  logging.basicConfig(level=logging.INFO,\
#      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
#      datefmt='%Y-%m-%d %H:%M:%S',\
#      filename=log_filenamme,\
#      filemode='w')
#  # 定义日志打印格式
#  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
#  #######################################
#  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
#  stdout_handler = logging.StreamHandler(sys.stdout)
#  stdout_handler.setLevel(logging.INFO)
#  # 设置日志打印格式
#  stdout_handler.setFormatter(formatter)
#  # 将定义好的stdout_handler日志handler添加到root logger
#  logging.getLogger('').addHandler(stdout_handler)
#
#def print_stats():
#  logging.info('==================================================================================')
#  logging.info('============================== STATISTICS BEGIN ==================================')
#  logging.info('==================================================================================')
#  logging.info('succeed run sql(except sql of special actions): \n\n%s\n', actions.get_succ_sql_list_str())
#  logging.info('commited sql(except sql of special actions): \n\n%s\n', actions.get_commit_sql_list_str())
#  logging.info('==================================================================================')
#  logging.info('=============================== STATISTICS END ===================================')
#  logging.info('==================================================================================')
#
#def do_upgrade(my_host, my_port, my_user, my_passwd, timeout, my_module_set, upgrade_params):
#  try:
#    conn = mysql.connector.connect(user = my_user,
#                                   password = my_passwd,
#                                   host = my_host,
#                                   port = my_port,
#                                   database = 'oceanbase',
#                                   raise_on_warnings = True)
#    cur = conn.cursor(buffered=True)
#    try:
#      query_cur = actions.QueryCursor(cur)
#      actions.check_server_version_by_cluster(cur)
#      conn.commit()
#
#      if run_modules.MODULE_HEALTH_CHECK in my_module_set:
#        logging.info('================begin to run health check action ===============')
#        upgrade_health_checker.do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout, False)  # need_check_major_status = False
#        logging.info('================succeed to run health check action ===============')
#
#      if run_modules.MODULE_END_ROLLING_UPGRADE in my_module_set:
#        logging.info('================begin to run end rolling upgrade action ===============')
#        conn.autocommit = True
#        actions.do_end_rolling_upgrade(cur, timeout)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run end rolling upgrade action ===============')
#
#      if run_modules.MODULE_TENANT_UPRADE in my_module_set:
#        logging.info('================begin to run tenant upgrade action ===============')
#        conn.autocommit = True
#        tenant_upgrade_action.do_upgrade(conn, cur, timeout, my_user, my_passwd)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run tenant upgrade action ===============')
#
#      if run_modules.MODULE_END_UPRADE in my_module_set:
#        logging.info('================begin to run end upgrade action ===============')
#        conn.autocommit = True
#        actions.do_end_upgrade(cur, timeout)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run end upgrade action ===============')
#
#      if run_modules.MODULE_POST_CHECK in my_module_set:
#        logging.info('================begin to run post check action ===============')
#        conn.autocommit = True
#        upgrade_post_checker.do_check(conn, cur, query_cur, timeout)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run post check action ===============')
#
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      # 打印统计信息
#      print_stats()
#      # 将回滚sql写到文件中
#      # actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
#      cur.close()
#      conn.close()
#  except mysql.connector.Error, e:
#    logging.exception('connection error')
#    raise e
#  except Exception, e:
#    logging.exception('normal error')
#    raise e
#
#def do_upgrade_by_argv(argv):
#  upgrade_params = UpgradeParams()
#  opts.change_opt_defult_value('log-file', upgrade_params.log_filename)
#  opts.parse_options(argv)
#  if not opts.has_no_local_opts():
#    opts.deal_with_local_opts('upgrade_post')
#  else:
#    opts.check_db_client_opts()
#    log_filename = opts.get_opt_log_file()
#    upgrade_params.log_filename = log_filename
#    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
#    config_logging_module(upgrade_params.log_filename)
#    try:
#      host = opts.get_opt_host()
#      port = int(opts.get_opt_port())
#      user = opts.get_opt_user()
#      password = opts.get_opt_password()
#      timeout = int(opts.get_opt_timeout())
#      cmd_module_str = opts.get_opt_module()
#      module_set = set([])
#      all_module_set = run_modules.get_all_module_set()
#      cmd_module_list = cmd_module_str.split(',')
#      for cmd_module in cmd_module_list:
#        if run_modules.ALL_MODULE == cmd_module:
#          module_set = module_set | all_module_set
#        elif cmd_module in all_module_set:
#          module_set.add(cmd_module)
#        else:
#          raise MyError('invalid module: {0}'.format(cmd_module))
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", timeout=\"%s\", module=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, timeout, module_set, log_filename)
#      do_upgrade(host, port, user, password, timeout, module_set, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
#      raise e
#
#
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:do_upgrade_pre.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import sys
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#
#import config
#import opts
#import run_modules
#import actions
#import special_upgrade_action_pre
#import upgrade_health_checker
#
## 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户
#
#class UpgradeParams:
#  log_filename = config.pre_upgrade_log_filename
#  sql_dump_filename = config.pre_upgrade_sql_filename
#  rollback_sql_filename = config.pre_upgrade_rollback_sql_filename
#
#def config_logging_module(log_filenamme):
#  logging.basicConfig(level=logging.INFO,\
#      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
#      datefmt='%Y-%m-%d %H:%M:%S',\
#      filename=log_filenamme,\
#      filemode='w')
#  # 定义日志打印格式
#  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
#  #######################################
#  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
#  stdout_handler = logging.StreamHandler(sys.stdout)
#  stdout_handler.setLevel(logging.INFO)
#  # 设置日志打印格式
#  stdout_handler.setFormatter(formatter)
#  # 将定义好的stdout_handler日志handler添加到root logger
#  logging.getLogger('').addHandler(stdout_handler)
#
#def print_stats():
#  logging.info('==================================================================================')
#  logging.info('============================== STATISTICS BEGIN ==================================')
#  logging.info('==================================================================================')
#  logging.info('succeed run sql(except sql of special actions): \n\n%s\n', actions.get_succ_sql_list_str())
#  logging.info('commited sql(except sql of special actions): \n\n%s\n', actions.get_commit_sql_list_str())
#  logging.info('==================================================================================')
#  logging.info('=============================== STATISTICS END ===================================')
#  logging.info('==================================================================================')
#
#def do_upgrade(my_host, my_port, my_user, my_passwd, timeout, my_module_set, upgrade_params):
#  try:
#    conn = mysql.connector.connect(user = my_user,
#                                   password = my_passwd,
#                                   host = my_host,
#                                   port = my_port,
#                                   database = 'oceanbase',
#                                   raise_on_warnings = True)
#    cur = conn.cursor(buffered=True)
#    try:
#      query_cur = actions.QueryCursor(cur)
#      actions.check_server_version_by_cluster(cur)
#
#      if run_modules.MODULE_BEGIN_UPGRADE in my_module_set:
#        logging.info('================begin to run begin upgrade action===============')
#        conn.autocommit = True
#        actions.do_begin_upgrade(cur, timeout)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run begin upgrade action===============')
#
#      if run_modules.MODULE_BEGIN_ROLLING_UPGRADE in my_module_set:
#        logging.info('================begin to run begin rolling upgrade action===============')
#        conn.autocommit = True
#        actions.do_begin_rolling_upgrade(cur, timeout)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run begin rolling upgrade action===============')
#
#      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
#        logging.info('================begin to run special action===============')
#        conn.autocommit = True
#        special_upgrade_action_pre.do_special_upgrade(conn, cur, timeout, my_user, my_passwd)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to run special action===============')
#
#      if run_modules.MODULE_HEALTH_CHECK in my_module_set:
#        logging.info('================begin to run health check action ===============')
#        upgrade_health_checker.do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout, True) # need_check_major_status = True
#        logging.info('================succeed to run health check action ===============')
#
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      # 打印统计信息
#      print_stats()
#      # 将回滚sql写到文件中
#      # actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
#      cur.close()
#      conn.close()
#  except mysql.connector.Error, e:
#    logging.exception('connection error')
#    raise e
#  except Exception, e:
#    logging.exception('normal error')
#    raise e
#
#def do_upgrade_by_argv(argv):
#  upgrade_params = UpgradeParams()
#  opts.change_opt_defult_value('log-file', upgrade_params.log_filename)
#  opts.parse_options(argv)
#  if not opts.has_no_local_opts():
#    opts.deal_with_local_opts('upgrade_pre')
#  else:
#    opts.check_db_client_opts()
#    log_filename = opts.get_opt_log_file()
#    upgrade_params.log_filename = log_filename
#    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
#    config_logging_module(upgrade_params.log_filename)
#    try:
#      host = opts.get_opt_host()
#      port = int(opts.get_opt_port())
#      user = opts.get_opt_user()
#      password = opts.get_opt_password()
#      timeout = int(opts.get_opt_timeout())
#      cmd_module_str = opts.get_opt_module()
#      module_set = set([])
#      all_module_set = run_modules.get_all_module_set()
#      cmd_module_list = cmd_module_str.split(',')
#      for cmd_module in cmd_module_list:
#        if run_modules.ALL_MODULE == cmd_module:
#          module_set = module_set | all_module_set
#        elif cmd_module in all_module_set:
#          module_set.add(cmd_module)
#        else:
#          raise MyError('invalid module: {0}'.format(cmd_module))
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", timeout=\"%s\", module=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, timeout, module_set, log_filename)
#      do_upgrade(host, port, user, password, timeout, module_set, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
#      raise e
#
#
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:my_error.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#class MyError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:my_utils.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import mysql.connector
#from mysql.connector import errorcode
#from my_error import MyError
#from actions import QueryCursor
#import logging
#
#def results_to_str(desc, results):
#  ret_str = ''
#  max_width_list = []
#  for col_desc in desc:
#    max_width_list.append(len(str(col_desc[0])))
#  col_count = len(max_width_list)
#  for result in results:
#    if col_count != len(result):
#      raise MyError('column count is not equal, desc column count: {0}, data column count: {1}'.format(col_count, len(result)))
#    for i in range(0, col_count):
#      result_col_width = len(str(result[i]))
#      if max_width_list[i] < result_col_width:
#        max_width_list[i] = result_col_width
#  # 打印列名
#  for i in range(0, col_count):
#    if i > 0:
#      ret_str += '    ' # 空四格
#    ret_str += str(desc[i][0])
#    # 补足空白
#    for j in range(0, max_width_list[i] - len(str(desc[i][0]))):
#      ret_str += ' '
#  # 打印数据
#  for result in results:
#    ret_str += '\n' # 先换行
#    for i in range(0, col_count):
#      if i > 0:
#        ret_str += '    ' # 空四格
#      ret_str += str(result[i])
#      # 补足空白
#      for j in range(0, max_width_list[i] - len(str(result[i]))):
#        ret_str += ' '
#  return ret_str
#
#def query_and_dump_results(query_cur, sql):
#  (desc, results) = query_cur.exec_query(sql)
#  result_str = results_to_str(desc, results)
#  logging.info('dump query results, sql: %s, results:\n%s', sql, result_str)
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:opts.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import sys
#import os
#import getopt
#
#pre_help_str = \
#"""
#Help:
#""" +\
#sys.argv[0] + """ [OPTIONS]""" +\
#'\n\n' +\
#'-I, --help          Display this help and exit.\n' +\
#'-V, --version       Output version information and exit.\n' +\
#'-h, --host=name     Connect to host.\n' +\
#'-P, --port=name     Port number to use for connection.\n' +\
#'-u, --user=name     User for login.\n' +\
#'-p, --password=name Password to use when connecting to server. If password is\n' +\
#'                    not given it\'s empty string "".\n' +\
#'-t, --timeout=name  Cmd/Query/Inspection execute timeout(s).\n' +\
#'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
#'                    the following strings:\n' +\
#'                    1. begin_upgrade \n' +\
#'                    2. begin_rolling_upgrade \n' +\
#'                    3. special_action \n' +\
#'                    4. health_check \n' +\
#'                    5. all: default value, run all sub modules above \n' +\
#'                    that all modules should be run. They are splitted by ",".\n' +\
#'                    For example: -m all, or --module=begin_upgrade,begin_rolling_upgrade,special_action\n' +\
#'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'
#
#post_help_str = \
#"""
#Help:
#""" +\
#sys.argv[0] + """ [OPTIONS]""" +\
#'\n\n' +\
#'-I, --help          Display this help and exit.\n' +\
#'-V, --version       Output version information and exit.\n' +\
#'-h, --host=name     Connect to host.\n' +\
#'-P, --port=name     Port number to use for connection.\n' +\
#'-u, --user=name     User for login.\n' +\
#'-p, --password=name Password to use when connecting to server. If password is\n' +\
#'                    not given it\'s empty string "".\n' +\
#'-t, --timeout=name  Cmd/Query/Inspection execute timeout(s).\n' +\
#'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
#'                    the following strings:\n' +\
#'                    1. health_check \n' +\
#'                    2. end_rolling_upgrade \n' +\
#'                    3. tenant_upgrade \n' +\
#'                    4. end_upgrade \n' +\
#'                    5. post_check \n' +\
#'                    6. all: default value, run all sub modules above \n' +\
#'                    that all modules should be run. They are splitted by ",".\n' +\
#'                    For example: -m all, or --module=health_check,end_rolling_upgrade\n' +\
#'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'
#
#version_str = """version 1.0.0"""
#
#class Option:
#  __g_short_name_set = set([])
#  __g_long_name_set = set([])
#  __short_name = None
#  __long_name = None
#  __is_with_param = None
#  __is_local_opt = None
#  __has_value = None
#  __value = None
#  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
#    if short_name in Option.__g_short_name_set:
#      raise MyError('duplicate option short name: {0}'.format(short_name))
#    elif long_name in Option.__g_long_name_set:
#      raise MyError('duplicate option long name: {0}'.format(long_name))
#    Option.__g_short_name_set.add(short_name)
#    Option.__g_long_name_set.add(long_name)
#    self.__short_name = short_name
#    self.__long_name = long_name
#    self.__is_with_param = is_with_param
#    self.__is_local_opt = is_local_opt
#    self.__has_value = False
#    if None != default_value:
#      self.set_value(default_value)
#  def is_with_param(self):
#    return self.__is_with_param
#  def get_short_name(self):
#    return self.__short_name
#  def get_long_name(self):
#    return self.__long_name
#  def has_value(self):
#    return self.__has_value
#  def get_value(self):
#    return self.__value
#  def set_value(self, value):
#    self.__value = value
#    self.__has_value = True
#  def is_local_opt(self):
#    return self.__is_local_opt
#  def is_valid(self):
#    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
#
#g_opts =\
#[\
#Option('I', 'help', False, True),\
#Option('V', 'version', False, True),\
#Option('h', 'host', True, False),\
#Option('P', 'port', True, False),\
#Option('u', 'user', True, False),\
#Option('t', 'timeout', True, False, 0),\
#Option('p', 'password', True, False, ''),\
## 要跑哪个模块，默认全跑
#Option('m', 'module', True, False, 'all'),\
## 日志文件路径，不同脚本的main函数中中会改成不同的默认值
#Option('l', 'log-file', True, False)
#]\
#
#def change_opt_defult_value(opt_long_name, opt_default_val):
#  global g_opts
#  for opt in g_opts:
#    if opt.get_long_name() == opt_long_name:
#      opt.set_value(opt_default_val)
#      return
#
#def has_no_local_opts():
#  global g_opts
#  no_local_opts = True
#  for opt in g_opts:
#    if opt.is_local_opt() and opt.has_value():
#      no_local_opts = False
#  return no_local_opts
#
#def check_db_client_opts():
#  global g_opts
#  for opt in g_opts:
#    if not opt.is_local_opt() and not opt.has_value():
#      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
#          .format(opt.get_short_name(), sys.argv[0]))
#
#def parse_option(opt_name, opt_val):
#  global g_opts
#  for opt in g_opts:
#    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
#      opt.set_value(opt_val)
#
#def parse_options(argv):
#  global g_opts
#  short_opt_str = ''
#  long_opt_list = []
#  for opt in g_opts:
#    if opt.is_with_param():
#      short_opt_str += opt.get_short_name() + ':'
#    else:
#      short_opt_str += opt.get_short_name()
#  for opt in g_opts:
#    if opt.is_with_param():
#      long_opt_list.append(opt.get_long_name() + '=')
#    else:
#      long_opt_list.append(opt.get_long_name())
#  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
#  for (opt_name, opt_val) in opts:
#    parse_option(opt_name, opt_val)
#  if has_no_local_opts():
#    check_db_client_opts()
#
#def deal_with_local_opt(opt, filename):
#  if 'help' == opt.get_long_name():
#    if 'upgrade_pre' == filename:
#      global pre_help_str
#      print pre_help_str
#    elif 'upgrade_post' == filename:
#      global post_help_str
#      print post_help_str
#    else:
#            raise MyError('not supported filename:{0} for help option'.format(filename))
#  elif 'version' == opt.get_long_name():
#    global version_str
#    print version_str
#
#def deal_with_local_opts(filename):
#  global g_opts
#  if has_no_local_opts():
#    raise MyError('no local options, can not deal with local options')
#  else:
#    for opt in g_opts:
#      if opt.is_local_opt() and opt.has_value():
#        deal_with_local_opt(opt, filename)
#        # 只处理一个
#        return
#
#def get_opt_host():
#  global g_opts
#  for opt in g_opts:
#    if 'host' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_port():
#  global g_opts
#  for opt in g_opts:
#    if 'port' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_user():
#  global g_opts
#  for opt in g_opts:
#    if 'user' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_password():
#  global g_opts
#  for opt in g_opts:
#    if 'password' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_timeout():
#  global g_opts
#  for opt in g_opts:
#    if 'timeout' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_module():
#  global g_opts
#  for opt in g_opts:
#    if 'module' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_log_file():
#  global g_opts
#  for opt in g_opts:
#    if 'log-file' == opt.get_long_name():
#      return opt.get_value()
#
##parse_options(sys.argv[1:])
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:reset_upgrade_scripts.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import os
#
#def clear_action_codes(action_filename_list, action_begin_line, \
#    action_end_line, is_special_upgrade_code):
#  char_enter = '\n'
#  for action_filename in action_filename_list:
#    new_action_file_lines = []
#    action_file = open(action_filename, 'r')
#    action_file_lines = action_file.readlines()
#    is_action_codes = False
#    for action_file_line in action_file_lines:
#      if is_action_codes and action_file_line == (action_end_line + char_enter):
#        is_action_codes = False
#      if not is_action_codes:
#        new_action_file_lines.append(action_file_line)
#      if not is_action_codes and action_file_line == (action_begin_line + char_enter):
#        is_action_codes = True
#    action_file.close()
#    new_action_file = open(action_filename, 'w')
#    for new_action_file_line in new_action_file_lines:
#      if is_special_upgrade_code:
#        if new_action_file_line == (action_end_line + char_enter):
#          new_action_file.write('  return\n')
#      new_action_file.write(new_action_file_line)
#    new_action_file.close()
#
#def regenerate_upgrade_script():
#  print('\n=========run gen_upgrade_scripts.py, begin=========\n')
#  info = os.popen('./gen_upgrade_scripts.py;')
#  print(info.read())
#  print('\n=========run gen_upgrade_scripts.py, end=========\n')
#
#if __name__ == '__main__':
#  action_begin_line = '####========******####======== actions begin ========####******========####'
#  action_end_line = '####========******####========= actions end =========####******========####'
#  action_filename_list = \
#      [\
#      'normal_ddl_actions_pre.py',\
#      'normal_ddl_actions_post.py',\
#      'normal_dml_actions_pre.py',\
#      'normal_dml_actions_post.py',\
#      'each_tenant_dml_actions_pre.py',\
#      'each_tenant_dml_actions_post.py',\
#      'each_tenant_ddl_actions_post.py'\
#      ]
#  special_upgrade_filename_list = \
#      [\
#      'special_upgrade_action_pre.py',\
#      'special_upgrade_action_post.py'
#      ]
#  clear_action_codes(action_filename_list, action_begin_line, action_end_line, False)
#  clear_action_codes(special_upgrade_filename_list, action_begin_line, action_end_line, True)
#  regenerate_upgrade_script()
#
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:run_modules.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#ALL_MODULE = 'all'
#
## module for upgrade_pre.py
#MODULE_BEGIN_UPGRADE          = 'begin_upgrade'
#MODULE_BEGIN_ROLLING_UPGRADE  = 'begin_rolling_upgrade'
#MODULE_SPECIAL_ACTION         = 'special_action'
##MODULE_HEALTH_CHECK          = 'health_check'
#
## module for upgrade_post.py
#MODULE_HEALTH_CHECK           = 'health_check'
#MODULE_END_ROLLING_UPGRADE    = 'end_rolling_upgrade'
#MODULE_TENANT_UPRADE          = 'tenant_upgrade'
#MODULE_END_UPRADE             = 'end_upgrade'
#MODULE_POST_CHECK             = 'post_check'
#
#def get_all_module_set():
#  import run_modules
#  module_set = set([])
#  attrs_from_run_module = dir(run_modules)
#  for attr in attrs_from_run_module:
#    if attr.startswith('MODULE_'):
#      module = getattr(run_modules, attr)
#      module_set.add(module)
#  return module_set
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:special_upgrade_action_pre.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import time
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#import re
#import string
#from random import Random
#from actions import DMLCursor
#from actions import QueryCursor
#import binascii
#import my_utils
#import actions
#import sys
#import upgrade_health_checker
#
## 主库需要执行的升级动作
#def do_special_upgrade(conn, cur, timeout, user, passwd):
#  # special upgrade action
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
#  current_version = actions.fetch_observer_version(cur)
#  target_version = actions.get_current_cluster_version()
#  # when upgrade across version, disable enable_ddl/major_freeze
#  if current_version != target_version:
#    actions.set_parameter(cur, 'enable_ddl', 'False', timeout)
#    actions.set_parameter(cur, 'enable_major_freeze', 'False', timeout)
#    actions.set_tenant_parameter(cur, '_enable_adaptive_compaction', 'False', timeout)
#    # wait scheduler in storage to notice adaptive_compaction is switched to false
#    time.sleep(60 * 2)
#    query_cur = actions.QueryCursor(cur)
#    wait_major_timeout = 600
#    upgrade_health_checker.check_major_merge(query_cur, wait_major_timeout)
#    actions.do_suspend_merge(cur, timeout)
#  # When upgrading from a version prior to 4.2 to version 4.2, the bloom_filter should be disabled.
#  # The param _bloom_filter_enabled is no longer in use as of version 4.2, there is no need to enable it again.
#  if actions.get_version(current_version) < actions.get_version('4.2.0.0')\
#      and actions.get_version(target_version) >= actions.get_version('4.2.0.0'):
#    actions.set_tenant_parameter(cur, '_bloom_filter_enabled', 'False', timeout)
#
#####========******####======== actions begin ========####******========####
#  return
#####========******####========= actions end =========####******========####
#
#def query(cur, sql):
#  log(sql)
#  cur.execute(sql)
#  results = cur.fetchall()
#  return results
#
#def log(msg):
#  logging.info(msg)
#
#def get_oracle_tenant_ids(cur):
#  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant where compatibility_mode = 1')]
#
#def get_tenant_ids(cur):
#  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant')]
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:tenant_upgrade_action.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import logging
#import time
#from actions import Cursor
#from actions import DMLCursor
#from actions import QueryCursor
#import mysql.connector
#from mysql.connector import errorcode
#import actions
#
#def do_upgrade(conn, cur, timeout, user, pwd):
#  # upgrade action
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
#  across_version = upgrade_across_version(cur)
#  if across_version:
#    run_upgrade_job(conn, cur, "UPGRADE_ALL", timeout)
#  else:
#    run_upgrade_job(conn, cur, "UPGRADE_VIRTUAL_SCHEMA", timeout)
#
#  # just to make __all_virtual_upgrade_inspection avaliable
#  timeout_ts = (timeout if timeout > 0 else 600) * 1000 * 1000
#  sql = "set @@session.ob_query_timeout = {0}".format(timeout_ts)
#  logging.info(sql)
#  cur.execute(sql)
#  sql = "alter system run job 'root_inspection'"
#  logging.info(sql)
#  cur.execute(sql)
#  sql = "set @@session.ob_query_timeout = 10000000"
#  logging.info(sql)
#  cur.execute(sql)
#####========******####======== actions begin ========####******========####
#  upgrade_syslog_level(conn, cur)
#  return
#
#def upgrade_syslog_level(conn, cur):
#  try:
#    cur.execute("""select svr_ip, svr_port, value from oceanbase.__all_virtual_sys_parameter_stat where name = 'syslog_level'""")
#    result = cur.fetchall()
#    for r in result:
#      logging.info("syslog level before upgrade: ip: {0}, port: {1}, value: {2}".format(r[0], r[1], r[2]))
#    cur.execute("""select count(*) cnt from oceanbase.__all_virtual_sys_parameter_stat where name = 'syslog_level' and value = 'INFO'""")
#    result = cur.fetchall()
#    info_cnt = result[0][0]
#    if info_cnt > 0:
#      actions.set_parameter(cur, "syslog_level", "WDIAG")
#
#  except Exception, e:
#    logging.warn("upgrade syslog level failed!")
#    raise e
#####========******####========= actions end =========####******========####
#
#def query(cur, sql):
#  cur.execute(sql)
#  logging.info(sql)
#  results = cur.fetchall()
#  return results
#
#def get_tenant_ids(cur):
#  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant')]
#
#def upgrade_across_version(cur):
#  current_data_version = actions.get_current_data_version()
#  int_current_data_version = actions.get_version(current_data_version)
#
#  across_version = False
#  sys_tenant_id = 1
#
#  # 1. check if target_data_version/current_data_version match with current_data_version
#  sql = "select count(*) from oceanbase.__all_table where table_name = '__all_virtual_core_table'"
#  results = query(cur, sql)
#  if len(results) < 1 or len(results[0]) < 1:
#    logging.warn("row/column cnt not match")
#    raise e
#  elif results[0][0] <= 0:
#    # __all_virtual_core_table doesn't exist, this cluster is upgraded from 4.0.0.0
#    across_version = True
#  else:
#    # check
#    tenant_ids = get_tenant_ids(cur)
#    if len(tenant_ids) <= 0:
#      logging.warn("tenant_ids count is unexpected")
#      raise e
#    tenant_count = len(tenant_ids)
#
#    sql = "select count(*) from __all_virtual_core_table where column_name in ('target_data_version', 'current_data_version') and column_value = {0}".format(int_current_data_version)
#    results = query(cur, sql)
#    if len(results) != 1 or len(results[0]) != 1:
#      logging.warn('result cnt not match')
#      raise e
#    elif 2 * tenant_count != results[0][0]:
#      logging.info('target_data_version/current_data_version not match with {0}, tenant_cnt:{1}, result_cnt:{2}'.format(current_data_version, tenant_count, results[0][0]))
#      across_version = True
#    else:
#      logging.info("all tenant's target_data_version/current_data_version are match with {0}".format(current_data_version))
#      across_version = False
#
#  # 2. check if compatible match with current_data_version
#  if not across_version:
#    sql = "select count(*) from oceanbase.__all_virtual_tenant_parameter_info where name = 'compatible' and value != '{0}'".format(current_data_version)
#    results = query(cur, sql)
#    if len(results) < 1 or len(results[0]) < 1:
#      logging.warn("row/column cnt not match")
#      raise e
#    elif results[0][0] == 0:
#      logging.info("compatible are all matched")
#    else:
#      logging.info("compatible unmatched")
#      across_version = True
#
#  return across_version
#
#def get_max_used_job_id(cur):
#  try:
#    max_job_id = 0
#    sql = "select job_id from oceanbase.__all_rootservice_job order by job_id desc limit 1"
#    results = query(cur, sql)
#
#    if (len(results) == 0):
#      max_job_id = 0
#    elif (len(results) != 1 or len(results[0]) != 1):
#      logging.warn("row cnt not match")
#      raise e
#    else:
#      max_job_id = results[0][0]
#
#    logging.info("get max_used_job_id:{0}".format(max_job_id))
#
#    return max_job_id
#  except Exception, e:
#    logging.warn("failed to get max_used_job_id")
#    raise e
#
#def check_can_run_upgrade_job(cur, job_name):
#  try:
#    sql = """select job_status from oceanbase.__all_rootservice_job
#             where job_type = '{0}' order by job_id desc limit 1""".format(job_name)
#    results = query(cur, sql)
#
#    bret = True
#    if (len(results) == 0):
#      bret = True
#      logging.info("upgrade job not created yet, should run upgrade job")
#    elif (len(results) != 1 or len(results[0]) != 1):
#      logging.warn("row cnt not match")
#      raise e
#    elif ("INPROGRESS" == results[0][0]):
#      logging.warn("upgrade job still running, should wait")
#      raise e
#    elif ("SUCCESS" == results[0][0]):
#      bret = True
#      logging.info("maybe upgrade job remained, can run again")
#    elif ("FAILED" == results[0][0]):
#      bret = True
#      logging.info("execute upgrade job failed, should run again")
#    else:
#      logging.warn("invalid job status: {0}".format(results[0][0]))
#      raise e
#
#    return bret
#  except Exception, e:
#    logging.warn("failed to check if upgrade job can run")
#    raise e
#
#def check_upgrade_job_result(cur, job_name, timeout, max_used_job_id):
#  try:
#    times = (timeout if timeout > 0 else 3600) / 10
#    while (times >= 0):
#      sql = """select job_status, rs_svr_ip, rs_svr_port, gmt_create from oceanbase.__all_rootservice_job
#               where job_type = '{0}' and job_id > {1} order by job_id desc limit 1
#            """.format(job_name, max_used_job_id)
#      results = query(cur, sql)
#
#      if (len(results) == 0):
#        logging.info("upgrade job not created yet")
#      elif (len(results) != 1 or len(results[0]) != 4):
#        logging.warn("row cnt not match")
#        raise e
#      elif ("INPROGRESS" == results[0][0]):
#        logging.info("upgrade job is still running")
#        # check if rs change
#        if times % 10 == 0:
#          ip = results[0][1]
#          port = results[0][2]
#          gmt_create = results[0][3]
#          sql = """select count(*) from oceanbase.__all_virtual_core_meta_table where role = 1 and svr_ip = '{0}' and svr_port = {1}""".format(ip, port)
#          results = query(cur, sql)
#          if (len(results) != 1 or len(results[0]) != 1):
#            logging.warn("row/column cnt not match")
#            raise e
#          elif results[0][0] == 1:
#            sql = """select count(*) from oceanbase.__all_rootservice_event_history where gmt_create > '{0}' and event = 'full_rootservice'""".format(gmt_create)
#            results = query(cur, sql)
#            if (len(results) != 1 or len(results[0]) != 1):
#              logging.warn("row/column cnt not match")
#              raise e
#            elif results[0][0] > 0:
#              logging.warn("rs changed, should check if upgrade job is still running")
#              raise e
#            else:
#              logging.info("rs[{0}:{1}] still exist, keep waiting".format(ip, port))
#          else:
#            logging.warn("rs changed or not exist, should check if upgrade job is still running")
#            raise e
#      elif ("SUCCESS" == results[0][0]):
#        logging.info("execute upgrade job successfully")
#        break;
#      elif ("FAILED" == results[0][0]):
#        logging.warn("execute upgrade job failed")
#        raise e
#      else:
#        logging.warn("invalid job status: {0}".format(results[0][0]))
#        raise e
#
#      times = times - 1
#      if times == -1:
#        logging.warn("""check {0} job timeout""".format(job_name))
#        raise e
#      time.sleep(10)
#  except Exception, e:
#    logging.warn("failed to check upgrade job result")
#    raise e
#
#def run_upgrade_job(conn, cur, job_name, timeout):
#  try:
#    logging.info("start to run upgrade job, job_name:{0}".format(job_name))
#    # pre check
#    if check_can_run_upgrade_job(cur, job_name):
#      conn.autocommit = True
#      # disable enable_ddl
#      ori_enable_ddl = actions.get_ori_enable_ddl(cur, timeout)
#      if ori_enable_ddl == 0:
#        actions.set_parameter(cur, 'enable_ddl', 'True', timeout)
#      # enable_sys_table_ddl
#      actions.set_parameter(cur, 'enable_sys_table_ddl', 'True', timeout)
#      # get max_used_job_id
#      max_used_job_id = get_max_used_job_id(cur)
#      # run upgrade job
#      sql = """alter system run upgrade job '{0}'""".format(job_name)
#      logging.info(sql)
#      cur.execute(sql)
#      # check upgrade job result
#      check_upgrade_job_result(cur, job_name, timeout, max_used_job_id)
#      # reset enable_sys_table_ddl
#      actions.set_parameter(cur, 'enable_sys_table_ddl', 'False', timeout)
#      # reset enable_ddl
#      if ori_enable_ddl == 0:
#        actions.set_parameter(cur, 'enable_ddl', 'False', timeout)
#  except Exception, e:
#    logging.warn("run upgrade job failed, :{0}".format(job_name))
#    raise e
#  logging.info("run upgrade job success, job_name:{0}".format(job_name))
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_checker.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import sys
#import os
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#import getopt
#import time
#
#class UpgradeParams:
#  log_filename = 'upgrade_checker.log'
#  old_version = '4.0.0.0'
##### --------------start : my_error.py --------------
#class MyError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
##### --------------start : actions.py------------
#class Cursor:
#  __cursor = None
#  def __init__(self, cursor):
#    self.__cursor = cursor
#  def exec_sql(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
#      return rowcount
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
#  def exec_query(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      results = self.__cursor.fetchall()
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
#      return (self.__cursor.description, results)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
#
#def set_parameter(cur, parameter, value):
#  sql = """alter system set {0} = '{1}'""".format(parameter, value)
#  logging.info(sql)
#  cur.execute(sql)
#  wait_parameter_sync(cur, parameter, value)
#
#def wait_parameter_sync(cur, key, value):
#  sql = """select count(*) as cnt from oceanbase.__all_virtual_sys_parameter_stat
#           where name = '{0}' and value != '{1}'""".format(key, value)
#  times = 10
#  while times > 0:
#    logging.info(sql)
#    cur.execute(sql)
#    result = cur.fetchall()
#    if len(result) != 1 or len(result[0]) != 1:
#      logging.exception('result cnt not match')
#      raise e
#    elif result[0][0] == 0:
#      logging.info("""{0} is sync, value is {1}""".format(key, value))
#      break
#    else:
#      logging.info("""{0} is not sync, value should be {1}""".format(key, value))
#
#    times -= 1
#    if times == 0:
#      logging.exception("""check {0}:{1} sync timeout""".format(key, value))
#      raise e
#    time.sleep(5)
#
##### --------------start :  opt.py --------------
#help_str = \
#"""
#Help:
#""" +\
#sys.argv[0] + """ [OPTIONS]""" +\
#'\n\n' +\
#'-I, --help          Display this help and exit.\n' +\
#'-V, --version       Output version information and exit.\n' +\
#'-h, --host=name     Connect to host.\n' +\
#'-P, --port=name     Port number to use for connection.\n' +\
#'-u, --user=name     User for login.\n' +\
#'-t, --timeout=name  Cmd/Query/Inspection execute timeout(s).\n' +\
#'-p, --password=name Password to use when connecting to server. If password is\n' +\
#'                    not given it\'s empty string "".\n' +\
#'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
#'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
#'                    system_variable_dml, special_action, all. "all" represents\n' +\
#'                    that all modules should be run. They are splitted by ",".\n' +\
#'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
#'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'
#
#version_str = """version 1.0.0"""
#
#class Option:
#  __g_short_name_set = set([])
#  __g_long_name_set = set([])
#  __short_name = None
#  __long_name = None
#  __is_with_param = None
#  __is_local_opt = None
#  __has_value = None
#  __value = None
#  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
#    if short_name in Option.__g_short_name_set:
#      raise MyError('duplicate option short name: {0}'.format(short_name))
#    elif long_name in Option.__g_long_name_set:
#      raise MyError('duplicate option long name: {0}'.format(long_name))
#    Option.__g_short_name_set.add(short_name)
#    Option.__g_long_name_set.add(long_name)
#    self.__short_name = short_name
#    self.__long_name = long_name
#    self.__is_with_param = is_with_param
#    self.__is_local_opt = is_local_opt
#    self.__has_value = False
#    if None != default_value:
#      self.set_value(default_value)
#  def is_with_param(self):
#    return self.__is_with_param
#  def get_short_name(self):
#    return self.__short_name
#  def get_long_name(self):
#    return self.__long_name
#  def has_value(self):
#    return self.__has_value
#  def get_value(self):
#    return self.__value
#  def set_value(self, value):
#    self.__value = value
#    self.__has_value = True
#  def is_local_opt(self):
#    return self.__is_local_opt
#  def is_valid(self):
#    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
#
#g_opts =\
#[\
#Option('I', 'help', False, True),\
#Option('V', 'version', False, True),\
#Option('h', 'host', True, False),\
#Option('P', 'port', True, False),\
#Option('u', 'user', True, False),\
#Option('t', 'timeout', True, False, 0),\
#Option('p', 'password', True, False, ''),\
## 要跑哪个模块，默认全跑
#Option('m', 'module', True, False, 'all'),\
## 日志文件路径，不同脚本的main函数中中会改成不同的默认值
#Option('l', 'log-file', True, False)
#]\
#
#def change_opt_defult_value(opt_long_name, opt_default_val):
#  global g_opts
#  for opt in g_opts:
#    if opt.get_long_name() == opt_long_name:
#      opt.set_value(opt_default_val)
#      return
#
#def has_no_local_opts():
#  global g_opts
#  no_local_opts = True
#  for opt in g_opts:
#    if opt.is_local_opt() and opt.has_value():
#      no_local_opts = False
#  return no_local_opts
#
#def check_db_client_opts():
#  global g_opts
#  for opt in g_opts:
#    if not opt.is_local_opt() and not opt.has_value():
#      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
#          .format(opt.get_short_name(), sys.argv[0]))
#
#def parse_option(opt_name, opt_val):
#  global g_opts
#  for opt in g_opts:
#    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
#      opt.set_value(opt_val)
#
#def parse_options(argv):
#  global g_opts
#  short_opt_str = ''
#  long_opt_list = []
#  for opt in g_opts:
#    if opt.is_with_param():
#      short_opt_str += opt.get_short_name() + ':'
#    else:
#      short_opt_str += opt.get_short_name()
#  for opt in g_opts:
#    if opt.is_with_param():
#      long_opt_list.append(opt.get_long_name() + '=')
#    else:
#      long_opt_list.append(opt.get_long_name())
#  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
#  for (opt_name, opt_val) in opts:
#    parse_option(opt_name, opt_val)
#  if has_no_local_opts():
#    check_db_client_opts()
#
#def deal_with_local_opt(opt):
#  if 'help' == opt.get_long_name():
#    global help_str
#    print help_str
#  elif 'version' == opt.get_long_name():
#    global version_str
#    print version_str
#
#def deal_with_local_opts():
#  global g_opts
#  if has_no_local_opts():
#    raise MyError('no local options, can not deal with local options')
#  else:
#    for opt in g_opts:
#      if opt.is_local_opt() and opt.has_value():
#        deal_with_local_opt(opt)
#        # 只处理一个
#        return
#
#def get_opt_host():
#  global g_opts
#  for opt in g_opts:
#    if 'host' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_port():
#  global g_opts
#  for opt in g_opts:
#    if 'port' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_user():
#  global g_opts
#  for opt in g_opts:
#    if 'user' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_password():
#  global g_opts
#  for opt in g_opts:
#    if 'password' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_timeout():
#  global g_opts
#  for opt in g_opts:
#    if 'timeout' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_module():
#  global g_opts
#  for opt in g_opts:
#    if 'module' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_log_file():
#  global g_opts
#  for opt in g_opts:
#    if 'log-file' == opt.get_long_name():
#      return opt.get_value()
##### ---------------end----------------------
#
##### --------------start :  do_upgrade_pre.py--------------
#def config_logging_module(log_filenamme):
#  logging.basicConfig(level=logging.INFO,\
#      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
#      datefmt='%Y-%m-%d %H:%M:%S',\
#      filename=log_filenamme,\
#      filemode='w')
#  # 定义日志打印格式
#  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
#  #######################################
#  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
#  stdout_handler = logging.StreamHandler(sys.stdout)
#  stdout_handler.setLevel(logging.INFO)
#  # 设置日志打印格式
#  stdout_handler.setFormatter(formatter)
#  # 将定义好的stdout_handler日志handler添加到root logger
#  logging.getLogger('').addHandler(stdout_handler)
##### ---------------end----------------------
#
#
#fail_list=[]
#
#def get_version(version_str):
#  versions = version_str.split(".")
#
#  if len(versions) != 4:
#    logging.exception("""version:{0} is invalid""".format(version_str))
#    raise e
#
#  major = int(versions[0])
#  minor = int(versions[1])
#  major_patch = int(versions[2])
#  minor_patch = int(versions[3])
#
#  if major > 0xffffffff or minor > 0xffff or major_patch > 0xff or minor_patch > 0xff:
#    logging.exception("""version:{0} is invalid""".format(version_str))
#    raise e
#
#  version = (major << 32) | (minor << 16) | (major_patch << 8) | (minor_patch)
#  return version
#
##### START ####
## 1. 检查前置版本
#def check_observer_version(query_cur, upgrade_params):
#  (desc, results) = query_cur.exec_query("""select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'""")
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif cmp(results[0][0], upgrade_params.old_version) < 0 :
#    fail_list.append('old observer version is expected equal or higher then: {0}, actual version:{1}'.format(upgrade_params.old_version, results[0][0]))
#  logging.info('check observer version success, version = {0}'.format(results[0][0]))
#
#def check_data_version(query_cur):
#  min_cluster_version = 0
#  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif len(results[0]) != 1:
#    fail_list.append('column cnt not match')
#  else:
#    min_cluster_version = get_version(results[0][0])
#
#    # check data version
#    if min_cluster_version < get_version("4.1.0.0"):
#      # last barrier cluster version should be 4.1.0.0
#      fail_list.append('last barrier cluster version is 4.1.0.0. prohibit cluster upgrade from cluster version less than 4.1.0.0')
#    else:
#      data_version_str = ''
#      data_version = 0
#      # check compatible is same
#      sql = """select distinct value from oceanbase.__all_virtual_tenant_parameter_info where name='compatible'"""
#      (desc, results) = query_cur.exec_query(sql)
#      if len(results) != 1:
#        fail_list.append('compatible is not sync')
#      elif len(results[0]) != 1:
#        fail_list.append('column cnt not match')
#      else:
#        data_version_str = results[0][0]
#        data_version = get_version(results[0][0])
#
#        if data_version < get_version("4.1.0.0"):
#          # last barrier data version should be 4.1.0.0
#          fail_list.append('last barrier data version is 4.1.0.0. prohibit cluster upgrade from data version less than 4.1.0.0')
#        else:
#          # check target_data_version/current_data_version
#          sql = "select count(*) from oceanbase.__all_tenant"
#          (desc, results) = query_cur.exec_query(sql)
#          if len(results) != 1 or len(results[0]) != 1:
#            fail_list.append('result cnt not match')
#          else:
#            tenant_count = results[0][0]
#
#            sql = "select count(*) from __all_virtual_core_table where column_name in ('target_data_version', 'current_data_version') and column_value = {0}".format(data_version)
#            (desc, results) = query_cur.exec_query(sql)
#            if len(results) != 1 or len(results[0]) != 1:
#              fail_list.append('result cnt not match')
#            elif 2 * tenant_count != results[0][0]:
#              fail_list.append('target_data_version/current_data_version not match with {0}, tenant_cnt:{1}, result_cnt:{2}'.format(data_version_str, tenant_count, results[0][0]))
#            else:
#              logging.info("check data version success, all tenant's compatible/target_data_version/current_data_version is {0}".format(data_version_str))
#
## 2. 检查paxos副本是否同步, paxos副本是否缺失
#def check_paxos_replica(query_cur):
#  # 2.1 检查paxos副本是否同步
#  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from GV$OB_LOG_STAT where in_sync = 'NO'""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} replicas unsync, please check'.format(results[0][0]))
#  # 2.2 检查paxos副本是否有缺失 TODO
#  logging.info('check paxos replica success')
#
## 3. 检查是否有做balance, locality变更
#def check_rebalance_task(query_cur):
#  # 3.1 检查是否有做locality变更
#  (desc, results) = query_cur.exec_query("""select count(1) as cnt from DBA_OB_TENANT_JOBS where job_status='INPROGRESS' and result_code is null""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} locality tasks is doing, please check'.format(results[0][0]))
#  # 3.2 检查是否有做balance
#  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from CDB_OB_LS_REPLICA_TASKS""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} rebalance tasks is doing, please check'.format(results[0][0]))
#  logging.info('check rebalance task success')
#
## 4. 检查集群状态
#def check_cluster_status(query_cur):
#  # 4.1 检查是否非合并状态
#  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_MAJOR_COMPACTION where (GLOBAL_BROADCAST_SCN > LAST_SCN or STATUS != 'IDLE')""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} tenant is merging, please check'.format(results[0][0]))
#  (desc, results) = query_cur.exec_query("""select /*+ query_timeout(1000000000) */ count(1) from __all_virtual_tablet_compaction_info where max_received_scn > finished_scn and max_received_scn > 0""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} tablet is merging, please check'.format(results[0][0]))
#  logging.info('check cluster status success')
#
## 5. 检查是否有异常租户(creating，延迟删除，恢复中)
#def check_tenant_status(query_cur):
#
#  # check tenant schema
#  (desc, results) = query_cur.exec_query("""select count(*) as count from DBA_OB_TENANTS where status != 'NORMAL'""")
#  if len(results) != 1 or len(results[0]) != 1:
#    fail_list.append('results len not match')
#  elif 0 != results[0][0]:
#    fail_list.append('has abnormal tenant, should stop')
#  else:
#    logging.info('check tenant status success')
#
#  # check tenant info
#  # don't support restore tenant upgrade
#  (desc, results) = query_cur.exec_query("""select count(*) as count from oceanbase.__all_virtual_tenant_info where tenant_role != 'PRIMARY' and tenant_role != 'STANDBY'""")
#  if len(results) != 1 or len(results[0]) != 1:
#    fail_list.append('results len not match')
#  elif 0 != results[0][0]:
#    fail_list.append('has abnormal tenant info, should stop')
#  else:
#    logging.info('check tenant info success')
#
## 6. 检查无恢复任务
#def check_restore_job_exist(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_RESTORE_PROGRESS""")
#  if len(results) != 1 or len(results[0]) != 1:
#    fail_list.append('failed to restore job cnt')
#  elif results[0][0] != 0:
#      fail_list.append("""still has restore job, upgrade is not allowed temporarily""")
#  logging.info('check restore job success')
#
#def check_is_primary_zone_distributed(primary_zone_str):
#  semicolon_pos = len(primary_zone_str)
#  for i in range(len(primary_zone_str)):
#    if primary_zone_str[i] == ';':
#      semicolon_pos = i
#      break
#  comma_pos = len(primary_zone_str)
#  for j in range(len(primary_zone_str)):
#    if primary_zone_str[j] == ',':
#      comma_pos = j
#      break
#  if comma_pos < semicolon_pos:
#    return True
#  else:
#    return False
#
## 7. 升级前需要primary zone只有一个
#def check_tenant_primary_zone(query_cur):
#  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif len(results[0]) != 1:
#    fail_list.append('column cnt not match')
#  else:
#    min_cluster_version = get_version(results[0][0])
#    if min_cluster_version < get_version("4.1.0.0"):
#      (desc, results) = query_cur.exec_query("""select tenant_name,primary_zone from DBA_OB_TENANTS where  tenant_id != 1""");
#      for item in results:
#        if cmp(item[1], "RANDOM") == 0:
#          fail_list.append('{0} tenant primary zone random before update not allowed'.format(item[0]))
#        elif check_is_primary_zone_distributed(item[1]):
#          fail_list.append('{0} tenant primary zone distributed before update not allowed'.format(item[0]))
#      logging.info('check tenant primary zone success')
#
## 8. 修改永久下线的时间，避免升级过程中缺副本
#def modify_server_permanent_offline_time(cur):
#  set_parameter(cur, 'server_permanent_offline_time', '72h')
#
## 9. 检查是否有DDL任务在执行
#def check_ddl_task_execute(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(1) from __all_virtual_ddl_task_status""")
#  if 0 != results[0][0]:
#    fail_list.append("There are DDL task in progress")
#  logging.info('check ddl task execut status success')
#
## 10. 检查无备份任务
#def check_backup_job_exist(query_cur):
#  # Backup jobs cannot be in-progress during upgrade.
#  (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_BACKUP_JOBS""")
#  if len(results) != 1 or len(results[0]) != 1:
#    fail_list.append('failed to backup job cnt')
#  elif results[0][0] != 0:
#    fail_list.append("""still has backup job, upgrade is not allowed temporarily""")
#  else:
#    logging.info('check backup job success')
#
## 11. 检查无归档任务
#def check_archive_job_exist(query_cur):
#  min_cluster_version = 0
#  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif len(results[0]) != 1:
#    fail_list.append('column cnt not match')
#  else:
#    min_cluster_version = get_version(results[0][0])
#
#    # Archive jobs cannot be in-progress before upgrade from 4.0.
#    if min_cluster_version < get_version("4.1.0.0"):
#      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_ARCHIVELOG where status!='STOP'""")
#      if len(results) != 1 or len(results[0]) != 1:
#        fail_list.append('failed to archive job cnt')
#      elif results[0][0] != 0:
#        fail_list.append("""still has archive job, upgrade is not allowed temporarily""")
#      else:
#        logging.info('check archive job success')
#
## 12. 检查归档路径是否清空
#def check_archive_dest_exist(query_cur):
#  min_cluster_version = 0
#  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif len(results[0]) != 1:
#    fail_list.append('column cnt not match')
#  else:
#    min_cluster_version = get_version(results[0][0])
#    # archive dest need to be cleaned before upgrade from 4.0.
#    if min_cluster_version < get_version("4.1.0.0"):
#      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_ARCHIVE_DEST""")
#      if len(results) != 1 or len(results[0]) != 1:
#        fail_list.append('failed to archive dest cnt')
#      elif results[0][0] != 0:
#        fail_list.append("""still has archive destination, upgrade is not allowed temporarily""")
#      else:
#        logging.info('check archive destination success')
#
## 13. 检查备份路径是否清空
#def check_backup_dest_exist(query_cur):
#  min_cluster_version = 0
#  sql = """select distinct value from GV$OB_PARAMETERS  where name='min_observer_version'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1:
#    fail_list.append('min_observer_version is not sync')
#  elif len(results[0]) != 1:
#    fail_list.append('column cnt not match')
#  else:
#    min_cluster_version = get_version(results[0][0])
#    # backup dest need to be cleaned before upgrade from 4.0.
#    if min_cluster_version < get_version("4.1.0.0"):
#      (desc, results) = query_cur.exec_query("""select count(1) from CDB_OB_BACKUP_PARAMETER where name='data_backup_dest' and (value!=NULL or value!='')""")
#      if len(results) != 1 or len(results[0]) != 1:
#        fail_list.append('failed to data backup dest cnt')
#      elif results[0][0] != 0:
#        fail_list.append("""still has backup destination, upgrade is not allowed temporarily""")
#      else:
#        logging.info('check backup destination success')
#
#def check_server_version(query_cur):
#    sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server""";
#    (desc, results) = query_cur.exec_query(sql);
#    if len(results) != 1:
#      fail_list.append("servers build_version not match")
#    else:
#      logging.info("check server version success")
#
## 14. 检查server是否可服务
#def check_observer_status(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_server where (start_service_time <= 0 or status != "active")""")
#  if results[0][0] > 0 :
#    fail_list.append('{0} observer not available , please check'.format(results[0][0]))
#  logging.info('check observer status success')
#
## 15  检查schema是否刷新成功
#def check_schema_status(query_cur):
#  (desc, results) = query_cur.exec_query("""select if (a.cnt = b.cnt, 1, 0) as passed from (select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version > 1 and refreshed_schema_version % 8 = 0) as a join (select count(*) as cnt from oceanbase.__all_server join oceanbase.__all_tenant) as b""")
#  if results[0][0] != 1 :
#    fail_list.append('{0} schema not available, please check'.format(results[0][0]))
#  logging.info('check schema status success')
#
## 16. 检查是否存在名为all/all_user/all_meta的租户
#def check_not_supported_tenant_name(query_cur):
#  names = ["all", "all_user", "all_meta"]
#  (desc, results) = query_cur.exec_query("""select tenant_name from oceanbase.DBA_OB_TENANTS""")
#  for i in range(len(results)):
#    if results[i][0].lower() in names:
#      fail_list.append('a tenant named all/all_user/all_meta (case insensitive) cannot exist in the cluster, please rename the tenant')
#      break
#  logging.info('check special tenant name success')
#
## last check of do_check, make sure no function execute after check_fail_list
#def check_fail_list():
#  if len(fail_list) != 0 :
#     error_msg ="upgrade checker failed with " + str(len(fail_list)) + " reasons: " + ", ".join(['['+x+"] " for x in fail_list])
#     raise MyError(error_msg)
#
#def set_query_timeout(query_cur, timeout):
#  if timeout != 0:
#    sql = """set @@session.ob_query_timeout = {0}""".format(timeout * 1000 * 1000)
#    query_cur.exec_sql(sql)
#
## 开始升级前的检查
#def do_check(my_host, my_port, my_user, my_passwd, timeout, upgrade_params):
#  try:
#    conn = mysql.connector.connect(user = my_user,
#                                   password = my_passwd,
#                                   host = my_host,
#                                   port = my_port,
#                                   database = 'oceanbase',
#                                   raise_on_warnings = True)
#    conn.autocommit = True
#    cur = conn.cursor(buffered=True)
#    try:
#      query_cur = Cursor(cur)
#      set_query_timeout(query_cur, timeout)
#      check_observer_version(query_cur, upgrade_params)
#      check_data_version(query_cur)
#      check_paxos_replica(query_cur)
#      check_rebalance_task(query_cur)
#      check_cluster_status(query_cur)
#      check_tenant_status(query_cur)
#      check_restore_job_exist(query_cur)
#      check_tenant_primary_zone(query_cur)
#      check_ddl_task_execute(query_cur)
#      check_backup_job_exist(query_cur)
#      check_archive_job_exist(query_cur)
#      check_archive_dest_exist(query_cur)
#      check_backup_dest_exist(query_cur)
#      check_observer_status(query_cur)
#      check_schema_status(query_cur)
#      check_server_version(query_cur)
#      check_not_supported_tenant_name(query_cur)
#      # all check func should execute before check_fail_list
#      check_fail_list()
#      modify_server_permanent_offline_time(cur)
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      cur.close()
#      conn.close()
#  except mysql.connector.Error, e:
#    logging.exception('connection error')
#    raise e
#  except Exception, e:
#    logging.exception('normal error')
#    raise e
#
#if __name__ == '__main__':
#  upgrade_params = UpgradeParams()
#  change_opt_defult_value('log-file', upgrade_params.log_filename)
#  parse_options(sys.argv[1:])
#  if not has_no_local_opts():
#    deal_with_local_opts()
#  else:
#    check_db_client_opts()
#    log_filename = get_opt_log_file()
#    upgrade_params.log_filename = log_filename
#    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
#    config_logging_module(upgrade_params.log_filename)
#    try:
#      host = get_opt_host()
#      port = int(get_opt_port())
#      user = get_opt_user()
#      password = get_opt_password()
#      timeout = int(get_opt_timeout())
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", timeout=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, timeout, log_filename)
#      do_check(host, port, user, password, timeout, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_health_checker.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import sys
#import os
#import time
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#import getopt
#
#class UpgradeParams:
#  log_filename = 'upgrade_cluster_health_checker.log'
#
##### --------------start : my_error.py --------------
#class MyError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
#
##### --------------start : actions.py 只允许执行查询语句--------------
#class QueryCursor:
#  __cursor = None
#  def __init__(self, cursor):
#    self.__cursor = cursor
#  def exec_sql(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
#      return rowcount
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
#  def exec_query(self, sql, print_when_succ = True):
#    try:
#      self.__cursor.execute(sql)
#      results = self.__cursor.fetchall()
#      rowcount = self.__cursor.rowcount
#      if True == print_when_succ:
#        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
#      return (self.__cursor.description, results)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connector error, fail to execute sql: %s', sql)
#      raise e
#    except Exception, e:
#      logging.exception('normal error, fail to execute sql: %s', sql)
#      raise e
##### ---------------end----------------------
#
##### --------------start :  opt.py --------------
#help_str = \
#"""
#Help:
#""" +\
#sys.argv[0] + """ [OPTIONS]""" +\
#'\n\n' +\
#'-I, --help          Display this help and exit.\n' +\
#'-V, --version       Output version information and exit.\n' +\
#'-h, --host=name     Connect to host.\n' +\
#'-P, --port=name     Port number to use for connection.\n' +\
#'-u, --user=name     User for login.\n' +\
#'-p, --password=name Password to use when connecting to server. If password is\n' +\
#'                    not given it\'s empty string "".\n' +\
#'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
#'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
#'                    system_variable_dml, special_action, all. "all" represents\n' +\
#'                    that all modules should be run. They are splitted by ",".\n' +\
#'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
#'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
#'-t, --timeout=name  check timeout, default: 600(s).\n' + \
#'-z, --zone=name     If zone is not specified, check all servers status in cluster. \n' +\
#'                    Otherwise, only check servers status in specified zone. \n' + \
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'
#
#version_str = """version 1.0.0"""
#
#class Option:
#  __g_short_name_set = set([])
#  __g_long_name_set = set([])
#  __short_name = None
#  __long_name = None
#  __is_with_param = None
#  __is_local_opt = None
#  __has_value = None
#  __value = None
#  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
#    if short_name in Option.__g_short_name_set:
#      raise MyError('duplicate option short name: {0}'.format(short_name))
#    elif long_name in Option.__g_long_name_set:
#      raise MyError('duplicate option long name: {0}'.format(long_name))
#    Option.__g_short_name_set.add(short_name)
#    Option.__g_long_name_set.add(long_name)
#    self.__short_name = short_name
#    self.__long_name = long_name
#    self.__is_with_param = is_with_param
#    self.__is_local_opt = is_local_opt
#    self.__has_value = False
#    if None != default_value:
#      self.set_value(default_value)
#  def is_with_param(self):
#    return self.__is_with_param
#  def get_short_name(self):
#    return self.__short_name
#  def get_long_name(self):
#    return self.__long_name
#  def has_value(self):
#    return self.__has_value
#  def get_value(self):
#    return self.__value
#  def set_value(self, value):
#    self.__value = value
#    self.__has_value = True
#  def is_local_opt(self):
#    return self.__is_local_opt
#  def is_valid(self):
#    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
#
#g_opts =\
#[\
#Option('I', 'help', False, True),\
#Option('V', 'version', False, True),\
#Option('h', 'host', True, False),\
#Option('P', 'port', True, False),\
#Option('u', 'user', True, False),\
#Option('p', 'password', True, False, ''),\
## 要跑哪个模块，默认全跑
#Option('m', 'module', True, False, 'all'),\
## 日志文件路径，不同脚本的main函数中中会改成不同的默认值
#Option('l', 'log-file', True, False),\
## 一些检查的超时时间，默认是600s
#Option('t', 'timeout', True, False, '600'),\
#Option('z', 'zone', True, False, ''),\
#]\
#
#def change_opt_defult_value(opt_long_name, opt_default_val):
#  global g_opts
#  for opt in g_opts:
#    if opt.get_long_name() == opt_long_name:
#      opt.set_value(opt_default_val)
#      return
#
#def has_no_local_opts():
#  global g_opts
#  no_local_opts = True
#  for opt in g_opts:
#    if opt.is_local_opt() and opt.has_value():
#      no_local_opts = False
#  return no_local_opts
#
#def check_db_client_opts():
#  global g_opts
#  for opt in g_opts:
#    if not opt.is_local_opt() and not opt.has_value():
#      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
#          .format(opt.get_short_name(), sys.argv[0]))
#
#def parse_option(opt_name, opt_val):
#  global g_opts
#  for opt in g_opts:
#    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
#      opt.set_value(opt_val)
#
#def parse_options(argv):
#  global g_opts
#  short_opt_str = ''
#  long_opt_list = []
#  for opt in g_opts:
#    if opt.is_with_param():
#      short_opt_str += opt.get_short_name() + ':'
#    else:
#      short_opt_str += opt.get_short_name()
#  for opt in g_opts:
#    if opt.is_with_param():
#      long_opt_list.append(opt.get_long_name() + '=')
#    else:
#      long_opt_list.append(opt.get_long_name())
#  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
#  for (opt_name, opt_val) in opts:
#    parse_option(opt_name, opt_val)
#  if has_no_local_opts():
#    check_db_client_opts()
#
#def deal_with_local_opt(opt):
#  if 'help' == opt.get_long_name():
#    global help_str
#    print help_str
#  elif 'version' == opt.get_long_name():
#    global version_str
#    print version_str
#
#def deal_with_local_opts():
#  global g_opts
#  if has_no_local_opts():
#    raise MyError('no local options, can not deal with local options')
#  else:
#    for opt in g_opts:
#      if opt.is_local_opt() and opt.has_value():
#        deal_with_local_opt(opt)
#        # 只处理一个
#        return
#
#def get_opt_host():
#  global g_opts
#  for opt in g_opts:
#    if 'host' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_port():
#  global g_opts
#  for opt in g_opts:
#    if 'port' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_user():
#  global g_opts
#  for opt in g_opts:
#    if 'user' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_password():
#  global g_opts
#  for opt in g_opts:
#    if 'password' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_module():
#  global g_opts
#  for opt in g_opts:
#    if 'module' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_log_file():
#  global g_opts
#  for opt in g_opts:
#    if 'log-file' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_timeout():
#  global g_opts
#  for opt in g_opts:
#    if 'timeout' == opt.get_long_name():
#      return opt.get_value()
#
#def get_opt_zone():
#  global g_opts
#  for opt in g_opts:
#    if 'zone' == opt.get_long_name():
#      return opt.get_value()
##### ---------------end----------------------
#
##### --------------start :  do_upgrade_pre.py--------------
#def config_logging_module(log_filenamme):
#  logging.basicConfig(level=logging.INFO,\
#      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
#      datefmt='%Y-%m-%d %H:%M:%S',\
#      filename=log_filenamme,\
#      filemode='w')
#  # 定义日志打印格式
#  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
#  #######################################
#  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
#  stdout_handler = logging.StreamHandler(sys.stdout)
#  stdout_handler.setLevel(logging.INFO)
#  # 设置日志打印格式
#  stdout_handler.setFormatter(formatter)
#  # 将定义好的stdout_handler日志handler添加到root logger
#  logging.getLogger('').addHandler(stdout_handler)
##### ---------------end----------------------
#
#def check_zone_valid(query_cur, zone):
#  if zone != '':
#    sql = """select count(*) from oceanbase.DBA_OB_ZONES where zone = '{0}'""".format(zone)
#    (desc, results) = query_cur.exec_query(sql);
#    if len(results) != 1 or len(results[0]) != 1:
#      raise MyError("unmatched row/column cnt")
#    elif results[0][0] == 0:
#      raise MyError("zone:{0} doesn't exist".format(zone))
#    else:
#      logging.info("zone:{0} is valid".format(zone))
#  else:
#    logging.info("zone is empty, check all servers in cluster")
#
##### START ####
## 0. 检查server版本是否严格一致
#def check_server_version_by_zone(query_cur, zone):
#  if zone == '':
#    logging.info("skip check server version by cluster")
#  else:
#    sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server where zone = '{0}'""".format(zone);
#    (desc, results) = query_cur.exec_query(sql);
#    if len(results) != 1:
#      raise MyError("servers build_version not match")
#    else:
#      logging.info("check server version success")
#
## 1. 检查paxos副本是否同步, paxos副本是否缺失
#def check_paxos_replica(query_cur, timeout):
#  # 1.1 检查paxos副本是否同步
#  sql = """select count(*) from GV$OB_LOG_STAT where in_sync = 'NO'"""
#  check_until_timeout(query_cur, sql, 0, timeout)
#
#  # 1.2 检查paxos副本是否有缺失 TODO
#  logging.info('check paxos replica success')
#
## 2. 检查observer是否可服务
#def check_observer_status(query_cur, zone, timeout):
#  sql = """select count(*) from oceanbase.__all_server where (start_service_time <= 0 or status='inactive')"""
#  if zone != '':
#    sql += """ and zone = '{0}'""".format(zone)
#  check_until_timeout(query_cur, sql, 0, timeout)
#
## 3. 检查schema是否刷新成功
#def check_schema_status(query_cur, timeout):
#  sql = """select if (a.cnt = b.cnt, 1, 0) as passed from (select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version > 1 and refreshed_schema_version % 8 = 0) as a join (select count(*) as cnt from oceanbase.__all_server join oceanbase.__all_tenant) as b"""
#  check_until_timeout(query_cur, sql, 1, timeout)
#
## 4. check major finish
#def check_major_merge(query_cur, timeout):
#  need_check = 0
#  (desc, results) = query_cur.exec_query("""select distinct value from  GV$OB_PARAMETERs where name = 'enable_major_freeze';""")
#  if len(results) != 1:
#    need_check = 1
#  elif results[0][0] != 'True':
#    need_check = 1
#  if need_check == 1:
#    sql = """select count(1) from CDB_OB_MAJOR_COMPACTION where (GLOBAL_BROADCAST_SCN > LAST_SCN or STATUS != 'IDLE')"""
#    check_until_timeout(query_cur, sql, 0, timeout)
#    sql2 = """select /*+ query_timeout(1000000000) */ count(1) from __all_virtual_tablet_compaction_info where max_received_scn > finished_scn and max_received_scn > 0"""
#    check_until_timeout(query_cur, sql2, 0, timeout)
#
#def check_until_timeout(query_cur, sql, value, timeout):
#  times = timeout / 10
#  while times >= 0:
#    (desc, results) = query_cur.exec_query(sql)
#
#    if len(results) != 1 or len(results[0]) != 1:
#      raise MyError("unmatched row/column cnt")
#    elif results[0][0] == value:
#      logging.info("check value is {0} success".format(value))
#      break
#    else:
#      logging.info("value is {0}, expected value is {1}, not matched".format(results[0][0], value))
#
#    times -= 1
#    if times == -1:
#      logging.warn("""check {0} job timeout""".format(job_name))
#      raise e
#    time.sleep(10)
#
## 开始健康检查
#def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout, need_check_major_status, zone = ''):
#  try:
#    conn = mysql.connector.connect(user = my_user,
#                                   password = my_passwd,
#                                   host = my_host,
#                                   port = my_port,
#                                   database = 'oceanbase',
#                                   raise_on_warnings = True)
#    conn.autocommit = True
#    cur = conn.cursor(buffered=True)
#    timeout = timeout if timeout > 0 else 600
#    try:
#      query_cur = QueryCursor(cur)
#      check_zone_valid(query_cur, zone)
#      check_observer_status(query_cur, zone, timeout)
#      check_paxos_replica(query_cur, timeout)
#      check_schema_status(query_cur, timeout)
#      check_server_version_by_zone(query_cur, zone)
#      if True == need_check_major_status:
#        check_major_merge(query_cur, timeout)
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      cur.close()
#      conn.close()
#  except mysql.connector.Error, e:
#    logging.exception('connection error')
#    raise e
#  except Exception, e:
#    logging.exception('normal error')
#    raise e
#
#if __name__ == '__main__':
#  upgrade_params = UpgradeParams()
#  change_opt_defult_value('log-file', upgrade_params.log_filename)
#  parse_options(sys.argv[1:])
#  if not has_no_local_opts():
#    deal_with_local_opts()
#  else:
#    check_db_client_opts()
#    log_filename = get_opt_log_file()
#    upgrade_params.log_filename = log_filename
#    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
#    config_logging_module(upgrade_params.log_filename)
#    try:
#      host = get_opt_host()
#      port = int(get_opt_port())
#      user = get_opt_user()
#      password = get_opt_password()
#      timeout = int(get_opt_timeout())
#      zone = get_opt_zone()
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s, zone=\"%s\"', \
#          host, port, user, password, log_filename, timeout, zone)
#      do_check(host, port, user, password, upgrade_params, timeout, False, zone) # need_check_major_status = False
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_post_checker.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import sys
#import os
#import time
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#import time
#import actions
#
##### START
## 1 检查版本号
#def check_cluster_version(cur, timeout):
#  current_cluster_version = actions.get_current_cluster_version()
#  actions.wait_parameter_sync(cur, False, "min_observer_version", current_cluster_version, timeout)
#
## 2 检查租户版本号
#def check_data_version(cur, query_cur, timeout):
#
#  # get tenant except standby tenant
#  sql = "select tenant_id from oceanbase.__all_tenant except select tenant_id from oceanbase.__all_virtual_tenant_info where tenant_role = 'STANDBY'"
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) == 0:
#    logging.warn('result cnt not match')
#    raise e
#  tenant_count = len(results)
#  tenant_ids_str = ''
#  for index, row in enumerate(results):
#    tenant_ids_str += """{0}{1}""".format((',' if index > 0 else ''), row[0])
#
#  # get server cnt
#  sql = "select count(*) from oceanbase.__all_server";
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1 or len(results[0]) != 1:
#    logging.warn('result cnt not match')
#    raise e
#  server_count = results[0][0]
#
#  # check compatible sync
#  parameter_count = int(server_count) * int(tenant_count)
#  current_data_version = actions.get_current_data_version()
#  sql = """select count(*) as cnt from oceanbase.__all_virtual_tenant_parameter_info where name = 'compatible' and value = '{0}' and tenant_id in ({1})""".format(current_data_version, tenant_ids_str)
#  times = (timeout if timeout > 0 else 60) / 5
#  while times >= 0:
#    logging.info(sql)
#    cur.execute(sql)
#    result = cur.fetchall()
#    if len(result) != 1 or len(result[0]) != 1:
#      logging.exception('result cnt not match')
#      raise e
#    elif result[0][0] == parameter_count:
#      logging.info("""'compatible' is sync, value is {0}""".format(current_data_version))
#      break
#    else:
#      logging.info("""'compatible' is not sync, value should be {0}, expected_cnt should be {1}, current_cnt is {2}""".format(current_data_version, parameter_count, result[0][0]))
#
#    times -= 1
#    if times == -1:
#      logging.exception("""check compatible:{0} sync timeout""".format(current_data_version))
#      raise e
#    time.sleep(5)
#
#  # check target_data_version/current_data_version from __all_core_table
#  int_current_data_version = actions.get_version(current_data_version)
#  sql = "select count(*) from __all_virtual_core_table where column_name in ('target_data_version', 'current_data_version') and column_value = {0} and tenant_id in ({1})".format(int_current_data_version, tenant_ids_str)
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1 or len(results[0]) != 1:
#    logging.warn('result cnt not match')
#    raise e
#  elif 2 * tenant_count != results[0][0]:
#    logging.warn('target_data_version/current_data_version not match with {0}, tenant_cnt:{1}, result_cnt:{2}'.format(current_data_version, tenant_count, results[0][0]))
#    raise e
#  else:
#    logging.info("all tenant's target_data_version/current_data_version are match with {0}".format(current_data_version))
#
## 3 检查内部表自检是否成功
#def check_root_inspection(query_cur, timeout):
#  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where info != 'succeed'"
#  times = timeout if timeout > 0 else 180
#  while times > 0 :
#    (desc, results) = query_cur.exec_query(sql)
#    if results[0][0] == 0:
#      break
#    time.sleep(10)
#    times -= 1
#  if times == 0:
#    logging.warn('check root inspection failed!')
#    raise e
#  logging.info('check root inspection success')
#
## 4 开ddl
#def enable_ddl(cur, timeout):
#  actions.set_parameter(cur, 'enable_ddl', 'True', timeout)
#
## 5 打开rebalance
#def enable_rebalance(cur, timeout):
#  actions.set_parameter(cur, 'enable_rebalance', 'True', timeout)
#
## 6 打开rereplication
#def enable_rereplication(cur, timeout):
#  actions.set_parameter(cur, 'enable_rereplication', 'True', timeout)
#
## 7 打开major freeze
#def enable_major_freeze(cur, timeout):
#  actions.set_parameter(cur, 'enable_major_freeze', 'True', timeout)
#  actions.set_tenant_parameter(cur, '_enable_adaptive_compaction', 'True', timeout)
#  actions.do_resume_merge(cur, timeout)
#
## 开始升级后的检查
#def do_check(conn, cur, query_cur, timeout):
#  try:
#    check_cluster_version(cur, timeout)
#    check_data_version(cur, query_cur, timeout)
#    check_root_inspection(query_cur, timeout)
#    enable_ddl(cur, timeout)
#    enable_rebalance(cur, timeout)
#    enable_rereplication(cur, timeout)
#    enable_major_freeze(cur, timeout)
#  except Exception, e:
#    logging.exception('run error')
#    raise e
####====XXXX======######==== I am a splitter ====######======XXXX====####
#sub file module end


import os
import sys
import datetime
from random import Random

class SplitError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

def random_str(rand_str_len = 8):
  str = ''
  chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
  length = len(chars) - 1
  random = Random()
  for i in range(rand_str_len):
    str += chars[random.randint(0, length)]
  return str

def split_py_files(sub_files_dir):
  char_enter = '\n'
  file_splitter_line = '####====XXXX======######==== I am a splitter ====######======XXXX====####'
  sub_filename_line_prefix = '#filename:'
  sub_file_module_end_line = '#sub file module end'
  os.makedirs(sub_files_dir)
  print('succeed to create run dir: ' + sub_files_dir + char_enter)
  cur_file = open(sys.argv[0], 'r')
  cur_file_lines = cur_file.readlines()
  cur_file_lines_count = len(cur_file_lines)
  sub_file_lines = []
  sub_filename = ''
  begin_read_sub_py_file = False
  is_first_splitter_line = True
  i = 0
  while i < cur_file_lines_count:
    if (file_splitter_line + char_enter) != cur_file_lines[i]:
      if begin_read_sub_py_file:
        sub_file_lines.append(cur_file_lines[i])
    else:
      if is_first_splitter_line:
        is_first_splitter_line = False
      else:
        #读完一个子文件了，写到磁盘中
        sub_file = open(sub_files_dir + '/' + sub_filename, 'w')
        for sub_file_line in sub_file_lines:
          sub_file.write(sub_file_line[1:])
        sub_file.close()
        #清空sub_file_lines
        sub_file_lines = []
      #再读取下一行的文件名或者结束标记
      i += 1
      if i >= cur_file_lines_count:
        raise SplitError('invalid line index:' + str(i) + ', lines_count:' + str(cur_file_lines_count))
      elif (sub_file_module_end_line + char_enter) == cur_file_lines[i]:
        print 'succeed to split all sub py files'
        break
      else:
        mark_idx = cur_file_lines[i].find(sub_filename_line_prefix)
        if 0 != mark_idx:
          raise SplitError('invalid sub file name line, mark_idx = ' + str(mark_idx) + ', line = ' + cur_file_lines[i])
        else:
          sub_filename = cur_file_lines[i][len(sub_filename_line_prefix):-1]
          begin_read_sub_py_file = True
    i += 1
  cur_file.close()



if __name__ == '__main__':
  cur_filename = sys.argv[0][sys.argv[0].rfind(os.sep)+1:]
  (cur_file_short_name,cur_file_ext_name1) = os.path.splitext(sys.argv[0])
  (cur_file_real_name,cur_file_ext_name2) = os.path.splitext(cur_filename)
  sub_files_dir_suffix = '_extract_files_' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '_' + random_str()
  sub_files_dir = cur_file_short_name + sub_files_dir_suffix
  sub_files_short_dir = cur_file_real_name + sub_files_dir_suffix
  split_py_files(sub_files_dir)
  exec('from ' + sub_files_short_dir + '.do_upgrade_post import do_upgrade_by_argv')
  do_upgrade_by_argv(sys.argv[1:])
