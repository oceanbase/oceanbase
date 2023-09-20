#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import re
import json
import traceback
import sys
import mysql.connector
from mysql.connector import errorcode
from my_error import MyError
import logging

class SqlItem:
  action_sql = None
  rollback_sql = None
  def __init__(self, action_sql, rollback_sql):
    self.action_sql = action_sql
    self.rollback_sql = rollback_sql

current_cluster_version = "4.3.0.0"
current_data_version = "4.3.0.0"
g_succ_sql_list = []
g_commit_sql_list = []

def get_current_cluster_version():
  return current_cluster_version

def get_current_data_version():
  return current_data_version

def refresh_commit_sql_list():
  global g_succ_sql_list
  global g_commit_sql_list
  if len(g_commit_sql_list) < len(g_succ_sql_list):
    for i in range(len(g_commit_sql_list), len(g_succ_sql_list)):
      g_commit_sql_list.append(g_succ_sql_list[i])

def get_succ_sql_list_str():
  global g_succ_sql_list
  ret_str = ''
  for i in range(0, len(g_succ_sql_list)):
    if i > 0:
      ret_str += '\n'
    ret_str += g_succ_sql_list[i].action_sql + ';'
  return ret_str

def get_commit_sql_list_str():
  global g_commit_sql_list
  ret_str = ''
  for i in range(0, len(g_commit_sql_list)):
    if i > 0:
      ret_str += '\n'
    ret_str += g_commit_sql_list[i].action_sql + ';'
  return ret_str

def get_rollback_sql_file_lines_str():
  global g_commit_sql_list
  ret_str = ''
  g_commit_sql_list_len = len(g_commit_sql_list)
  for i in range(0, g_commit_sql_list_len):
    if i > 0:
      ret_str += '\n'
    idx = g_commit_sql_list_len - 1 - i
    ret_str += '/*\n' + g_commit_sql_list[idx].action_sql + ';\n*/\n'
    ret_str += g_commit_sql_list[idx].rollback_sql + ';'
  return ret_str

def dump_rollback_sql_to_file(rollback_sql_filename):
  logging.info('===================== begin to dump rollback sql file ============================')
  rollback_sql_file = open(rollback_sql_filename, 'w')
  rollback_sql_file.write('# 此文件是回滚用的sql。\n')
  rollback_sql_file.write('# 注释的sql是已经成功commit的sql，它的下一条没被注释的sql则是对应的回滚sql。回滚的sql的排序跟commit的sql的排序刚好相反。\n')
  rollback_sql_file.write('# 跑升级脚本失败的时候可以参考本文件来进行回滚。\n')
  rollback_sql_file.write('\n')
  rollback_sql_file_lines_str = get_rollback_sql_file_lines_str()
  rollback_sql_file.write(rollback_sql_file_lines_str)
  rollback_sql_file.close()
  logging.info('=========== succeed to dump rollback sql file to: ' + rollback_sql_filename + '===============')

def check_is_ddl_sql(sql):
  word_list = sql.split()
  if len(word_list) < 1:
    raise MyError('sql is empty, sql="{0}"'.format(sql))
  key_word = word_list[0].lower()
  if 'create' != key_word and 'alter' != key_word:
    raise MyError('sql must be ddl, key_word="{0}", sql="{1}"'.format(key_word, sql))

def check_is_query_sql(sql):
  word_list = sql.split()
  if len(word_list) < 1:
    raise MyError('sql is empty, sql="{0}"'.format(sql))
  key_word = word_list[0].lower()
  if 'select' != key_word and 'show' != key_word and 'desc' != key_word:
    raise MyError('sql must be query, key_word="{0}", sql="{1}"'.format(key_word, sql))

def check_is_update_sql(sql):
  word_list = sql.split()
  if len(word_list) < 1:
    raise MyError('sql is empty, sql="{0}"'.format(sql))
  key_word = word_list[0].lower()
  if 'insert' != key_word and 'update' != key_word and 'replace' != key_word and 'set' != key_word and 'delete' != key_word:
    # 还有类似这种：select @current_ts := now()
    if not (len(word_list) >= 3 and 'select' == word_list[0].lower()\
        and word_list[1].lower().startswith('@') and ':=' == word_list[2].lower()):
      raise MyError('sql must be update, key_word="{0}", sql="{1}"'.format(key_word, sql))

def get_min_cluster_version(cur):
  min_cluster_version = 0
  sql = """select distinct value from oceanbase.GV$OB_PARAMETERS where name='min_observer_version'"""
  logging.info(sql)
  cur.execute(sql)
  results = cur.fetchall()
  if len(results) != 1:
    logging.exception('min_observer_version is not sync')
    raise e
  elif len(results[0]) != 1:
    logging.exception('column cnt not match')
    raise e
  else:
    min_cluster_version = get_version(results[0][0])
  return min_cluster_version

def set_parameter(cur, parameter, value, timeout = 0):
  sql = """alter system set {0} = '{1}'""".format(parameter, value)
  logging.info(sql)
  cur.execute(sql)
  wait_parameter_sync(cur, False, parameter, value, timeout)

def set_tenant_parameter(cur, parameter, value, timeout = 0):
  tenants_list = []
  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
    tenants_list = ['all']
  else:
    tenants_list = ['sys', 'all_user', 'all_meta']
  for tenants in tenants_list:
    sql = """alter system set {0} = '{1}' tenant = '{2}'""".format(parameter, value, tenants)
    logging.info(sql)
    cur.execute(sql)
  wait_parameter_sync(cur, True, parameter, value, timeout)

def get_ori_enable_ddl(cur, timeout):
  ori_value_str = fetch_ori_enable_ddl(cur)
  wait_parameter_sync(cur, False, 'enable_ddl', ori_value_str, timeout)
  ori_value = (ori_value_str == 'True')
  return ori_value

def fetch_ori_enable_ddl(cur):
  ori_value = 'True'
  sql = """select value from oceanbase.__all_sys_parameter where name = 'enable_ddl'"""

  logging.info(sql)
  cur.execute(sql)
  result = cur.fetchall()

  if len(result) == 0:
    # means default value, is True
    ori_value = 'True'
  elif len(result) != 1 or len(result[0]) != 1:
    logging.exception('result cnt not match')
    raise e
  elif result[0][0].lower() in ["1", "true", "on", "yes", 't']:
    ori_value = 'True'
  elif result[0][0].lower() in ["0", "false", "off", "no", 'f']:
    ori_value = 'False'
  else:
    logging.exception("""result value is invalid, result:{0}""".format(result[0][0]))
    raise e
  return ori_value

# print version like "x.x.x.x"
def print_version(version):
  version = int(version)
  major = (version >> 32) & 0xffffffff
  minor = (version >> 16) & 0xffff
  major_patch = (version >> 8) & 0xff
  minor_patch = version & 0xff
  version_str = "{0}.{1}.{2}.{3}".format(major, minor, major_patch, minor_patch)

# version str should like "x.x.x.x"
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

def check_server_version_by_cluster(cur):
  sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server""";
  logging.info(sql)
  cur.execute(sql)
  result = cur.fetchall()
  if len(result) != 1:
    raise MyError("servers build_version not match")
  else:
    logging.info("check server version success")

def check_parameter(cur, is_tenant_config, key, value):
  table_name = "GV$OB_PARAMETERS" if not is_tenant_config else "__all_virtual_tenant_parameter_info"
  sql = """select * from oceanbase.{0}
           where name = '{1}' and value = '{2}'""".format(table_name, key, value)
  logging.info(sql)
  cur.execute(sql)
  result = cur.fetchall()
  bret = False
  if len(result) > 0:
    bret = True
  else:
    bret = False
  return bret

def wait_parameter_sync(cur, is_tenant_config, key, value, timeout):
  table_name = "GV$OB_PARAMETERS" if not is_tenant_config else "__all_virtual_tenant_parameter_info"
  sql = """select count(*) as cnt from oceanbase.{0}
           where name = '{1}' and value != '{2}'""".format(table_name, key, value)
  times = (timeout if timeout > 0 else 60) / 5
  while times >= 0:
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
    if times == -1:
      logging.exception("""check {0}:{1} sync timeout""".format(key, value))
      raise e
    time.sleep(5)

def do_begin_upgrade(cur, timeout):

  if not check_parameter(cur, False, "enable_upgrade_mode", "True"):
    action_sql = "alter system begin upgrade"
    rollback_sql = "alter system end upgrade"
    logging.info(action_sql)

    cur.execute(action_sql)

    global g_succ_sql_list
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

  wait_parameter_sync(cur, False, "enable_upgrade_mode", "True", timeout)


def do_begin_rolling_upgrade(cur, timeout):

  if not check_parameter(cur, False, "_upgrade_stage", "DBUPGRADE"):
    action_sql = "alter system begin rolling upgrade"
    rollback_sql = "alter system end upgrade"

    logging.info(action_sql)
    cur.execute(action_sql)

    global g_succ_sql_list
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

  wait_parameter_sync(cur, False, "_upgrade_stage", "DBUPGRADE", timeout)


def do_end_rolling_upgrade(cur, timeout):

  # maybe in upgrade_post_check stage or never run begin upgrade
  if check_parameter(cur, False, "enable_upgrade_mode", "False"):
    return

  current_cluster_version = get_current_cluster_version()
  if not check_parameter(cur, False, "_upgrade_stage", "POSTUPGRADE") or not check_parameter(cur, False, "min_observer_version", current_cluster_version):
    action_sql = "alter system end rolling upgrade"
    rollback_sql = "alter system end upgrade"

    logging.info(action_sql)
    cur.execute(action_sql)

    global g_succ_sql_list
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

  wait_parameter_sync(cur, False, "min_observer_version", current_data_version, timeout)
  wait_parameter_sync(cur, False, "_upgrade_stage", "POSTUPGRADE", timeout)


def do_end_upgrade(cur, timeout):

  if not check_parameter(cur, False, "enable_upgrade_mode", "False"):
    action_sql = "alter system end upgrade"
    rollback_sql = ""

    logging.info(action_sql)
    cur.execute(action_sql)

    global g_succ_sql_list
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

  wait_parameter_sync(cur, False, "enable_upgrade_mode", "False", timeout)

def do_suspend_merge(cur, timeout):
  tenants_list = []
  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
    tenants_list = ['all']
  else:
    tenants_list = ['sys', 'all_user', 'all_meta']
  for tenants in tenants_list:
    action_sql = "alter system suspend merge tenant = {0}".format(tenants)
    rollback_sql = "alter system resume merge tenant = {0}".format(tenants)
    logging.info(action_sql)
    cur.execute(action_sql)

def do_resume_merge(cur, timeout):
  tenants_list = []
  if get_min_cluster_version(cur) < get_version("4.2.1.0"):
    tenants_list = ['all']
  else:
    tenants_list = ['sys', 'all_user', 'all_meta']
  for tenants in tenants_list:
    action_sql = "alter system resume merge tenant = {0}".format(tenants)
    rollback_sql = "alter system suspend merge tenant = {0}".format(tenants)
    logging.info(action_sql)
    cur.execute(action_sql)

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

class DDLCursor:
  _cursor = None
  def __init__(self, cursor):
    self._cursor = Cursor(cursor)
  def exec_ddl(self, sql, print_when_succ = True):
    try:
      # 这里检查是不是ddl，不是ddl就抛错
      check_is_ddl_sql(sql)
      return self._cursor.exec_sql(sql, print_when_succ)
    except Exception, e:
      logging.exception('fail to execute ddl: %s', sql)
      raise e

class QueryCursor:
  _cursor = None
  def __init__(self, cursor):
    self._cursor = Cursor(cursor)
  def exec_query(self, sql, print_when_succ = True):
    try:
      # 这里检查是不是query，不是query就抛错
      check_is_query_sql(sql)
      return self._cursor.exec_query(sql, print_when_succ)
    except Exception, e:
      logging.exception('fail to execute dml query: %s', sql)
      raise e

class DMLCursor(QueryCursor):
  def exec_update(self, sql, print_when_succ = True):
    try:
      # 这里检查是不是update，不是update就抛错
      check_is_update_sql(sql)
      return self._cursor.exec_sql(sql, print_when_succ)
    except Exception, e:
      logging.exception('fail to execute dml update: %s', sql)
      raise e

class BaseDDLAction():
  __ddl_cursor = None
  _query_cursor = None
  def __init__(self, cursor):
    self.__ddl_cursor = DDLCursor(cursor)
    self._query_cursor = QueryCursor(cursor)
  def do_action(self):
    global g_succ_sql_list
    action_sql = self.get_action_ddl()
    rollback_sql = self.get_rollback_sql()
    self.__ddl_cursor.exec_ddl(action_sql)
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
    # ddl马上就提交了，因此刷新g_commit_sql_list
    refresh_commit_sql_list()

class BaseDMLAction():
  __dml_cursor = None
  _query_cursor = None
  def __init__(self, cursor):
    self.__dml_cursor = DMLCursor(cursor)
    self._query_cursor = QueryCursor(cursor)
  def do_action(self):
    global g_succ_sql_list
    action_sql = self.get_action_dml()
    rollback_sql = self.get_rollback_sql()
    self.__dml_cursor.exec_update(action_sql)
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

class BaseEachTenantDMLAction():
  __dml_cursor = None
  _query_cursor = None
  _tenant_id_list = None
  _cursor = None
  def __init__(self, cursor, tenant_id_list):
    self.__dml_cursor = DMLCursor(cursor)
    self._query_cursor = QueryCursor(cursor)
    self._tenant_id_list = tenant_id_list
    self._cursor = Cursor(cursor)
  def get_tenant_id_list(self):
    return self._tenant_id_list
  def do_each_tenant_action(self, tenant_id):
    global g_succ_sql_list
    action_sql = self.get_each_tenant_action_dml(tenant_id)
    rollback_sql = self.get_each_tenant_rollback_sql(tenant_id)
    self.__dml_cursor.exec_update(action_sql)
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))

class BaseEachTenantDDLAction():
  __dml_cursor = None
  _query_cursor = None
  _tenant_id_list = None
  _all_table_name = "__all_table"
  def __init__(self, cursor, tenant_id_list):
    self.__ddl_cursor = DDLCursor(cursor)
    self._query_cursor = QueryCursor(cursor)
    self._tenant_id_list = tenant_id_list
  def get_tenant_id_list(self):
    return self._tenant_id_list
  def get_all_table_name(self):
    return self._all_table_name
  def set_all_table_name(self, table_name):
    self._all_table_name = table_name
  def do_each_tenant_action(self, tenant_id):
    global g_succ_sql_list
    action_sql = self.get_each_tenant_action_ddl(tenant_id)
    rollback_sql = self.get_each_tenant_rollback_sql(tenant_id)
    self.__ddl_cursor.exec_ddl(action_sql)
    g_succ_sql_list.append(SqlItem(action_sql, rollback_sql))
    # ddl马上就提交了，因此刷新g_commit_sql_list
    refresh_commit_sql_list()

def actions_cls_compare(x, y):
  diff = x.get_seq_num() - y.get_seq_num()
  if 0 == diff:
    raise MyError('seq num is equal')
  elif diff < 0:
    return -1
  else:
    return 1

def reflect_action_cls_list(action_module, action_name_prefix):
  action_cls_list = []
  cls_from_actions = dir(action_module)
  for cls in cls_from_actions:
    if cls.startswith(action_name_prefix):
      action_cls = getattr(action_module, cls)
      action_cls_list.append(action_cls)
  action_cls_list.sort(actions_cls_compare)
  return action_cls_list

def fetch_observer_version(cur):
  sql = """select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'"""
  logging.info(sql)
  cur.execute(sql)
  result = cur.fetchall()
  if len(result) != 1:
    raise MyError('query results count is not 1')
  else:
    logging.info('get observer version success, version = {0}'.format(result[0][0]))
  return result[0][0]

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

