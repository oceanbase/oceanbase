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

g_succ_sql_list = []
g_commit_sql_list = []

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

def set_parameter(cur, parameter, value):
  sql = """alter system set {0} = '{1}'""".format(parameter, value)
  logging.info(sql)
  cur.execute(sql)
  wait_parameter_sync(cur, parameter, value)

def get_ori_enable_ddl(cur):
  ori_value_str = fetch_ori_enable_ddl(cur)
  wait_parameter_sync(cur, 'enable_ddl', ori_value_str)
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
  def change_tenant(self, tenant_id):
    self._cursor.exec_sql("alter system change tenant tenant_id = {0}".format(tenant_id))
    (desc, results) = self._query_cursor.exec_query("select effective_tenant_id()")
    if (1 != len(results) or 1 != len(results[0])):
      raise MyError("results cnt not match")
    elif (tenant_id != results[0][0]):
      raise MyError("change tenant failed, effective_tenant_id:{0}, tenant_id:{1}"
                    .format(results[0][0], tenant_id))

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

def fetch_observer_version(query_cur):
  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
  if len(results) != 1:
    raise MyError('query results count is not 1')
  else:
    logging.info('get observer version success, version = {0}'.format(results[0][0]))
  return results[0][0]

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
                                         database =  'oceanbase',
                                         raise_on_warnings = True)

      tmp_cur = tmp_conn.cursor(buffered=True)
      tmp_conn.autocommit = True
      tmp_query_cur = QueryCursor(tmp_cur)
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

def check_ddl_and_dml_sync_by_cluster(standby_cluster_info, sys_infos):
  try:
    # connect
    logging.info("start to check ddl and dml sync by cluster: cluster_id = {0}"
                 .format(standby_cluster_info['cluster_id']))
    logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
                 .format(standby_cluster_info['cluster_id'],
                         standby_cluster_info['ip'],
                         standby_cluster_info['port']))
    tmp_conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
                                       password =  standby_cluster_info['pwd'],
                                       host     =  standby_cluster_info['ip'],
                                       port     =  standby_cluster_info['port'],
                                       database =  'oceanbase',
                                       raise_on_warnings = True)
    tmp_cur = tmp_conn.cursor(buffered=True)
    tmp_conn.autocommit = True
    tmp_query_cur = QueryCursor(tmp_cur)
    is_primary = check_current_cluster_is_primary(tmp_query_cur)
    if is_primary:
      logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                        .format(standby_cluster_info['cluster_id'],
                                standby_cluster_info['ip'],
                                standby_cluster_info['port']))
      raise e

    for sys_info in sys_infos:
      check_ddl_and_dml_sync_by_tenant(tmp_query_cur, sys_info)

    # close
    tmp_cur.close()
    tmp_conn.close()
    logging.info("""check_ddl_and_dml_sync_by_cluster success : cluster_id = {0}, ip = {1}, port = {2}"""
                    .format(standby_cluster_info['cluster_id'],
                            standby_cluster_info['ip'],
                            standby_cluster_info['port']))

  except Exception, e:
    logging.exception("""fail to check ddl and dml sync : cluster_id = {0}, ip = {1}, port = {2}"""
                         .format(standby_cluster_info['cluster_id'],
                                 standby_cluster_info['ip'],
                                 standby_cluster_info['port']))
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
