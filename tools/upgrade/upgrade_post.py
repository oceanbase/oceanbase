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
#g_succ_sql_list = []
#g_commit_sql_list = []
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
#def set_parameter(cur, parameter, value):
#  sql = """alter system set {0} = '{1}'""".format(parameter, value)
#  logging.info(sql)
#  cur.execute(sql)
#  wait_parameter_sync(cur, parameter, value)
#
#def get_ori_enable_ddl(cur):
#  ori_value_str = fetch_ori_enable_ddl(cur)
#  wait_parameter_sync(cur, 'enable_ddl', ori_value_str)
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
#  def change_tenant(self, tenant_id):
#    self._cursor.exec_sql("alter system change tenant tenant_id = {0}".format(tenant_id))
#    (desc, results) = self._query_cursor.exec_query("select effective_tenant_id()")
#    if (1 != len(results) or 1 != len(results[0])):
#      raise MyError("results cnt not match")
#    elif (tenant_id != results[0][0]):
#      raise MyError("change tenant failed, effective_tenant_id:{0}, tenant_id:{1}"
#                    .format(results[0][0], tenant_id))
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
#def fetch_observer_version(query_cur):
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('query results count is not 1')
#  else:
#    logging.info('get observer version success, version = {0}'.format(results[0][0]))
#  return results[0][0]
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
#def check_current_cluster_is_primary(query_cur):
#  try:
#    sql = """SELECT * FROM v$ob_cluster
#             WHERE cluster_role = "PRIMARY"
#             AND cluster_status = "VALID"
#             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
#    (desc, results) = query_cur.exec_query(sql)
#    is_primary = len(results) > 0
#    return is_primary
#  except Exception, e:
#    logging.exception("""fail to check current is primary""")
#    raise e
#
#def fetch_standby_cluster_infos(conn, query_cur, user, pwd):
#  try:
#    is_primary = check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.exception("""should be primary cluster""")
#      raise e
#
#    standby_cluster_infos = []
#    sql = """SELECT cluster_id, rootservice_list from v$ob_standby_status"""
#    (desc, results) = query_cur.exec_query(sql)
#
#    for r in results:
#      standby_cluster_info = {}
#      if 2 != len(r):
#        logging.exception("length not match")
#        raise e
#      standby_cluster_info['cluster_id'] = r[0]
#      standby_cluster_info['user'] = user
#      standby_cluster_info['pwd'] = pwd
#      # construct ip/port
#      address = r[1].split(";")[0] # choose first address in rs_list
#      standby_cluster_info['ip'] = str(address.split(":")[0])
#      standby_cluster_info['port'] = address.split(":")[2]
#      # append
#      standby_cluster_infos.append(standby_cluster_info)
#      logging.info("""cluster_info :  cluster_id = {0}, ip = {1}, port = {2}"""
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#    conn.commit()
#    # check standby cluster
#    for standby_cluster_info in standby_cluster_infos:
#      # connect
#      logging.info("""create connection : cluster_id = {0}, ip = {1}, port = {2}"""
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#
#      tmp_conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                         password =  standby_cluster_info['pwd'],
#                                         host     =  standby_cluster_info['ip'],
#                                         port     =  standby_cluster_info['port'],
#                                         database =  'oceanbase',
#                                         raise_on_warnings = True)
#
#      tmp_cur = tmp_conn.cursor(buffered=True)
#      tmp_conn.autocommit = True
#      tmp_query_cur = QueryCursor(tmp_cur)
#      is_primary = check_current_cluster_is_primary(tmp_query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#      # close
#      tmp_cur.close()
#      tmp_conn.close()
#
#    return standby_cluster_infos
#  except Exception, e:
#    logging.exception('fail to fetch standby cluster info')
#    raise e
#
#
#def check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_ids):
#  try:
#    conn.commit()
#    # check if need check ddl and dml sync
#    is_primary = check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.exception("""should be primary cluster""")
#      raise e
#
#    # fetch sys stats
#    sys_infos = []
#    sql = """SELECT tenant_id,
#                    refreshed_schema_version,
#                    min_sys_table_scn,
#                    min_user_table_scn
#             FROM oceanbase.v$ob_cluster_stats
#             ORDER BY tenant_id desc"""
#    (desc, results) = query_cur.exec_query(sql)
#    if len(tenant_ids) != len(results):
#      logging.exception("result not match")
#      raise e
#    else:
#      for i in range(len(results)):
#        if len(results[i]) != 4:
#          logging.exception("length not match")
#          raise e
#        elif results[i][0] != tenant_ids[i]:
#          logging.exception("tenant_id not match")
#          raise e
#        else:
#          sys_info = {}
#          sys_info['tenant_id'] = results[i][0]
#          sys_info['refreshed_schema_version'] = results[i][1]
#          sys_info['min_sys_table_scn'] = results[i][2]
#          sys_info['min_user_table_scn'] = results[i][3]
#          logging.info("sys info : {0}".format(sys_info))
#          sys_infos.append(sys_info)
#    conn.commit()
#
#    # check ddl and dml by cluster
#    for standby_cluster_info in standby_cluster_infos:
#      check_ddl_and_dml_sync_by_cluster(standby_cluster_info, sys_infos)
#
#  except Exception, e:
#    logging.exception("fail to check ddl and dml sync")
#    raise e
#
#def check_ddl_and_dml_sync_by_cluster(standby_cluster_info, sys_infos):
#  try:
#    # connect
#    logging.info("start to check ddl and dml sync by cluster: cluster_id = {0}"
#                 .format(standby_cluster_info['cluster_id']))
#    logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                 .format(standby_cluster_info['cluster_id'],
#                         standby_cluster_info['ip'],
#                         standby_cluster_info['port']))
#    tmp_conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                       password =  standby_cluster_info['pwd'],
#                                       host     =  standby_cluster_info['ip'],
#                                       port     =  standby_cluster_info['port'],
#                                       database =  'oceanbase',
#                                       raise_on_warnings = True)
#    tmp_cur = tmp_conn.cursor(buffered=True)
#    tmp_conn.autocommit = True
#    tmp_query_cur = QueryCursor(tmp_cur)
#    is_primary = check_current_cluster_is_primary(tmp_query_cur)
#    if is_primary:
#      logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                        .format(standby_cluster_info['cluster_id'],
#                                standby_cluster_info['ip'],
#                                standby_cluster_info['port']))
#      raise e
#
#    for sys_info in sys_infos:
#      check_ddl_and_dml_sync_by_tenant(tmp_query_cur, sys_info)
#
#    # close
#    tmp_cur.close()
#    tmp_conn.close()
#    logging.info("""check_ddl_and_dml_sync_by_cluster success : cluster_id = {0}, ip = {1}, port = {2}"""
#                    .format(standby_cluster_info['cluster_id'],
#                            standby_cluster_info['ip'],
#                            standby_cluster_info['port']))
#
#  except Exception, e:
#    logging.exception("""fail to check ddl and dml sync : cluster_id = {0}, ip = {1}, port = {2}"""
#                         .format(standby_cluster_info['cluster_id'],
#                                 standby_cluster_info['ip'],
#                                 standby_cluster_info['port']))
#    raise e
#
#def check_ddl_and_dml_sync_by_tenant(query_cur, sys_info):
#  try:
#    times = 1800 # 30min
#    logging.info("start to check ddl and dml sync by tenant : {0}".format(sys_info))
#    start_time = time.time()
#    sql = ""
#    if 1 == sys_info['tenant_id'] :
#      # 备库系统租户DML不走物理同步，需要升级脚本负责写入，系统租户仅校验DDL同步
#      sql = """SELECT count(*)
#               FROM oceanbase.v$ob_cluster_stats
#               WHERE tenant_id = {0}
#                     AND refreshed_schema_version >= {1}
#            """.format(sys_info['tenant_id'],
#                       sys_info['refreshed_schema_version'])
#    else:
#      sql = """SELECT count(*)
#               FROM oceanbase.v$ob_cluster_stats
#               WHERE tenant_id = {0}
#                     AND refreshed_schema_version >= {1}
#                     AND min_sys_table_scn >= {2}
#                     AND min_user_table_scn >= {3}
#            """.format(sys_info['tenant_id'],
#                       sys_info['refreshed_schema_version'],
#                       sys_info['min_sys_table_scn'],
#                       sys_info['min_user_table_scn'])
#    while times > 0 :
#      (desc, results) = query_cur.exec_query(sql)
#      if len(results) == 1 and results[0][0] == 1:
#        break;
#      time.sleep(1)
#      times -= 1
#    if times == 0:
#      logging.exception("check ddl and dml sync timeout! : {0}, cost = {1}"
#                    .format(sys_info, time.time() - start_time))
#      raise e
#    else:
#      logging.info("check ddl and dml sync success! : {0}, cost = {1}"
#                   .format(sys_info, time.time() - start_time))
#
#  except Exception, e:
#    logging.exception("fail to check ddl and dml sync : {0}".format(sys_info))
#    raise e
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
#import normal_ddl_actions_post
#import normal_dml_actions_post
#import each_tenant_dml_actions_post
#import each_tenant_ddl_actions_post
#import special_upgrade_action_post
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
#
#def dump_sql_to_file(dump_filename, tenant_id_list):
#  normal_ddls_str = normal_ddl_actions_post.get_normal_ddl_actions_sqls_str()
#  normal_dmls_str = normal_dml_actions_post.get_normal_dml_actions_sqls_str()
#  each_tenant_dmls_str = each_tenant_dml_actions_post.get_each_tenant_dml_actions_sqls_str(tenant_id_list)
#  dump_file = open(dump_filename, 'w')
#  dump_file.write('# 以下是upgrade_post.py脚本中的步骤\n')
#  dump_file.write('# 仅供upgrade_post.py脚本运行失败需要人肉的时候参考\n')
#  dump_file.write('\n\n')
#  dump_file.write('# normal ddl\n')
#  dump_file.write(normal_ddls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# normal dml\n')
#  dump_file.write(normal_dmls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# each tenant dml\n')
#  dump_file.write(each_tenant_dmls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# do special upgrade actions\n')
#  dump_file.write('# please run ./upgrade_post.py -h [host] -P [port] -u [user] -p [password] -m special_action\n')
#  dump_file.write('\n\n')
#  dump_file.close()
#
#def check_before_upgrade(query_cur, upgrade_params):
#  check_server_version(query_cur)
#  return
#
## 混部阶段执行POST脚本，会导致租户级系统表创建可能多数派处于旧binary而被GC,
## 需要规避OCP升级流程异常导致的混部阶段执行POST脚本的问题
#def check_server_version(query_cur):
#  sql = """select distinct(substring_index(build_version, '_', 1)) from __all_server""";
#  (desc, results) = query_cur.exec_query(sql);
#  if len(results) != 1:
#    raise MyError("servers build_version not match")
#  else:
#    logging.info("check server version success")
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
#def do_upgrade(my_host, my_port, my_user, my_passwd, my_module_set, upgrade_params):
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
#      # 开始升级前的检查
#      check_before_upgrade(query_cur, upgrade_params)
#      # 获取租户id列表
#      tenant_id_list = actions.fetch_tenant_ids(query_cur)
#      if len(tenant_id_list) <= 0:
#        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
#        raise MyError('no tenant id')
#      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
#      conn.commit()
#
#      actions.refresh_commit_sql_list()
#      dump_sql_to_file(upgrade_params.sql_dump_filename, tenant_id_list)
#      logging.info('================succeed to dump sql to file: {0}==============='.format(upgrade_params.sql_dump_filename))
#
#      if run_modules.MODULE_DDL in my_module_set:
#        logging.info('================begin to run ddl===============')
#        conn.autocommit = True
#        normal_ddl_actions_post.do_normal_ddl_actions(cur)
#        logging.info('================succeed to run ddl===============')
#        conn.autocommit = False
#
#      if run_modules.MODULE_EACH_TENANT_DDL in my_module_set:
#        has_run_ddl = True
#        logging.info('================begin to run each tenant ddl===============')
#        conn.autocommit = True
#        each_tenant_ddl_actions_post.do_each_tenant_ddl_actions(cur, tenant_id_list)
#        logging.info('================succeed to run each tenant ddl===============')
#        conn.autocommit = False
#
#      if run_modules.MODULE_NORMAL_DML in my_module_set:
#        logging.info('================begin to run normal dml===============')
#        normal_dml_actions_post.do_normal_dml_actions(cur)
#        logging.info('================succeed to run normal dml===============')
#        conn.commit()
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit dml===============')
#
#      if run_modules.MODULE_EACH_TENANT_DML in my_module_set:
#        logging.info('================begin to run each tenant dml===============')
#        conn.autocommit = True
#        each_tenant_dml_actions_post.do_each_tenant_dml_actions(cur, tenant_id_list)
#        conn.autocommit = False
#        logging.info('================succeed to run each tenant dml===============')
#
#      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
#        logging.info('================begin to run special action===============')
#        conn.autocommit = True
#        special_upgrade_action_post.do_special_upgrade(conn, cur, tenant_id_list, my_user, my_passwd)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit special action===============')
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      # 打印统计信息
#      print_stats()
#      # 将回滚sql写到文件中
#      actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
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
#    opts.deal_with_local_opts()
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", module=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, module_set, log_filename)
#      do_upgrade(host, port, user, password, module_set, upgrade_params)
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
#import normal_ddl_actions_pre
#import normal_dml_actions_pre
#import each_tenant_dml_actions_pre
#import upgrade_sys_vars
#import special_upgrade_action_pre
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
#def dump_sql_to_file(cur, query_cur, dump_filename, tenant_id_list, update_sys_var_list, add_sys_var_list):
#  normal_ddls_str = normal_ddl_actions_pre.get_normal_ddl_actions_sqls_str(query_cur)
#  normal_dmls_str = normal_dml_actions_pre.get_normal_dml_actions_sqls_str()
#  each_tenant_dmls_str = each_tenant_dml_actions_pre.get_each_tenant_dml_actions_sqls_str(tenant_id_list)
#  sys_vars_upgrade_dmls_str = upgrade_sys_vars.get_sys_vars_upgrade_dmls_str(cur, query_cur, tenant_id_list, update_sys_var_list, add_sys_var_list)
#  dump_file = open(dump_filename, 'w')
#  dump_file.write('# 以下是upgrade_pre.py脚本中的步骤\n')
#  dump_file.write('# 仅供upgrade_pre.py脚本运行失败需要人肉的时候参考\n')
#  dump_file.write('\n\n')
#  dump_file.write('# normal ddl\n')
#  dump_file.write(normal_ddls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# normal dml\n')
#  dump_file.write(normal_dmls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# each tenant dml\n')
#  dump_file.write(each_tenant_dmls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# upgrade sys vars\n')
#  dump_file.write(sys_vars_upgrade_dmls_str + '\n')
#  dump_file.write('\n\n')
#  dump_file.write('# do special upgrade actions\n')
#  dump_file.write('# please run ./upgrade_pre.py -h [host] -P [port] -u [user] -p [password] -m special_action\n')
#  dump_file.write('\n\n')
#  dump_file.close()
#
#def check_before_upgrade(query_cur, upgrade_params):
#  return
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
#def do_upgrade(my_host, my_port, my_user, my_passwd, my_module_set, upgrade_params):
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
#      # 开始升级前的检查
#      check_before_upgrade(query_cur, upgrade_params)
#      # get min_observer_version
#      version = actions.fetch_observer_version(query_cur)
#      need_check_standby_cluster = cmp(version, '2.2.40') >= 0
#      # 获取租户id列表
#      tenant_id_list = actions.fetch_tenant_ids(query_cur)
#      if len(tenant_id_list) <= 0:
#        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
#        raise MyError('no tenant id')
#      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
#      # 计算需要添加或更新的系统变量
#      conn.commit()
#
#      conn.autocommit = True
#      (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = upgrade_sys_vars.calc_diff_sys_var(cur, tenant_id_list[0])
#      dump_sql_to_file(cur, query_cur, upgrade_params.sql_dump_filename, tenant_id_list, update_sys_var_list, add_sys_var_list)
#      conn.autocommit = False
#      conn.commit()
#      logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
#      logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
#      logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
#      logging.info('================succeed to dump sql to file: {0}==============='.format(upgrade_params.sql_dump_filename))
#
#      if run_modules.MODULE_DDL in my_module_set:
#        logging.info('================begin to run ddl===============')
#        conn.autocommit = True
#        normal_ddl_actions_pre.do_normal_ddl_actions(cur)
#        logging.info('================succeed to run ddl===============')
#        conn.autocommit = False
#
#      if run_modules.MODULE_NORMAL_DML in my_module_set:
#        logging.info('================begin to run normal dml===============')
#        normal_dml_actions_pre.do_normal_dml_actions(cur)
#        logging.info('================succeed to run normal dml===============')
#        conn.commit()
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit dml===============')
#
#      if run_modules.MODULE_EACH_TENANT_DML in my_module_set:
#        logging.info('================begin to run each tenant dml===============')
#        conn.autocommit = True
#        each_tenant_dml_actions_pre.do_each_tenant_dml_actions(cur, tenant_id_list)
#        conn.autocommit = False
#        logging.info('================succeed to run each tenant dml===============')
#
#      # 更新系统变量
#      if run_modules.MODULE_SYSTEM_VARIABLE_DML in my_module_set:
#        logging.info('================begin to run system variable dml===============')
#        conn.autocommit = True
#        upgrade_sys_vars.exec_sys_vars_upgrade_dml(cur, tenant_id_list)
#        conn.autocommit = False
#        logging.info('================succeed to run system variable dml===============')
#
#      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
#        logging.info('================begin to run special action===============')
#        conn.autocommit = True
#        special_upgrade_action_pre.do_special_upgrade(conn, cur, tenant_id_list, my_user, my_passwd)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit special action===============')
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      # 打印统计信息
#      print_stats()
#      # 将回滚sql写到文件中
#      actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
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
#    opts.deal_with_local_opts()
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", module=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, module_set, log_filename)
#      do_upgrade(host, port, user, password, module_set, upgrade_params)
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
#filename:each_tenant_ddl_actions_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#from actions import BaseEachTenantDDLAction
#from actions import reflect_action_cls_list
#from actions import fetch_observer_version
#from actions import QueryCursor
#import logging
#import time
#import my_utils
#import actions
#
#'''
#添加一条each tenant ddl的方法:
#
#在本文件中，添加一个类名以"EachTenantDDLActionPost"开头并且继承自BaseEachTenantDDLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)dump_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前把一些相关数据dump到日志中。
#(5)check_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前的检查。
#(6)@staticmethod get_each_tenant_action_ddl(tenant_id):
#返回用参数tenant_id拼成的一条action sql，并且该sql必须为ddl。
#(7)@staticmethod get_each_tenant_rollback_sql(tenant_id):
#返回一条sql，用于回滚get_each_tenant_action_ddl(tenant_id)返回的sql。
#(8)dump_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后把一些相关数据dump到日志中。
#(9)check_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后的检查。
#(10)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(11)check_after_do_action(self):
#执行action sql之后的检查。
#(12)skip_pre_check(self):
#check if check_before_do_action() can be skipped
#(13)skip_each_tenant_action(self):
#check if check_before_do_each_tenant_action() and do_each_tenant_action() can be skipped
#
#举例: 以下为schema拆分后加租户级系统表的示例
#class EachTenantDDLActionPostCreateAllTenantBackupBackupLogArchiveStatus(BaseEachTenantDDLAction):
#  table_name = '__all_tenant_backup_backup_log_archive_status'
#  @staticmethod
#  def get_seq_num():
#    return 24
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
#  def skip_pre_check(self):
#    return True
#  def skip_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
#    return (1 == len(results))
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
#    if len(results) > 0:
#      raise MyError("""{0} already created""".format(self.table_name))
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
#    if len(results) > 0:
#      raise MyError("""tenant_id:{0} has already create table {1}""".format(tenant_id, self.table_name))
#  @staticmethod
#  def get_each_tenant_action_ddl(tenant_id):
#    pure_table_id = 303
#    table_id = (tenant_id << 40) | pure_table_id
#    return """CREATE TABLE `__all_tenant_backup_backup_log_archive_status` (
#  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#  `tenant_id` bigint(20) NOT NULL,
#  `incarnation` bigint(20) NOT NULL,
#  `log_archive_round` bigint(20) NOT NULL,
#  `copy_id` bigint(20) NOT NULL,
#  `min_first_time` timestamp(6) NOT NULL,
#  `max_next_time` timestamp(6) NOT NULL,
#  `input_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `output_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `deleted_input_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `deleted_output_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `pg_count` bigint(20) NOT NULL DEFAULT '0',
#  `status` varchar(64) NOT NULL DEFAULT '',
#  PRIMARY KEY (`tenant_id`, `incarnation`, `log_archive_round`, `copy_id`)
#) TABLE_ID={0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
#    """.format(table_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """select 1"""
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
#    if len(results) != 1:
#      raise MyError("""tenant_id:{0} create table {1} failed""".format(tenant_id, self.table_name))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError("""there should be {0} rows in {1} whose table_name is {2}, but there has {3} rows like that""".format(len(self.get_tenant_id_list()), self.get_all_table_name(), self.table_name, len(results)))
#'''
#
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#class EachTenantDDLActionPostCreateAllKvTTLTasks(BaseEachTenantDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 0
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task'""".format(self.get_all_table_name()))
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task'""".format(self.get_all_table_name()))
#    if len(results) > 0:
#      raise MyError('__all_kv_ttl_task already created')
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#  def skip_pre_check(self):
#    return True
#  def skip_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    return (1 == len(results))
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    if len(results) > 0:
#      raise MyError('tenant_id:{0} has already create table __all_kv_ttl_task'.format(tenant_id))
#  @staticmethod
#  def get_each_tenant_action_ddl(tenant_id):
#    pure_table_id = 410
#    table_id = (tenant_id << 40) | pure_table_id
#    return """CREATE TABLE `__all_kv_ttl_task` (
#              `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#              `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#              `tenant_id` bigint(20) NOT NULL,
#              `task_id` bigint(20) NOT NULL,
#              `table_id` bigint(20) NOT NULL,
#              `partition_id` bigint(20) NOT NULL,
#              `task_start_time` bigint(20) NOT NULL,
#              `task_update_time` bigint(20) NOT NULL,
#              `trigger_type` bigint(20) NOT NULL,
#              `status` bigint(20) NOT NULL,
#              `ttl_del_cnt` bigint(20) NOT NULL,
#              `max_version_del_cnt` bigint(20) NOT NULL,
#              `scan_cnt` bigint(20) NOT NULL,
#              `row_key` varbinary(2048) NOT NULL,
#              `ret_code` varchar(512) NOT NULL,
#              PRIMARY KEY (`tenant_id`, `task_id`, `table_id`, `partition_id`)
#            ) TABLE_ID={0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = COMPACT COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' """.format(table_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """select 1"""
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    if len(results) != 1:
#      raise MyError('tenant_id:{0} create table __all_kv_ttl_task failed'.format(tenant_id))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task'""".format(self.get_all_table_name()))
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task'""".format(self.get_all_table_name()))
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError('there should be {0} rows in {1} whose table_name is __all_kv_ttl_task, but there has {2} rows like that'.format(len(self.get_tenant_id_list()), self.get_all_table_name(), len(results)))  
#
#class EachTenantDDLActionPostCreateAllKvTTLTaskHistory(BaseEachTenantDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 1
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history'""".format(self.get_all_table_name()))
#  def skip_pre_check(self):
#    return True
#  def skip_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    return (1 == len(results))
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history'""".format(self.get_all_table_name()))
#    if len(results) > 0:
#      raise MyError('__all_kv_ttl_task_history already created')
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    if len(results) > 0:
#      raise MyError('tenant_id:{0} has already create table __all_kv_ttl_task_history'.format(tenant_id))
#  @staticmethod
#  def get_each_tenant_action_ddl(tenant_id):
#    pure_table_id = 411
#    table_id = (tenant_id << 40) | pure_table_id
#    return """CREATE TABLE `__all_kv_ttl_task_history` (
#              `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#              `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#              `tenant_id` bigint(20) NOT NULL,
#              `task_id` bigint(20) NOT NULL,
#              `table_id` bigint(20) NOT NULL,
#              `partition_id` bigint(20) NOT NULL,
#              `task_start_time` bigint(20) NOT NULL,
#              `task_update_time` bigint(20) NOT NULL,
#              `trigger_type` bigint(20) NOT NULL,
#              `status` bigint(20) NOT NULL,
#              `ttl_del_cnt` bigint(20) NOT NULL,
#              `max_version_del_cnt` bigint(20) NOT NULL,
#              `scan_cnt` bigint(20) NOT NULL,
#              `row_key` varbinary(2048) NOT NULL,
#              `ret_code` varchar(512) NOT NULL,
#              PRIMARY KEY (`tenant_id`, `task_id`, `table_id`, `partition_id`)
#            ) TABLE_ID={0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = COMPACT COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' """.format(table_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """select 1"""
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history' and tenant_id = {1}""".format(self.get_all_table_name(), tenant_id))
#    if len(results) != 1:
#      raise MyError('tenant_id:{0} create table __all_kv_ttl_task_history failed'.format(tenant_id))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history'""".format(self.get_all_table_name()))
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '__all_kv_ttl_task_history'""".format(self.get_all_table_name()))
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError('there should be {0} rows in {1} whose table_name is __all_kv_ttl_task_history, but there has {2} rows like that'.format(len(self.get_tenant_id_list()), self.get_all_table_name(), len(results)))    
#####========******####========= actions end =========####******========####
#
#def do_each_tenant_ddl_actions(cur, tenant_id_list):
#  import each_tenant_ddl_actions_post
#  # 组户级系统表没法通过虚拟表暴露，需要根据版本决定查哪张实体表
#  query_cur = QueryCursor(cur)
#  version = fetch_observer_version(query_cur)
#  all_table_name = "__all_table"
#  if (cmp(version, "2.2.60") >= 0) :
#    all_table_name = "__all_table_v2"
#
#  cls_list = reflect_action_cls_list(each_tenant_ddl_actions_post, 'EachTenantDDLActionPost')
#
#  # set parameter
#  if len(cls_list) > 0:
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'True')
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#
#  for cls in cls_list:
#    logging.info('do each tenant ddl acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur, tenant_id_list)
#    action.set_all_table_name(all_table_name)
#    action.dump_before_do_action()
#    if False == action.skip_pre_check():
#      action.check_before_do_action()
#    else:
#      logging.info("skip pre check. seq_num: %d", cls.get_seq_num())
#    # 系统租户组户级系统表创建成功会覆盖普通租户系统表，所以系统租户要最后建表
#    for tenant_id in action.get_tenant_id_list():
#      action.dump_before_do_each_tenant_action(tenant_id)
#      if False == action.skip_each_tenant_action(tenant_id):
#        action.check_before_do_each_tenant_action(tenant_id)
#        action.do_each_tenant_action(tenant_id)
#      else:
#        logging.info("skip each tenant ddl action, seq_num: %d, tenant_id: %d", cls.get_seq_num(), tenant_id)
#      action.dump_after_do_each_tenant_action(tenant_id)
#      action.check_after_do_each_tenant_action(tenant_id)
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#  # reset parameter
#  if len(cls_list) > 0:
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl' , 'False')
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'False')
#
#def get_each_tenant_ddl_actions_sqls_str(tenant_id_list):
#  import each_tenant_ddl_actions_post
#  ret_str = ''
#  cls_list = reflect_action_cls_list(each_tenant_ddl_actions_post, 'EachTenantDDLActionPost')
#  for i in range(0, len(cls_list)):
#    for j in range(0, len(tenant_id_list)):
#      if i > 0 or j > 0:
#        ret_str += '\n'
#      ret_str += cls_list[i].get_each_tenant_action_ddl(tenant_id_list[j]) + ';'
#  return ret_str
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:each_tenant_dml_actions_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import mysql.connector
#from mysql.connector import errorcode
#from actions import BaseEachTenantDMLAction
#from actions import reflect_action_cls_list
#from actions import QueryCursor
#from actions import check_current_cluster_is_primary
#import logging
#import my_utils
#
#'''
#添加一条each tenant dml的方法:
#
#在本文件中，添加一个类名以"EachTenantDMLActionPost"开头并且继承自BaseEachTenantDMLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)dump_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前把一些相关数据dump到日志中。
#(5)check_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前的检查。
#(6)@staticmethod get_each_tenant_action_dml(tenant_id):
#返回用参数tenant_id拼成的一条action sql，并且该sql必须为dml。
#(7)@staticmethod get_each_tenant_rollback_sql(tenant_id):
#返回一条sql，用于回滚get_each_tenant_action_dml(tenant_id)返回的sql。
#(8)dump_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后把一些相关数据dump到日志中。
#(9)check_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后的检查。
#(10)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(11)check_after_do_action(self):
#执行action sql之后的检查。
#(12)skip_pre_check(self):
#check if check_before_do_action() can be skipped
#(13)skip_each_tenant_action(self):
#check if check_before_do_each_tenant_action() and do_each_tenant_action() can be skipped
#
#举例:
#class EachTenantDMLActionPost1(BaseEachTenantDMLAction):
#  @staticmethod
#  def get_seq_num():
#    return 0
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
#  def skip_pre_check(self):
#    return True
#  def skip_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    return (len(results) > 0)
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    if len(results) > 0:
#      raise MyError('some rows in table test.for_test1 whose c2 column is 9494 already exists')
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
#    if len(results) > 0:
#      raise MyError('some rows in table test.for_test1 whose pk is {0} already exists'.format(tenant_id))
#  @staticmethod
#  def get_each_tenant_action_dml(tenant_id):
#    return """insert into test.for_test1 value ({0}, 'for test 1', 9494)""".format(tenant_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """delete from test.for_test1 where pk = {0}""".format(tenant_id)
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
#    if len(results) != 1:
#      raise MyError('there should be only one row whose primary key is {0} in table test.for_test1, but there has {1} rows like that'.format(tenant_id, len(results)))
#    elif results[0][0] != tenant_id or results[0][1] != 'for test 1' or results[0][2] != 9494:
#      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError('there should be {0} rows whose c2 column is 9494 in table test.for_test1, but there has {1} rows like that'.format(len(self.get_tenant_id_list()), len(results)))
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#####========******####========= actions end =========####******========####
#def get_actual_tenant_id(tenant_id):
#  return tenant_id if (1 == tenant_id) else 0;
#
#def do_each_tenant_dml_actions_by_standby_cluster(standby_cluster_infos):
#  try:
#    tenant_id_list = [1]
#    for standby_cluster_info in standby_cluster_infos:
#      logging.info("do_each_tenant_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                     password =  standby_cluster_info['pwd'],
#                                     host     =  standby_cluster_info['ip'],
#                                     port     =  standby_cluster_info['port'],
#                                     database =  'oceanbase',
#                                     raise_on_warnings = True)
#
#      cur = conn.cursor(buffered=True)
#      conn.autocommit = True
#      query_cur = QueryCursor(cur)
#      is_primary = check_current_cluster_is_primary(query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#
#      ## process
#      do_each_tenant_dml_actions(cur, tenant_id_list)
#
#      cur.close()
#      conn.close()
#  except Exception, e:
#    logging.exception("""do_each_tenant_dml_actions_by_standby_cluster failed""")
#    raise e
#
#def do_each_tenant_dml_actions(cur, tenant_id_list):
#  import each_tenant_dml_actions_post
#  cls_list = reflect_action_cls_list(each_tenant_dml_actions_post, 'EachTenantDMLActionPost')
#  for cls in cls_list:
#    logging.info('do each tenant dml acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur, tenant_id_list)
#    sys_tenant_id = 1
#    action.change_tenant(sys_tenant_id)
#    action.dump_before_do_action()
#    if False == action.skip_pre_check():
#      action.check_before_do_action()
#    else:
#      logging.info("skip pre check. seq_num: %d", cls.get_seq_num())
#    for tenant_id in action.get_tenant_id_list():
#      action.change_tenant(tenant_id)
#      action.dump_before_do_each_tenant_action(tenant_id)
#      if False == action.skip_each_tenant_action(tenant_id):
#        action.check_before_do_each_tenant_action(tenant_id)
#        action.do_each_tenant_action(tenant_id)
#      else:
#        logging.info("skip each tenant dml action, seq_num: %d, tenant_id: %d", cls.get_seq_num(), tenant_id)
#      action.dump_after_do_each_tenant_action(tenant_id)
#      action.check_after_do_each_tenant_action(tenant_id)
#    action.change_tenant(sys_tenant_id)
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#def get_each_tenant_dml_actions_sqls_str(tenant_id_list):
#  import each_tenant_dml_actions_post
#  ret_str = ''
#  cls_list = reflect_action_cls_list(each_tenant_dml_actions_post, 'EachTenantDMLActionPost')
#  for i in range(0, len(cls_list)):
#    for j in range(0, len(tenant_id_list)):
#      if i > 0 or j > 0:
#        ret_str += '\n'
#      ret_str += cls_list[i].get_each_tenant_action_dml(tenant_id_list[j]) + ';'
#  return ret_str
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:each_tenant_dml_actions_pre.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import mysql.connector
#from mysql.connector import errorcode
#from actions import BaseEachTenantDMLAction
#from actions import reflect_action_cls_list
#from actions import fetch_observer_version
#from actions import QueryCursor
#from actions import check_current_cluster_is_primary
#import logging
#import my_utils
#import actions
#import re
#
#'''
#添加一条each tenant dml的方法:
#
#在本文件中，添加一个类名以"EachTenantDMLActionPre"开头并且继承自BaseEachTenantDMLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)dump_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前把一些相关数据dump到日志中。
#(5)check_before_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之前的检查。
#(6)@staticmethod get_each_tenant_action_dml(tenant_id):
#返回用参数tenant_id拼成的一条action sql，并且该sql必须为dml。
#(7)@staticmethod get_each_tenant_rollback_sql(tenant_id):
#返回一条sql，用于回滚get_each_tenant_action_dml(tenant_id)返回的sql。
#(8)dump_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后把一些相关数据dump到日志中。
#(9)check_after_do_each_tenant_action(self, tenant_id):
#执行用参数tenant_id拼成的这条action sql之后的检查。
#(10)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(11)check_after_do_action(self):
#执行action sql之后的检查。
#(12)skip_pre_check(self):
#check if check_before_do_action() can be skipped
#(13)skip_each_tenant_action(self):
#check if check_before_do_each_tenant_action() and do_each_tenant_action() can be skipped
#
#举例:
#class EachTenantDMLActionPre1(BaseEachTenantDMLAction):
#  @staticmethod
#  def get_seq_num():
#    return 0
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
#  def skip_pre_check(self):
#    return True
#  def skip_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    return (len(results) > 0)
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    if len(results) > 0:
#      raise MyError('some rows in table test.for_test1 whose c2 column is 9494 already exists')
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
#    if len(results) > 0:
#      raise MyError('some rows in table test.for_test1 whose pk is {0} already exists'.format(tenant_id))
#  @staticmethod
#  def get_each_tenant_action_dml(tenant_id):
#    return """insert into test.for_test1 value ({0}, 'for test 1', 9494)""".format(tenant_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """delete from test.for_test1 where pk = {0}""".format(tenant_id)
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
#    if len(results) != 1:
#      raise MyError('there should be only one row whose primary key is {0} in table test.for_test1, but there has {1} rows like that'.format(tenant_id, len(results)))
#    elif results[0][0] != tenant_id or results[0][1] != 'for test 1' or results[0][2] != 9494:
#      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError('there should be {0} rows whose c2 column is 9494 in table test.for_test1, but there has {1} rows like that'.format(len(self.get_tenant_id_list()), len(results)))
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#####========******####========= actions end =========####******========####
#def get_actual_tenant_id(tenant_id):
#  return tenant_id if (1 == tenant_id) else 0;
#
#def do_each_tenant_dml_actions(cur, tenant_id_list):
#  import each_tenant_dml_actions_pre
#  cls_list = reflect_action_cls_list(each_tenant_dml_actions_pre, 'EachTenantDMLActionPre')
#
#  # check if pre upgrade script can run reentrantly
#  query_cur = QueryCursor(cur)
#  version = fetch_observer_version(query_cur)
#  can_skip = False
#  if (cmp(version, "2.2.77") >= 0 and cmp(version, "3.0.0") < 0):
#    can_skip = True
#  elif (cmp(version, "3.1.1") >= 0):
#    can_skip = True
#  else:
#    can_skip = False
#
#  for cls in cls_list:
#    logging.info('do each tenant dml acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur, tenant_id_list)
#    sys_tenant_id = 1
#    action.change_tenant(sys_tenant_id)
#    action.dump_before_do_action()
#    if False == can_skip or False == action.skip_pre_check():
#      action.check_before_do_action()
#    else:
#      logging.info("skip pre check. seq_num: %d", cls.get_seq_num())
#    for tenant_id in action.get_tenant_id_list():
#      action.change_tenant(tenant_id)
#      action.dump_before_do_each_tenant_action(tenant_id)
#      if False == can_skip or False == action.skip_each_tenant_action(tenant_id):
#        action.check_before_do_each_tenant_action(tenant_id)
#        action.do_each_tenant_action(tenant_id)
#      else:
#        logging.info("skip each tenant dml action, seq_num: %d, tenant_id: %d", cls.get_seq_num(), tenant_id)
#      action.dump_after_do_each_tenant_action(tenant_id)
#      action.check_after_do_each_tenant_action(tenant_id)
#    action.change_tenant(sys_tenant_id)
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#def do_each_tenant_dml_actions_by_standby_cluster(standby_cluster_infos):
#  try:
#    tenant_id_list = [1]
#    for standby_cluster_info in standby_cluster_infos:
#      logging.info("do_each_tenant_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                     password =  standby_cluster_info['pwd'],
#                                     host     =  standby_cluster_info['ip'],
#                                     port     =  standby_cluster_info['port'],
#                                     database =  'oceanbase',
#                                     raise_on_warnings = True)
#
#      cur = conn.cursor(buffered=True)
#      conn.autocommit = True
#      query_cur = QueryCursor(cur)
#      is_primary = check_current_cluster_is_primary(query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#
#      ## process
#      do_each_tenant_dml_actions(cur, tenant_id_list)
#
#      cur.close()
#      conn.close()
#  except Exception, e:
#    logging.exception("""do_each_tenant_dml_actions_by_standby_cluster failed""")
#    raise e
#
#def get_each_tenant_dml_actions_sqls_str(tenant_id_list):
#  import each_tenant_dml_actions_pre
#  ret_str = ''
#  cls_list = reflect_action_cls_list(each_tenant_dml_actions_pre, 'EachTenantDMLActionPre')
#  for i in range(0, len(cls_list)):
#    for j in range(0, len(tenant_id_list)):
#      if i > 0 or j > 0:
#        ret_str += '\n'
#      ret_str += cls_list[i].get_each_tenant_action_dml(tenant_id_list[j]) + ';'
#  return ret_str
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
#filename:normal_ddl_actions_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#from actions import BaseDDLAction
#from actions import reflect_action_cls_list
#import logging
#import time
#import my_utils
#import actions
#
#'''
#添加一条normal ddl的方法:
#
#在本文件中，添加一个类名以"NormalDDLActionPost"开头并且继承自BaseDDLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)@staticmethod get_action_ddl():
#返回action sql，并且该sql必须为ddl。
#(5)@staticmethod get_rollback_sql():
#返回回滚该action的sql。
#(6)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(7)check_after_do_action(self):
#执行action sql之后的检查。
#(8)skip_action(self):
#check if check_before_do_action() and do_action() can be skipped
#
#改列示例：
#class NormalDDLActionPostModifyAllRestoreInfoValue(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 12
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
#  def skip_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
#    return len(results) > 0
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info like 'value'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_rootservice_event_history column value not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_restore_info modify column `value` longtext NOT NULL"""
#  @staticmethod
#  def get_rollback_sql():
#    return """"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column value for oceanbase.__all_restore_info')
#
#加列示例：
#class NormalDDLActionPostAllTenantProfileAddVerifyFunction(BaseDDLAction):
# @staticmethod
# def get_seq_num():
#   return 0
# def dump_before_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
# def skip_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   return len(results) > 0;
# def check_before_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   if len(results) != 0:
#     raise MyError('password_verify_function column alread exists')
# @staticmethod
# def get_action_ddl():
#   return """alter table oceanbase.__all_tenant_profile add column `password_verify_function` varchar(30) DEFAULT NULL id 23"""
# @staticmethod
# def get_rollback_sql():
#   return """alter table oceanbase.__all_tenant_profile drop column password_verify_function"""
# def dump_after_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
# def check_after_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   if len(results) != 1:
#     raise MyError('failed to add column password_verify_function for oceanbase.__all_tenant_profile')
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#####========******####========= actions end =========####******========####
#def do_normal_ddl_actions(cur):
#  import normal_ddl_actions_post
#  cls_list = reflect_action_cls_list(normal_ddl_actions_post, 'NormalDDLActionPost')
#
#  # set parameter
#  if len(cls_list) > 0:
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'True')
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#
#  for cls in cls_list:
#    logging.info('do normal ddl acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur)
#    action.dump_before_do_action()
#    if False == action.skip_action():
#      action.check_before_do_action()
#      action.do_action()
#    else:
#      logging.info("skip ddl action, seq_num: %d", cls.get_seq_num())
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#  # reset parameter
#  if len(cls_list) > 0:
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl' , 'False')
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'False')
#
#def get_normal_ddl_actions_sqls_str():
#  import normal_ddl_actions_post
#  ret_str = ''
#  cls_list = reflect_action_cls_list(normal_ddl_actions_post, 'NormalDDLActionPost')
#  for i in range(0, len(cls_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += cls_list[i].get_action_ddl() + ';'
#  return ret_str
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:normal_ddl_actions_pre.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#from my_error import MyError
#from actions import BaseDDLAction
#from actions import reflect_action_cls_list
#from actions import fetch_observer_version
#from actions import QueryCursor
#import logging
#import time
#import my_utils
#import actions
#import re
#
#class UpgradeParams:
#  low_version = '1.4.73'
#  high_version = '2.0.0'
#
#'''
#添加一条normal ddl的方法:
#
#在本文件中，添加一个类名以"NormalDDLActionPre"开头并且继承自BaseDDLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)@staticmethod get_action_ddl():
#返回action sql，并且该sql必须为ddl。
#(5)@staticmethod get_rollback_sql():
#返回回滚该action的sql。
#(6)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(7)check_after_do_action(self):
#执行action sql之后的检查。
#(8)skip_action(self):
#check if check_before_do_action() and do_action() can be skipped
#
#加表示例：
#class NormalDDLActionPreAddAllBackupBackupLogArchiveStatusHistory(BaseDDLAction):
#  table_name = '__all_backup_backup_log_archive_stat'
#  @staticmethod
#  def get_seq_num():
#    return 102
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '{0}'""".format(self.table_name))
#  def skip_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
#    return (len(results) > 0)
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
#    if len(results) > 0:
#      raise MyError("""table oceanabse.{0} already exists""".format(self.table_name))
#  @staticmethod
#  def get_action_ddl():
#    return """
#    CREATE TABLE `__all_backup_backup_log_archive_status_history` (
#  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#  `tenant_id` bigint(20) NOT NULL,
#  `incarnation` bigint(20) NOT NULL,
#  `log_archive_round` bigint(20) NOT NULL,
#  `copy_id` bigint(20) NOT NULL,
#  `min_first_time` timestamp(6) NOT NULL,
#  `max_next_time` timestamp(6) NOT NULL,
#  `input_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `output_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `deleted_input_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `deleted_output_bytes` bigint(20) NOT NULL DEFAULT '0',
#  `pg_count` bigint(20) NOT NULL DEFAULT '0',
#  `backup_dest` varchar(2048) DEFAULT NULL,
#  `is_mark_deleted` tinyint(4) DEFAULT NULL,
#  PRIMARY KEY (`tenant_id`, `incarnation`, `log_archive_round`, `copy_id`)
#) TABLE_ID = 1099511628080 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
#           """
#  @staticmethod
#  def get_rollback_sql():
#    return """"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '{0}'""".format(self.table_name))
#    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.{0}""".format(self.table_name))
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
#    if len(results) != 1:
#      raise MyError("""table oceanbase.{0} not exists""".format(self.table_name))
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.{0}""".format(self.table_name))
#    if len(results) != 15:
#      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
#
#改列示例：
#class NormalDDLActionPreModifyAllRestoreInfoValue(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 12
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
#  def skip_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
#    return len(results) > 0
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info like 'value'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_rootservice_event_history column value not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_restore_info modify column `value` longtext NOT NULL"""
#  @staticmethod
#  def get_rollback_sql():
#    return """"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column value for oceanbase.__all_restore_info')
#
#加列示例：
#class NormalDDLActionPreAllTenantProfileAddVerifyFunction(BaseDDLAction):
# @staticmethod
# def get_seq_num():
#   return 0
# def dump_before_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
# def skip_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   return len(results) > 0;
# def check_before_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   if len(results) != 0:
#     raise MyError('password_verify_function column alread exists')
# @staticmethod
# def get_action_ddl():
#   return """alter table oceanbase.__all_tenant_profile add column `password_verify_function` varchar(30) DEFAULT NULL id 23"""
# @staticmethod
# def get_rollback_sql():
#   return """alter table oceanbase.__all_tenant_profile drop column password_verify_function"""
# def dump_after_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
# def check_after_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
#   if len(results) != 1:
#     raise MyError('failed to add column password_verify_function for oceanbase.__all_tenant_profile')
#
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####        
#####========******####========= actions end =========####******========####
#def do_normal_ddl_actions(cur):
#  import normal_ddl_actions_pre
#  upgrade_params = UpgradeParams()
#  cls_list = reflect_action_cls_list(normal_ddl_actions_pre, 'NormalDDLActionPre')
#
#  # check if pre upgrade script can run reentrantly
#  query_cur = QueryCursor(cur)
#  version = fetch_observer_version(query_cur)
#  can_skip = False
#  if (cmp(version, "2.2.77") >= 0 and cmp(version, "3.0.0") < 0):
#    can_skip = True
#  elif (cmp(version, "3.1.1") >= 0):
#    can_skip = True
#  else:
#    can_skip = False
#
#  # set parameter
#  if len(cls_list) > 0:
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'True')
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#
#  for cls in cls_list:
#    logging.info('do normal ddl acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur)
#    action.dump_before_do_action()
#    if False == can_skip or False == action.skip_action():
#      action.check_before_do_action()
#      action.do_action()
#    else:
#      logging.info("skip ddl action, seq_num: %d", cls.get_seq_num())
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#  # reset parameter
#  if len(cls_list) > 0:
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl' , 'False')
#    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'False')
#
#def get_normal_ddl_actions_sqls_str(query_cur):
#  import normal_ddl_actions_pre
#  ret_str = ''
#  cls_list = reflect_action_cls_list(normal_ddl_actions_pre, 'NormalDDLActionPre')
#  for i in range(0, len(cls_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += cls_list[i].get_action_ddl() + ';'
#  return ret_str
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:normal_dml_actions_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import mysql.connector
#from mysql.connector import errorcode
#from actions import BaseDMLAction
#from actions import reflect_action_cls_list
#from actions import QueryCursor
#from actions import check_current_cluster_is_primary
#import logging
#import my_utils
#
#'''
#添加一条normal dml的方法:
#
#在本文件中，添加一个类名以"NormalDMLActionPost"开头并且继承自BaseDMLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)@staticmethod get_action_dml():
#返回action sql，并且该sql必须为dml。
#(5)@staticmethod get_rollback_sql():
#返回回滚该action的sql。
#(6)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(7)check_after_do_action(self):
#执行action sql之后的检查。
#(8)skip_action(self):
#check if check_before_do_action() and do_action() can be skipped
#
#举例:
#class NormalDMLActionPost1(BaseDMLAction):
#  @staticmethod
#  def get_seq_num():
#    return 0
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
#  def skip_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    return (len(results) > 0)
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    if len(results) > 0:
#      raise MyError('some row in table test.for_test whose primary key is 9 already exists')
#  @staticmethod
#  def get_action_dml():
#    return """insert into test.for_test values (9, 'haha', 99)"""
#  @staticmethod
#  def get_rollback_sql():
#    return """delete from test.for_test where pk = 9"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    if len(results) != 1:
#      raise MyError('there should be only one row whose primary key is 9 in table test.for_test, but there has {0} rows like that'.format(len(results)))
#    elif results[0][0] != 9 or results[0][1] != 'haha' or results[0][2] != 99:
#      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#####========******####========= actions end =========####******========####
#
#def do_normal_dml_actions_by_standby_cluster(standby_cluster_infos):
#  try:
#    for standby_cluster_info in standby_cluster_infos:
#      logging.info("do_normal_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                     password =  standby_cluster_info['pwd'],
#                                     host     =  standby_cluster_info['ip'],
#                                     port     =  standby_cluster_info['port'],
#                                     database =  'oceanbase',
#                                     raise_on_warnings = True)
#
#      cur = conn.cursor(buffered=True)
#      conn.autocommit = True
#      query_cur = QueryCursor(cur)
#      is_primary = check_current_cluster_is_primary(query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#
#      ## process
#      do_normal_dml_actions(cur)
#
#      cur.close()
#      conn.close()
#  except Exception, e:
#    logging.exception("""do_normal_dml_actions_by_standby_cluster failed""")
#    raise e
#
#def do_normal_dml_actions(cur):
#  import normal_dml_actions_post
#  cls_list = reflect_action_cls_list(normal_dml_actions_post, 'NormalDMLActionPost')
#  for cls in cls_list:
#    logging.info('do normal dml acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur)
#    action.dump_before_do_action()
#    if False == action.skip_action():
#      action.check_before_do_action()
#      action.do_action()
#    else:
#      logging.info("skip dml action, seq_num: %d", cls.get_seq_num())
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#def get_normal_dml_actions_sqls_str():
#  import normal_dml_actions_post
#  ret_str = ''
#  cls_list = reflect_action_cls_list(normal_dml_actions_post, 'NormalDMLActionPost')
#  for i in range(0, len(cls_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += cls_list[i].get_action_dml() + ';'
#  return ret_str
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:normal_dml_actions_pre.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#from my_error import MyError
#import mysql.connector
#from mysql.connector import errorcode
#from actions import BaseDMLAction
#from actions import reflect_action_cls_list
#from actions import fetch_observer_version
#from actions import QueryCursor
#from actions import check_current_cluster_is_primary
#import logging
#import my_utils
#
#'''
#添加一条normal dml的方法:
#
#在本文件中，添加一个类名以"NormalDMLActionPre"开头并且继承自BaseDMLAction的类，
#然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
#(1)@staticmethod get_seq_num():
#返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
#(2)dump_before_do_action(self):
#执行action sql之前把一些相关数据dump到日志中。
#(3)check_before_do_action(self):
#执行action sql之前的检查。
#(4)@staticmethod get_action_dml():
#返回action sql，并且该sql必须为dml。
#(5)@staticmethod get_rollback_sql():
#返回回滚该action的sql。
#(6)dump_after_do_action(self):
#执行action sql之后把一些相关数据dump到日志中。
#(7)check_after_do_action(self):
#执行action sql之后的检查。
#(8)skip_action(self):
#check if check_before_do_action() and do_action() can be skipped
#
#举例:
#class NormalDMLActionPre1(BaseDMLAction):
#  @staticmethod
#  def get_seq_num():
#    return 0
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
#  def skip_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    return (len(results) > 0)
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    if len(results) > 0:
#      raise MyError('some row in table test.for_test whose primary key is 9 already exists')
#  @staticmethod
#  def get_action_dml():
#    return """insert into test.for_test values (9, 'haha', 99)"""
#  @staticmethod
#  def get_rollback_sql():
#    return """delete from test.for_test where pk = 9"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
#    if len(results) != 1:
#      raise MyError('there should be only one row whose primary key is 9 in table test.for_test, but there has {0} rows like that'.format(len(results)))
#    elif results[0][0] != 9 or results[0][1] != 'haha' or results[0][2] != 99:
#      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
#'''
#
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。
#
#####========******####======== actions begin ========####******========####
#####========******####========= actions end =========####******========####
#
#
#def do_normal_dml_actions(cur):
#  import normal_dml_actions_pre
#  cls_list = reflect_action_cls_list(normal_dml_actions_pre, 'NormalDMLActionPre')
#
#  # check if pre upgrade script can run reentrantly
#  query_cur = QueryCursor(cur)
#  version = fetch_observer_version(query_cur)
#  can_skip = False
#  if (cmp(version, "2.2.77") >= 0 and cmp(version, "3.0.0") < 0):
#    can_skip = True
#  elif (cmp(version, "3.1.1") >= 0):
#    can_skip = True
#  else:
#    can_skip = False
#
#  for cls in cls_list:
#    logging.info('do normal dml acion, seq_num: %d', cls.get_seq_num())
#    action = cls(cur)
#    action.dump_before_do_action()
#    if False == can_skip or False == action.skip_action():
#      action.check_before_do_action()
#      action.do_action()
#    else:
#      logging.info("skip dml action, seq_num: %d", cls.get_seq_num())
#    action.dump_after_do_action()
#    action.check_after_do_action()
#
#def do_normal_dml_actions_by_standby_cluster(standby_cluster_infos):
#  try:
#    for standby_cluster_info in standby_cluster_infos:
#      logging.info("do_normal_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#      conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                     password =  standby_cluster_info['pwd'],
#                                     host     =  standby_cluster_info['ip'],
#                                     port     =  standby_cluster_info['port'],
#                                     database =  'oceanbase',
#                                     raise_on_warnings = True)
#
#      cur = conn.cursor(buffered=True)
#      conn.autocommit = True
#      query_cur = QueryCursor(cur)
#      is_primary = check_current_cluster_is_primary(query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#
#      ## process
#      do_normal_dml_actions(cur)
#
#      cur.close()
#      conn.close()
#  except Exception, e:
#    logging.exception("""do_normal_dml_actions_by_standby_cluster failed""")
#    raise e
#
#def get_normal_dml_actions_sqls_str():
#  import normal_dml_actions_pre
#  ret_str = ''
#  cls_list = reflect_action_cls_list(normal_dml_actions_pre, 'NormalDMLActionPre')
#  for i in range(0, len(cls_list)):
#    if i > 0:
#      ret_str += '\n'
#    ret_str += cls_list[i].get_action_dml() + ';'
#  return ret_str
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
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
#filename:priv_checker.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:__init__.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:actions.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import time
##import re
##import json
##import traceback
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
#import string
#
#
#def dump_sql_to_file(dump_filename, tenant_id_list):
##  normal_ddls_str = normal_ddl_actions_post.get_normal_ddl_actions_sqls_str()
##  normal_dmls_str = normal_dml_actions_post.get_normal_dml_actions_sqls_str()
#  #each_tenant_dmls_str = each_tenant_dml_actions_post.get_each_tenant_dml_actions_sqls_str(tenant_id_list)
#  dump_file = open(dump_filename, 'w')
#  dump_file.write('# 以下是priv_checker.py脚本中的步骤\n')
#  dump_file.write('# 仅供priv_checker.py脚本运行失败需要人肉的时候参考\n')
#  dump_file.close()
#
##def print_stats():
##  logging.info('==================================================================================')
##  logging.info('============================== STATISTICS BEGIN ==================================')
##  logging.info('==================================================================================')
##  logging.info('succeed run sql(except sql of special actions): \n\n%s\n', actions.get_succ_sql_list_str())
##  logging.info('commited sql(except sql of special actions): \n\n%s\n', actions.get_commit_sql_list_str())
##  logging.info('==================================================================================')
##  logging.info('=============================== STATISTICS END ===================================')
##  logging.info('==================================================================================')
##
##def do_upgrade(my_host, my_port, my_user, my_passwd, my_module_set, upgrade_params):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = actions.QueryCursor(cur)
##      # 开始升级前的检查
##      check_before_upgrade(query_cur, upgrade_params)
##      # 获取租户id列表
##      tenant_id_list = actions.fetch_tenant_ids(query_cur)
##      if len(tenant_id_list) <= 0:
##        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
##        raise MyError('no tenant id')
##      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
##      conn.commit()
##
##      # 获取standby_cluster_info列表
##      standby_cluster_infos = actions.fetch_standby_cluster_infos(conn, query_cur, my_user, my_passwd)
##      # check ddl and dml sync
##      actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##
##      actions.refresh_commit_sql_list()
##      dump_sql_to_file(upgrade_params.sql_dump_filename, tenant_id_list)
##      logging.info('================succeed to dump sql to file: {0}==============='.format(upgrade_params.sql_dump_filename))
##
##      if run_modules.MODULE_DDL in my_module_set:
##        logging.info('================begin to run ddl===============')
##        conn.autocommit = True
##        normal_ddl_actions_post.do_normal_ddl_actions(cur)
##        logging.info('================succeed to run ddl===============')
##        conn.autocommit = False
##        # check ddl and dml sync
##        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##
##      if run_modules.MODULE_EACH_TENANT_DDL in my_module_set:
##        has_run_ddl = True
##        logging.info('================begin to run each tenant ddl===============')
##        conn.autocommit = True
##        each_tenant_ddl_actions_post.do_each_tenant_ddl_actions(cur, tenant_id_list)
##        logging.info('================succeed to run each tenant ddl===============')
##        conn.autocommit = False
##        # check ddl and dml sync
##        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##
##      if run_modules.MODULE_NORMAL_DML in my_module_set:
##        logging.info('================begin to run normal dml===============')
##        normal_dml_actions_post.do_normal_dml_actions(cur)
##        logging.info('================succeed to run normal dml===============')
##        conn.commit()
##        actions.refresh_commit_sql_list()
##        logging.info('================succeed to commit dml===============')
##        # check ddl and dml sync
##        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##
##      if run_modules.MODULE_EACH_TENANT_DML in my_module_set:
##        logging.info('================begin to run each tenant dml===============')
##        conn.autocommit = True
##        each_tenant_dml_actions_post.do_each_tenant_dml_actions(cur, tenant_id_list)
##        conn.autocommit = False
##        logging.info('================succeed to run each tenant dml===============')
##        # check ddl and dml sync
##        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##
##      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
##        logging.info('================begin to run special action===============')
##        conn.autocommit = True
##        special_upgrade_action_post.do_special_upgrade(conn, cur, tenant_id_list)
##        conn.autocommit = False
##        actions.refresh_commit_sql_list()
##        logging.info('================succeed to commit special action===============')
##        # check ddl and dml sync
##        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      # 打印统计信息
##      print_stats()
##      # 将回滚sql写到文件中
##      actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##class EachTenantDDLActionPostCreateAllTenantSecurityRecordAudit(BaseEachTenantDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 15
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
##    if len(results) > 0:
##      raise MyError('__all_tenant_security_audit_record already created')
##  def dump_before_do_each_tenant_action(self, tenant_id):
##    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
##  def check_before_do_each_tenant_action(self, tenant_id):
##    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
##    if len(results) > 0:
##      raise MyError('tenant_id:{0} has already create table __all_tenant_security_audit_record'.format(tenant_id))
##  @staticmethod
##  def get_each_tenant_action_ddl(tenant_id):
##    pure_table_id = 259
##    table_id = (tenant_id << 40) | pure_table_id
##    return """
##      CREATE TABLE `__all_tenant_security_audit_record` (
##          `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
##          `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
##          `tenant_id` bigint(20) NOT NULL,
##          `svr_ip` varchar(46) NOT NULL,
##          `svr_port` bigint(20) NOT NULL,
##          `record_timestamp_us` timestamp(6) NOT NULL,
##          `user_id` bigint(20) unsigned NOT NULL,
##          `user_name` varchar(64) NOT NULL,
##          `effective_user_id` bigint(20) unsigned NOT NULL,
##          `effective_user_name` varchar(64) NOT NULL,
##          `client_ip` varchar(46) NOT NULL,
##          `user_client_ip` varchar(46) NOT NULL,
##          `proxy_session_id` bigint(20) unsigned NOT NULL,
##          `session_id` bigint(20) unsigned NOT NULL,
##          `entry_id` bigint(20) unsigned NOT NULL,
##          `statement_id` bigint(20) unsigned NOT NULL,
##          `trans_id` varchar(512) NOT NULL,
##          `commit_version` bigint(20) NOT NULL,
##          `trace_id` varchar(64) NOT NULL,
##          `db_id` bigint(20) unsigned NOT NULL,
##          `cur_db_id` bigint(20) unsigned NOT NULL,
##          `sql_timestamp_us` timestamp(6) NOT NULL,
##          `audit_id` bigint(20) unsigned NOT NULL,
##          `audit_type` bigint(20) unsigned NOT NULL,
##          `operation_type` bigint(20) unsigned NOT NULL,
##          `action_id` bigint(20) unsigned NOT NULL,
##          `return_code` bigint(20) NOT NULL,
##          `obj_owner_name` varchar(64) DEFAULT NULL,
##          `obj_name` varchar(256) DEFAULT NULL,
##          `new_obj_owner_name` varchar(64) DEFAULT NULL,
##          `new_obj_name` varchar(256) DEFAULT NULL,
##          `auth_privileges` varchar(256) DEFAULT NULL,
##          `auth_grantee` varchar(256) DEFAULT NULL,
##          `logoff_logical_read` bigint(20) unsigned NOT NULL,
##          `logoff_physical_read` bigint(20) unsigned NOT NULL,
##          `logoff_logical_write` bigint(20) unsigned NOT NULL,
##          `logoff_lock_count` bigint(20) unsigned NOT NULL,
##          `logoff_dead_lock` varchar(40) DEFAULT NULL,
##          `logoff_cpu_time_us` bigint(20) unsigned NOT NULL,
##          `logoff_exec_time_us` bigint(20) unsigned NOT NULL,
##          `logoff_alive_time_us` bigint(20) unsigned NOT NULL,
##          `comment_text` varchar(65536) DEFAULT NULL,
##          `sql_bind` varchar(65536) DEFAULT NULL,
##          `sql_text` varchar(65536) DEFAULT NULL,
##          PRIMARY KEY (`tenant_id`, `svr_ip`, `svr_port`, `record_timestamp_us`)          
##      ) TABLE_ID = {0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1
##        BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
##    """.format(table_id)
##  @staticmethod
##  def get_each_tenant_rollback_sql(tenant_id):
##    return """select 1"""
##  def dump_after_do_each_tenant_action(self, tenant_id):
##    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
##  def check_after_do_each_tenant_action(self, tenant_id):
##    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
##    if len(results) != 1:
##      raise MyError('tenant_id:{0} create table __all_tenant_security_audit_record failed'.format(tenant_id))
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
##    if len(results) != len(self.get_tenant_id_list()):
##      raise MyError('there should be {0} rows in __all_table whose table_name is __all_tenant_security_audit_record, but there has {1} rows like that'.format(len(self.get_tenant_id_list()), len(results)))
##
##class NormalDDLActionPreAddAllUserType(BaseDDLAction):
##    @staticmethod
##    def get_seq_num():
##      return 2
##    def dump_before_do_action(self):
##      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
##    def check_before_do_action(self):
##      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'type'""")
##      if len(results) > 0:
##        raise MyError('column oceanbase.__all_user.type already exists')
##    @staticmethod
##    def get_action_ddl():
##      return """alter table oceanbase.__all_user add column `type` bigint(20) DEFAULT '0'"""
##    @staticmethod
##    def get_rollback_sql():
##      return """alter table oceanbase.__all_user drop column type"""
##    def dump_after_do_action(self):
##      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
##    def check_after_do_action(self):
##      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'type'""")
##      if len(results) != 1:
##        raise MyError('failed to add column status for oceanbase.__all_user')
##
##class NormalDDLActionPreAddAllUserHistoryType(BaseDDLAction):
##    @staticmethod
##    def get_seq_num():
##      return 3
##    def dump_before_do_action(self):
##      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
##    def check_before_do_action(self):
##      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'type'""")
##      if len(results) > 0:
##        raise MyError('column oceanbase.__all_user_history.type already exists')
##    @staticmethod
##    def get_action_ddl():
##      return """alter table oceanbase.__all_user_history add column `type` bigint(20) DEFAULT '0'"""
##    @staticmethod
##    def get_rollback_sql():
##      return """alter table oceanbase.__all_user_history drop column type"""
##    def dump_after_do_action(self):
##      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
##    def check_after_do_action(self):
##      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'type'""")
##      if len(results) != 1:
##        raise MyError('failed to add column status for oceanbase.__all_user_history')
##
##
##class NormalDDLActionPreAddProfileIdAllUser(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 4
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'profile_id'""")
##    if len(results) != 0:
##      raise MyError('profile_id column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_user add column `profile_id` bigint(20) NOT NULL DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_user drop column profile_id"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'profile_id'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_user')
##
##class NormalDDLActionPreAddProfileIdAllUserHistory(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 5
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'profile_id'""")
##    if len(results) != 0:
##      raise MyError('profile_id column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_user_history add column `profile_id` bigint(20) DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_user_history drop column profile_id"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'profile_id'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_user_history')
##
##class NormalDDLActionPreModifyAllRootServiceEventHistoryIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 6
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_rootservice_event_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_rootservice_event_history like 'rs_svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_rootservice_event_history column rs_svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_rootservice_event_history modify column rs_svr_ip varchar(46) default ''"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_rootservice_event_history modify column rs_svr_ip varchar(32) default ''"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_rootservice_event_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_rootservice_event_history like 'rs_svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column rs_svr_ip for oceanbase.__all_rootservice_event_history')
##
##class NormalDDLActionPreModifyAllSStableChecksumIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 7
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sstable_checksum""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sstable_checksum like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_sstable_checksum column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_sstable_checksum modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_sstable_checksum modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sstable_checksum""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sstable_checksum like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_sstable_checksum')
##
##class NormalDDLActionPreModifyAllClogHistoryInfoV2IpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 8
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_clog_history_info_v2""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_clog_history_info_v2 like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_clog_history_info_v2 column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_clog_history_info_v2 modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_clog_history_info_v2 modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_clog_history_info_v2""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_clog_history_info_v2 like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_clog_history_info_v2')
##
##class NormalDDLActionPreModifyAllSysParameterIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 9
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sys_parameter""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sys_parameter like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_sys_parameter column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_sys_parameter modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_sys_parameter modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sys_parameter""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sys_parameter like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_sys_parameter')
##
##class NormalDDLActionPreModifyAllLocalIndexStatusIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 10
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_local_index_status""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_local_index_status like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_local_index_status column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_local_index_status modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_local_index_status modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_local_index_status""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_local_index_status like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_local_index_status')
##
##class NormalDDLActionPreModifyAllTenantResourceUsageIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 11
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_resource_usage""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_resource_usage like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_tenant_resource_usage column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_tenant_resource_usage modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_tenant_resource_usage modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_resource_usage""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_resource_usage like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_tenant_resource_usage')
##
##class NormalDDLActionPreModifyAllRootTableIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 12
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_root_table""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_root_table like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_root_table column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_root_table modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_root_table modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_root_table""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_root_table like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_root_table')
##
##class NormalDDLActionPreModifyAllServerEventHistoryIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 13
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_server_event_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_server_event_history like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_server_event_history column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_server_event_history modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_server_event_history modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_server_event_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_server_event_history like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_server_event_history')
##
##class NormalDDLActionPreModifyAllIndexScheduleTaskIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 14
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_index_schedule_task""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_index_schedule_task like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_index_schedule_task column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_index_schedule_task modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_index_schedule_task modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_index_schedule_task""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_index_schedule_task like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_index_schedule_task')
##
##class NormalDDLActionPreModifyAllMetaTableIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 15
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_meta_table""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_meta_table like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_meta_table column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_meta_table modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_meta_table modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_meta_table""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_meta_table like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_meta_table')
##
##class NormalDDLActionPreModifyAllSqlExecuteTaskIpColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 16
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sql_execute_task""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sql_execute_task like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_sql_execute_task column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_sql_execute_task modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_sql_execute_task modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sql_execute_task""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sql_execute_task like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_sql_execute_task')
##
#
##
##class NormalDDLActionPreModifyAllGlobalIndexDataSrcDataPartitionIDColumn(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 39
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_global_index_data_src""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_global_index_data_src like 'data_partition_id,'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_global_index_data_src column data_partition_id, not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_global_index_data_src change `data_partition_id,`  `data_partition_id` bigint(20) NOT NULL"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_global_index_data_src change `data_partition_id`  `data_partition_id,` bigint(20) NOT NULL"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_global_index_data_src""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_global_index_data_src like 'data_partition_id'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column data_partition_id, for oceanbase.__all_global_index_data_src')
##
##class NormalDDLActionPreAddAllForeignKeyEnableFlag(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 40
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'enable_flag'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key.enable_flag already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key add column `enable_flag` tinyint(4) NOT NULL DEFAULT '1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key drop column enable_flag"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'enable_flag'""")
##    if len(results) != 1:
##      raise MyError('failed to add column enable_flag for oceanbase.__all_foreign_key')
##
##class NormalDDLActionPreAddAllForeignKeyHistoryEnableFlag(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 41
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'enable_flag'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key_history.enable_flag already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key_history add column `enable_flag` tinyint(4) DEFAULT '1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key_history drop column enable_flag"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'enable_flag'""")
##    if len(results) != 1:
##      raise MyError('failed to add column enable_flag for oceanbase.__all_foreign_key_history')
##
##class NormalDDLActionPreAddAllForeignKeyRefCstType(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 42
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_type'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key.ref_cst_type already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key add column `ref_cst_type` bigint(20) NOT NULL DEFAULT '0'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key drop column ref_cst_type"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_type'""")
##    if len(results) != 1:
##      raise MyError('failed to add column ref_cst_type for oceanbase.__all_foreign_key')
##
##class NormalDDLActionPreAddAllForeignKeyHistoryRefCstType(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 43
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_type'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key_history.ref_cst_type already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key_history add column `ref_cst_type` bigint(20) DEFAULT '0'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key_history drop column ref_cst_type"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_type'""")
##    if len(results) != 1:
##      raise MyError('failed to add column ref_cst_type for oceanbase.__all_foreign_key_history')
##
##class NormalDDLActionPreAddAllForeignKeyRefCstId(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 44
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_id'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key.ref_cst_id already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key add column `ref_cst_id` bigint(20) NOT NULL DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key drop column ref_cst_id"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_id'""")
##    if len(results) != 1:
##      raise MyError('failed to add column ref_cst_id for oceanbase.__all_foreign_key')
##
##class NormalDDLActionPreAddAllForeignKeyHistoryRefCstId(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 45
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_id'""")
##    if len(results) > 0:
##      raise MyError('column oceanbase.__all_foreign_key_history.ref_cst_id already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_foreign_key_history add column `ref_cst_id` bigint(20) DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_foreign_key_history drop column ref_cst_id"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_id'""")
##    if len(results) != 1:
##      raise MyError('failed to add column ref_cst_id for oceanbase.__all_foreign_key_history')
##
##class NormalDDLActionPreModifyAllSeedParameterSvrIp(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 46
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_seed_parameter""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_seed_parameter like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_seed_parameter column svr_ip not exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_seed_parameter modify column svr_ip varchar(46) not null"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_seed_parameter modify column svr_ip varchar(32) not null"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_seed_parameter""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_seed_parameter like 'svr_ip'""")
##    if len(results) != 1:
##      raise MyError('fail to modify column svr_ip for oceanbase.__all_seed_parameter')
##
##class NormalDDLActionPreAddClusterStatus(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 47
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_cluster""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_cluster like 'cluster_status'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_cluster add column cluster_status bigint(20) NOT NULL default 1"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_cluster drop column cluster_status"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_cluster""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_cluster like 'cluster_status'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_cluster')
##
##class NormalDDLActionPreAllTableAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 48
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_table add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_table drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_table')
##
##class NormalDDLActionPreAllTablegroupAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 49
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_tablegroup add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_tablegroup drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_tablegroup')
##
##class NormalDDLActionPreAllPartAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 50
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_part add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_part drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_part')
##
##class NormalDDLActionPreAllTableHistoryAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 51
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_table_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_table_history drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_table_history')
##
##class NormalDDLActionPreAllTablegroupHistoryAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 52
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup_history like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_tablegroup_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_tablegroup_history drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup_history like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_tablegroup_history')
##
##class NormalDDLActionPreAllPartHistoryAddDropSchemaVersion(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 53
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'drop_schema_version'""")
##    if len(results) != 0:
##      raise MyError('cluster_status column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_part_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_part_history drop column `drop_schema_version`"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'drop_schema_version'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_part_history')
##
##class NormalDDLActionPreAddBuildingSnapshot(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 54
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_ori_schema_version""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_ori_schema_version like 'building_snapshot'""")
##    if len(results) != 0:
##      raise MyError('building_snapshot column alread exists')
##  @staticmethod
##  def get_action_ddl():
##    return """alter table oceanbase.__all_ori_schema_version add column building_snapshot bigint(20) NOT NULL default '0'"""
##  @staticmethod
##  def get_rollback_sql():
##    return """alter table oceanbase.__all_ori_schema_version drop column building_snapshot"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_ori_schema_version""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_ori_schema_version like 'building_snapshot'""")
##    if len(results) != 1:
##      raise MyError('failed to add column for oceanbase.__all_ori_schema_version')
##
##class NormalDDLActionPreAddAllRestoreInfo(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 55
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_restore_info'""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_restore_info'""")
##    if len(results) > 0:
##      raise MyError('table oceanbase.__all_restore_info already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """ CREATE TABLE `__all_restore_info` (
##                 `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
##                 `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
##                 `job_id` bigint(20) NOT NULL,
##                 `name` varchar(1024) NOT NULL,
##                 `value` varchar(4096) NOT NULL,
##                 PRIMARY KEY (`job_id`, `name`)
##               ) TABLE_ID = 1099511628041 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
##  """
##  @staticmethod
##  def get_rollback_sql():
##    return """drop table oceanbase.__all_restore_info"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_restore_info'""")
##    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.__all_restore_info""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_restore_info'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_restore_info not exists')
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info""")
##    if len(results) != 5:
##      raise MyError('table oceanbase.__all_restore_info has invalid column descs')
##
##class NormalDDLActionPreAddAllFailoverInfo(BaseDDLAction):
##  @staticmethod
##  def get_seq_num():
##    return 56
##  def dump_before_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_failover_info'""")
##  def check_before_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_failover_info'""")
##    if len(results) > 0:
##      raise MyError('table oceanbase.__all_failover_info already exists')
##  @staticmethod
##  def get_action_ddl():
##    return """ CREATE TABLE `__all_failover_info` (
##                 `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
##                 `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
##                 `failover_epoch` bigint(20) NOT NULL,
##                 `tenant_id` bigint(20) NOT NULL,
##                 `sys_table_scn` bigint(20) NOT NULL,
##                 `user_table_scn` bigint(20) NOT NULL,
##                 `schema_version` bigint(20) NOT NULL,
##                   PRIMARY KEY (`failover_epoch`, `tenant_id`)
##               ) TABLE_ID = 1099511628047 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
##  """
##  @staticmethod
##  def get_rollback_sql():
##    return """drop table oceanbase.__all_failover_info"""
##  def dump_after_do_action(self):
##    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_failover_info'""")
##    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.__all_failover_info""")
##  def check_after_do_action(self):
##    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_failover_info'""")
##    if len(results) != 1:
##      raise MyError('table oceanbase.__all_failover_info not exists')
##    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_failover_info""")
##    if len(results) != 7:
##      raise MyError('table oceanbase.__all_failover_info has invalid column descs')
##
##class NormalDDLActionPreAddAllForeignKeyValidateFlag(BaseDDLAction):
## @staticmethod
## def get_seq_num():
##   return 57
## def dump_before_do_action(self):
##   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
## def check_before_do_action(self):
##   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'validate_flag'""")
##   if len(results) > 0:
##     raise MyError('column oceanbase.__all_foreign_key.validate_flag already exists')
## @staticmethod
## def get_action_ddl():
##   return """alter table oceanbase.__all_foreign_key add column `validate_flag` tinyint(4) NOT NULL DEFAULT '1'"""
## @staticmethod
## def get_rollback_sql():
##   return """alter table oceanbase.__all_foreign_key drop column validate_flag"""
## def dump_after_do_action(self):
##   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
## def check_after_do_action(self):
##   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'validate_flag'""")
##   if len(results) != 1:
##     raise MyError('failed to add column validate_flag for oceanbase.__all_foreign_key')
##
##    if 'password' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_module():
##  global g_opts
##  for opt in g_opts:
##    if 'module' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_log_file():
##  global g_opts
##  for opt in g_opts:
##    if 'log-file' == opt.get_long_name():
##      return opt.get_value()
###### ---------------end----------------------
##
###### --------------start :  do_upgrade_pre.py--------------
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
###### ---------------end----------------------
##
##
##
##
###### START ####
### 1. 检查前置版本
##def check_observer_version(query_cur, upgrade_params):
##  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
##  if len(results) != 1:
##    raise MyError('query results count is not 1')
##  elif cmp(results[0][0], upgrade_params.old_version) < 0 :
##    raise MyError('old observer version is expected equal or higher then: {0}, actual version:{1}'.format(upgrade_params.old_version, results[0][0]))
##  logging.info('check observer version success, version = {0}'.format(results[0][0]))
##
### 2. 检查paxos副本是否同步, paxos副本是否缺失
##def check_paxos_replica(query_cur):
##  # 2.1 检查paxos副本是否同步
##  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
##  if results[0][0] > 0 :
##    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
##  # 2.2 检查paxos副本是否有缺失 TODO
##  logging.info('check paxos replica success')
##
### 3. 检查是否有做balance, locality变更
##def check_rebalance_task(query_cur):
##  # 3.1 检查是否有做locality变更
##  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
##  if results[0][0] > 0 :
##    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
##  # 3.2 检查是否有做balance
##  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
##  if results[0][0] > 0 :
##    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
##  logging.info('check rebalance task success')
##
### 4. 检查集群状态
##def check_cluster_status(query_cur):
##  # 4.1 检查是否非合并状态
##  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
##  if cmp(results[0][0], 'IDLE')  != 0 :
##    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
##  logging.info('check cluster status success')
##  # 4.2 检查合并版本是否>=3
##  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
##  if results[0][0] < 2 :
##    raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
##  logging.info('check global last_merged_version success')
##
### 5. 检查没有打开enable_separate_sys_clog
##def check_disable_separate_sys_clog(query_cur):
##  (desc, results) = query_cur.exec_query("""select count(1) from __all_sys_parameter where name like 'enable_separate_sys_clog'""")
##  if results[0][0] > 0 :
##    raise MyError('enable_separate_sys_clog is true, unexpected')
##  logging.info('check separate_sys_clog success')
##
### 6. 检查配置宏块的data_seq
##def check_macro_block_data_seq(query_cur):
##  query_cur.exec_sql("""set ob_query_timeout=1800000000""")
##  row_count = query_cur.exec_sql("""select * from __all_virtual_partition_sstable_macro_info where data_seq < 0 limit 1""")
##  if row_count != 0:
##    raise MyError('check_macro_block_data_seq failed, too old macro block needs full merge')
##  logging.info('check_macro_block_data_seq success')
##
### 7.检查所有的表格的sstable checksum method都升级到2
##def check_sstable_column_checksum_method_new(query_cur):
##  query_cur.exec_sql("""set ob_query_timeout=1800000000""")
##  row_count = query_cur.exec_sql("""select * from __all_table where table_id in (select index_id from __all_sstable_column_checksum where checksum_method = 1 and data_table_id != 0) limit 1""")
##  if row_count != 0:
##    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade column checksum method using progressive compaction')
##  row_count = query_cur.exec_sql("""select column_value from __all_core_table where table_name = '__all_table' and column_name = 'table_id' and column_value in (select index_id from __all_sstable_column_checksum where checksum_method = 1 and data_table_id != 0) limit 1""")
##  if row_count != 0:
##    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade system table column checksum method using progressive compaction')
##  row_count = query_cur.exec_sql("""select index_id from __all_sstable_column_checksum where checksum_method = 1 and index_id = 1099511627777 and data_table_id != 0 limit 1""")
##  if row_count != 0:
##    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade system table column checksum method using progressive compaction')
##  logging.info('check_sstable_column_checksum_method_new success')
##
### 8. 检查租户的resource_pool内存规格, 要求F类型unit最小内存大于5G，L类型unit最小内存大于2G.
##def check_tenant_resource_pool(query_cur):
##  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=0 and b.min_memory < 5368709120""")
##  if results[0][0] > 0 :
##    raise MyError('{0} tenant resource pool unit config is less than 5G, please check'.format(results[0][0]))
##  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=5 and b.min_memory < 2147483648""")
##  if results[0][0] > 0 :
##    raise MyError('{0} tenant logonly resource pool unit config is less than 2G, please check'.format(results[0][0]))
##
### 9. 检查是否有日志型副本分布在Full类型unit中
##def check_logonly_replica_unit(query_cur):
##  # 统计all_tnant表中每个tenant的L类型的zone个数
##  (desc, res_tenant_log_zone_cnt) = query_cur.exec_query("""select  tenant_id, (char_length(locality) - char_length(replace(locality, 'LOGONLY', ''))) / char_length('LOGONLY') as log_zone_cnt from __all_tenant""")
##  # 统计all_resource_pool中每个tenant的L类型的resource_pool中包含的zone个数
##  (desc, res_respool_log_zone_cnt) = query_cur.exec_query("""select tenant_id, sum(lzone_cnt) as rp_log_zone_cnt from (select  tenant_id, ((char_length(zone_list) - char_length(replace(zone_list, ';', ''))) + 1) as lzone_cnt from __all_resource_pool where replica_type=5) as vt group by tenant_id""")
##  # 比较两个数量是否一致，不一致则报错
##  for line in res_tenant_log_zone_cnt:
##    tenant_id = line[0]
##    tenant_log_zone_cnt = line[1]
##    if tenant_log_zone_cnt > 0:
##      is_found = False
##      for rpline in res_respool_log_zone_cnt:
##        rp_tenant_id = rpline[0]
##        rp_log_zone_cnt = rpline[1]
##        if tenant_id == rp_tenant_id:
##          is_found = True
##          if tenant_log_zone_cnt > rp_log_zone_cnt:
##            raise MyError('{0} {1} check_logonly_replica_unit failed, log_type zone count not match with resource_pool'.format(line, rpline))
##          break
##      if is_found == False:
##        raise MyError('{0} check_logonly_replica_unit failed, not found log_type resource_pool for this tenant'.format(line))
##  logging.info('check_logonly_replica_unit success')
##
### 10. 检查租户分区数是否超出内存限制
##def check_tenant_part_num(query_cur):
##  # 统计每个租户在各个server上的分区数量
##  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
##  # 计算每个租户在每个server上的max_memory
##  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
##  # 查询每个server的memstore_limit_percentage
##  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
##  part_static_cost = 128 * 1024
##  part_dynamic_cost = 400 * 1024
##  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
##  part_num_reserved = 512
##  for line in res_part_num:
##    svr_ip = line[0]
##    svr_port = line[1]
##    tenant_id = line[2]
##    part_num = line[3]
##    for uline in res_unit_memory:
##      uip = uline[0]
##      uport = uline[1]
##      utid = uline[2]
##      umem = uline[3]
##      utype = uline[4]
##      if svr_ip == uip and svr_port == uport and tenant_id == utid:
##        for mpline in res_svr_memstore_percent:
##          mpip = mpline[0]
##          mpport = mpline[1]
##          if mpip == uip and mpport == uport:
##            mspercent = int(mpline[3])
##            mem_limit = umem
##            if 0 == utype:
##              # full类型的unit需要为memstore预留内存
##              mem_limit = umem * (100 - mspercent) / 100
##            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
##            if part_num_limit <= 1000:
##              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
##            if part_num >= (part_num_limit - part_num_reserved):
##              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
##            break
##  logging.info('check tenant partition num success')
##
### 11. 检查存在租户partition，但是不存在unit的observer
##def check_tenant_resource(query_cur):
##  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
##  for line in res_unit:
##    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
##  logging.info("check tenant resource success")
##
### 12. 检查系统表(__all_table_history)索引生效情况
##def check_sys_index_status(query_cur):
##  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where data_table_id = 1099511627890 and table_id = 1099511637775 and index_type = 1 and index_status = 2""")
##  if len(results) != 1 or results[0][0] != 1:
##    raise MyError("""__all_table_history's index status not valid""")
##  logging.info("""check __all_table_history's index status success""")
##
### 14. 检查升级前是否有只读zone
##def check_readonly_zone(query_cur):
##   (desc, results) = query_cur.exec_query("""select count(*) from __all_zone where name='zone_type' and info='ReadOnly'""")
##   if results[0][0] != 0:
##       raise MyError("""check_readonly_zone failed, ob2.2 not support readonly_zone""")
##   logging.info("""check_readonly_zone success""")
##
### 16. 修改永久下线的时间，避免升级过程中缺副本
##def modify_server_permanent_offline_time(query_cur):
##  query_cur.exec_sql("""alter system set server_permanent_offline_time='72h'""")
##  logging.info("modify server_permanent_offline_time success")
##
### 17. 修改安全删除副本时间
##def modify_replica_safe_remove_time(query_cur):
##  query_cur.exec_sql("""alter system set replica_safe_remove_time='72h'""")
##  logging.info("modify modify_replica_safe_remove_time success")
##
### 18. 检查progressive_merge_round都升到1
##def check_progressive_merge_round(query_cur):
##  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4)""")
##  if results[0][0] != 0:
##      raise MyError("""progressive_merge_round should all be 1""")
##  logging.info("""check progressive_merge_round status success""")
##
###19. 检查是否有内置用户，角色
##def check_inner_user_role_exists(query_cur):
##   (desc, results) = query_cur.exec_query(
##     """select count(*) 
##          from oceanbase.__all_tenant t, oceanbase.__all_virtual_user u 
##        where t.tenant_id = u.tenant_id and 
##              t.compatibility_mode=1 and 
##              u.user_name in ('DBA','CONNECT','RESOURCE','LBACSYS','ORAAUDITOR');""")
##   if results[0][0] != 0:
##       raise MyError("""check_inner_user_role_exists failed, ob2.2 not support user_name in 'dba' etc... """)
##   logging.info("""check_inner_user_role_exists success""")
##
### 19. 从小于224的版本升级上来时，需要确认high_priority_net_thread_count配置项值为0 (224版本开始该值默认为1)
##def check_high_priority_net_thread_count_before_224(query_cur):
##  # 获取最小版本
##  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
##  if len(results) != 1:
##    raise MyError('distinct observer version exist')
##  elif cmp(results[0][0], "2.2.40") >= 0 :
##    # 最小版本大于等于2.2.40，忽略检查
##    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check standby cluster'.format(results[0][0]))
##  else:
##    # 低于224版本的需要确认配置项值为0
##    logging.info('cluster version is ({0}), need check high_priority_net_thread_count'.format(results[0][0]))
##    (desc, results) = query_cur.exec_query("""select value from __all_sys_parameter where name like 'high_priority_net_thread_count'""")
##    for line in results:
##        thread_cnt = line[0]
##        if thread_cnt > 0:
##            raise MyError('high_priority_net_thread_count is greater than 0, unexpected')
##    logging.info('check high_priority_net_thread_count success')
##
### 20. 从小于224的版本升级上来时，要求不能有备库存在
##def check_standby_cluster(query_cur):
##  # 获取最小版本
##  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
##  if len(results) != 1:
##    raise MyError('distinct observer version exist')
##  elif cmp(results[0][0], "2.2.40") >= 0 :
##    # 最小版本大于等于2.2.40，忽略检查
##    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check standby cluster'.format(results[0][0]))
##  else:
##    logging.info('cluster version is ({0}), need check standby cluster'.format(results[0][0]))
##    (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where table_name = '__all_cluster'""")
##    if results[0][0] == 0:
##      logging.info('cluster ({0}) has no __all_cluster table, no standby cluster'.format(results[0][0]))
##    else:
##      (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_cluster""")
##      if results[0][0] > 1:
##        raise MyError("""multiple cluster exist in __all_cluster, maybe standby clusters added, not supported""")
##  logging.info('check standby cluster from __all_cluster success')
##
### 开始升级前的检查
##def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    conn.autocommit = True
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = QueryCursor(cur)
##      check_observer_version(query_cur, upgrade_params)
##      check_paxos_replica(query_cur)
##      check_rebalance_task(query_cur)
##      check_cluster_status(query_cur)
##      check_disable_separate_sys_clog(query_cur)
##      check_macro_block_data_seq(query_cur)
##      check_sstable_column_checksum_method_new(query_cur)
##      check_tenant_resource_pool(query_cur)
##      check_logonly_replica_unit(query_cur)
##      check_tenant_part_num(query_cur)
##      check_tenant_resource(query_cur)
##      check_readonly_zone(query_cur)
##      modify_server_permanent_offline_time(query_cur)
##      modify_replica_safe_remove_time(query_cur)
##      check_progressive_merge_round(query_cur)
##      check_inner_user_role_exists(query_cur)
##      check_high_priority_net_thread_count_before_224(query_cur)
##      check_standby_cluster(query_cur)
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##if __name__ == '__main__':
##  upgrade_params = UpgradeParams()
##  change_opt_defult_value('log-file', upgrade_params.log_filename)
##  parse_options(sys.argv[1:])
##  if not has_no_local_opts():
##    deal_with_local_opts()
##  else:
##    check_db_client_opts()
##    log_filename = get_opt_log_file()
##    upgrade_params.log_filename = log_filename
##    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
##    config_logging_module(upgrade_params.log_filename)
##    try:
##      host = get_opt_host()
##      port = int(get_opt_port())
##      user = get_opt_user()
##      password = get_opt_password()
##      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
##          host, port, user, password, log_filename)
##      do_check(host, port, user, password, upgrade_params)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connctor error')
##      raise e
##    except Exception, e:
##      logging.exception('normal error')
##      raise e
##
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:upgrade_cluster_health_checker.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import sys
##import os
##import time
##import mysql.connector
##from mysql.connector import errorcode
##import logging
##import getopt
##
##class UpgradeParams:
##  log_filename = 'upgrade_cluster_health_checker.log'
##
###### --------------start : my_error.py --------------
##class MyError(Exception):
##  def __init__(self, value):
##    self.value = value
##  def __str__(self):
##    return repr(self.value)
##
###### --------------start : actions.py 只允许执行查询语句--------------
##class QueryCursor:
##  __cursor = None
##  def __init__(self, cursor):
##    self.__cursor = cursor
##  def exec_sql(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
##      return rowcount
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
##  def exec_query(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      results = self.__cursor.fetchall()
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
##      return (self.__cursor.description, results)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
###### ---------------end----------------------
##
###### --------------start :  opt.py --------------
##help_str = \
##"""
##Help:
##""" +\
##sys.argv[0] + """ [OPTIONS]""" +\
##'\n\n' +\
##'-I, --help          Display this help and exit.\n' +\
##'-V, --version       Output version information and exit.\n' +\
##'-h, --host=name     Connect to host.\n' +\
##'-P, --port=name     Port number to use for connection.\n' +\
##'-u, --user=name     User for login.\n' +\
##'-p, --password=name Password to use when connecting to server. If password is\n' +\
##'                    not given it\'s empty string "".\n' +\
##'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
##'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
##'                    system_variable_dml, special_action, all. "all" represents\n' +\
##'                    that all modules should be run. They are splitted by ",".\n' +\
##'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
##'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
##'-t, --timeout=name  check timeout, default: 600(s).\n' + \
##'\n\n' +\
##'Maybe you want to run cmd like that:\n' +\
##sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
##
##version_str = """version 1.0.0"""
##
##class Option:
##  __g_short_name_set = set([])
##  __g_long_name_set = set([])
##  __short_name = None
##  __long_name = None
##  __is_with_param = None
##  __is_local_opt = None
##  __has_value = None
##  __value = None
##  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
##    if short_name in Option.__g_short_name_set:
##      raise MyError('duplicate option short name: {0}'.format(short_name))
##    elif long_name in Option.__g_long_name_set:
##      raise MyError('duplicate option long name: {0}'.format(long_name))
##    Option.__g_short_name_set.add(short_name)
##    Option.__g_long_name_set.add(long_name)
##    self.__short_name = short_name
##    self.__long_name = long_name
##    self.__is_with_param = is_with_param
##    self.__is_local_opt = is_local_opt
##    self.__has_value = False
##    if None != default_value:
##      self.set_value(default_value)
##  def is_with_param(self):
##    return self.__is_with_param
##  def get_short_name(self):
##    return self.__short_name
##  def get_long_name(self):
##    return self.__long_name
##  def has_value(self):
##    return self.__has_value
##  def get_value(self):
##    return self.__value
##  def set_value(self, value):
##    self.__value = value
##    self.__has_value = True
##  def is_local_opt(self):
##    return self.__is_local_opt
##  def is_valid(self):
##    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
##
##g_opts =\
##[\
##Option('I', 'help', False, True),\
##Option('V', 'version', False, True),\
##Option('h', 'host', True, False),\
##Option('P', 'port', True, False),\
##Option('u', 'user', True, False),\
##Option('p', 'password', True, False, ''),\
### 要跑哪个模块，默认全跑
##Option('m', 'module', True, False, 'all'),\
### 日志文件路径，不同脚本的main函数中中会改成不同的默认值
##Option('l', 'log-file', True, False),\
### 一些检查的超时时间，默认是600s
##Option('t', 'timeout', True, False, '600')
##]\
##
##def change_opt_defult_value(opt_long_name, opt_default_val):
##  global g_opts
##  for opt in g_opts:
##    if opt.get_long_name() == opt_long_name:
##      opt.set_value(opt_default_val)
##      return
##
##def has_no_local_opts():
##  global g_opts
##  no_local_opts = True
##  for opt in g_opts:
##    if opt.is_local_opt() and opt.has_value():
##      no_local_opts = False
##  return no_local_opts
##
##def check_db_client_opts():
##  global g_opts
##  for opt in g_opts:
##    if not opt.is_local_opt() and not opt.has_value():
##      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
##          .format(opt.get_short_name(), sys.argv[0]))
##
##def parse_option(opt_name, opt_val):
##  global g_opts
##  for opt in g_opts:
##    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
##      opt.set_value(opt_val)
##
##def parse_options(argv):
##  global g_opts
##  short_opt_str = ''
##  long_opt_list = []
##  for opt in g_opts:
##    if opt.is_with_param():
##      short_opt_str += opt.get_short_name() + ':'
##    else:
##      short_opt_str += opt.get_short_name()
##  for opt in g_opts:
##    if opt.is_with_param():
##      long_opt_list.append(opt.get_long_name() + '=')
##    else:
##      long_opt_list.append(opt.get_long_name())
##  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
##  for (opt_name, opt_val) in opts:
##    parse_option(opt_name, opt_val)
##  if has_no_local_opts():
##    check_db_client_opts()
##
##def deal_with_local_opt(opt):
##  if 'help' == opt.get_long_name():
##    global help_str
##    print help_str
##  elif 'version' == opt.get_long_name():
##    global version_str
##    print version_str
##
##def deal_with_local_opts():
##  global g_opts
##  if has_no_local_opts():
##    raise MyError('no local options, can not deal with local options')
##  else:
##    for opt in g_opts:
##      if opt.is_local_opt() and opt.has_value():
##        deal_with_local_opt(opt)
##        # 只处理一个
##        return
##
##def get_opt_host():
##  global g_opts
##  for opt in g_opts:
##    if 'host' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_port():
##  global g_opts
##  for opt in g_opts:
##    if 'port' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_user():
##  global g_opts
##  for opt in g_opts:
##    if 'user' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_password():
##  global g_opts
##  for opt in g_opts:
##    if 'password' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_module():
##  global g_opts
##  for opt in g_opts:
##    if 'module' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_log_file():
##  global g_opts
##  for opt in g_opts:
##    if 'log-file' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_timeout():
##  global g_opts
##  for opt in g_opts:
##    if 'timeout' == opt.get_long_name():
##      return opt.get_value()
###### ---------------end----------------------
##
###### --------------start :  do_upgrade_pre.py--------------
##def config_logging_module(log_filenamme):
##  logging.basicConfig(level=logging.INFO,\
##      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
##      datefmt='%Y-%m-%d %H:%M:%S',\
##      filename=log_filenamme,\
##      filemode='w')
##  # 定义日志打印格式
##  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
##  #######################################
##  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
##  stdout_handler = logging.StreamHandler(sys.stdout)
##  stdout_handler.setLevel(logging.INFO)
##  # 设置日志打印格式
##  stdout_handler.setFormatter(formatter)
##  # 将定义好的stdout_handler日志handler添加到root logger
##  logging.getLogger('').addHandler(stdout_handler)
###### ---------------end----------------------
##
###### START ####
### 1. 检查paxos副本是否同步, paxos副本是否缺失
##def check_paxos_replica(query_cur):
##  # 2.1 检查paxos副本是否同步
##  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
##  if results[0][0] > 0 :
##    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
##  # 2.2 检查paxos副本是否有缺失 TODO
##  logging.info('check paxos replica success')
##
### 2. 检查是否有做balance, locality变更
##def check_rebalance_task(query_cur):
##  # 3.1 检查是否有做locality变更
##  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
##  if results[0][0] > 0 :
##    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
##  # 3.2 检查是否有做balance
##  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
##  if results[0][0] > 0 :
##    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
##  logging.info('check rebalance task success')
##
### 3. 检查集群状态
##def check_cluster_status(query_cur):
##  # 4.1 检查是否非合并状态
##  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
##  if cmp(results[0][0], 'IDLE')  != 0 :
##    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
##  logging.info('check cluster status success')
##  # 4.2 检查合并版本是否>=3
##  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
##  if results[0][0] < 2:
##      raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
##  logging.info('check global last_merged_version success')
##
### 4. 检查租户分区数是否超出内存限制
##def check_tenant_part_num(query_cur):
##  # 统计每个租户在各个server上的分区数量
##  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
##  # 计算每个租户在每个server上的max_memory
##  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
##  # 查询每个server的memstore_limit_percentage
##  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
##  part_static_cost = 128 * 1024
##  part_dynamic_cost = 400 * 1024
##  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
##  part_num_reserved = 512
##  for line in res_part_num:
##    svr_ip = line[0]
##    svr_port = line[1]
##    tenant_id = line[2]
##    part_num = line[3]
##    for uline in res_unit_memory:
##      uip = uline[0]
##      uport = uline[1]
##      utid = uline[2]
##      umem = uline[3]
##      utype = uline[4]
##      if svr_ip == uip and svr_port == uport and tenant_id == utid:
##        for mpline in res_svr_memstore_percent:
##          mpip = mpline[0]
##          mpport = mpline[1]
##          if mpip == uip and mpport == uport:
##            mspercent = int(mpline[3])
##            mem_limit = umem
##            if 0 == utype:
##              # full类型的unit需要为memstore预留内存
##              mem_limit = umem * (100 - mspercent) / 100
##            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
##            if part_num_limit <= 1000:
##              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
##            if part_num >= (part_num_limit - part_num_reserved):
##              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
##            break
##  logging.info('check tenant partition num success')
##
### 5. 检查存在租户partition，但是不存在unit的observer
##def check_tenant_resource(query_cur):
##  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
##  for line in res_unit:
##    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
##  logging.info("check tenant resource success")
##
### 6. 检查progressive_merge_round都升到1
##def check_progressive_merge_round(query_cur):
##  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id = 0""")
##  if results[0][0] != 0:
##    raise MyError("""progressive_merge_round of main table should all be 1""")
##  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id > 0 and data_table_id in (select table_id from __all_virtual_table where table_type not in (1,2,4) and data_table_id = 0)""")
##  if results[0][0] != 0:
##    raise MyError("""progressive_merge_round of index should all be 1""")
##  logging.info("""check progressive_merge_round status success""")
##
### 主库状态检查
##def check_primary_cluster_sync_status(query_cur, timeout):
##  (desc, res) = query_cur.exec_query("""select  current_scn  from oceanbase.v$ob_cluster where cluster_role='PRIMARY' and cluster_status='VALID'""")
##  if len(res) != 1:
##      raise MyError('query results count is not 1')
##  query_sql = "select count(*) from oceanbase.v$ob_standby_status where cluster_role != 'PHYSICAL STANDBY' or cluster_status != 'VALID' or current_scn < {0}".format(res[0][0]);  
##  times = timeout
##  print times
##  while times > 0 :
##    (desc, res1) = query_cur.exec_query(query_sql)
##    if len(res1) == 1 and res1[0][0] == 0:
##      break;
##    time.sleep(1)
##    times -=1
##  if times == 0:
##    raise MyError("there exists standby cluster not synchronizing, checking primary cluster status failed!!!")
##  else:
##    logging.info("check primary cluster sync status success")
##
### 备库状态检查
##def check_standby_cluster_sync_status(query_cur, timeout):
##  (desc, res) = query_cur.exec_query("""select time_to_usec(now(6)) from dual""")
##  query_sql = "select count(*) from oceanbase.v$ob_cluster where (cluster_role != 'PHYSICAL STANDBY') or (cluster_status != 'VALID') or (current_scn  < {0}) or (switchover_status != 'NOT ALLOWED')".format(res[0][0]);
##  times = timeout
##  while times > 0 :
##    (desc, res2) = query_cur.exec_query(query_sql)
##    if len(res2) == 1 and res2[0][0] == 0:
##      break
##    time.sleep(1)
##    times -= 1
##  if times == 0:
##    raise MyError('current standby cluster not synchronizing, please check!!!')
##  else:
##    logging.info("check standby cluster sync status success")
##
### 判断是主库还是备库
##def check_cluster_sync_status(query_cur, timeout):
##  (desc, res) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
##  if res[0][0] == 'PRIMARY':
##    check_primary_cluster_sync_status(query_cur, timeout)
##  else:
##    check_standby_cluster_sync_status(query_cur, timeout)
##  
##
### 开始升级前的检查
##def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    conn.autocommit = True
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = QueryCursor(cur)
##      check_paxos_replica(query_cur)
##      check_rebalance_task(query_cur)
##      check_cluster_status(query_cur)
##      check_tenant_part_num(query_cur)
##      check_tenant_resource(query_cur)
##      check_cluster_sync_status(query_cur, timeout)
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##if __name__ == '__main__':
##  upgrade_params = UpgradeParams()
##  change_opt_defult_value('log-file', upgrade_params.log_filename)
##  parse_options(sys.argv[1:])
##  if not has_no_local_opts():
##    deal_with_local_opts()
##  else:
##    check_db_client_opts()
##    log_filename = get_opt_log_file()
##    upgrade_params.log_filename = log_filename
##    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
##    config_logging_module(upgrade_params.log_filename)
##    try:
##      host = get_opt_host()
##      port = int(get_opt_port())
##      user = get_opt_user()
##      password = get_opt_password()
##      timeout = int(get_opt_timeout())
##      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s', \
##          host, port, user, password, log_filename, timeout)
##      do_check(host, port, user, password, upgrade_params, timeout)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connctor error')
##      raise e
##    except Exception, e:
##      logging.exception('normal error')
##      raise e
##
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:upgrade_post_checker.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import sys
##import os
##import time
##import mysql.connector
##from mysql.connector import errorcode
##import logging
##import getopt
##
##class UpgradeParams:
##  log_filename = 'upgrade_post_checker.log'
##  new_version = '3.0.0'
###### --------------start : my_error.py --------------
##class MyError(Exception):
##  def __init__(self, value):
##    self.value = value
##  def __str__(self):
##    return repr(self.value)
##
###### --------------start : actions.py--------------
##class QueryCursor:
##  __cursor = None
##  def __init__(self, cursor):
##    self.__cursor = cursor
##  def exec_sql(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
##      return rowcount
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
##  def exec_query(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      results = self.__cursor.fetchall()
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
##      return (self.__cursor.description, results)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
###### ---------------end----------------------
##
###### --------------start :  opt.py --------------
##help_str = \
##"""
##Help:
##""" +\
##sys.argv[0] + """ [OPTIONS]""" +\
##'\n\n' +\
##'-I, --help          Display this help and exit.\n' +\
##'-V, --version       Output version information and exit.\n' +\
##'-h, --host=name     Connect to host.\n' +\
##'-P, --port=name     Port number to use for connection.\n' +\
##'-u, --user=name     User for login.\n' +\
##'-p, --password=name Password to use when connecting to server. If password is\n' +\
##'                    not given it\'s empty string "".\n' +\
##'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
##'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
##'                    system_variable_dml, special_action, all. "all" represents\n' +\
##'                    that all modules should be run. They are splitted by ",".\n' +\
##'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
##'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
##'\n\n' +\
##'Maybe you want to run cmd like that:\n' +\
##sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
##
##version_str = """version 1.0.0"""
##
##class Option:
##  __g_short_name_set = set([])
##  __g_long_name_set = set([])
##  __short_name = None
##  __long_name = None
##  __is_with_param = None
##  __is_local_opt = None
##  __has_value = None
##  __value = None
##  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
##    if short_name in Option.__g_short_name_set:
##      raise MyError('duplicate option short name: {0}'.format(short_name))
##    elif long_name in Option.__g_long_name_set:
##      raise MyError('duplicate option long name: {0}'.format(long_name))
##    Option.__g_short_name_set.add(short_name)
##    Option.__g_long_name_set.add(long_name)
##    self.__short_name = short_name
##    self.__long_name = long_name
##    self.__is_with_param = is_with_param
##    self.__is_local_opt = is_local_opt
##    self.__has_value = False
##    if None != default_value:
##      self.set_value(default_value)
##  def is_with_param(self):
##    return self.__is_with_param
##  def get_short_name(self):
##    return self.__short_name
##  def get_long_name(self):
##    return self.__long_name
##  def has_value(self):
##    return self.__has_value
##  def get_value(self):
##    return self.__value
##  def set_value(self, value):
##    self.__value = value
##    self.__has_value = True
##  def is_local_opt(self):
##    return self.__is_local_opt
##  def is_valid(self):
##    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
##
##g_opts =\
##[\
##Option('I', 'help', False, True),\
##Option('V', 'version', False, True),\
##Option('h', 'host', True, False),\
##Option('P', 'port', True, False),\
##Option('u', 'user', True, False),\
##Option('p', 'password', True, False, ''),\
### 要跑哪个模块，默认全跑
##Option('m', 'module', True, False, 'all'),\
### 日志文件路径，不同脚本的main函数中中会改成不同的默认值
##Option('l', 'log-file', True, False)
##]\
##
##def change_opt_defult_value(opt_long_name, opt_default_val):
##  global g_opts
##  for opt in g_opts:
##    if opt.get_long_name() == opt_long_name:
##      opt.set_value(opt_default_val)
##      return
##
##def has_no_local_opts():
##  global g_opts
##  no_local_opts = True
##  for opt in g_opts:
##    if opt.is_local_opt() and opt.has_value():
##      no_local_opts = False
##  return no_local_opts
##
##def check_db_client_opts():
##  global g_opts
##  for opt in g_opts:
##    if not opt.is_local_opt() and not opt.has_value():
##      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
##          .format(opt.get_short_name(), sys.argv[0]))
##
##def parse_option(opt_name, opt_val):
##  global g_opts
##  for opt in g_opts:
##    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
##      opt.set_value(opt_val)
##
##def parse_options(argv):
##  global g_opts
##  short_opt_str = ''
##  long_opt_list = []
##  for opt in g_opts:
##    if opt.is_with_param():
##      short_opt_str += opt.get_short_name() + ':'
##    else:
##      short_opt_str += opt.get_short_name()
##  for opt in g_opts:
##    if opt.is_with_param():
##      long_opt_list.append(opt.get_long_name() + '=')
##    else:
##      long_opt_list.append(opt.get_long_name())
##  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
##  for (opt_name, opt_val) in opts:
##    parse_option(opt_name, opt_val)
##  if has_no_local_opts():
##    check_db_client_opts()
##
##def deal_with_local_opt(opt):
##  if 'help' == opt.get_long_name():
##    global help_str
##    print help_str
##  elif 'version' == opt.get_long_name():
##    global version_str
##    print version_str
##
##def deal_with_local_opts():
##  global g_opts
##  if has_no_local_opts():
##    raise MyError('no local options, can not deal with local options')
##  else:
##    for opt in g_opts:
##      if opt.is_local_opt() and opt.has_value():
##        deal_with_local_opt(opt)
##        # 只处理一个
##        return
##
##def get_opt_host():
##  global g_opts
##  for opt in g_opts:
##    if 'host' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_port():
##  global g_opts
##  for opt in g_opts:
##    if 'port' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_user():
##  global g_opts
##  for opt in g_opts:
##    if 'user' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_password():
##  global g_opts
##  for opt in g_opts:
##    if 'password' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_module():
##  global g_opts
##  for opt in g_opts:
##    if 'module' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_log_file():
##  global g_opts
##  for opt in g_opts:
##    if 'log-file' == opt.get_long_name():
##      return opt.get_value()
###### ---------------end----------------------
##
##def config_logging_module(log_filenamme):
##  logging.basicConfig(level=logging.INFO,\
##      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
##      datefmt='%Y-%m-%d %H:%M:%S',\
##      filename=log_filenamme,\
##      filemode='w')
##  # 定义日志打印格式
##  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
##  #######################################
##  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
##  stdout_handler = logging.StreamHandler(sys.stdout)
##  stdout_handler.setLevel(logging.INFO)
##  # 设置日志打印格式
##  stdout_handler.setFormatter(formatter)
##  # 将定义好的stdout_handler日志handler添加到root logger
##  logging.getLogger('').addHandler(stdout_handler)
##
###### STAR
### 1 检查版本号
##def check_cluster_version(query_cur):
##  # 一方面配置项生效是个异步生效任务,另一方面是2.2.0之后新增租户级配置项刷新，和系统级配置项刷新复用同一个timer，这里暂且等一下。
##  times = 30
##  sql="select distinct value = '{0}' from oceanbase.__all_virtual_sys_parameter_stat where name='min_observer_version'".format(upgrade_params.new_version)
##  while times > 0 :
##    (desc, results) = query_cur.exec_query(sql)
##    if len(results) == 1 and results[0][0] == 1:
##      break;
##    time.sleep(1)
##    times -=1
##  if times == 0:
##    raise MyError("check cluster version timeout!")
##  else:
##    logging.info("check_cluster_version success")
##
##def check_storage_format_version(query_cur):
##  # Specified expected version each time want to upgrade (see OB_STORAGE_FORMAT_VERSION_MAX)
##  expect_version = 4;
##  sql = "select value from oceanbase.__all_zone where zone = '' and name = 'storage_format_version'"
##  times = 180
##  while times > 0 :
##    (desc, results) = query_cur.exec_query(sql)
##    if len(results) == 1 and results[0][0] == expect_version:
##      break
##    time.sleep(10)
##    times -= 1
##  if times == 0:
##    raise MyError("check storage format version timeout! Expected version {0}".format(expect_version))
##  else:
##    logging.info("check expected storage format version '{0}' success".format(expect_version))
##
##def upgrade_table_schema_version(conn, cur):
##  try:
##    sql = """SELECT * FROM v$ob_cluster
##             WHERE cluster_role = "PRIMARY"
##             AND cluster_status = "VALID"
##             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
##    (desc, results) = cur.exec_query(sql)
##    is_primary = len(results) > 0
##    if is_primary:
##      sql = "alter system run job 'UPDATE_TABLE_SCHEMA_VERSION';"
##      logging.info(sql)
##      cur.exec_sql(sql)
##    else:
##      logging.info("standby cluster no need to run job update_table_schema_ersion")
##  except Exception, e:
##    logging.warn("update table schema failed")
##    raise MyError("update table schema failed")
##  logging.info("update table schema finish")
##
##def upgrade_storage_format_version(conn, cur):
##  try:
##    # enable_ddl
##    sql = "alter system set enable_ddl = true;"
##    logging.info(sql)
##    cur.execute(sql)
##    time.sleep(10)
##
##    # run job
##    sql = "alter system run job 'UPGRADE_STORAGE_FORMAT_VERSION';"
##    logging.info(sql)
##    cur.execute(sql)
##
##  except Exception, e:
##    logging.warn("upgrade storage format version failed")
##    raise MyError("upgrade storage format version failed")
##  logging.info("upgrade storage format version finish")
##
### 2 检查内部表自检是否成功
##def check_root_inspection(query_cur):
##  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where info != 'succeed'"
##  times = 180
##  while times > 0 :
##    (desc, results) = query_cur.exec_query(sql)
##    if results[0][0] == 0:
##      break
##    time.sleep(10)
##    times -= 1
##  if times == 0:
##    raise MyError('check root inspection failed!')
##  logging.info('check root inspection success')
##
### 3 开ddl
##def enable_ddl(query_cur):
##  query_cur.exec_sql("""alter system set enable_ddl = true""")
##  logging.info("enable_ddl success")
##
### 4 打开rebalance
##def enable_rebalance(query_cur):
##  query_cur.exec_sql("""alter system set enable_rebalance = true""")
##  logging.info("enable_rebalance success")
##
### 5 打开rereplication
##def enable_rereplication(query_cur):
##  query_cur.exec_sql("""alter system set enable_rereplication = true""")
##  logging.info("enable_rereplication success")
##
### 6 打开major freeze
##def enable_major_freeze(query_cur):
##  query_cur.exec_sql("""alter system set enable_major_freeze=true""")
##  logging.info("enable_major_freeze success")
##
### 开始升级后的检查
##def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    conn.autocommit = True
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = QueryCursor(cur)
##      try:
##        check_cluster_version(query_cur)
##        #upgrade_storage_format_version(conn, cur)
##        #check_storage_format_version(query_cur)
##        upgrade_table_schema_version(conn, query_cur)
##        check_root_inspection(query_cur)
##        enable_ddl(query_cur)
##        enable_rebalance(query_cur)
##        enable_rereplication(query_cur )
##        enable_major_freeze(query_cur)
##      except Exception, e:
##        logging.exception('run error')
##        raise e
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##if __name__ == '__main__':
##  upgrade_params = UpgradeParams()
##  change_opt_defult_value('log-file', upgrade_params.log_filename)
##  parse_options(sys.argv[1:])
##  if not has_no_local_opts():
##    deal_with_local_opts()
##  else:
##    check_db_client_opts()
##    log_filename = get_opt_log_file()
##    upgrade_params.log_filename = log_filename
##    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
##    config_logging_module(upgrade_params.log_filename)
##    try:
##      host = get_opt_host()
##      port = int(get_opt_port())
##      user = get_opt_user()
##      password = get_opt_password()
##      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
##          host, port, user, password, log_filename)
##      do_check(host, port, user, password, upgrade_params)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connctor error')
##      raise e
##    except Exception, e:
##      logging.exception('normal error')
##      raise e
##
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:upgrade_rolling_post.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import sys
##import os
##import time
##import mysql.connector
##from mysql.connector import errorcode
##import logging
##import getopt
##
##class UpgradeParams:
##  log_filename = 'upgrade_rolling_post.log'
###### --------------start : my_error.py --------------
##class MyError(Exception):
##  def __init__(self, value):
##    self.value = value
##  def __str__(self):
##    return repr(self.value)
##
###### --------------start : actions.py--------------
##class QueryCursor:
##  __cursor = None
##  def __init__(self, cursor):
##    self.__cursor = cursor
##  def exec_sql(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
##      return rowcount
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
##  def exec_query(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      results = self.__cursor.fetchall()
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
##      return (self.__cursor.description, results)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
###### ---------------end----------------------
##
###### --------------start :  opt.py --------------
##help_str = \
##"""
##Help:
##""" +\
##sys.argv[0] + """ [OPTIONS]""" +\
##'\n\n' +\
##'-I, --help          Display this help and exit.\n' +\
##'-V, --version       Output version information and exit.\n' +\
##'-h, --host=name     Connect to host.\n' +\
##'-P, --port=name     Port number to use for connection.\n' +\
##'-u, --user=name     User for login.\n' +\
##'-p, --password=name Password to use when connecting to server. If password is\n' +\
##'                    not given it\'s empty string "".\n' +\
##'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
##'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
##'                    system_variable_dml, special_action, all. "all" represents\n' +\
##'                    that all modules should be run. They are splitted by ",".\n' +\
##'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
##'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
##'\n\n' +\
##'Maybe you want to run cmd like that:\n' +\
##sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
##
##version_str = """version 1.0.0"""
##
##class Option:
##  __g_short_name_set = set([])
##  __g_long_name_set = set([])
##  __short_name = None
##  __long_name = None
##  __is_with_param = None
##  __is_local_opt = None
##  __has_value = None
##  __value = None
##  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
##    if short_name in Option.__g_short_name_set:
##      raise MyError('duplicate option short name: {0}'.format(short_name))
##    elif long_name in Option.__g_long_name_set:
##      raise MyError('duplicate option long name: {0}'.format(long_name))
##    Option.__g_short_name_set.add(short_name)
##    Option.__g_long_name_set.add(long_name)
##    self.__short_name = short_name
##    self.__long_name = long_name
##    self.__is_with_param = is_with_param
##    self.__is_local_opt = is_local_opt
##    self.__has_value = False
##    if None != default_value:
##      self.set_value(default_value)
##  def is_with_param(self):
##    return self.__is_with_param
##  def get_short_name(self):
##    return self.__short_name
##  def get_long_name(self):
##    return self.__long_name
##  def has_value(self):
##    return self.__has_value
##  def get_value(self):
##    return self.__value
##  def set_value(self, value):
##    self.__value = value
##    self.__has_value = True
##  def is_local_opt(self):
##    return self.__is_local_opt
##  def is_valid(self):
##    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
##
##g_opts =\
##[\
##Option('I', 'help', False, True),\
##Option('V', 'version', False, True),\
##Option('h', 'host', True, False),\
##Option('P', 'port', True, False),\
##Option('u', 'user', True, False),\
##Option('p', 'password', True, False, ''),\
### 要跑哪个模块，默认全跑
##Option('m', 'module', True, False, 'all'),\
### 日志文件路径，不同脚本的main函数中中会改成不同的默认值
##Option('l', 'log-file', True, False)
##]\
##
##def change_opt_defult_value(opt_long_name, opt_default_val):
##  global g_opts
##  for opt in g_opts:
##    if opt.get_long_name() == opt_long_name:
##      opt.set_value(opt_default_val)
##      return
##
##def has_no_local_opts():
##  global g_opts
##  no_local_opts = True
##  for opt in g_opts:
##    if opt.is_local_opt() and opt.has_value():
##      no_local_opts = False
##  return no_local_opts
##
##def check_db_client_opts():
##  global g_opts
##  for opt in g_opts:
##    if not opt.is_local_opt() and not opt.has_value():
##      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
##          .format(opt.get_short_name(), sys.argv[0]))
##
##def parse_option(opt_name, opt_val):
##  global g_opts
##  for opt in g_opts:
##    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
##      opt.set_value(opt_val)
##
##def parse_options(argv):
##  global g_opts
##  short_opt_str = ''
##  long_opt_list = []
##  for opt in g_opts:
##    if opt.is_with_param():
##      short_opt_str += opt.get_short_name() + ':'
##    else:
##      short_opt_str += opt.get_short_name()
##  for opt in g_opts:
##    if opt.is_with_param():
##      long_opt_list.append(opt.get_long_name() + '=')
##    else:
##      long_opt_list.append(opt.get_long_name())
##  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
##  for (opt_name, opt_val) in opts:
##    parse_option(opt_name, opt_val)
##  if has_no_local_opts():
##    check_db_client_opts()
##
##def deal_with_local_opt(opt):
##  if 'help' == opt.get_long_name():
##    global help_str
##    print help_str
##  elif 'version' == opt.get_long_name():
##    global version_str
##    print version_str
##
##def deal_with_local_opts():
##  global g_opts
##  if has_no_local_opts():
##    raise MyError('no local options, can not deal with local options')
##  else:
##    for opt in g_opts:
##      if opt.is_local_opt() and opt.has_value():
##        deal_with_local_opt(opt)
##        # 只处理一个
##        return
##
##def get_opt_host():
##  global g_opts
##  for opt in g_opts:
##    if 'host' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_port():
##  global g_opts
##  for opt in g_opts:
##    if 'port' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_user():
##  global g_opts
##  for opt in g_opts:
##    if 'user' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_password():
##  global g_opts
##  for opt in g_opts:
##    if 'password' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_module():
##  global g_opts
##  for opt in g_opts:
##    if 'module' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_log_file():
##  global g_opts
##  for opt in g_opts:
##    if 'log-file' == opt.get_long_name():
##      return opt.get_value()
###### ---------------end----------------------
##
##def config_logging_module(log_filenamme):
##  logging.basicConfig(level=logging.INFO,\
##      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
##      datefmt='%Y-%m-%d %H:%M:%S',\
##      filename=log_filenamme,\
##      filemode='w')
##  # 定义日志打印格式
##  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
##  #######################################
##  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
##  stdout_handler = logging.StreamHandler(sys.stdout)
##  stdout_handler.setLevel(logging.INFO)
##  # 设置日志打印格式
##  stdout_handler.setFormatter(formatter)
##  # 将定义好的stdout_handler日志handler添加到root logger
##  logging.getLogger('').addHandler(stdout_handler)
##
##def run(my_host, my_port, my_user, my_passwd, upgrade_params):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    conn.autocommit = True
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = QueryCursor(cur)
##      (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
##      if len(results) != 1:
##          raise MyError('distinct observer version not exist')
##      #rolling upgrade 在2.2.50版本后支持
##      elif cmp(results[0][0], "2.2.50") >= 0:
##          query_cur.exec_sql("""ALTER SYSTEM END ROLLING UPGRADE""")
##          logging.info("END ROLLING UPGRADE success")
##      else:
##          logging.info("cluster version ({0}) less than 2.2.50, skip".format(results[0][0]))
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##if __name__ == '__main__':
##  upgrade_params = UpgradeParams()
##  change_opt_defult_value('log-file', upgrade_params.log_filename)
##  parse_options(sys.argv[1:])
##  if not has_no_local_opts():
##    deal_with_local_opts()
##  else:
##    check_db_client_opts()
##    log_filename = get_opt_log_file()
##    upgrade_params.log_filename = log_filename
##    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
##    config_logging_module(upgrade_params.log_filename)
##    try:
##      host = get_opt_host()
##      port = int(get_opt_port())
##      user = get_opt_user()
##      password = get_opt_password()
##      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
##          host, port, user, password, log_filename)
##      run(host, port, user, password, upgrade_params)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connctor error')
##      raise e
##    except Exception, e:
##      logging.exception('normal error')
##      raise e
##
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:upgrade_rolling_pre.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import sys
##import os
##import time
##import mysql.connector
##from mysql.connector import errorcode
##import logging
##import getopt
##
##class UpgradeParams:
##  log_filename = 'upgrade_rolling_pre.log'
###### --------------start : my_error.py --------------
##class MyError(Exception):
##  def __init__(self, value):
##    self.value = value
##  def __str__(self):
##    return repr(self.value)
##
###### --------------start : actions.py--------------
##class QueryCursor:
##  __cursor = None
##  def __init__(self, cursor):
##    self.__cursor = cursor
##  def exec_sql(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
##      return rowcount
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
##  def exec_query(self, sql, print_when_succ = True):
##    try:
##      self.__cursor.execute(sql)
##      results = self.__cursor.fetchall()
##      rowcount = self.__cursor.rowcount
##      if True == print_when_succ:
##        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
##      return (self.__cursor.description, results)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connector error, fail to execute sql: %s', sql)
##      raise e
##    except Exception, e:
##      logging.exception('normal error, fail to execute sql: %s', sql)
##      raise e
###### ---------------end----------------------
##
###### --------------start :  opt.py --------------
##help_str = \
##"""
##Help:
##""" +\
##sys.argv[0] + """ [OPTIONS]""" +\
##'\n\n' +\
##'-I, --help          Display this help and exit.\n' +\
##'-V, --version       Output version information and exit.\n' +\
##'-h, --host=name     Connect to host.\n' +\
##'-P, --port=name     Port number to use for connection.\n' +\
##'-u, --user=name     User for login.\n' +\
##'-p, --password=name Password to use when connecting to server. If password is\n' +\
##'                    not given it\'s empty string "".\n' +\
##'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
##'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
##'                    system_variable_dml, special_action, all. "all" represents\n' +\
##'                    that all modules should be run. They are splitted by ",".\n' +\
##'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
##'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
##'\n\n' +\
##'Maybe you want to run cmd like that:\n' +\
##sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
##
##version_str = """version 1.0.0"""
##
##class Option:
##  __g_short_name_set = set([])
##  __g_long_name_set = set([])
##  __short_name = None
##  __long_name = None
##  __is_with_param = None
##  __is_local_opt = None
##  __has_value = None
##  __value = None
##  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
##    if short_name in Option.__g_short_name_set:
##      raise MyError('duplicate option short name: {0}'.format(short_name))
##    elif long_name in Option.__g_long_name_set:
##      raise MyError('duplicate option long name: {0}'.format(long_name))
##    Option.__g_short_name_set.add(short_name)
##    Option.__g_long_name_set.add(long_name)
##    self.__short_name = short_name
##    self.__long_name = long_name
##    self.__is_with_param = is_with_param
##    self.__is_local_opt = is_local_opt
##    self.__has_value = False
##    if None != default_value:
##      self.set_value(default_value)
##  def is_with_param(self):
##    return self.__is_with_param
##  def get_short_name(self):
##    return self.__short_name
##  def get_long_name(self):
##    return self.__long_name
##  def has_value(self):
##    return self.__has_value
##  def get_value(self):
##    return self.__value
##  def set_value(self, value):
##    self.__value = value
##    self.__has_value = True
##  def is_local_opt(self):
##    return self.__is_local_opt
##  def is_valid(self):
##    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value
##
##g_opts =\
##[\
##Option('I', 'help', False, True),\
##Option('V', 'version', False, True),\
##Option('h', 'host', True, False),\
##Option('P', 'port', True, False),\
##Option('u', 'user', True, False),\
##Option('p', 'password', True, False, ''),\
### 要跑哪个模块，默认全跑
##Option('m', 'module', True, False, 'all'),\
### 日志文件路径，不同脚本的main函数中中会改成不同的默认值
##Option('l', 'log-file', True, False)
##]\
##
##def change_opt_defult_value(opt_long_name, opt_default_val):
##  global g_opts
##  for opt in g_opts:
##    if opt.get_long_name() == opt_long_name:
##      opt.set_value(opt_default_val)
##      return
##
##def has_no_local_opts():
##  global g_opts
##  no_local_opts = True
##  for opt in g_opts:
##    if opt.is_local_opt() and opt.has_value():
##      no_local_opts = False
##  return no_local_opts
##
##def check_db_client_opts():
##  global g_opts
##  for opt in g_opts:
##    if not opt.is_local_opt() and not opt.has_value():
##      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
##          .format(opt.get_short_name(), sys.argv[0]))
##
##def parse_option(opt_name, opt_val):
##  global g_opts
##  for opt in g_opts:
##    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
##      opt.set_value(opt_val)
##
##def parse_options(argv):
##  global g_opts
##  short_opt_str = ''
##  long_opt_list = []
##  for opt in g_opts:
##    if opt.is_with_param():
##      short_opt_str += opt.get_short_name() + ':'
##    else:
##      short_opt_str += opt.get_short_name()
##  for opt in g_opts:
##    if opt.is_with_param():
##      long_opt_list.append(opt.get_long_name() + '=')
##    else:
##      long_opt_list.append(opt.get_long_name())
##  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
##  for (opt_name, opt_val) in opts:
##    parse_option(opt_name, opt_val)
##  if has_no_local_opts():
##    check_db_client_opts()
##
##def deal_with_local_opt(opt):
##  if 'help' == opt.get_long_name():
##    global help_str
##    print help_str
##  elif 'version' == opt.get_long_name():
##    global version_str
##    print version_str
##
##def deal_with_local_opts():
##  global g_opts
##  if has_no_local_opts():
##    raise MyError('no local options, can not deal with local options')
##  else:
##    for opt in g_opts:
##      if opt.is_local_opt() and opt.has_value():
##        deal_with_local_opt(opt)
##        # 只处理一个
##        return
##
##def get_opt_host():
##  global g_opts
##  for opt in g_opts:
##    if 'host' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_port():
##  global g_opts
##  for opt in g_opts:
##    if 'port' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_user():
##  global g_opts
##  for opt in g_opts:
##    if 'user' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_password():
##  global g_opts
##  for opt in g_opts:
##    if 'password' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_module():
##  global g_opts
##  for opt in g_opts:
##    if 'module' == opt.get_long_name():
##      return opt.get_value()
##
##def get_opt_log_file():
##  global g_opts
##  for opt in g_opts:
##    if 'log-file' == opt.get_long_name():
##      return opt.get_value()
###### ---------------end----------------------
##
##def config_logging_module(log_filenamme):
##  logging.basicConfig(level=logging.INFO,\
##      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
##      datefmt='%Y-%m-%d %H:%M:%S',\
##      filename=log_filenamme,\
##      filemode='w')
##  # 定义日志打印格式
##  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
##  #######################################
##  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
##  stdout_handler = logging.StreamHandler(sys.stdout)
##  stdout_handler.setLevel(logging.INFO)
##  # 设置日志打印格式
##  stdout_handler.setFormatter(formatter)
##  # 将定义好的stdout_handler日志handler添加到root logger
##  logging.getLogger('').addHandler(stdout_handler)
##
##def run(my_host, my_port, my_user, my_passwd, upgrade_params):
##  try:
##    conn = mysql.connector.connect(user = my_user,
##                                   password = my_passwd,
##                                   host = my_host,
##                                   port = my_port,
##                                   database = 'oceanbase',
##                                   raise_on_warnings = True)
##    conn.autocommit = True
##    cur = conn.cursor(buffered=True)
##    try:
##      query_cur = QueryCursor(cur)
##      (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
##      if len(results) != 1:
##          raise MyError('distinct observer version not exist')
##      #rolling upgrade 在2.2.50版本后支持
##      elif cmp(results[0][0], "2.2.50") >= 0:
##          query_cur.exec_sql("""ALTER SYSTEM BEGIN ROLLING UPGRADE""")
##          logging.info("BEGIN ROLLING UPGRADE success")
##      else:
##          logging.info("cluster version ({0}) less than 2.2.50, skip".format(results[0][0]))
##    except Exception, e:
##      logging.exception('run error')
##      raise e
##    finally:
##      cur.close()
##      conn.close()
##  except mysql.connector.Error, e:
##    logging.exception('connection error')
##    raise e
##  except Exception, e:
##    logging.exception('normal error')
##    raise e
##
##if __name__ == '__main__':
##  upgrade_params = UpgradeParams()
##  change_opt_defult_value('log-file', upgrade_params.log_filename)
##  parse_options(sys.argv[1:])
##  if not has_no_local_opts():
##    deal_with_local_opts()
##  else:
##    check_db_client_opts()
##    log_filename = get_opt_log_file()
##    upgrade_params.log_filename = log_filename
##    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
##    config_logging_module(upgrade_params.log_filename)
##    try:
##      host = get_opt_host()
##      port = int(get_opt_port())
##      user = get_opt_user()
##      password = get_opt_password()
##      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
##          host, port, user, password, log_filename)
##      run(host, port, user, password, upgrade_params)
##    except mysql.connector.Error, e:
##      logging.exception('mysql connctor error')
##      raise e
##    except Exception, e:
##      logging.exception('normal error')
##      raise e
##
#####====XXXX======######==== I am a splitter ====######======XXXX====####
##filename:upgrade_sys_vars.py
###!/usr/bin/env python
### -*- coding: utf-8 -*-
##
##import new
##import time
##import re
##import json
##import traceback
##import sys
##import mysql.connector
##from mysql.connector import errorcode
##import logging
##from my_error import MyError
##import actions
##from actions import DMLCursor
##from actions import QueryCursor
##from sys_vars_dict import sys_var_dict
##import my_utils
##
##
### 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户
##
##def calc_diff_sys_var(cur, tenant_id):
##  try:
##    change_tenant(cur, tenant_id)
##    actual_tenant_id = get_actual_tenant_id(tenant_id)
##    cur.execute("""select name, data_type, value, info, flags, min_val, max_val from __all_sys_variable_history where tenant_id=%s and (tenant_id, zone, name, schema_version) in (select tenant_id, zone, name, max(schema_version) from __all_sys_variable_history where tenant_id=%s group by tenant_id, zone, name);"""%(actual_tenant_id, actual_tenant_id))
##    results = cur.fetchall()
##    logging.info('there has %s system variable of tenant id %d', len(results), tenant_id)
##    update_sys_var_list = []
##    update_sys_var_ori_list = []
##    add_sys_var_list = []
##    for r in results:
##      if sys_var_dict.has_key(r[0]):
##        sys_var = sys_var_dict[r[0]]
##        if long(sys_var["data_type"]) != long(r[1]) or sys_var["info"].strip() != r[3].strip() or long(sys_var["flags"]) != long(r[4]) or ("min_val" in sys_var.keys() and sys_var["min_val"] != r[5]) or ("max_val" in sys_var.keys() and sys_var["max_val"] != r[6]):
##          update_sys_var_list.append(sys_var)
##          update_sys_var_ori_list.append(r)
##    for (name, sys_var) in sys_var_dict.items():
##      sys_var_exist = 0
##      for r in results:
##        if r[0] == sys_var["name"]:
##          sys_var_exist = 1
##          break
##      if 0 == sys_var_exist:
##        add_sys_var_list.append(sys_var)
##    # reset
##    sys_tenant_id = 1
##    change_tenant(cur, sys_tenant_id)
##    return (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list)
##  except Exception, e:
##    logging.exception('fail to calc diff sys var')
##    raise e
##
##def gen_update_sys_var_sql_for_tenant(tenant_id, sys_var):
##  actual_tenant_id = get_actual_tenant_id(tenant_id)
##  update_sql = 'update oceanbase.__all_sys_variable set data_type = ' + str(sys_var["data_type"])\
##      + ', info = \'' + sys_var["info"].strip() + '\', flags = ' + str(sys_var["flags"])
##  update_sql = update_sql\
##      + ((', min_val = \'' + sys_var["min_val"] + '\'') if "min_val" in sys_var.keys() else '')\
##      + ((', max_val = \'' + sys_var["max_val"] + '\'') if "max_val" in sys_var.keys() else '')
##  update_sql = update_sql + ' where tenant_id = ' + str(actual_tenant_id) + ' and name = \'' + sys_var["name"] + '\''
##  return update_sql
##
##def gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
##  try:
##    actual_tenant_id = get_actual_tenant_id(tenant_id)
##    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
##                                            where tenant_id = {0} and name = '{1}'
##                                            order by schema_version desc limit 1"""
##                                            .format(actual_tenant_id, sys_var["name"]))
##    schema_version = results[0][0]
##    (desc, results) = dml_cur.exec_query("""select value from __all_sys_variable where tenant_id={0} and name='{1}' limit 1"""
##                                         .format(actual_tenant_id, sys_var["name"]))
##    res_len = len(results)
##    if res_len != 1:
##      logging.error('fail to get value from __all_sys_variable, result count:'+ str(res_len))
##      raise MyError('fail to get value from __all_sys_variable')
##    value = results[0][0]
##    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
##    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
##    replace_sql = """replace into oceanbase.__all_sys_variable_history(
##                          tenant_id,
##                          zone,
##                          name,
##                          schema_version,
##                          is_deleted,
##                          data_type,
##                          value,
##                          info,
##                          flags,
##                          min_val,
##                          max_val)
##                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
##                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], value, sys_var["info"], sys_var["flags"], min_val, max_val)
##    return replace_sql
##  except Exception, e:
##    logging.exception('fail to gen replace sys var history sql')
##    raise e
##
##def gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
##  try:
##    actual_tenant_id = get_actual_tenant_id(tenant_id)
##    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
##                                            where tenant_id={0} order by schema_version asc limit 1""".format(actual_tenant_id))
##    schema_version = results[0][0]
##    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
##    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
##    replace_sql = """replace into oceanbase.__all_sys_variable_history(
##                          tenant_id,
##                          zone,
##                          name,
##                          schema_version,
##                          is_deleted,
##                          data_type,
##                          value,
##                          info,
##                          flags,
##                          min_val,
##                          max_val)
##                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
##                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], sys_var["value"], sys_var["info"], sys_var["flags"], min_val, max_val)
##    return replace_sql
##  except Exception, e:
##    logging.exception('fail to gen replace sys var history sql')
##    raise e
##
##
##def gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list):
##  update_sqls = ''
##  for i in range(0, len(update_sys_var_list)):
##    sys_var = update_sys_var_list[i]
##    if i > 0:
##      update_sqls += '\n'
##    update_sqls += gen_update_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
##    update_sqls += gen_update_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
##  return update_sqls
##
##def update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list):
##  try:
##    for i in range(0, len(update_sys_var_list)):
##      sys_var = update_sys_var_list[i]
##      update_sql = gen_update_sys_var_sql_for_tenant(tenant_id, sys_var)
##      rowcount = dml_cur.exec_update(update_sql)
##      if 1 != rowcount:
##        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
##        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
##      else:
##        logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
###replace update sys var to __all_sys_variable_history
##      replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
##      rowcount = dml_cur.exec_update(replace_sql)
##      if 1 != rowcount and 2 != rowcount:
##        logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
##        raise MyError('fail to repalce sysvar')
##      else:
##        logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
##  except Exception, e:
##    logging.exception('fail to update for tenant, tenant_id: %d', tenant_id)
##    raise e
##
##def gen_add_sys_var_sql_for_tenant(tenant_id, sys_var):
##  actual_tenant_id = get_actual_tenant_id(tenant_id)
##  add_sql = 'replace into oceanbase.__all_sys_variable(tenant_id, zone, name, data_type, value, info, flags, min_val, max_val) values('\
##      + str(actual_tenant_id) +', \'\', \'' + sys_var["name"] + '\', ' + str(sys_var["data_type"]) + ', \'' + sys_var["value"] + '\', \''\
##      + sys_var["info"].strip() + '\', ' + str(sys_var["flags"]) + ', \''
##  add_sql = add_sql + (sys_var["min_val"] if "min_val" in sys_var.keys() else '') + '\', \''\
##      + (sys_var["max_val"] if "max_val" in sys_var.keys() else '') + '\')'
##  return add_sql
##
##def gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list):
##  add_sqls = ''
##  for i in range(0, len(add_sys_var_list)):
##    sys_var = add_sys_var_list[i]
##    if i > 0:
##      add_sqls += '\n'
##    add_sqls += gen_add_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
##    add_sqls += gen_replace_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
##  return add_sqls
##
##def add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list):
##  try:
##    for i in range(0, len(add_sys_var_list)):
##      sys_var = add_sys_var_list[i]
##      add_sql = gen_add_sys_var_sql_for_tenant(tenant_id, sys_var)
##      rowcount = dml_cur.exec_update(add_sql)
##      if 1 != rowcount:
##        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
##        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
##      else:
##        logging.info('succeed to insert sys var for tenant, sql: %s, tenant_id: %d', add_sql, tenant_id)
##      replace_sql = gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
##      rowcount = dml_cur.exec_update(replace_sql)
##      if 1 != rowcount:
##        logging.error('fail to replace system variable history, sql:%s'%replace_sql)
##        raise MyError('fail to replace system variable history')
##      else:
##        logging.info('succeed to replace sys var for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
##  except Exception, e:
##    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
##    raise e
##
##
##def gen_sys_var_special_update_sqls_for_tenant(tenant_id):
##  special_update_sqls = ''
##  return special_update_sqls
##
##def special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, sys_var_name, sys_var_value):
##  try:
##    sys_var = None
##    for i in range(0, len(add_sys_var_list)):
##      if (sys_var_name == add_sys_var_list[i]["name"]):
##        sys_var = add_sys_var_list[i]
##        break;
##
##    if None == sys_var:
##      logging.info('%s is not new, no need special update again', sys_var_name)
##      return
##
##    sys_var["value"] = sys_var_value;
##    update_sql = gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var)
##    rowcount = dml_cur.exec_update(update_sql)
##    if 1 != rowcount:
##      # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
##      logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
##    else:
##      logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
##    #replace update sys var to __all_sys_variable_history
##    replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
##    rowcount = dml_cur.exec_update(replace_sql)
##    if 1 != rowcount and 2 != rowcount:
##      logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
##      raise MyError('fail to repalce sysvar')
##    else:
##      logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
##  except Exception, e:
##    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
##    raise e
##
##def get_sys_vars_upgrade_dmls_str(cur, query_cur, tenant_id_list, update_sys_var_list, add_sys_var_list):
##  ret_str = ''
##  if len(tenant_id_list) <= 0:
##    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
##    raise MyError('invalid arg')
##  for i in range(0, len(tenant_id_list)):
##    tenant_id = tenant_id_list[i]
##    change_tenant(cur, tenant_id)
##    if i > 0:
##      ret_str += '\n'
##    ret_str += gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list)
##  if ret_str != '' and len(add_sys_var_list) > 0:
##    ret_str += '\n'
##  for i in range(0, len(tenant_id_list)):
##    tenant_id = tenant_id_list[i]
##    change_tenant(cur, tenant_id)
##    if i > 0:
##      ret_str += '\n'
##    ret_str += gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list)
##  if ret_str != '' and gen_sys_var_special_update_sqls_for_tenant(tenant_id_list[0]) != '':
##    ret_str += '\n'
##  for i in range(0, len(tenant_id_list)):
##    tenant_id = tenant_id_list[i]
##    change_tenant(cur, tenant_id)
##    if i > 0:
##      ret_str += '\n'
##    ret_str += gen_sys_var_special_update_sqls_for_tenant(tenant_id)
##  sys_tenant_id= 1
##  change_tenant(cur, sys_tenant_id)
##  return ret_str
##
##def gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var):
##  update_sql = ('update oceanbase.__all_sys_variable set value = \'' + str(sys_var["value"])
##      + '\' where tenant_id = ' + str(tenant_id) + ' and name = \'' + sys_var["name"] + '\'')
##  return update_sql
##
##def exec_sys_vars_upgrade_dml(cur, tenant_id_list):
##  if len(tenant_id_list) <= 0:
##    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
##    raise MyError('invalid arg')
##  dml_cur = DMLCursor(cur)
##  # 操作前先dump出oceanbase.__all_sys_variable表的所有数据
##  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable""")
##  # 操作前先dump出oceanbase.__all_sys_variable_history表的所有数据
##  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable_history""")
##
##  for i in range(0, len(tenant_id_list)):
##    tenant_id = tenant_id_list[i]
##    # calc diff
##    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
##    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
##    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
##    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
##    # update
##    change_tenant(cur, tenant_id)
##    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
##    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');
##  # reset
##  sys_tenant_id = 1
##  change_tenant(cur, sys_tenant_id)
##
##def exec_sys_vars_upgrade_dml_in_standby_cluster(standby_cluster_infos):
##  try:
##    for standby_cluster_info in standby_cluster_infos:
##      exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info)
##  except Exception, e:
##    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed""")
##    raise e
##
##def exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info):
##  try:
##
##    logging.info("exec_sys_vars_upgrade_dml_by_cluster : cluster_id = {0}, ip = {1}, port = {2}"
##                 .format(standby_cluster_info['cluster_id'],
##                         standby_cluster_info['ip'],
##                         standby_cluster_info['port']))
##    logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
##                 .format(standby_cluster_info['cluster_id'],
##                         standby_cluster_info['ip'],
##                         standby_cluster_info['port']))
##    conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
##                                   password =  standby_cluster_info['pwd'],
##                                   host     =  standby_cluster_info['ip'],
##                                   port     =  standby_cluster_info['port'],
##                                   database =  'oceanbase',
##                                   raise_on_warnings = True)
##    cur = conn.cursor(buffered=True)
##    conn.autocommit = True
##    dml_cur = DMLCursor(cur)
##    query_cur = QueryCursor(cur)
##    is_primary = actions.check_current_cluster_is_primary(conn, query_cur)
##    if is_primary:
##      logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
##                        .format(standby_cluster_info['cluster_id'],
##                                standby_cluster_info['ip'],
##                                standby_cluster_info['port']))
##      raise e
##
##    # only update sys tenant in standby cluster
##    tenant_id = 1
##    # calc diff
##    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
##    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
##    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
##    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
##    # update
##    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
##    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
##    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');
##
##    cur.close()
##    conn.close()
##
##  except Exception, e:
##    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed :
##                         cluster_id = {0}, ip = {1}, port = {2}"""
##                         .format(standby_cluster_info['cluster_id'],
##                                 standby_cluster_info['ip'],
##                                 standby_cluster_info['port']))
##    raise e
##
##
#
#
#import os
#import sys
#import datetime
#from random import Random
#
##def get_actual_tenant_id(tenant_id):
##  return tenant_id if (1 == tenant_id) else 0;
#
#def change_tenant(cur, tenant_id):
#  # change tenant
#  sql = "alter system change tenant tenant_id = {0};".format(tenant_id)
#  logging.info(sql);
#  cur.execute(sql);
#  # check
#  sql = "select effective_tenant_id();"
#  cur.execute(sql)
#  result = cur.fetchall()
#  if (1 != len(result) or 1 != len(result[0])):
#    raise MyError("invalid result cnt")
#  elif (tenant_id != result[0][0]):
#    raise MyError("effective_tenant_id:{0} , tenant_id:{1}".format(result[0][0], tenant_id))
#
#
#def fetch_tenant_ids(query_cur):
#  try:
#    tenant_id_list = []
#    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant where compatibility_mode=1 order by tenant_id desc""")
#    for r in results:
#      tenant_id_list.append(r[0])
#    return tenant_id_list
#  except Exception, e:
#    logging.exception('fail to fetch distinct tenant ids')
#    raise e
#
#
#class SplitError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
#
#def random_str(rand_str_len = 8):
#  str = ''
#  chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
#  length = len(chars) - 1
#  random = Random()
#  for i in range(rand_str_len):
#    str += chars[random.randint(0, length)]
#  return str
#
#def split_py_files(sub_files_dir):
#  char_enter = '\n'
#  file_splitter_line = '####====XXXX======######==== I am a splitter ====######======XXXX====####'
#  sub_filename_line_prefix = '#filename:'
#  sub_file_module_end_line = '#sub file module end'
#  os.makedirs(sub_files_dir)
#  print('succeed to create run dir: ' + sub_files_dir + char_enter)
#  cur_file = open(sys.argv[0], 'r')
#  cur_file_lines = cur_file.readlines()
#  cur_file_lines_count = len(cur_file_lines)
#  sub_file_lines = []
#  sub_filename = ''
#  begin_read_sub_py_file = False
#  is_first_splitter_line = True
#  i = 0
#  while i < cur_file_lines_count:
#    if (file_splitter_line + char_enter) != cur_file_lines[i]:
#      if begin_read_sub_py_file:
#        sub_file_lines.append(cur_file_lines[i])
#    else:
#      if is_first_splitter_line:
#        is_first_splitter_line = False
#      else:
#        #读完一个子文件了，写到磁盘中
#        sub_file = open(sub_files_dir + '/' + sub_filename, 'w')
#        for sub_file_line in sub_file_lines:
#          sub_file.write(sub_file_line[1:])
#        sub_file.close()
#        #清空sub_file_lines
#        sub_file_lines = []
#      #再读取下一行的文件名或者结束标记
#      i += 1
#      if i >= cur_file_lines_count:
#        raise SplitError('invalid line index:' + str(i) + ', lines_count:' + str(cur_file_lines_count))
#      elif (sub_file_module_end_line + char_enter) == cur_file_lines[i]:
#        print 'succeed to split all sub py files'
#        break
#      else:
#        mark_idx = cur_file_lines[i].find(sub_filename_line_prefix)
#        if 0 != mark_idx:
#          raise SplitError('invalid sub file name line, mark_idx = ' + str(mark_idx) + ', line = ' + cur_file_lines[i])
#        else:
#          sub_filename = cur_file_lines[i][len(sub_filename_line_prefix):-1]
#          begin_read_sub_py_file = True
#    i += 1
#  cur_file.close()
#
#class UpgradeParams:
#  log_filename = config.post_upgrade_log_filename
#  sql_dump_filename = config.post_upgrade_sql_filename
#  rollback_sql_filename =  config.post_upgrade_rollback_sql_filename
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
#def fetch_user_names(query_cur):
#  try:
#    user_name_list = []
#    (desc, results) = query_cur.exec_query("""select user_name from oceanbase.__all_user where type=0""")
#    for r in results:
#      user_name_list.append(r[0])
#    return user_name_list
#  except Exception, e:
#    logging.exception('fail to fetch user names')
#    raise e
#
#def get_priv_info(m_p):
#  priv = []
#  priv_str = ''
#  obj_name=''
#  words = m_p.split(" ")
#  # build priv list
#  i = 1
#  while words[i].upper() != 'ON':
#    if (words[i].upper() == 'ALL'):
#      priv.append('ALL')
#    else:
#      if (words[i].upper() != 'PRIVILEGES'):
#        priv.append(words[i])
#    i= i+1
#  # Jump 'ON'
#  i=i+1
#  #print words
#  if (words[i] == '*.*'):
#    priv_level = 'USER'
#  else:
#    ind = string.find(words[i], '.*')
#    if (ind != -1):
#      priv_level = 'DB'
#      obj_name = words[i][0: ind]
#    else:
#      priv_level = 'OBJ'
#      obj_name = words[i]
#  #jump 'TO'
#  i = i + 2
#  user_name = words[i]
#
#  return (priv_level, user_name, priv, obj_name)
#
#def owner_priv_to_str(priv):
#  priv_str = '';
#  i = 0
#  while i <  len(priv):
#    if (string.find(priv[i].uppper(), 'CREATE') == -1):
#      if (priv[i].upper() == 'SHOW VIEW'):
#        priv_str = 'SELECT ,'
#      else:
#        priv_str = priv[i] + ' ,'
#    i = i + 1
#  priv_str = priv_str[0: len(priv_str) -1]
#  return priv_str
#
#def owner_db_priv_to_str(priv):
#  priv_str = ''
#  i = 0
#  while i <  len(priv):
#    if (string.find(priv[i].uppper(), 'CREATE') != -1):
#      priv_str = priv[i] + ' ,'
#    i = i + 1
#  priv_str = priv_str[0: len(priv_str) -1]
#  return priv_str
#
#def other_db_priv_to_str(priv):
#  priv_str = ''
#  i = 0
#  while i <  len(priv):
#    if (string.find(priv[i].uppper(), 'CREATE') != -1):
#      priv_str = 'CREATE TABLE ,'
#    elif (string.find(priv[i].upper(), 'CREATE VIEW') != -1):
#      priv_str = 'CREATE VIEW ,'
#    i = i + 1
#  priv_str = priv_str[0: len(priv_str) -1]
#  return priv_str
#
#def user_priv_to_str(priv):
#  priv_str = ''
#  i = 0
#  while i <  len(priv):
#    if (string.find(priv[i].upper(), 'SUPER') != -1):
#      priv_str = 'ALTER SYSTEM ,'
#    elif (string.find(priv[i].upper(), 'CREATE USER') != -1):
#      priv_str = 'CREATE USER, ALTER USER, DROP USER,'
#    elif (string.find(priv[i].upper(), 'PROCESS') != -1):
#      priv_str = 'SHOW PROCESS,'
#    i = i + 1
#  priv_str = priv_str[0: len(priv_str) -1]
#
#owner_db_all_privs = 'CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE '
#
#other_db_all_privs = 'create any table, create any view, create any  procedure,\
#create any synonym, create any sequence, create any trigger, create any type,\
#alter any table,  alter any procedure, alter any sequence,\
#alter any trigger, alter any type, drop any table, drop any view,\
#drop any procedure, drop any synonym, drop any sequence, drop any trigger,\
#drop any type, select any table, insert any table, update any table, delete any table,\
#flashback any table, create any index, alter any index, drop any index,\
#execute any procedure, create public synonym, drop public synonym,\
#select any sequence, execute any type, create tablespace,\
#alter tablespace, drop tablespace '
#
#other_obj_all_privs = 'alter, index, select, insert, update, delete '
#
#def map_priv(m_p):
#   (priv_level, user_name, priv, obj_name)  = get_priv_info(m_p)
#   #print priv_level, user_name, priv, obj_name
#   if priv_level == 'DB':
#     if user_name == obj_name:
#       if priv[0] == 'ALL':
#         return 'GRANT ' + owner_db_all_privs + ' TO ' + user_name
#       else:
#         priv_str = owner_db_priv_to_str(priv)
#         return 'GRANT ' +  priv_str + ' TO ' + user_name
#     else:
#       if priv[0] == 'ALL':
#         return 'GRANT ' + other_db_all_privs + ' TO ' + user_name
#       else:
#         priv_str = other_db_priv_to_str(priv)
#         if (priv_str != ''):
#           return 'GRANT ' + priv_str + 'TO ' + user_name
#         else:
#           return '';
#   else:
#     if priv_level == 'USER':
#       if priv[0] == 'ALL':
#         return 'GRANT DBA TO ' + user_name
#       else:
#         priv_str = user_priv_to_str(priv)
#         return 'GRANT ' + priv_set + 'TO ' + user_name
#     else:
#       if user_name == obj_name:
#         return ''
#       else:
#         if priv[0] == 'ALL':
#           return 'GRANT ' + other_obj_all_privs + ' TO ' + user_name
#         else:
#           priv_str = other_priv_to_str(priv)
#           return 'GRANT ' + priv_str + 'ON ' + obj_name + 'TO ' + user_name
#
###################################################################
## check的条件：
## 1. 存在库级grant并且用户名和库名不一致，并且用户不是SYS, 则check不通过
## 
##
##
###################################################################
#def check_grant_sql(grant_sql):
#  (priv_level, user_name, priv, obj_name)  = get_priv_info(grant_sql)
#  if priv_level == 'DB':
#     if user_name != obj_name and user_name.upper() != 'SYS' and user_name.upper() != '\'SYS\'':
#       return False
#  return True
#
#def fetch_user_privs(query_cur, user_name):
#  try:
#    grant_sql_list = []
#    (desc, results) = query_cur.exec_query("""show grants for """ + user_name)
#    for r in results:
#      grant_sql_list.append(r[0])
#    return grant_sql_list
#  except Exception, e:
#    logging.exception('fail to fetch user privs')
#    raise e
#
#def check_is_dcl_sql(sql):
#  word_list = sql.split()
#  if len(word_list) < 1:
#    raise MyError('sql is empty, sql="{0}"'.format(sql))
#  key_word = word_list[0].lower()
#  if 'grant' != key_word and 'revoke' != key_word:
#    raise MyError('sql must be dcl, key_word="{0}", sql="{1}"'.format(key_word, sql))
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
#class DCLCursor:
#  _cursor = None
#  def __init__(self, cursor):
#    self._cursor = Cursor(cursor)
#  def exec_dcl(self, sql, print_when_succ = True):
#    try:
#      # 这里检查是不是ddl，不是ddl就抛错
#      check_is_dcl_sql(sql)
#      return self._cursor.exec_sql(sql, print_when_succ)
#    except Exception, e:
#      logging.exception('fail to execute dcl: %s', sql)
#      raise e
#
#def do_priv_check_by_tenant_id(cur, tenant_id_list):
#  if len(tenant_id_list) <= 0:
#    logging.info('This is no oraclemode tenant in cluster')
#  
#  simple_priv = True
#
#  query_cur = actions.QueryCursor(cur)
#  dcl_cur   = DCLCursor(cur)
#  for i in range(len(tenant_id_list)): 
#    if simple_priv == False:
#      break
#    else:
#      tenant_id = tenant_id_list[i]
#      change_tenant(cur, tenant_id)
#      user_name_list = fetch_user_names(query_cur)
#      for j in range(len(user_name_list)):
#        if simple_priv == False:
#          break
#        else:
#          user_name = user_name_list[j]
#          grant_sql_list = fetch_user_privs(query_cur, user_name)
#          for k in range(len(grant_sql_list)):
#            grant_sql = grant_sql_list[k]
#            is_ok = check_grant_sql(grant_sql)
#            if (False == is_ok):
#              simple_priv = False
#              logging.info("--------------------------------------------------------------------")
#              logging.info("--------------------------------------------------------------------")
#              logging.warning('Database privs exists:' + grant_sql)
#              logging.info("--------------------------------------------------------------------")
#              logging.info("--------------------------------------------------------------------")
#              break
#              
#  return simple_priv
#
#def do_priv_check(my_host, my_port, my_user, my_passwd, upgrade_params):
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
#      # 获取租户id列表
#      tenant_id_list = fetch_tenant_ids(query_cur)
#      if len(tenant_id_list) <= 0:
#        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
#        raise MyError('no tenant id')
#      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
#      conn.commit()
#
#      simple_priv = do_priv_check_by_tenant_id(cur, tenant_id_list)
#      conn.commit()
#
#      if simple_priv == True:
#        logging.info("****************************************************************************")
#        logging.info("****************************************************************************")
#        logging.info("")
#        logging.info("No Database Privs Exists.")
#        logging.info(tenant_id_list)
#        logging.info("")
#        logging.info("****************************************************************************")
#        logging.info("****************************************************************************")
#      
#      # reset
#      sys_tenant_id = 1
#      change_tenant(cur, sys_tenant_id)
#
#    except Exception, e:
#      logging.exception('run error')
#      raise e
#    finally:
#      # 将回滚sql写到文件中
#      cur.close()
#      conn.close()
#  except mysql.connector.Error, e:
#    logging.exception('connection error')
#    raise e
#  except Exception, e:
#    logging.exception('normal error')
#    raise e
#
#def do_priv_check_by_argv(argv):
#  upgrade_params = UpgradeParams()
#  opts.change_opt_defult_value('log-file', upgrade_params.log_filename)
#  opts.parse_options(argv)
#  if not opts.has_no_local_opts():
#    opts.deal_with_local_opts()
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
#      
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, log_filename)
#      do_priv_check(host, port, user, password, upgrade_params)
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
#if __name__ == '__main__':
#  #cur_filename = sys.argv[0][sys.argv[0].rfind(os.sep)+1:]
#  #(cur_file_short_name,cur_file_ext_name1) = os.path.splitext(sys.argv[0])
#  #(cur_file_real_name,cur_file_ext_name2) = os.path.splitext(cur_filename)
#  #sub_files_dir_suffix = '_extract_files_' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '_' + random_str()
#  #sub_files_dir = cur_file_short_name + sub_files_dir_suffix
#  #sub_files_short_dir = cur_file_real_name + sub_files_dir_suffix
#  #split_py_files(sub_files_dir)
#  #print sub_files_dir, sub_files_short_dir
#  #print sys.argv[1:]
#  do_priv_check_by_argv(sys.argv[1:])
#  #exec('from ' + sub_files_short_dir + '.do_upgrade_post import do_upgrade_by_argv')
#  #do_upgrade_by_argv(sys.argv[1:])
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
#MODULE_DDL = 'ddl'
#MODULE_NORMAL_DML = 'normal_dml'
#MODULE_EACH_TENANT_DML = 'each_tenant_dml'
#MODULE_EACH_TENANT_DDL = 'each_tenant_ddl'
#MODULE_SYSTEM_VARIABLE_DML = 'system_variable_dml'
#MODULE_SPECIAL_ACTION = 'special_action'
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
#filename:special_upgrade_action_post.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import logging
#import time
#from actions import Cursor
#from actions import DMLCursor
#from actions import QueryCursor
#from actions import check_current_cluster_is_primary
#import mysql.connector
#from mysql.connector import errorcode
#import actions
#
#def do_special_upgrade(conn, cur, tenant_id_list, user, pwd):
#  # special upgrade action
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
#####========******####======== actions begin ========####******========####
#  run_upgrade_job(conn, cur, "3.1.4")
#  return
#####========******####========= actions end =========####******========####
#
#def trigger_schema_split_job(conn, cur, user, pwd):
#  try:
#    query_cur = actions.QueryCursor(cur)
#    is_primary = actions.check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.warn("current cluster should by primary")
#      raise e
#
#    # primary cluster
#    trigger_schema_split_job_by_cluster(conn, cur)
#
#    # stanby cluster
#    standby_cluster_list = actions.fetch_standby_cluster_infos(conn, query_cur, user, pwd)
#    for standby_cluster in standby_cluster_list:
#      # connect
#      logging.info("start to trigger schema split by cluster: cluster_id = {0}"
#                   .format(standby_cluster['cluster_id']))
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster['cluster_id'],
#                           standby_cluster['ip'],
#                           standby_cluster['port']))
#      tmp_conn = mysql.connector.connect(user     =  standby_cluster['user'],
#                                         password =  standby_cluster['pwd'],
#                                         host     =  standby_cluster['ip'],
#                                         port     =  standby_cluster['port'],
#                                         database =  'oceanbase')
#      tmp_cur = tmp_conn.cursor(buffered=True)
#      tmp_conn.autocommit = True
#      tmp_query_cur = actions.QueryCursor(tmp_cur)
#      # check if stanby cluster
#      is_primary = actions.check_current_cluster_is_primary(tmp_query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster['cluster_id'],
#                                  standby_cluster['ip'],
#                                  standby_cluster['port']))
#        raise e
#      # trigger schema split
#      trigger_schema_split_job_by_cluster(tmp_conn, tmp_cur)
#      # close
#      tmp_cur.close()
#      tmp_conn.close()
#      logging.info("""trigger schema split success : cluster_id = {0}, ip = {1}, port = {2}"""
#                      .format(standby_cluster['cluster_id'],
#                              standby_cluster['ip'],
#                              standby_cluster['port']))
#
#  except Exception, e:
#    logging.warn("trigger schema split failed")
#    raise e
#  logging.info("trigger schema split success")
#
#def trigger_schema_split_job_by_cluster(conn, cur):
#  try:
#    # check record in rs_job
#    sql = "select count(*) from oceanbase.__all_rootservice_job where job_type = 'SCHEMA_SPLIT_V2';"
#    logging.info(sql)
#    cur.execute(sql)
#    result = cur.fetchall()
#    if 1 != len(result) or 1 != len(result[0]):
#      logging.warn("unexpected result cnt")
#      raise e
#    elif 0 == result[0][0]:
#      # insert fail record to start job
#      sql = "replace into oceanbase.__all_rootservice_job(job_id, job_type, job_status, progress, rs_svr_ip, rs_svr_port) values (0, 'SCHEMA_SPLIT_V2', 'FAILED', 100, '0.0.0.0', '0');"
#      logging.info(sql)
#      cur.execute(sql)
#      # check record in rs_job
#      sql = "select count(*) from oceanbase.__all_rootservice_job where job_type = 'SCHEMA_SPLIT_V2' and job_status = 'FAILED';"
#      logging.info(sql)
#      cur.execute(sql)
#      result = cur.fetchall()
#      if 1 != len(result) or 1 != len(result[0]) or 1 != result[0][0]:
#        logging.warn("schema split record should be 1")
#        raise e
#
#  except Exception, e:
#    logging.warn("start schema split task failed")
#    raise e
#  logging.info("start schema split task success")
#
#def query(cur, sql):
#  cur.execute(sql)
#  results = cur.fetchall()
#  return results
#
#def get_tenant_names(cur):
#  return [_[0] for _ in query(cur, 'select tenant_name from oceanbase.__all_tenant')]
#
#def update_cluster_update_table_schema_version(conn, cur):
#  time.sleep(30)
#  try:
#    query_timeout_sql = "set ob_query_timeout = 30000000;"
#    logging.info(query_timeout_sql)
#    cur.execute(query_timeout_sql)
#    sql = "alter system run job 'UPDATE_TABLE_SCHEMA_VERSION';"
#    logging.info(sql)
#    cur.execute(sql)
#  except Exception, e:
#    logging.warn("run update table schema version job failed")
#    raise e
#  logging.info("run update table schema version job success")
#
#
#def run_create_inner_schema_job(conn, cur):
#  try:
#    ###### enable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#
#    # check record in rs_job
#    count_sql = """select count(*) from oceanbase.__all_rootservice_job
#            where job_type = 'CREATE_INNER_SCHEMA';"""
#    result = query(cur, count_sql)
#    job_count = 0
#    if (1 != len(result)) :
#      logging.warn("unexpected sql output")
#      raise e
#    else :
#      job_count = result[0][0]
#      # run job
#      sql = "alter system run job 'CREATE_INNER_SCHEMA';"
#      logging.info(sql)
#      cur.execute(sql)
#
#      # wait job finish
#      times = 180
#      ## 先检查job count变化
#      count_check = False
#      while times > 0:
#        result = query(cur, count_sql)
#        if (1 != len(result)):
#          logging.warn("unexpected sql output")
#          raise e
#        elif (result[0][0] > job_count):
#          count_check = True
#          logging.info('create_inner_schema job detected')
#          break
#        time.sleep(10)
#        times -= 1
#        if times == 0:
#          raise MyError('check create_inner_schema job failed!')
#
#      ## 继续检查job status状态
#      status_sql = """select job_status from oceanbase.__all_rootservice_job
#                      where job_type = 'CREATE_INNER_SCHEMA' order by job_id desc limit 1;"""
#      status_check = False
#      while times > 0 and count_check == True:
#        result = query(cur, status_sql)
#        if (0 == len(result)):
#          logging.warn("unexpected sql output")
#          raise e
#        elif (1 != len(result) or 1 != len(result[0])):
#          logging.warn("result len not match")
#          raise e
#        elif result[0][0] == "FAILED":
#          logging.warn("run create_inner_schema job faild")
#          raise e
#        elif result[0][0] == "INPROGRESS":
#          logging.info('create_inner_schema job is still running')
#        elif result[0][0] == "SUCCESS":
#          status_check = True
#          break;
#        else:
#          logging.warn("invalid result: {0}" % (result[0][0]))
#          raise e
#        time.sleep(10)
#        times -= 1
#        if times == 0:
#          raise MyError('check create_inner_schema job failed!')
#
#      if (status_check == True and count_check == True):
#        logging.info('check create_inner_schema job success')
#      else:
#        logging.warn("run create_inner_schema job faild")
#        raise e
#
#    # disable ddl
#    if ori_enable_ddl == 0:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#  except Exception, e:
#    logging.warn("run create_inner_schema job failed")
#    raise e
#  logging.info("run create_inner_schema job success")
#
#def statistic_primary_zone_count(conn, cur):
#  try:
#    ###### disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#
#    # check record in rs_job
#    count_sql = """select count(*) from oceanbase.__all_rootservice_job
#            where job_type = 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT';"""
#    result = query(cur, count_sql)
#    job_count = 0
#    if (1 != len(result)) :
#      logging.warn("unexpected sql output")
#      raise e
#    else :
#      job_count = result[0][0]
#      # run job
#      sql = "alter system run job 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT';"
#      logging.info(sql)
#      cur.execute(sql)
#      # wait job finish
#      times = 180
#      ## 先检查job count变化
#      count_check = False
#      while times > 0:
#        result = query(cur, count_sql)
#        if (1 != len(result)):
#          logging.warn("unexpected sql output")
#          raise e
#        elif (result[0][0] > job_count):
#          count_check = True
#          logging.info('statistic_primary_zone_entity_count job detected')
#          break
#        time.sleep(10)
#        times -= 1
#        if times == 0:
#          raise MyError('statistic_primary_zone_entity_count job failed!')
#
#      ## 继续检查job status状态
#      status_sql = """select job_status from oceanbase.__all_rootservice_job
#                      where job_type = 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT' order by job_id desc limit 1;"""
#      status_check = False
#      while times > 0 and count_check == True:
#        result = query(cur, status_sql)
#        if (0 == len(result)):
#          logging.warn("unexpected sql output")
#          raise e
#        elif (1 != len(result) or 1 != len(result[0])):
#          logging.warn("result len not match")
#          raise e
#        elif result[0][0] == "FAILED":
#          logging.warn("run statistic_primary_zone_entity_count job faild")
#          raise e
#        elif result[0][0] == "INPROGRESS":
#          logging.info('statistic_primary_zone_entity_count job is still running')
#        elif result[0][0] == "SUCCESS":
#          status_check = True
#          break;
#        else:
#          logging.warn("invalid result: {0}" % (result[0][0]))
#          raise e
#        time.sleep(10)
#        times -= 1
#        if times == 0:
#          raise MyError('check statistic_primary_zone_entity_count job failed!')
#
#      if (status_check == True and count_check == True):
#        logging.info('check statistic_primary_zone_entity_count job success')
#      else:
#        logging.warn("run statistic_primary_zone_entity_count job faild")
#        raise e
#
#    # enable ddl
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("run statistic_primary_zone_entity_count job failed")
#    raise e
#  logging.info("run statistic_primary_zone_entity_count job success")
#
#def disable_major_freeze(conn, cur):
#  try:
#    actions.set_parameter(cur, "enable_major_freeze", 'False')
#  except Exception, e:
#    logging.warn("disable enable_major_freeze failed")
#    raise e
#  logging.info("disable enable_major_freeze finish")
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
#def check_can_run_upgrade_job(cur, version):
#  try:
#    sql = """select job_status from oceanbase.__all_rootservice_job
#             where job_type = 'RUN_UPGRADE_POST_JOB' and extra_info = '{0}'
#             order by job_id desc limit 1""".format(version)
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
#      bret = False
#      logging.info("execute upgrade job successfully, skip run upgrade job")
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
#
#def check_upgrade_job_result(cur, version, max_used_job_id):
#  try:
#    times = 0
#    while (times <= 180):
#      sql = """select job_status from oceanbase.__all_rootservice_job
#               where job_type = 'RUN_UPGRADE_POST_JOB' and extra_info = '{0}'
#               and job_id > {1} order by job_id desc limit 1""".format(version, max_used_job_id)
#      results = query(cur, sql)
#
#      if (len(results) == 0):
#        logging.info("upgrade job not created yet")
#      elif (len(results) != 1 or len(results[0]) != 1):
#        logging.warn("row cnt not match")
#        raise e
#      elif ("INPROGRESS" == results[0][0]):
#        logging.info("upgrade job is still running")
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
#      times = times + 1
#      time.sleep(10)
#  except Exception, e:
#    logging.warn("failed to check upgrade job result")
#    raise e
#
#
#def run_upgrade_job(conn, cur, version):
#  try:
#    logging.info("start to run upgrade job, version:{0}".format(version))
#    # pre check
#    if (check_can_run_upgrade_job(cur, version) == True):
#      conn.autocommit = True
#      # disable enable_ddl
#      ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#      if ori_enable_ddl == 1:
#        actions.set_parameter(cur, 'enable_ddl', 'False')
#      # get max_used_job_id
#      max_used_job_id = get_max_used_job_id(cur)
#      # run upgrade job
#      sql = """alter system run upgrade job '{0}'""".format(version)
#      logging.info(sql)
#      cur.execute(sql)
#      # check upgrade job result
#      check_upgrade_job_result(cur, version, max_used_job_id)
#      # reset enable_ddl
#      if ori_enable_ddl == 1:
#        actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("run upgrade job failed, version:{0}".format(version))
#    raise e
#  logging.info("run upgrade job success, version:{0}".format(version))
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
#from actions import check_current_cluster_is_primary
#import binascii
#import my_utils
#import actions
#import sys
#
##def modify_schema_history(conn, cur, tenant_ids):
##  try:
##    conn.autocommit = True
##
##    # disable ddl
##    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
##    if ori_enable_ddl == 1:
##      actions.set_parameter(cur, 'enable_ddl', 'False')
##    log('tenant_ids: {0}'.format(tenant_ids))
##    for tenant_id in tenant_ids:
##      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
##      log(sql)
##      cur.execute(sql)
#
##    #####implement#####
##
##    # 还原默认 tenant
##    sys_tenant_id = 1
##    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
##    log(sql)
##    cur.execute(sql)
##
##    # enable ddl
##    if ori_enable_ddl == 1:
##      actions.set_parameter(cur, 'enable_ddl', 'True')
##  except Exception, e:
##    logging.warn("exec modify trigger failed")
##    raise e
##  logging.info('exec modify trigger finish')
#
#
## 主库需要执行的升级动作
#def do_special_upgrade(conn, cur, tenant_id_list, user, passwd):
#  # special upgrade action
##升级语句对应的action要写在下面的actions begin和actions end这两行之间，
##因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
##这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
## 主库升级流程没加滚动升级步骤，或混部阶段DDL测试有相关case覆盖前，混部开始禁DDL
#  actions.set_parameter(cur, 'enable_ddl', 'False')
#####========******####======== actions begin ========####******========####
#  return
#####========******####========= actions end =========####******========####
#
#def do_add_recovery_status_to_all_zone(conn, cur):
#  try:
#    logging.info('add recovery status row to __all_zone for each zone')
#    zones = [];
#    recovery_status = [];
#
#    # pre-check, may skip
#    check_updated_sql = "select * from oceanbase.__all_zone where zone !='' AND name='recovery_status'"
#    cur.execute(check_updated_sql)
#    recovery_status = cur.fetchall()
#    if 0 < len(recovery_status):
#      logging.info('[recovery_status] row already exists, no need to add')
#
#    # get zones
#    if 0 >= len(recovery_status):
#      all_zone_sql = "select distinct(zone) zone from oceanbase.__all_zone where zone !=''"
#      cur.execute(all_zone_sql)
#      zone_results = cur.fetchall()
#      for r in zone_results:
#        zones.append("('" + r[0] + "', 'recovery_status', 0, 'NORMAL')")
#
#    # add rows
#    if 0 < len(zones):
#      upgrade_sql = "insert into oceanbase.__all_zone(zone, name, value, info) values " + ','.join(zones)
#      logging.info(upgrade_sql)
#      cur.execute(upgrade_sql)
#      conn.commit()
#
#    # check result
#    if 0 < len(zones):
#      cur.execute(check_updated_sql)
#      check_results = cur.fetchall()
#      if len(check_results) != len(zones):
#        raise MyError('fail insert [recovery_status] row into __all_zone')
#
#  except Exception, e:
#    logging.exception('do_add_recovery_status_to_all_zone error')
#    raise e
#
#def do_add_storage_type_to_all_zone(conn, cur):
#  try:
#    logging.info('add storage type row to __all_zone for each zone')
#    zones = [];
#    storage_types = [];
#
#    # pre-check, may skip
#    check_updated_sql = "select * from oceanbase.__all_zone where zone !='' AND name='storage_type'"
#    cur.execute(check_updated_sql)
#    storage_types = cur.fetchall()
#    if 0 < len(storage_types):
#      logging.info('[storage_types] row already exists, no need to add')
#
#    # get zones
#    if 0 >= len(storage_types):
#      all_zone_sql = "select distinct(zone) zone from oceanbase.__all_zone where zone !=''"
#      cur.execute(all_zone_sql)
#      zone_results = cur.fetchall()
#      for r in zone_results:
#        zones.append("('" + r[0] + "', 'storage_type', 0, 'LOCAL')")
#
#    # add rows
#    if 0 < len(zones):
#      upgrade_sql = "insert into oceanbase.__all_zone(zone, name, value, info) values " + ','.join(zones)
#      logging.info(upgrade_sql)
#      cur.execute(upgrade_sql)
#      conn.commit()
#
#    # check result
#    if 0 < len(zones):
#      cur.execute(check_updated_sql)
#      check_results = cur.fetchall()
#      if len(check_results) != len(zones):
#        raise MyError('fail insert [storage_type] row into __all_zone')
#
#  except Exception, e:
#    logging.exception('do_add_storage_type_to_all_zone error')
#    raise e
#
#def modify_trigger(conn, cur, tenant_ids):
#  try:
#    conn.autocommit = True
#    # disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#    log('tenant_ids: {0}'.format(tenant_ids))
#    for tenant_id in tenant_ids:
#      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
#      log(sql)
#      cur.execute(sql)
#      #####implement#####
#      trigger_sql = """
#update __all_tenant_trigger
#set
#package_spec_source = replace(
#package_spec_source,
#'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL\;',
#'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL\;'
#),
#package_body_source = replace(replace(
#package_body_source,
#'
#PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
#BEGIN
#  NULL\;
#END\;
#',
#'
#PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
#BEGIN
#  update_columns_ := STRINGARRAY()\;
#  update_columns_.EXTEND(update_columns.COUNT)\;
#  FOR i IN 1 .. update_columns.COUNT LOOP
#    update_columns_(i) := update_columns(i)\;
#  END LOOP\;
#END\;
#'),
#'
#FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS
#BEGIN
#  RETURN (dml_event_ = 4)\;
#END\;
#',
#'
#FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS
#  is_updating BOOL\;
#BEGIN
#  is_updating := (dml_event_ = 4)\;
#  IF (is_updating AND column_name IS NOT NULL) THEN
#    is_updating := FALSE\;
#    FOR i IN 1 .. update_columns_.COUNT LOOP
#      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE\; EXIT\; END IF\;
#    END LOOP\;
#  END IF\;
#  RETURN is_updating\;
#END\;
#'); """
#
#      log(trigger_sql)
#      cur.execute(trigger_sql)
#      log("update rows = " + str(cur.rowcount))
#
#      trigger_history_sql = """
#update __all_tenant_trigger_history
#set
#package_spec_source = replace(
#package_spec_source,
#'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL\;',
#'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL\;'
#),
#package_body_source = replace(replace(
#package_body_source,
#'
#PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
#BEGIN
#  NULL\;
#END\;
#',
#'
#PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
#BEGIN
#  update_columns_ := STRINGARRAY()\;
#  update_columns_.EXTEND(update_columns.COUNT)\;
#  FOR i IN 1 .. update_columns.COUNT LOOP
#    update_columns_(i) := update_columns(i)\;
#  END LOOP\;
#END\;
#'),
#'
#FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS
#BEGIN
#  RETURN (dml_event_ = 4)\;
#END\;
#',
#'
#FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS
#  is_updating BOOL\;
#BEGIN
#  is_updating := (dml_event_ = 4)\;
#  IF (is_updating AND column_name IS NOT NULL) THEN
#    is_updating := FALSE\;
#    FOR i IN 1 .. update_columns_.COUNT LOOP
#      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE\; EXIT\; END IF\;
#    END LOOP\;
#  END IF\;
#  RETURN is_updating\;
#END\;
#')
#where is_deleted = 0; """
#
#      log(trigger_history_sql)
#      cur.execute(trigger_history_sql)
#      log("update rows = " + str(cur.rowcount))
#
#      #####implement end#####
#
#    # 还原默认 tenant
#    sys_tenant_id = 1
#    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
#    log(sql)
#    cur.execute(sql)
#
#    # enable ddl
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("exec modify trigger failed")
#    raise e
#  logging.info('exec modify trigger finish')
#
#def fill_priv_file_column_for_all_user(conn, cur):
#  try:
#    conn.autocommit = True
#    # disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#    tenant_ids = get_tenant_ids(cur)
#    log('tenant_ids: {0}'.format(tenant_ids))
#    for tenant_id in tenant_ids:
#      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
#      log(sql)
#      cur.execute(sql)
#      tenant_id_in_sql = 0
#      if 1 == tenant_id:
#        tenant_id_in_sql = 1
#
#      begin_user_id = 0
#      begin_schema_version = 0
#      fetch_num = 1000
#
#      while (True):
#        query_limit = """
#                  where tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
#                  order by tenant_id, user_id, schema_version
#                  limit {3}""".format(tenant_id_in_sql, begin_user_id, begin_schema_version, fetch_num)
#
#        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ user_id, schema_version
#                 from oceanbase.__all_user_history""" + query_limit
#        log(sql)
#        result_rows = query(cur, sql)
#        log("select rows = " + str(cur.rowcount))
#
#        if len(result_rows) <= 0:
#          break
#        else:
#          last_schema_version = result_rows[-1][1]
#          last_user_id = result_rows[-1][0]
#          condition = """
#                  where priv_alter = 1 and priv_create = 1 and priv_create_user = 1 and priv_delete = 1
#                         and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1
#                         and priv_index = 1 and priv_create_view = 1 and priv_show_view = 1 and priv_show_db = 1
#                         and priv_super = 1 and priv_create_synonym = 1
#                         and tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
#                         and (user_id, schema_version) <= ({3}, {4})
#                  """.format(tenant_id_in_sql, begin_user_id, begin_schema_version, last_user_id, last_schema_version)
#
#          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user_history
#                   set priv_file = 1""" + condition
#          log(sql)
#          cur.execute(sql)
#          log("update rows = " + str(cur.rowcount))
#
#          condition = """
#                  where priv_super = 1 and tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
#                         and (user_id, schema_version) <= ({3}, {4})
#                  """.format(tenant_id_in_sql, begin_user_id, begin_schema_version, last_user_id, last_schema_version)
#
#          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user_history
#                   set priv_alter_tenant = 1,
#                       priv_alter_system = 1,
#                       priv_create_resource_unit = 1,
#                       priv_create_resource_pool = 1 """ + condition
#          log(sql)
#          cur.execute(sql)
#
#          begin_schema_version = last_schema_version
#          begin_user_id = last_user_id
#
#      begin_user_id = 0
#      while (True):
#        query_limit = """
#                   where tenant_id = {0} and user_id > {1}
#                   order by tenant_id, user_id
#                   limit {2}""".format(tenant_id_in_sql, begin_user_id, fetch_num)
#        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ user_id
#                 from oceanbase.__all_user""" + query_limit
#        log(sql)
#        result_rows = query(cur, sql)
#        log("select rows = " + str(cur.rowcount))
#
#        if len(result_rows) <= 0:
#          break
#        else:
#          end_user_id = result_rows[-1][0]
#          condition = """
#                   where priv_alter = 1 and priv_create = 1 and priv_create_user = 1 and priv_delete = 1
#                         and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1
#                         and priv_index = 1 and priv_create_view = 1 and priv_show_view = 1 and priv_show_db = 1
#                         and priv_super = 1 and priv_create_synonym = 1
#                         and tenant_id = {0} and user_id > {1} and user_id <= {2}
#                  """.format(tenant_id_in_sql, begin_user_id, end_user_id)
#          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user
#                   set priv_file = 1 """ + condition
#          log(sql)
#          cur.execute(sql)
#          log("update rows = " + str(cur.rowcount))
#
#          condition = """
#                   where priv_super = 1
#                         and tenant_id = {0} and user_id > {1} and user_id <= {2}
#                  """.format(tenant_id_in_sql, begin_user_id, end_user_id)
#          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user
#                   set priv_alter_tenant = 1,
#                       priv_alter_system = 1,
#                       priv_create_resource_unit = 1,
#                       priv_create_resource_pool = 1 """ + condition
#          log(sql)
#          cur.execute(sql)
#
#          begin_user_id = end_user_id
#
#    # 还原默认 tenant
#    sys_tenant_id = 1
#    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
#    log(sql)
#    cur.execute(sql)
#
#    # enable ddl
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("exec fill priv_file to all_user failed")
#    raise e
#  logging.info('exec fill priv_file to all_user finish')
#
#def insert_split_schema_version_v2(conn, cur, user, pwd):
#  try:
#    query_cur = actions.QueryCursor(cur)
#    is_primary = actions.check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.warn("should run in primary cluster")
#      raise e
#
#    # primary cluster
#    dml_cur = actions.DMLCursor(cur)
#    sql = """replace into __all_core_table(table_name, row_id, column_name, column_value)
#              values ('__all_global_stat', 1, 'split_schema_version_v2', '-1');"""
#    rowcount = dml_cur.exec_update(sql)
#    if rowcount <= 0:
#      logging.warn("invalid rowcount : {0}".format(rowcount))
#      raise e
#
#    # standby cluster
#    standby_cluster_list = actions.fetch_standby_cluster_infos(conn, query_cur, user, pwd)
#    for standby_cluster in standby_cluster_list:
#      # connect
#      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                   .format(standby_cluster['cluster_id'],
#                           standby_cluster['ip'],
#                           standby_cluster['port']))
#      tmp_conn = mysql.connector.connect(user     =  standby_cluster['user'],
#                                         password =  standby_cluster['pwd'],
#                                         host     =  standby_cluster['ip'],
#                                         port     =  standby_cluster['port'],
#                                         database =  'oceanbase')
#      tmp_cur = tmp_conn.cursor(buffered=True)
#      tmp_conn.autocommit = True
#      tmp_query_cur = actions.QueryCursor(tmp_cur)
#      # check if stanby cluster
#      is_primary = actions.check_current_cluster_is_primary(tmp_query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster['cluster_id'],
#                                  standby_cluster['ip'],
#                                  standby_cluster['port']))
#        raise e
#      # replace
#      tmp_dml_cur = actions.DMLCursor(tmp_cur)
#      sql = """replace into __all_core_table(table_name, row_id, column_name, column_value)
#                values ('__all_global_stat', 1, 'split_schema_version_v2', '-1');"""
#      rowcount = tmp_dml_cur.exec_update(sql)
#      if rowcount <= 0:
#        logging.warn("invalid rowcount : {0}".format(rowcount))
#        raise e
#      # close
#      tmp_cur.close()
#      tmp_conn.close()
#  except Exception, e:
#    logging.warn("init split_schema_version_v2 failed")
#    raise e
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
## 修正升级上来的旧外键在 __all_foreign_key_column 和 __all_foreign_key_column_history 中 position 列的数据
#def modify_foreign_key_column_position_info(conn, cur):
#  try:
#    conn.autocommit = True
#    # disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#    tenant_ids = get_tenant_ids(cur)
#    log('tenant_ids: {0}'.format(tenant_ids))
#    for tenant_id in tenant_ids:
#      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
#      log(sql)
#      cur.execute(sql)
#      tenant_id_in_sql = 0
#      if 1 == tenant_id:
#        tenant_id_in_sql = 1
#      # 查出租户下所有未被删除的外键
#      sql = """select /*+ QUERY_TIMEOUT(1500000000) */ foreign_key_id from oceanbase.__all_foreign_key where tenant_id = {0}""".format(tenant_id_in_sql)
#      log(sql)
#      foreign_key_id_rows = query(cur, sql)
#      fk_num = len(foreign_key_id_rows)
#      cnt = 0
#      # 遍历每个外键，检查 oceanbase.__all_foreign_key_column 中记录的 position 信息是否为 0，如果为 0，需要更新为正确的值
#      while cnt < fk_num:
#        foreign_key_id = foreign_key_id_rows[cnt][0]
#        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ child_column_id, parent_column_id from oceanbase.__all_foreign_key_column where foreign_key_id = {0} and position = 0 and tenant_id = {1} order by gmt_create asc""".format(foreign_key_id, tenant_id_in_sql)
#        log(sql)
#        need_update_rows = query(cur, sql)
#        fk_col_num = len(need_update_rows)
#        if fk_col_num > 0:
#          position = 1
#          # 遍历特定外键里的每个 position 信息为 0 的列
#          while position <= fk_col_num:
#            child_column_id = need_update_rows[position - 1][0]
#            parent_column_id = need_update_rows[position - 1][1]
#            # 在 oceanbase.__all_foreign_key_column_history 里面更新 position 的值
#            sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_foreign_key_column_history set position = {0} where foreign_key_id = {1} and child_column_id = {2} and parent_column_id = {3} and tenant_id = {4}""".format(position, foreign_key_id, child_column_id, parent_column_id, tenant_id_in_sql)
#            log(sql)
#            cur.execute(sql)
#            if cur.rowcount == 0:
#              logging.warn("affected rows is 0 when update oceanbase.__all_foreign_key_column_history")
#              raise e
#            # 在 oceanbase.__all_foreign_key_column 里面更新 position 的值
#            sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_foreign_key_column set position = {0} where foreign_key_id = {1} and child_column_id = {2} and parent_column_id = {3} and tenant_id = {4}""".format(position, foreign_key_id, child_column_id, parent_column_id, tenant_id_in_sql)
#            log(sql)
#            cur.execute(sql)
#            if cur.rowcount != 1:
#              logging.warn("affected rows is not 1 when update oceanbase.__all_foreign_key_column")
#              raise e
#            position = position + 1
#        cnt = cnt + 1
#    # 还原默认 tenant
#    sys_tenant_id = 1
#    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
#    log(sql)
#    cur.execute(sql)
#
#    # enable ddl
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("modify foreign key column position failed")
#    raise e
#  logging.info('modify foreign key column position finish')
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:sys_vars_dict.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
## sys_vars_dict.py is generated by gen_ob_sys_variables.py, according ob_system_variable_init.json and upgrade_sys_var_base_script.py, DO NOT edited directly
#sys_var_dict = {}
#sys_var_dict["auto_increment_increment"] = {"id": 0, "name": "auto_increment_increment", "value": "1", "data_type": 10, "info": " ", "flags": 131, "min_val": "1", "max_val": "65535"}
#sys_var_dict["auto_increment_offset"] = {"id": 1, "name": "auto_increment_offset", "value": "1", "data_type": 10, "info": " ", "flags": 3, "min_val": "1", "max_val": "65535"}
#sys_var_dict["autocommit"] = {"id": 2, "name": "autocommit", "value": "1", "data_type": 5, "info": " ", "flags": 131}
#sys_var_dict["character_set_client"] = {"id": 3, "name": "character_set_client", "value": "45", "data_type": 5, "info": "The character set in which statements are sent by the client", "flags": 163}
#sys_var_dict["character_set_connection"] = {"id": 4, "name": "character_set_connection", "value": "45", "data_type": 5, "info": "The character set which should be translated to after receiving the statement", "flags": 163}
#sys_var_dict["character_set_database"] = {"id": 5, "name": "character_set_database", "value": "45", "data_type": 5, "info": "The character set of the default database", "flags": 4131}
#sys_var_dict["character_set_results"] = {"id": 6, "name": "character_set_results", "value": "45", "data_type": 5, "info": "The character set which server should translate to before shipping result sets or error message back to the client", "flags": 35}
#sys_var_dict["character_set_server"] = {"id": 7, "name": "character_set_server", "value": "45", "data_type": 5, "info": "The server character set", "flags": 4131}
#sys_var_dict["character_set_system"] = {"id": 8, "name": "character_set_system", "value": "45", "data_type": 5, "info": "The character set used by the server for storing identifiers.", "flags": 7}
#sys_var_dict["collation_connection"] = {"id": 9, "name": "collation_connection", "value": "45", "data_type": 5, "info": "The collation which the server should translate to after receiving the statement", "flags": 227}
#sys_var_dict["collation_database"] = {"id": 10, "name": "collation_database", "value": "45", "data_type": 5, "info": "The collation of the default database", "flags": 4259}
#sys_var_dict["collation_server"] = {"id": 11, "name": "collation_server", "value": "45", "data_type": 5, "info": "The server collation", "flags": 4259}
#sys_var_dict["interactive_timeout"] = {"id": 12, "name": "interactive_timeout", "value": "28800", "data_type": 5, "info": "The number of seconds the server waits for activity on an interactive connection before closing it.", "flags": 3, "min_val": "1", "max_val": "31536000"}
#sys_var_dict["last_insert_id"] = {"id": 13, "name": "last_insert_id", "value": "0", "data_type": 10, "info": " ", "flags": 2, "min_val": "0", "max_val": "18446744073709551615"}
#sys_var_dict["max_allowed_packet"] = {"id": 14, "name": "max_allowed_packet", "value": "4194304", "data_type": 5, "info": "Max packet length to send to or receive from the server", "flags": 139, "min_val": "1024", "max_val": "1073741824"}
#sys_var_dict["sql_mode"] = {"id": 15, "name": "sql_mode", "value": "4194304", "data_type": 10, "info": " ", "flags": 4291}
#sys_var_dict["time_zone"] = {"id": 16, "name": "time_zone", "value": "+8:00", "data_type": 22, "info": " ", "flags": 131}
#sys_var_dict["tx_isolation"] = {"id": 17, "name": "tx_isolation", "value": "READ-COMMITTED", "data_type": 22, "info": "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE", "flags": 131}
#sys_var_dict["version_comment"] = {"id": 18, "name": "version_comment", "value": "OceanBase 1.0.0", "data_type": 22, "info": " ", "flags": 5}
#sys_var_dict["wait_timeout"] = {"id": 19, "name": "wait_timeout", "value": "28800", "data_type": 5, "info": "The number of seconds the server waits for activity on a noninteractive connection before closing it.", "flags": 3, "min_val": "1", "max_val": "31536000"}
#sys_var_dict["binlog_row_image"] = {"id": 20, "name": "binlog_row_image", "value": "2", "data_type": 5, "info": "control row cells to logged", "flags": 195}
#sys_var_dict["character_set_filesystem"] = {"id": 21, "name": "character_set_filesystem", "value": "63", "data_type": 5, "info": " ", "flags": 35}
#sys_var_dict["connect_timeout"] = {"id": 22, "name": "connect_timeout", "value": "10", "data_type": 5, "info": " ", "flags": 1, "min_val": "2", "max_val": "31536000"}
#sys_var_dict["datadir"] = {"id": 23, "name": "datadir", "value": "/usr/local/mysql/data/", "data_type": 22, "info": " ", "flags": 5}
#sys_var_dict["debug_sync"] = {"id": 24, "name": "debug_sync", "value": "", "data_type": 22, "info": "Debug sync facility", "flags": 18}
#sys_var_dict["div_precision_increment"] = {"id": 25, "name": "div_precision_increment", "value": "4", "data_type": 5, "info": " ", "flags": 195, "min_val": "0", "max_val": "30"}
#sys_var_dict["explicit_defaults_for_timestamp"] = {"id": 26, "name": "explicit_defaults_for_timestamp", "value": "1", "data_type": 5, "info": "whether use traditional mode for timestamp", "flags": 195}
#sys_var_dict["group_concat_max_len"] = {"id": 27, "name": "group_concat_max_len", "value": "1024", "data_type": 10, "info": " ", "flags": 131, "min_val": "4", "max_val": "18446744073709551615"}
#sys_var_dict["identity"] = {"id": 28, "name": "identity", "value": "0", "data_type": 10, "info": " ", "flags": 2, "min_val": "0", "max_val": "18446744073709551615"}
#sys_var_dict["lower_case_table_names"] = {"id": 29, "name": "lower_case_table_names", "value": "1", "data_type": 5, "info": "how table database names are stored and compared, 0 means stored using the lettercase in the CREATE_TABLE or CREATE_DATABASE statement. Name comparisons are case sensitive; 1 means that table and database names are stored in lowercase abd name comparisons are not case sensitive.", "flags": 133, "min_val": "0", "max_val": "2"}
#sys_var_dict["net_read_timeout"] = {"id": 30, "name": "net_read_timeout", "value": "30", "data_type": 5, "info": " ", "flags": 3, "min_val": "1", "max_val": "31536000"}
#sys_var_dict["net_write_timeout"] = {"id": 31, "name": "net_write_timeout", "value": "60", "data_type": 5, "info": " ", "flags": 3, "min_val": "1", "max_val": "31536000"}
#sys_var_dict["read_only"] = {"id": 32, "name": "read_only", "value": "0", "data_type": 5, "info": " ", "flags": 65}
#sys_var_dict["sql_auto_is_null"] = {"id": 33, "name": "sql_auto_is_null", "value": "0", "data_type": 5, "info": " ", "flags": 195}
#sys_var_dict["sql_select_limit"] = {"id": 34, "name": "sql_select_limit", "value": "9223372036854775807", "data_type": 5, "info": " ", "flags": 131, "min_val": "0", "max_val": "9223372036854775807"}
#sys_var_dict["timestamp"] = {"id": 35, "name": "timestamp", "value": "0", "data_type": 15, "info": " ", "flags": 2, "min_val": "0"}
#sys_var_dict["tx_read_only"] = {"id": 36, "name": "tx_read_only", "value": "0", "data_type": 5, "info": " ", "flags": 131}
#sys_var_dict["version"] = {"id": 37, "name": "version", "value": "5.6.25", "data_type": 22, "info": " ", "flags": 5}
#sys_var_dict["sql_warnings"] = {"id": 38, "name": "sql_warnings", "value": "0", "data_type": 5, "info": " ", "flags": 3}
#sys_var_dict["max_user_connections"] = {"id": 39, "name": "max_user_connections", "value": "0", "data_type": 10, "info": " ", "flags": 11, "min_val": "0", "max_val": "4294967295"}
#sys_var_dict["init_connect"] = {"id": 40, "name": "init_connect", "value": "", "data_type": 22, "info": " ", "flags": 1}
#sys_var_dict["license"] = {"id": 41, "name": "license", "value": "", "data_type": 22, "info": " ", "flags": 5}
#sys_var_dict["net_buffer_length"] = {"id": 42, "name": "net_buffer_length", "value": "16384", "data_type": 5, "info": "Buffer length for TCP/IP and socket communication", "flags": 11, "min_val": "1024", "max_val": "1048576"}
#sys_var_dict["system_time_zone"] = {"id": 43, "name": "system_time_zone", "value": "CST", "data_type": 22, "info": "The server system time zone", "flags": 133}
#sys_var_dict["query_cache_size"] = {"id": 44, "name": "query_cache_size", "value": "1048576", "data_type": 10, "info": "The memory allocated to store results from old queries(not used yet)", "flags": 1, "min_val": "0", "max_val": "18446744073709551615"}
#sys_var_dict["query_cache_type"] = {"id": 45, "name": "query_cache_type", "value": "0", "data_type": 5, "info": "OFF = Do not cache or retrieve results. ON = Cache all results except SELECT SQL_NO_CACHE ... queries. DEMAND = Cache only SELECT SQL_CACHE ... queries(not used yet)", "flags": 3}
#sys_var_dict["sql_quote_show_create"] = {"id": 46, "name": "sql_quote_show_create", "value": "1", "data_type": 5, "info": " ", "flags": 3}
#sys_var_dict["max_sp_recursion_depth"] = {"id": 47, "name": "max_sp_recursion_depth", "value": "0", "data_type": 5, "info": "The number of times that any given stored procedure may be called recursively.", "flags": 131, "min_val": "0", "max_val": "255"}
#sys_var_dict["sql_safe_updates"] = {"id": 48, "name": "sql_safe_updates", "value": "0", "data_type": 5, "info": "enable mysql sql safe updates", "flags": 4227}
#sys_var_dict["concurrent_insert"] = {"id": 49, "name": "concurrent_insert", "value": "AUTO", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["default_authentication_plugin"] = {"id": 50, "name": "default_authentication_plugin", "value": "mysql_native_password", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["disabled_storage_engines"] = {"id": 51, "name": "disabled_storage_engines", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["error_count"] = {"id": 52, "name": "error_count", "value": "0", "data_type": 10, "info": "", "flags": 4098}
#sys_var_dict["general_log"] = {"id": 53, "name": "general_log", "value": "0", "data_type": 5, "info": "", "flags": 4099}
#sys_var_dict["have_openssl"] = {"id": 54, "name": "have_openssl", "value": "YES", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["have_profiling"] = {"id": 55, "name": "have_profiling", "value": "YES", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["have_ssl"] = {"id": 56, "name": "have_ssl", "value": "YES", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["hostname"] = {"id": 57, "name": "hostname", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["lc_messages"] = {"id": 58, "name": "lc_messages", "value": "en_US", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["local_infile"] = {"id": 59, "name": "local_infile", "value": "1", "data_type": 5, "info": "", "flags": 4099}
#sys_var_dict["lock_wait_timeout"] = {"id": 60, "name": "lock_wait_timeout", "value": "31536000", "data_type": 5, "info": "", "flags": 4099, "min_val": "1", "max_val": "31536000"}
#sys_var_dict["long_query_time"] = {"id": 61, "name": "long_query_time", "value": "10", "data_type": 15, "info": "", "flags": 4099, "min_val": "0"}
#sys_var_dict["max_connections"] = {"id": 62, "name": "max_connections", "value": "4294967295", "data_type": 10, "info": "", "flags": 4097, "min_val": "1", "max_val": "4294967295"}
#sys_var_dict["max_execution_time"] = {"id": 63, "name": "max_execution_time", "value": "0", "data_type": 5, "info": "", "flags": 4099}
#sys_var_dict["protocol_version"] = {"id": 64, "name": "protocol_version", "value": "10", "data_type": 5, "info": "", "flags": 4099}
#sys_var_dict["server_id"] = {"id": 65, "name": "server_id", "value": "0", "data_type": 5, "info": "", "flags": 4099, "min_val": "0", "max_val": "4294967295"}
#sys_var_dict["ssl_ca"] = {"id": 66, "name": "ssl_ca", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_capath"] = {"id": 67, "name": "ssl_capath", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_cert"] = {"id": 68, "name": "ssl_cert", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_cipher"] = {"id": 69, "name": "ssl_cipher", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_crl"] = {"id": 70, "name": "ssl_crl", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_crlpath"] = {"id": 71, "name": "ssl_crlpath", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["ssl_key"] = {"id": 72, "name": "ssl_key", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["time_format"] = {"id": 73, "name": "time_format", "value": "%H:%i:%s", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["tls_version"] = {"id": 74, "name": "tls_version", "value": "", "data_type": 22, "info": "TLSv1,TLSv1.1,TLSv1.2", "flags": 4099}
#sys_var_dict["tmp_table_size"] = {"id": 75, "name": "tmp_table_size", "value": "16777216", "data_type": 10, "info": "", "flags": 4099, "min_val": "1024", "max_val": "18446744073709551615"}
#sys_var_dict["tmpdir"] = {"id": 76, "name": "tmpdir", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["unique_checks"] = {"id": 77, "name": "unique_checks", "value": "1", "data_type": 5, "info": "", "flags": 4099}
#sys_var_dict["version_compile_machine"] = {"id": 78, "name": "version_compile_machine", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["version_compile_os"] = {"id": 79, "name": "version_compile_os", "value": "", "data_type": 22, "info": "", "flags": 4099}
#sys_var_dict["warning_count"] = {"id": 80, "name": "warning_count", "value": "0", "data_type": 10, "info": "", "flags": 4098}
#sys_var_dict["session_track_schema"] = {"id": 81, "name": "session_track_schema", "value": "1", "data_type": 5, "info": "specifies whether return schema change info in ok packet", "flags": 4099}
#sys_var_dict["session_track_system_variables"] = {"id": 82, "name": "session_track_system_variables", "value": "time_zone, autocommit, character_set_client, character_set_results, character_set_connection", "data_type": 22, "info": "specifies whether return system variables change info in ok packet", "flags": 4099}
#sys_var_dict["session_track_state_change"] = {"id": 83, "name": "session_track_state_change", "value": "0", "data_type": 5, "info": "specifies whether return session state change info in ok packet", "flags": 4099}
#sys_var_dict["ob_default_replica_num"] = {"id": 10000, "name": "ob_default_replica_num", "value": "1", "data_type": 5, "info": "The default replica number of table per zone if not specified when creating table.", "flags": 3}
#sys_var_dict["ob_interm_result_mem_limit"] = {"id": 10001, "name": "ob_interm_result_mem_limit", "value": "2147483648", "data_type": 5, "info": "Indicate how many bytes the interm result manager can alloc most for this tenant", "flags": 131}
#sys_var_dict["ob_proxy_partition_hit"] = {"id": 10002, "name": "ob_proxy_partition_hit", "value": "1", "data_type": 5, "info": "Indicate whether sql stmt hit right partition, readonly to user, modify by ob", "flags": 22}
#sys_var_dict["ob_log_level"] = {"id": 10003, "name": "ob_log_level", "value": "disabled", "data_type": 22, "info": "log level in session", "flags": 3}
#sys_var_dict["ob_max_parallel_degree"] = {"id": 10004, "name": "ob_max_parallel_degree", "value": "16", "data_type": 5, "info": "Max parellel sub request to chunkservers for one request", "flags": 67}
#sys_var_dict["ob_query_timeout"] = {"id": 10005, "name": "ob_query_timeout", "value": "10000000", "data_type": 5, "info": "Query timeout in microsecond(us)", "flags": 131}
#sys_var_dict["ob_read_consistency"] = {"id": 10006, "name": "ob_read_consistency", "value": "3", "data_type": 5, "info": "read consistency level: 3=STRONG, 2=WEAK, 1=FROZEN", "flags": 195}
#sys_var_dict["ob_enable_transformation"] = {"id": 10007, "name": "ob_enable_transformation", "value": "1", "data_type": 5, "info": "whether use transform in session", "flags": 195}
#sys_var_dict["ob_trx_timeout"] = {"id": 10008, "name": "ob_trx_timeout", "value": "100000000", "data_type": 5, "info": "The max duration of one transaction", "flags": 131}
#sys_var_dict["ob_enable_plan_cache"] = {"id": 10009, "name": "ob_enable_plan_cache", "value": "1", "data_type": 5, "info": "whether use plan cache in session", "flags": 131}
#sys_var_dict["ob_enable_index_direct_select"] = {"id": 10010, "name": "ob_enable_index_direct_select", "value": "0", "data_type": 5, "info": "whether can select from index table", "flags": 195}
#sys_var_dict["ob_proxy_set_trx_executed"] = {"id": 10011, "name": "ob_proxy_set_trx_executed", "value": "0", "data_type": 5, "info": "this value is true if we have executed set transaction stmt, until a transaction commit(explicit or implicit) successfully", "flags": 22}
#sys_var_dict["ob_enable_aggregation_pushdown"] = {"id": 10012, "name": "ob_enable_aggregation_pushdown", "value": "1", "data_type": 5, "info": "enable aggregation function to be push-downed through exchange nodes", "flags": 195}
#sys_var_dict["ob_last_schema_version"] = {"id": 10013, "name": "ob_last_schema_version", "value": "0", "data_type": 5, "info": " ", "flags": 2}
#sys_var_dict["ob_global_debug_sync"] = {"id": 10014, "name": "ob_global_debug_sync", "value": "", "data_type": 22, "info": "Global debug sync facility", "flags": 18}
#sys_var_dict["ob_proxy_global_variables_version"] = {"id": 10015, "name": "ob_proxy_global_variables_version", "value": "0", "data_type": 5, "info": "this value is global variables last modified time when server session create, used for proxy to judge whether global vars has changed between two server session", "flags": 22}
#sys_var_dict["ob_enable_trace_log"] = {"id": 10016, "name": "ob_enable_trace_log", "value": "0", "data_type": 5, "info": "control whether use trace log", "flags": 131}
#sys_var_dict["ob_enable_hash_group_by"] = {"id": 10017, "name": "ob_enable_hash_group_by", "value": "1", "data_type": 5, "info": "", "flags": 195}
#sys_var_dict["ob_enable_blk_nestedloop_join"] = {"id": 10018, "name": "ob_enable_blk_nestedloop_join", "value": "0", "data_type": 5, "info": "", "flags": 195}
#sys_var_dict["ob_bnl_join_cache_size"] = {"id": 10019, "name": "ob_bnl_join_cache_size", "value": "10485760", "data_type": 5, "info": "", "flags": 195, "min_val": "1", "max_val": "9223372036854775807"}
#sys_var_dict["ob_proxy_user_privilege"] = {"id": 10020, "name": "ob_proxy_user_privilege", "value": "0", "data_type": 5, "info": "Indicate current client session user privilege, readonly after modified by first observer", "flags": 22, "min_val": "0", "max_val": "9223372036854775807"}
#sys_var_dict["ob_org_cluster_id"] = {"id": 10021, "name": "ob_org_cluster_id", "value": "0", "data_type": 5, "info": "When the DRC system copies data into the target cluster, it needs to be set to the CLUSTER_ID that should be written into commit log of OceanBase, in order to avoid loop replication of data. Normally, it does not need to be set, and OceanBase will use the default value, which is the CLUSTER_ID of current cluster of OceanBase. 0 indicates it is not set, please do not set it to 0", "flags": 130, "min_val": "0", "max_val": "4294967295"}
#sys_var_dict["ob_plan_cache_percentage"] = {"id": 10022, "name": "ob_plan_cache_percentage", "value": "5", "data_type": 5, "info": "percentage of tenant memory resources that can be used by plan cache", "flags": 129, "min_val": "0", "max_val": "100"}
#sys_var_dict["ob_plan_cache_evict_high_percentage"] = {"id": 10023, "name": "ob_plan_cache_evict_high_percentage", "value": "90", "data_type": 5, "info": "memory usage percentage of plan_cache_limit at which plan cache eviction will be trigger", "flags": 129, "min_val": "0", "max_val": "100"}
#sys_var_dict["ob_plan_cache_evict_low_percentage"] = {"id": 10024, "name": "ob_plan_cache_evict_low_percentage", "value": "50", "data_type": 5, "info": "memory usage percentage  of plan_cache_limit at which plan cache eviction will be stopped", "flags": 129, "min_val": "0", "max_val": "100"}
#sys_var_dict["recyclebin"] = {"id": 10025, "name": "recyclebin", "value": "0", "data_type": 5, "info": "When the recycle bin is enabled, dropped tables and their dependent objects are placed in the recycle bin. When the recycle bin is disabled, dropped tables and their dependent objects are not placed in the recycle bin; they are just dropped.", "flags": 3}
#sys_var_dict["ob_capability_flag"] = {"id": 10026, "name": "ob_capability_flag", "value": "0", "data_type": 10, "info": "Indicate features that observer supports, readonly after modified by first observer", "flags": 22, "min_val": "0", "max_val": "18446744073709551615"}
#sys_var_dict["ob_stmt_parallel_degree"] = {"id": 10027, "name": "ob_stmt_parallel_degree", "value": "1", "data_type": 5, "info": "The parallel degree of a job in a query, which represent how many tasks of a job can be run parallelly", "flags": 195, "min_val": "1", "max_val": "10240"}
#sys_var_dict["is_result_accurate"] = {"id": 10028, "name": "is_result_accurate", "value": "1", "data_type": 5, "info": "when query is with topk hint, is_result_accurate indicates whether the result is acuurate or not ", "flags": 130}
#sys_var_dict["error_on_overlap_time"] = {"id": 10029, "name": "error_on_overlap_time", "value": "0", "data_type": 5, "info": "The variable determines how OceanBase should handle an ambiguous boundary datetime value a case in which it is not clear whether the datetime is in standard or daylight saving time", "flags": 131}
#sys_var_dict["ob_compatibility_mode"] = {"id": 10030, "name": "ob_compatibility_mode", "value": "0", "data_type": 5, "info": "What DBMS is OceanBase compatible with? MYSQL means it behaves like MySQL while ORACLE means it behaves like Oracle.", "flags": 2183}
#sys_var_dict["ob_create_table_strict_mode"] = {"id": 10031, "name": "ob_create_table_strict_mode", "value": "0", "data_type": 5, "info": "If set true, create all the replicas according to the locality or the operation will fail.", "flags": 3}
#sys_var_dict["ob_sql_work_area_percentage"] = {"id": 10032, "name": "ob_sql_work_area_percentage", "value": "5", "data_type": 5, "info": "The percentage limitation of tenant memory for SQL execution.", "flags": 1, "min_val": "0", "max_val": "100"}
#sys_var_dict["ob_safe_weak_read_snapshot"] = {"id": 10033, "name": "ob_safe_weak_read_snapshot", "value": "1", "data_type": 5, "info": "The safe weak read snapshot version in one server", "flags": 146, "min_val": "0", "max_val": "9223372036854775807"}
#sys_var_dict["ob_route_policy"] = {"id": 10034, "name": "ob_route_policy", "value": "1", "data_type": 5, "info": "the routing policy of obproxy/java client and observer internal retry, 1=READONLY_ZONE_FIRST, 2=ONLY_READONLY_ZONE, 3=UNMERGE_ZONE_FIRST, 4=UNMERGE_FOLLOWER_FIRST", "flags": 195}
#sys_var_dict["ob_enable_transmission_checksum"] = {"id": 10035, "name": "ob_enable_transmission_checksum", "value": "1", "data_type": 5, "info": "whether do the checksum of the packet between the client and the server", "flags": 387}
#sys_var_dict["foreign_key_checks"] = {"id": 10036, "name": "foreign_key_checks", "value": "1", "data_type": 5, "info": "set to 1 (the default by MySQL), foreign key constraints are checked. If set to 0, foreign key constraints are ignored", "flags": 131}
#sys_var_dict["ob_statement_trace_id"] = {"id": 10037, "name": "ob_statement_trace_id", "value": "Y0-0", "data_type": 22, "info": "the trace id of current executing statement", "flags": 22}
#sys_var_dict["ob_enable_truncate_flashback"] = {"id": 10038, "name": "ob_enable_truncate_flashback", "value": "0", "data_type": 5, "info": "Enable the flashback of table truncation.", "flags": 3}
#sys_var_dict["ob_tcp_invited_nodes"] = {"id": 10039, "name": "ob_tcp_invited_nodes", "value": "127.0.0.1,::1", "data_type": 22, "info": "ip white list for tenant, support % and _ and multi ip(separated by commas), support ip match and wild match", "flags": 1}
#sys_var_dict["sql_throttle_current_priority"] = {"id": 10040, "name": "sql_throttle_current_priority", "value": "100", "data_type": 5, "info": "current priority used for SQL throttling", "flags": 3}
#sys_var_dict["sql_throttle_priority"] = {"id": 10041, "name": "sql_throttle_priority", "value": "-1", "data_type": 5, "info": "sql throttle priority, query may not be allowed to execute if its priority isnt greater than this value.", "flags": 1}
#sys_var_dict["sql_throttle_rt"] = {"id": 10042, "name": "sql_throttle_rt", "value": "-1", "data_type": 15, "info": "query may not be allowed to execute if its rt isnt less than this value.", "flags": 1}
#sys_var_dict["sql_throttle_cpu"] = {"id": 10043, "name": "sql_throttle_cpu", "value": "-1", "data_type": 15, "info": "query may not be allowed to execute if its CPU usage isnt less than this value.", "flags": 1}
#sys_var_dict["sql_throttle_io"] = {"id": 10044, "name": "sql_throttle_io", "value": "-1", "data_type": 5, "info": "query may not be allowed to execute if its number of IOs isnt less than this value.", "flags": 1}
#sys_var_dict["sql_throttle_network"] = {"id": 10045, "name": "sql_throttle_network", "value": "-1", "data_type": 15, "info": "query may not be allowed to execute if its network usage isnt less than this value.", "flags": 1}
#sys_var_dict["sql_throttle_logical_reads"] = {"id": 10046, "name": "sql_throttle_logical_reads", "value": "-1", "data_type": 5, "info": "query may not be allowed to execute if its number of logical reads isnt less than this value.", "flags": 1}
#sys_var_dict["auto_increment_cache_size"] = {"id": 10047, "name": "auto_increment_cache_size", "value": "1000000", "data_type": 5, "info": "auto_increment service cache size", "flags": 129, "min_val": "1", "max_val": "100000000"}
#sys_var_dict["ob_enable_jit"] = {"id": 10048, "name": "ob_enable_jit", "value": "0", "data_type": 5, "info": "JIT execution engine mode, default is AUTO", "flags": 195}
#sys_var_dict["ob_temp_tablespace_size_percentage"] = {"id": 10049, "name": "ob_temp_tablespace_size_percentage", "value": "0", "data_type": 5, "info": "the percentage limitation of some temp tablespace size in tenant disk.", "flags": 3}
#sys_var_dict["_optimizer_adaptive_cursor_sharing"] = {"id": 10050, "name": "_optimizer_adaptive_cursor_sharing", "value": "0", "data_type": 5, "info": "Enable use of adaptive cursor sharing", "flags": 147}
#sys_var_dict["ob_timestamp_service"] = {"id": 10051, "name": "ob_timestamp_service", "value": "1", "data_type": 5, "info": "the type of timestamp service", "flags": 129}
#sys_var_dict["plugin_dir"] = {"id": 10052, "name": "plugin_dir", "value": "./plugin_dir/", "data_type": 22, "info": "the dir to place plugin dll", "flags": 5}
#sys_var_dict["undo_retention"] = {"id": 10053, "name": "undo_retention", "value": "0", "data_type": 5, "info": "specifies (in seconds) the low threshold value of undo retention.", "flags": 1, "min_val": "0", "max_val": "4294967295"}
#sys_var_dict["_ob_use_parallel_execution"] = {"id": 10054, "name": "_ob_use_parallel_execution", "value": "1", "data_type": 5, "info": "auto use parallel execution", "flags": 211}
#sys_var_dict["ob_sql_audit_percentage"] = {"id": 10055, "name": "ob_sql_audit_percentage", "value": "3", "data_type": 5, "info": "The limited percentage of tenant memory for sql audit", "flags": 129, "min_val": "0", "max_val": "100"}
#sys_var_dict["ob_enable_sql_audit"] = {"id": 10056, "name": "ob_enable_sql_audit", "value": "1", "data_type": 5, "info": "wether use sql audit in session", "flags": 129}
#sys_var_dict["optimizer_use_sql_plan_baselines"] = {"id": 10057, "name": "optimizer_use_sql_plan_baselines", "value": "1", "data_type": 5, "info": "Enable use sql plan baseline", "flags": 131}
#sys_var_dict["optimizer_capture_sql_plan_baselines"] = {"id": 10058, "name": "optimizer_capture_sql_plan_baselines", "value": "1", "data_type": 5, "info": "optimizer_capture_sql_plan_baselines enables or disables automitic capture plan baseline.", "flags": 131}
#sys_var_dict["parallel_max_servers"] = {"id": 10059, "name": "parallel_max_servers", "value": "0", "data_type": 5, "info": "number of threads created to run parallel statements for each observer.", "flags": 129, "min_val": "0", "max_val": "1800"}
#sys_var_dict["parallel_servers_target"] = {"id": 10060, "name": "parallel_servers_target", "value": "0", "data_type": 5, "info": "number of threads allowed to run parallel statements before statement queuing will be used.", "flags": 1, "min_val": "0", "max_val": "9223372036854775807"}
#sys_var_dict["ob_early_lock_release"] = {"id": 10061, "name": "ob_early_lock_release", "value": "0", "data_type": 5, "info": "If set true, transaction open the elr optimization.", "flags": 129}
#sys_var_dict["ob_trx_idle_timeout"] = {"id": 10062, "name": "ob_trx_idle_timeout", "value": "120000000", "data_type": 5, "info": "The stmt interval timeout of transaction(us)", "flags": 131}
#sys_var_dict["block_encryption_mode"] = {"id": 10063, "name": "block_encryption_mode", "value": "0", "data_type": 5, "info": "specifies the encryption algorithm used in the functions aes_encrypt and aes_decrypt", "flags": 131}
#sys_var_dict["nls_date_format"] = {"id": 10064, "name": "nls_date_format", "value": "DD-MON-RR", "data_type": 22, "info": "specifies the default date format to use with the TO_CHAR and TO_DATE functions, (YYYY-MM-DD HH24:MI:SS) is Common value", "flags": 643}
#sys_var_dict["nls_timestamp_format"] = {"id": 10065, "name": "nls_timestamp_format", "value": "DD-MON-RR HH.MI.SSXFF AM", "data_type": 22, "info": "specifies the default date format to use with the TO_CHAR and TO_TIMESTAMP functions, (YYYY-MM-DD HH24:MI:SS.FF) is Common value", "flags": 643}
#sys_var_dict["nls_timestamp_tz_format"] = {"id": 10066, "name": "nls_timestamp_tz_format", "value": "DD-MON-RR HH.MI.SSXFF AM TZR", "data_type": 22, "info": "specifies the default timestamp with time zone format to use with the TO_CHAR and TO_TIMESTAMP_TZ functions, (YYYY-MM-DD HH24:MI:SS.FF TZR TZD) is common value", "flags": 643}
#sys_var_dict["ob_reserved_meta_memory_percentage"] = {"id": 10067, "name": "ob_reserved_meta_memory_percentage", "value": "10", "data_type": 5, "info": "percentage of tenant memory resources that can be used by tenant meta data", "flags": 129, "min_val": "1", "max_val": "100"}
#sys_var_dict["ob_check_sys_variable"] = {"id": 10068, "name": "ob_check_sys_variable", "value": "1", "data_type": 5, "info": "If set true, sql will update sys variable while schema version changed.", "flags": 131}
#sys_var_dict["nls_language"] = {"id": 10069, "name": "nls_language", "value": "AMERICAN", "data_type": 22, "info": "specifies the default language of the database, used for messages, day and month names, the default sorting mechanism, the default values of NLS_DATE_LANGUAGE and NLS_SORT.", "flags": 642}
#sys_var_dict["nls_territory"] = {"id": 10070, "name": "nls_territory", "value": "AMERICA", "data_type": 22, "info": "specifies the name of the territory whose conventions are to be followed for day and week numbering, establishes the default date format, the default decimal character and group separator, and the default ISO and local currency symbols.", "flags": 643}
#sys_var_dict["nls_sort"] = {"id": 10071, "name": "nls_sort", "value": "BINARY", "data_type": 22, "info": "specifies the collating sequence for character value comparison in various SQL operators and clauses.", "flags": 707}
#sys_var_dict["nls_comp"] = {"id": 10072, "name": "nls_comp", "value": "BINARY", "data_type": 22, "info": "specifies the collation behavior of the database session. value can be BINARY | LINGUISTIC | ANSI", "flags": 707}
#sys_var_dict["nls_characterset"] = {"id": 10073, "name": "nls_characterset", "value": "AL32UTF8", "data_type": 22, "info": "specifies the default characterset of the database, This parameter defines the encoding of the data in the CHAR, VARCHAR2, LONG and CLOB columns of a table.", "flags": 1733}
#sys_var_dict["nls_nchar_characterset"] = {"id": 10074, "name": "nls_nchar_characterset", "value": "AL32UTF8", "data_type": 22, "info": "specifies the default characterset of the database, This parameter defines the encoding of the data in the NCHAR, NVARCHAR2 and NCLOB columns of a table.", "flags": 705}
#sys_var_dict["nls_date_language"] = {"id": 10075, "name": "nls_date_language", "value": "AMERICAN", "data_type": 22, "info": "specifies the language to use for the spelling of day and month names and date abbreviations (a.m., p.m., AD, BC) returned by the TO_DATE and TO_CHAR functions.", "flags": 643}
#sys_var_dict["nls_length_semantics"] = {"id": 10076, "name": "nls_length_semantics", "value": "BYTE", "data_type": 22, "info": "specifies the default length semantics to use for VARCHAR2 and CHAR table columns, user-defined object attributes, and PL/SQL variables in database objects created in the session. SYS user use BYTE intead of NLS_LENGTH_SEMANTICS.", "flags": 707}
#sys_var_dict["nls_nchar_conv_excp"] = {"id": 10077, "name": "nls_nchar_conv_excp", "value": "FALSE", "data_type": 22, "info": "determines whether an error is reported when there is data loss during an implicit or explicit character type conversion between NCHAR/NVARCHAR2 and CHAR/VARCHAR2.", "flags": 707}
#sys_var_dict["nls_calendar"] = {"id": 10078, "name": "nls_calendar", "value": "GREGORIAN", "data_type": 22, "info": "specifies which calendar system Oracle uses.", "flags": 643}
#sys_var_dict["nls_numeric_characters"] = {"id": 10079, "name": "nls_numeric_characters", "value": ".,", "data_type": 22, "info": "specifies the characters to use as the decimal character and group separator, overrides those characters defined implicitly by NLS_TERRITORY.", "flags": 643}
#sys_var_dict["_nlj_batching_enabled"] = {"id": 10080, "name": "_nlj_batching_enabled", "value": "1", "data_type": 5, "info": "enable batching of the RHS IO in NLJ", "flags": 211}
#sys_var_dict["tracefile_identifier"] = {"id": 10081, "name": "tracefile_identifier", "value": "", "data_type": 22, "info": "The name of tracefile.", "flags": 130}
#sys_var_dict["_groupby_nopushdown_cut_ratio"] = {"id": 10082, "name": "_groupby_nopushdown_cut_ratio", "value": "3", "data_type": 10, "info": "ratio used to decide whether push down should be done in distribtued query optimization.", "flags": 147}
#sys_var_dict["_px_broadcast_fudge_factor"] = {"id": 10083, "name": "_px_broadcast_fudge_factor", "value": "100", "data_type": 5, "info": "set the tq broadcasting fudge factor percentage.", "flags": 82, "min_val": "0", "max_val": "100"}
#sys_var_dict["_primary_zone_entity_count"] = {"id": 10084, "name": "_primary_zone_entity_count", "value": "-1", "data_type": 5, "info": "statistic primary zone entity(table/tablegroup/database) count under tenant.", "flags": 21}
#sys_var_dict["transaction_isolation"] = {"id": 10085, "name": "transaction_isolation", "value": "READ-COMMITTED", "data_type": 22, "info": "Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE", "flags": 131}
#sys_var_dict["ob_trx_lock_timeout"] = {"id": 10086, "name": "ob_trx_lock_timeout", "value": "-1", "data_type": 5, "info": "the max duration of waiting on row lock of one transaction", "flags": 131}
#sys_var_dict["validate_password_check_user_name"] = {"id": 10087, "name": "validate_password_check_user_name", "value": "0", "data_type": 5, "info": "", "flags": 129}
#sys_var_dict["validate_password_length"] = {"id": 10088, "name": "validate_password_length", "value": "0", "data_type": 10, "info": "", "flags": 129, "min_val": "0", "max_val": "2147483647"}
#sys_var_dict["validate_password_mixed_case_count"] = {"id": 10089, "name": "validate_password_mixed_case_count", "value": "0", "data_type": 10, "info": "", "flags": 129, "min_val": "0", "max_val": "2147483647"}
#sys_var_dict["validate_password_number_count"] = {"id": 10090, "name": "validate_password_number_count", "value": "0", "data_type": 10, "info": "", "flags": 129, "min_val": "0", "max_val": "2147483647"}
#sys_var_dict["validate_password_policy"] = {"id": 10091, "name": "validate_password_policy", "value": "0", "data_type": 5, "info": "", "flags": 129}
#sys_var_dict["validate_password_special_char_count"] = {"id": 10092, "name": "validate_password_special_char_count", "value": "0", "data_type": 10, "info": "", "flags": 129, "min_val": "0", "max_val": "2147483647"}
#sys_var_dict["default_password_lifetime"] = {"id": 10093, "name": "default_password_lifetime", "value": "0", "data_type": 10, "info": "", "flags": 129, "min_val": "0", "max_val": "65535"}
#sys_var_dict["ob_trace_info"] = {"id": 10095, "name": "ob_trace_info", "value": "", "data_type": 22, "info": "store trace info", "flags": 2}
#sys_var_dict["ob_enable_batched_multi_statement"] = {"id": 10096, "name": "ob_enable_batched_multi_statement", "value": "0", "data_type": 5, "info": "enable use of batched multi statement", "flags": 131}
#sys_var_dict["_px_partition_scan_threshold"] = {"id": 10097, "name": "_px_partition_scan_threshold", "value": "64", "data_type": 5, "info": "least number of partitions per slave to start partition-based scan", "flags": 82, "min_val": "0", "max_val": "100"}
#sys_var_dict["_ob_px_bcast_optimization"] = {"id": 10098, "name": "_ob_px_bcast_optimization", "value": "1", "data_type": 5, "info": "broadcast optimization.", "flags": 147}
#sys_var_dict["_ob_px_slave_mapping_threshold"] = {"id": 10099, "name": "_ob_px_slave_mapping_threshold", "value": "200", "data_type": 5, "info": "percentage threshold to use slave mapping plan", "flags": 83, "min_val": "0", "max_val": "1000"}
#sys_var_dict["_enable_parallel_dml"] = {"id": 10100, "name": "_enable_parallel_dml", "value": "0", "data_type": 5, "info": "A DML statement can be parallelized only if you have explicitly enabled parallel DML in the session or in the SQL statement.", "flags": 210}
#sys_var_dict["_px_min_granules_per_slave"] = {"id": 10101, "name": "_px_min_granules_per_slave", "value": "13", "data_type": 5, "info": "minimum number of rowid range granules to generate per slave.", "flags": 82, "min_val": "0", "max_val": "100"}
#sys_var_dict["secure_file_priv"] = {"id": 10102, "name": "secure_file_priv", "value": "NULL", "data_type": 22, "info": "limit the effect of data import and export operations", "flags": 97}
#sys_var_dict["plsql_warnings"] = {"id": 10103, "name": "plsql_warnings", "value": "ENABLE:ALL", "data_type": 22, "info": "enables or disables the reporting of warning messages by the PL/SQL compiler, and specifies which warning messages to show as errors.", "flags": 643}
#sys_var_dict["_enable_parallel_query"] = {"id": 10104, "name": "_enable_parallel_query", "value": "1", "data_type": 5, "info": "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement.", "flags": 210}
#sys_var_dict["_force_parallel_query_dop"] = {"id": 10105, "name": "_force_parallel_query_dop", "value": "1", "data_type": 10, "info": "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement.", "flags": 210}
#sys_var_dict["_force_parallel_dml_dop"] = {"id": 10106, "name": "_force_parallel_dml_dop", "value": "1", "data_type": 10, "info": "A QUERY statement can be parallelized only if you have explicitly enabled parallel QUERY in the session or in the SQL statement.", "flags": 210}
#sys_var_dict["ob_pl_block_timeout"] = {"id": 10107, "name": "ob_pl_block_timeout", "value": "3216672000000000", "data_type": 5, "info": "PL/SQL timeout in microsecond(us)", "flags": 131, "min_val": "0", "max_val": "9223372036854775807"}
#sys_var_dict["transaction_read_only"] = {"id": 10108, "name": "transaction_read_only", "value": "0", "data_type": 5, "info": "Transaction access mode", "flags": 131}
#sys_var_dict["resource_manager_plan"] = {"id": 10109, "name": "resource_manager_plan", "value": "", "data_type": 22, "info": "specifies tenant resource plan.", "flags": 1}
#sys_var_dict["performance_schema"] = {"id": 10110, "name": "performance_schema", "value": "0", "data_type": 5, "info": "indicate whether the Performance Schema is enabled", "flags": 1}
#sys_var_dict["nls_currency"] = {"id": 10111, "name": "nls_currency", "value": "$", "data_type": 22, "info": "specifies the string to use as the local currency symbol for the L number format element. The default value of this parameter is determined by NLS_TERRITORY.", "flags": 643}
#sys_var_dict["nls_iso_currency"] = {"id": 10112, "name": "nls_iso_currency", "value": "AMERICA", "data_type": 22, "info": "specifies the string to use as the international currency symbol for the C number format element. The default value of this parameter is determined by NLS_TERRITORY", "flags": 643}
#sys_var_dict["nls_dual_currency"] = {"id": 10113, "name": "nls_dual_currency", "value": "$", "data_type": 22, "info": "specifies the dual currency symbol for the territory. The default is the dual currency symbol defined in the territory of your current language environment.", "flags": 643}
#sys_var_dict["_ob_proxy_session_temporary_table_used"] = {"id": 10116, "name": "_ob_proxy_session_temporary_table_used", "value": "0", "data_type": 5, "info": "this value is true if we have executed set transaction stmt, until a transaction commit(explicit or implicit) successfully", "flags": 22}
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
#  old_version = '3.1.0'
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
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
#
#
##### START ####
## 1. 检查前置版本
#def check_observer_version(query_cur, upgrade_params):
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('query results count is not 1')
#  elif cmp(results[0][0], upgrade_params.old_version) < 0 :
#    raise MyError('old observer version is expected equal or higher then: {0}, actual version:{1}'.format(upgrade_params.old_version, results[0][0]))
#  logging.info('check observer version success, version = {0}'.format(results[0][0]))
#
## 2. 检查paxos副本是否同步, paxos副本是否缺失
#def check_paxos_replica(query_cur):
#  # 2.1 检查paxos副本是否同步
#  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
#  if results[0][0] > 0 :
#    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
#  # 2.2 检查paxos副本是否有缺失 TODO
#  logging.info('check paxos replica success')
#
## 3. 检查是否有做balance, locality变更
#def check_rebalance_task(query_cur):
#  # 3.1 检查是否有做locality变更
#  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
#  if results[0][0] > 0 :
#    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
#  # 3.2 检查是否有做balance
#  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
#  if results[0][0] > 0 :
#    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
#  logging.info('check rebalance task success')
#
## 4. 检查集群状态
#def check_cluster_status(query_cur):
#  # 4.1 检查是否非合并状态
#  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
#  if cmp(results[0][0], 'IDLE')  != 0 :
#    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
#  logging.info('check cluster status success')
#  # 4.2 检查合并版本是否>=3
#  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
#  if results[0][0] < 2 :
#    raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
#  logging.info('check global last_merged_version success')
#
## 5. 检查没有打开enable_separate_sys_clog
#def check_disable_separate_sys_clog(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(1) from __all_sys_parameter where name like 'enable_separate_sys_clog'""")
#  if results[0][0] > 0 :
#    raise MyError('enable_separate_sys_clog is true, unexpected')
#  logging.info('check separate_sys_clog success')
#
## 6. 检查配置宏块的data_seq
#def check_macro_block_data_seq(query_cur):
#  query_cur.exec_sql("""set ob_query_timeout=1800000000""")
#  row_count = query_cur.exec_sql("""select * from __all_virtual_partition_sstable_macro_info where data_seq < 0 limit 1""")
#  if row_count != 0:
#    raise MyError('check_macro_block_data_seq failed, too old macro block needs full merge')
#  logging.info('check_macro_block_data_seq success')
#
## 8. 检查租户的resource_pool内存规格, 要求F类型unit最小内存大于5G，L类型unit最小内存大于2G.
#def check_tenant_resource_pool(query_cur):
#  (desc, results) = query_cur.exec_query("""select 1 from v$sysstat where name = 'is mini mode' and value = '1' and con_id = 1 limit 1""")
#  if len(results) > 0:
#     # mini部署的集群，租户规格可以很小，这里跳过检查
#     pass
#  else:
#    (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=0 and b.min_memory < 5368709120""")
#    if results[0][0] > 0 :
#      raise MyError('{0} tenant resource pool unit config is less than 5G, please check'.format(results[0][0]))
#    (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=5 and b.min_memory < 2147483648""")
#    if results[0][0] > 0 :
#      raise MyError('{0} tenant logonly resource pool unit config is less than 2G, please check'.format(results[0][0]))
#
## 9. 检查是否有日志型副本分布在Full类型unit中
## 2020-12-31 根据外部使用L副本且L型unit功能不成熟的需求，将这个检查去掉.
#
## 10. 检查租户分区数是否超出内存限制
#def check_tenant_part_num(query_cur):
#  # 统计每个租户在各个server上的分区数量
#  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
#  # 计算每个租户在每个server上的max_memory
#  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
#  # 查询每个server的memstore_limit_percentage
#  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
#  part_static_cost = 128 * 1024
#  part_dynamic_cost = 400 * 1024
#  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
#  part_num_reserved = 512
#  for line in res_part_num:
#    svr_ip = line[0]
#    svr_port = line[1]
#    tenant_id = line[2]
#    part_num = line[3]
#    for uline in res_unit_memory:
#      uip = uline[0]
#      uport = uline[1]
#      utid = uline[2]
#      umem = uline[3]
#      utype = uline[4]
#      if svr_ip == uip and svr_port == uport and tenant_id == utid:
#        for mpline in res_svr_memstore_percent:
#          mpip = mpline[0]
#          mpport = mpline[1]
#          if mpip == uip and mpport == uport:
#            mspercent = int(mpline[3])
#            mem_limit = umem
#            if 0 == utype:
#              # full类型的unit需要为memstore预留内存
#              mem_limit = umem * (100 - mspercent) / 100
#            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
#            if part_num_limit <= 1000:
#              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
#            if part_num >= (part_num_limit - part_num_reserved):
#              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
#            break
#  logging.info('check tenant partition num success')
#
## 11. 检查存在租户partition，但是不存在unit的observer
#def check_tenant_resource(query_cur):
#  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
#  for line in res_unit:
#    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
#  logging.info("check tenant resource success")
#
## 12. 检查系统表(__all_table_history)索引生效情况
#def check_sys_index_status(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where data_table_id = 1099511627890 and table_id = 1099511637775 and index_type = 1 and index_status = 2""")
#  if len(results) != 1 or results[0][0] != 1:
#    raise MyError("""__all_table_history's index status not valid""")
#  logging.info("""check __all_table_history's index status success""")
#
## 14. 检查升级前是否有只读zone
#def check_readonly_zone(query_cur):
#   (desc, results) = query_cur.exec_query("""select count(*) from __all_zone where name='zone_type' and info='ReadOnly'""")
#   if results[0][0] != 0:
#       raise MyError("""check_readonly_zone failed, ob2.2 not support readonly_zone""")
#   logging.info("""check_readonly_zone success""")
#
## 16. 修改永久下线的时间，避免升级过程中缺副本
#def modify_server_permanent_offline_time(cur):
#  set_parameter(cur, 'server_permanent_offline_time', '72h')
#
## 17. 修改安全删除副本时间
#def modify_replica_safe_remove_time(cur):
#  set_parameter(cur, 'replica_safe_remove_time', '72h')
#
## 18. 检查progressive_merge_round都升到1
#
## 19. 从小于224的版本升级上来时，需要确认high_priority_net_thread_count配置项值为0 (224版本开始该值默认为1)
#def check_high_priority_net_thread_count_before_224(query_cur):
#  # 获取最小版本
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('distinct observer version exist')
#  elif cmp(results[0][0], "2.2.40") >= 0 :
#    # 最小版本大于等于2.2.40，忽略检查
#    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check high_priority_net_thread_count'.format(results[0][0]))
#  else:
#    # 低于224版本的需要确认配置项值为0
#    logging.info('cluster version is ({0}), need check high_priority_net_thread_count'.format(results[0][0]))
#    (desc, results) = query_cur.exec_query("""select count(*) from __all_sys_parameter where name like 'high_priority_net_thread_count' and value not like '0'""")
#    if results[0][0] > 0:
#        raise MyError('high_priority_net_thread_count is greater than 0, unexpected')
#    logging.info('check high_priority_net_thread_count finished')
#
## 20. 从小于226的版本升级上来时，要求不能有备库存在
#def check_standby_cluster(query_cur):
#  # 获取最小版本
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('distinct observer version exist')
#  elif cmp(results[0][0], "2.2.60") >= 0 :
#    # 最小版本大于等于2.2.60，忽略检查
#    logging.info('cluster version ({0}) is greate than or equal to 2.2.60, need not check standby cluster'.format(results[0][0]))
#  else:
#    logging.info('cluster version is ({0}), need check standby cluster'.format(results[0][0]))
#    (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where table_name = '__all_cluster'""")
#    if results[0][0] == 0:
#      logging.info('cluster ({0}) has no __all_cluster table, no standby cluster'.format(results[0][0]))
#    else:
#      (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_cluster""")
#      if results[0][0] > 1:
#        raise MyError("""multiple cluster exist in __all_cluster, maybe standby clusters added, not supported""")
#  logging.info('check standby cluster from __all_cluster success')
#
## 21. 3.0是barrier版本，要求列column_id修正的升级任务做完
#def check_schema_split_v2_finish(query_cur):
#  # 获取最小版本
#  sql = """select cast(column_value as signed) as version from __all_core_table
#           where table_name='__all_global_stat' and column_name = 'split_schema_version_v2'"""
#  (desc, results) = query_cur.exec_query(sql)
#  if len(results) != 1 or len(results[0]) != 1:
#    raise MyError('row or column cnt not match')
#  elif results[0][0] < 0:
#    raise MyError('schema split v2 not finished yet')
#  else:
#    logging.info('check schema split v2 finish success')
#
#
#
## 23. 检查是否有异常租户(creating，延迟删除，恢复中)
#def check_tenant_status(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) as count from __all_tenant where status != 'TENANT_STATUS_NORMAL'""")
#  if len(results) != 1 or len(results[0]) != 1:
#    raise MyError('results len not match')
#  elif 0 != results[0][0]:
#    raise MyError('has abnormal tenant, should stop')
#  else:
#    logging.info('check tenant status success')
#
## 24. 所有版本升级都要检查micro_block_merge_verify_level
#def check_micro_block_verify_level(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(1) from __all_virtual_sys_parameter_stat where name='micro_block_merge_verify_level' and value < 2""")
#  if results[0][0] != 0:
#      raise MyError("""unexpected micro_block_merge_verify_level detected, upgrade is not allowed temporarily""")
#  logging.info('check micro_block_merge_verify_level success')
#
##25. 需要使用最大性能模式升级，227版本修改了模式切换相关的内部表
#def check_cluster_protection_mode(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) from __all_core_table where table_name = '__all_cluster' and column_name = 'protection_mode'""");
#  if len(results) != 1:
#    raise MyError('failed to get protection mode')
#  elif results[0][0] == 0:
#    logging.info('no need to check protection mode')
#  else:
#    (desc, results) = query_cur.exec_query("""select column_value from __all_core_table where table_name = '__all_cluster' and column_name = 'protection_mode'""");
#    if len(results) != 1:
#      raise MyError('failed to get protection mode')
#    elif cmp(results[0][0], '0') != 0:
#      raise MyError('cluster not maximum performance protection mode before update not allowed, protecion_mode={0}'.format(results[0][0]))
#    else:
#      logging.info('cluster protection mode legal before update!')
#
## 27. 检查无恢复任务
#def check_restore_job_exist(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(1) from __all_restore_job""")
#  if len(results) != 1 or len(results[0]) != 1:
#    raise MyError('failed to restore job cnt')
#  elif results[0][0] != 0:
#      raise MyError("""still has restore job, upgrade is not allowed temporarily""")
#  logging.info('check restore job success')
#
## 28. 检查系统租户系统表leader是否打散
#def check_sys_table_leader(query_cur):
#  (desc, results) = query_cur.exec_query("""select svr_ip, svr_port from oceanbase.__all_virtual_core_meta_table where role = 1""")
#  if len(results) != 1 or len(results[0]) != 2:
#    raise MyError('failed to rs leader')
#  else:
#    svr_ip = results[0][0]
#    svr_port = results[0][1]
#    # check __all_root_table's leader
#    (desc, results) = query_cur.exec_query("""select count(1) from oceanbase.__all_virtual_core_root_table
#                                           where role = 1 and svr_ip = '{0}' and svr_port = '{1}'""".format(svr_ip, svr_port))
#    if len(results) != 1 or len(results[0]) != 1:
#      raise MyError('failed to __all_root_table leader')
#    elif results[0][0] != 1:
#      raise MyError("""__all_root_table should be {0}:{1}""".format(svr_ip, svr_port))
#
#    # check sys tables' leader
#    (desc, results) = query_cur.exec_query("""select count(1) from oceanbase.__all_virtual_core_root_table
#                                           where tenant_id = 1 and role = 1 and (svr_ip != '{0}' or svr_port != '{1}')""" .format(svr_ip, svr_port))
#    if len(results) != 1 or len(results[0]) != 1:
#      raise MyError('failed to __all_root_table leader')
#    elif results[0][0] != 0:
#      raise MyError("""sys tables'leader should be {0}:{1}""".format(svr_ip, svr_port))
#
## 29. 检查版本号设置inner sql不走px.
#def check_and_modify_px_query(query_cur, cur):
#  (desc, results) = query_cur.exec_query("""select distinct value from oceanbase.__all_virtual_sys_parameter_stat where name = 'min_observer_version'""")
#  if (len(results) != 1) :
#    raise MyError('distinct observer version not exist')
#  elif cmp(results[0][0], "3.1.1") == 0 or cmp(results[0][0], "3.1.0") == 0:
#    if cmp(results[0][0], "3.1.1") == 0:
#      cur.execute("alter system set _ob_enable_px_for_inner_sql = false")
#    (desc, results) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
#    if (len(results) != 1) :
#      raise MyError('cluster role results is not valid')
#    elif (cmp(results[0][0], "PRIMARY") == 0):
#      tenant_id_list = []
#      (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
#      for r in results:
#        tenant_id_list.append(r[0])
#      for tenant_id in tenant_id_list:
#        cur.execute("alter system change tenant tenant_id = {0}".format(tenant_id))
#        sql = """set global _ob_use_parallel_execution = false"""
#        logging.info("tenant_id : %d , %s", tenant_id, sql)
#        cur.execute(sql)
#    elif (cmp(results[0][0], "PHYSICAL STANDBY") == 0):
#      sql = """set global _ob_use_parallel_execution = false"""
#      cur.execute(sql)
#      logging.info("execute sql in standby: %s", sql)
#    cur.execute("alter system change tenant tenant_id = 1")
#    time.sleep(5)
#    cur.execute("alter system flush plan cache global")
#    logging.info("execute: alter system flush plan cache global")
#  else:
#    logging.info('cluster version is not equal 3.1.1, skip px operate'.format(results[0][0]))
#
## 30. check duplicate index name in mysql
#def check_duplicate_index_name_in_mysql(query_cur, cur):
#  (desc, results) = query_cur.exec_query(
#                    """
#                    select /*+ OB_QUERY_TIMEOUT(100000000) */
#                    a.database_id, a.data_table_id, lower(substr(table_name, length(substring_index(a.table_name, "_", 4)) + 2)) as index_name
#                    from oceanbase.__all_virtual_table as a join oceanbase.__all_tenant as b on a.tenant_id = b.tenant_id
#                    where a.table_type = 5 and b.compatibility_mode = 0 and lower(table_name) like "__idx%" group by 1,2,3 having count(*) > 1
#                    """)
#  if (len(results) != 0) :
#    raise MyError("Duplicate index name exist in mysql tenant")
#
## 31. check the _max_trx_size
#def check_max_trx_size_config(query_cur, cur):
#  set_parameter(cur, '_max_trx_size', '100G')
#  logging.info('set _max_trx_size to default value 100G')
#
## 开始升级前的检查
#def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
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
#      check_observer_version(query_cur, upgrade_params)
#      check_paxos_replica(query_cur)
#      check_rebalance_task(query_cur)
#      check_cluster_status(query_cur)
#      check_disable_separate_sys_clog(query_cur)
#      check_macro_block_data_seq(query_cur)
#      check_tenant_resource_pool(query_cur)
#      check_tenant_status(query_cur)
#      check_tenant_part_num(query_cur)
#      check_tenant_resource(query_cur)
#      check_readonly_zone(query_cur)
#      modify_server_permanent_offline_time(cur)
#      modify_replica_safe_remove_time(cur)
#      check_high_priority_net_thread_count_before_224(query_cur)
#      check_standby_cluster(query_cur)
#      check_schema_split_v2_finish(query_cur)
#      check_micro_block_verify_level(query_cur)
#      check_restore_job_exist(query_cur)
#      check_sys_table_leader(query_cur)
#      check_and_modify_px_query(query_cur, cur)
#      check_duplicate_index_name_in_mysql(query_cur, cur)
#      check_max_trx_size_config(query_cur, cur)
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, log_filename)
#      do_check(host, port, user, password, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_cluster_health_checker.py
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
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
#Option('t', 'timeout', True, False, '600')
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
##### START ####
## 1. 检查paxos副本是否同步, paxos副本是否缺失
#def check_paxos_replica(query_cur):
#  # 2.1 检查paxos副本是否同步
#  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
#  if results[0][0] > 0 :
#    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
#  # 2.2 检查paxos副本是否有缺失 TODO
#  logging.info('check paxos replica success')
#
## 2. 检查是否有做balance, locality变更
#def check_rebalance_task(query_cur):
#  # 3.1 检查是否有做locality变更
#  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
#  if results[0][0] > 0 :
#    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
#  # 3.2 检查是否有做balance
#  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
#  if results[0][0] > 0 :
#    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
#  logging.info('check rebalance task success')
#
## 3. 检查集群状态
#def check_cluster_status(query_cur):
#  # 4.1 检查是否非合并状态
#  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
#  if cmp(results[0][0], 'IDLE')  != 0 :
#    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
#  logging.info('check cluster status success')
#  # 4.2 检查合并版本是否>=3
#  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
#  if results[0][0] < 2:
#      raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
#  logging.info('check global last_merged_version success')
#
## 4. 检查租户分区数是否超出内存限制
#def check_tenant_part_num(query_cur):
#  # 统计每个租户在各个server上的分区数量
#  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
#  # 计算每个租户在每个server上的max_memory
#  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
#  # 查询每个server的memstore_limit_percentage
#  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
#  part_static_cost = 128 * 1024
#  part_dynamic_cost = 400 * 1024
#  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
#  part_num_reserved = 512
#  for line in res_part_num:
#    svr_ip = line[0]
#    svr_port = line[1]
#    tenant_id = line[2]
#    part_num = line[3]
#    for uline in res_unit_memory:
#      uip = uline[0]
#      uport = uline[1]
#      utid = uline[2]
#      umem = uline[3]
#      utype = uline[4]
#      if svr_ip == uip and svr_port == uport and tenant_id == utid:
#        for mpline in res_svr_memstore_percent:
#          mpip = mpline[0]
#          mpport = mpline[1]
#          if mpip == uip and mpport == uport:
#            mspercent = int(mpline[3])
#            mem_limit = umem
#            if 0 == utype:
#              # full类型的unit需要为memstore预留内存
#              mem_limit = umem * (100 - mspercent) / 100
#            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
#            if part_num_limit <= 1000:
#              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
#            if part_num >= (part_num_limit - part_num_reserved):
#              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
#            break
#  logging.info('check tenant partition num success')
#
## 5. 检查存在租户partition，但是不存在unit的observer
#def check_tenant_resource(query_cur):
#  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
#  for line in res_unit:
#    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
#  logging.info("check tenant resource success")
#
## 6. 检查progressive_merge_round都升到1
#def check_progressive_merge_round(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id = 0""")
#  if results[0][0] != 0:
#    raise MyError("""progressive_merge_round of main table should all be 1""")
#  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4) and data_table_id > 0 and data_table_id in (select table_id from __all_virtual_table where table_type not in (1,2,4) and data_table_id = 0)""")
#  if results[0][0] != 0:
#    raise MyError("""progressive_merge_round of index should all be 1""")
#  logging.info("""check progressive_merge_round status success""")
#
## 主库状态检查
#def check_primary_cluster_sync_status(query_cur, timeout):
#  (desc, res) = query_cur.exec_query("""select  current_scn  from oceanbase.v$ob_cluster where cluster_role='PRIMARY' and cluster_status='VALID'""")
#  if len(res) != 1:
#      raise MyError('query results count is not 1')
#  query_sql = "select count(*) from oceanbase.v$ob_standby_status where cluster_role != 'PHYSICAL STANDBY' or cluster_status != 'VALID' or current_scn < {0}".format(res[0][0]);  
#  times = timeout
#  print times
#  while times > 0 :
#    (desc, res1) = query_cur.exec_query(query_sql)
#    if len(res1) == 1 and res1[0][0] == 0:
#      break;
#    time.sleep(1)
#    times -=1
#  if times == 0:
#    raise MyError("there exists standby cluster not synchronizing, checking primary cluster status failed!!!")
#  else:
#    logging.info("check primary cluster sync status success")
#
## 备库状态检查
#def check_standby_cluster_sync_status(query_cur, timeout):
#  (desc, res) = query_cur.exec_query("""select time_to_usec(now(6)) from dual""")
#  query_sql = "select count(*) from oceanbase.v$ob_cluster where (cluster_role != 'PHYSICAL STANDBY') or (cluster_status != 'VALID') or (current_scn  < {0}) or (switchover_status != 'NOT ALLOWED')".format(res[0][0]);
#  times = timeout
#  while times > 0 :
#    (desc, res2) = query_cur.exec_query(query_sql)
#    if len(res2) == 1 and res2[0][0] == 0:
#      break
#    time.sleep(1)
#    times -= 1
#  if times == 0:
#    raise MyError('current standby cluster not synchronizing, please check!!!')
#  else:
#    logging.info("check standby cluster sync status success")
#
## 判断是主库还是备库
#def check_cluster_sync_status(query_cur, timeout):
#  (desc, res) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
#  if res[0][0] == 'PRIMARY':
#    check_primary_cluster_sync_status(query_cur, timeout)
#  else:
#    check_standby_cluster_sync_status(query_cur, timeout)
#  
#
## 开始升级前的检查
#def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout):
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
#      query_cur = QueryCursor(cur)
#      check_paxos_replica(query_cur)
#      check_rebalance_task(query_cur)
#      check_cluster_status(query_cur)
#      check_tenant_part_num(query_cur)
#      check_tenant_resource(query_cur)
#      check_cluster_sync_status(query_cur, timeout)
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s', \
#          host, port, user, password, log_filename, timeout)
#      do_check(host, port, user, password, upgrade_params, timeout)
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
#import getopt
#import time
#
#class UpgradeParams:
#  log_filename = 'upgrade_post_checker.log'
#  new_version = '3.1.4'
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
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
##### STAR
## 1 检查版本号
#def check_cluster_version(query_cur):
#  # 一方面配置项生效是个异步生效任务,另一方面是2.2.0之后新增租户级配置项刷新，和系统级配置项刷新复用同一个timer，这里暂且等一下。
#  times = 30
#  sql="select distinct value = '{0}' from oceanbase.__all_virtual_sys_parameter_stat where name='min_observer_version'".format(upgrade_params.new_version)
#  while times > 0 :
#    (desc, results) = query_cur.exec_query(sql)
#    if len(results) == 1 and results[0][0] == 1:
#      break;
#    time.sleep(1)
#    times -=1
#  if times == 0:
#    logging.warn("check cluster version timeout!")
#    raise e
#  else:
#    logging.info("check_cluster_version success")
#
#def check_storage_format_version(query_cur):
#  # Specified expected version each time want to upgrade (see OB_STORAGE_FORMAT_VERSION_MAX)
#  expect_version = 4;
#  sql = "select value from oceanbase.__all_zone where zone = '' and name = 'storage_format_version'"
#  times = 180
#  while times > 0 :
#    (desc, results) = query_cur.exec_query(sql)
#    if len(results) == 1 and results[0][0] == expect_version:
#      break
#    time.sleep(10)
#    times -= 1
#  if times == 0:
#    logging.warn("check storage format version timeout! Expected version {0}".format(expect_version))
#    raise e
#  else:
#    logging.info("check expected storage format version '{0}' success".format(expect_version))
#
#def upgrade_table_schema_version(conn, cur):
#  try:
#    sql = """SELECT * FROM v$ob_cluster
#             WHERE cluster_role = "PRIMARY"
#             AND cluster_status = "VALID"
#             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
#    (desc, results) = cur.exec_query(sql)
#    is_primary = len(results) > 0
#    if is_primary:
#      sql = "set @@session.ob_query_timeout = 60000000;"
#      logging.info(sql)
#      cur.exec_sql(sql)
#
#      sql = "alter system run job 'UPDATE_TABLE_SCHEMA_VERSION';"
#      logging.info(sql)
#      cur.exec_sql(sql)
#    else:
#      logging.info("standby cluster no need to run job update_table_schema_ersion")
#  except Exception, e:
#    logging.warn("update table schema failed")
#    raise MyError("update table schema failed")
#  logging.info("update table schema finish")
#
#def upgrade_storage_format_version(conn, cur):
#  try:
#    # enable_ddl
#    set_parameter(cur, 'enable_ddl', 'True')
#
#    # run job
#    sql = "alter system run job 'UPGRADE_STORAGE_FORMAT_VERSION';"
#    logging.info(sql)
#    cur.execute(sql)
#
#  except Exception, e:
#    logging.warn("upgrade storage format version failed")
#    raise e
#  logging.info("upgrade storage format version finish")
#
## 2 检查内部表自检是否成功
#def check_root_inspection(query_cur):
#  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where info != 'succeed'"
#  times = 180
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
#def enable_ddl(cur):
#  set_parameter(cur, 'enable_ddl', 'True')
#
## 5 打开rebalance
#def enable_rebalance(cur):
#  set_parameter(cur, 'enable_rebalance', 'True')
#
## 6 打开rereplication
#def enable_rereplication(cur):
#  set_parameter(cur, 'enable_rereplication', 'True')
#
## 7 打开major freeze
#def enable_major_freeze(cur):
#  set_parameter(cur, 'enable_major_freeze', 'True')
#  
## 8 打开sql走px
#def enable_px_inner_sql(query_cur, cur):
#  cur.execute("alter system set _ob_enable_px_for_inner_sql = True")
#  (desc, results) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
#  if (len(results) != 1) :
#    raise MyError('cluster role results is not valid')
#  elif (cmp(results[0][0], "PRIMARY") == 0):
#    tenant_id_list = fetch_tenant_ids(query_cur)
#    for tenant_id in tenant_id_list:
#      cur.execute("alter system change tenant tenant_id = {0}".format(tenant_id))
#      sql = """set global _ob_use_parallel_execution = true"""
#      logging.info("tenant_id : %d , %s", tenant_id, sql)
#      cur.execute(sql)
#  elif (cmp(results[0][0], "PHYSICAL STANDBY") == 0):
#    sql = """set global _ob_use_parallel_execution = true"""
#    cur.execute(sql)
#    logging.info("execute sql in standby: %s", sql)
#
#def execute_schema_split_v2(conn, cur, query_cur, user, pwd):
#  try:
#    # check local cluster finish schema split
#    done = check_schema_split_v2_finish(query_cur)
#    if not done:
#      ###### disable ddl
#      set_parameter(cur, 'enable_ddl', 'False')
#
#      # run job
#      sql = """alter system run job 'SCHEMA_SPLIT_V2'"""
#      logging.info("""run job 'SCHEMA_SPLIT_V2'""")
#      query_cur.exec_sql(sql)
#      # check schema split v2 finish
#      check_schema_split_v2_finish_until_timeout(query_cur, 360)
#
#    # primary cluster should wait standby clusters' schema split results
#    is_primary = check_current_cluster_is_primary(query_cur)
#    if is_primary:
#      standby_cluster_list = fetch_standby_cluster_infos(conn, query_cur, user, pwd)
#      for standby_cluster in standby_cluster_list:
#        # connect
#        logging.info("start to check schema split result by cluster: cluster_id = {0}"
#                     .format(standby_cluster['cluster_id']))
#        logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                     .format(standby_cluster['cluster_id'],
#                             standby_cluster['ip'],
#                             standby_cluster['port']))
#        tmp_conn = mysql.connector.connect(user     =  standby_cluster['user'],
#                                           password =  standby_cluster['pwd'],
#                                           host     =  standby_cluster['ip'],
#                                           port     =  standby_cluster['port'],
#                                           database =  'oceanbase')
#        tmp_cur = tmp_conn.cursor(buffered=True)
#        tmp_conn.autocommit = True
#        tmp_query_cur = Cursor(tmp_cur)
#        # check if stanby cluster
#        is_primary = check_current_cluster_is_primary(tmp_query_cur)
#        if is_primary:
#          logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                            .format(standby_cluster['cluster_id'],
#                                    standby_cluster['ip'],
#                                    standby_cluster['port']))
#          raise e
#        # check schema split finish
#        check_schema_split_v2_finish_until_timeout(tmp_query_cur, 180)
#        # close
#        tmp_cur.close()
#        tmp_conn.close()
#        logging.info("""check schema split result success : cluster_id = {0}, ip = {1}, port = {2}"""
#                        .format(standby_cluster['cluster_id'],
#                                standby_cluster['ip'],
#                                standby_cluster['port']))
#  except Exception, e:
#    logging.warn("execute schema_split_v2 failed")
#    raise e
#  logging.info("execute schema_split_v2 success")
#
#def check_schema_split_v2_finish(query_cur):
#  done = False;
#  sql = "select count(*) from oceanbase.__all_virtual_upgrade_inspection where name = 'SCHEMA_SPLIT_V2' and info = 'succeed'"
#  logging.info(sql)
#  (desc, results) = query_cur.exec_query(sql)
#  if 1 != len(results) or 1 != len(results[0]):
#    logging.warn("should has one record")
#    raise e
#  elif 1 == results[0][0]:
#    done = True
#  else:
#    done = False
#  return done
#
#def check_schema_split_v2_finish_until_timeout(query_cur, times):
#  while times > 0:
#    done = check_schema_split_v2_finish(query_cur)
#    if done:
#      break;
#    else:
#      times -= 1
#      time.sleep(10)
#    if 0 == times:
#      logging.warn('check schema split v2 timeout!')
#      raise e
#  logging.info("check schema split v2 success")
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
#def check_current_cluster_is_primary(query_cur):
#  try:
#    sql = """SELECT * FROM v$ob_cluster
#             WHERE cluster_role = "PRIMARY"
#             AND cluster_status = "VALID"
#             AND (switchover_status = "NOT ALLOWED" OR switchover_status = "TO STANDBY") """
#    (desc, results) = query_cur.exec_query(sql)
#    is_primary = len(results) > 0
#    return is_primary
#  except Exception, e:
#    logging.exception("""fail to check current is primary""")
#    raise e
#
#def fetch_standby_cluster_infos(conn, query_cur, user, pwd):
#  try:
#    is_primary = check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.exception("""should be primary cluster""")
#      raise e
#
#    standby_cluster_infos = []
#    sql = """SELECT cluster_id, rootservice_list from v$ob_standby_status"""
#    (desc, results) = query_cur.exec_query(sql)
#
#    for r in results:
#      standby_cluster_info = {}
#      if 2 != len(r):
#        logging.exception("length not match")
#        raise e
#      standby_cluster_info['cluster_id'] = r[0]
#      standby_cluster_info['user'] = user
#      standby_cluster_info['pwd'] = pwd
#      # construct ip/port
#      address = r[1].split(";")[0] # choose first address in rs_list
#      standby_cluster_info['ip'] = str(address.split(":")[0])
#      standby_cluster_info['port'] = address.split(":")[2]
#      # append
#      standby_cluster_infos.append(standby_cluster_info)
#      logging.info("""cluster_info :  cluster_id = {0}, ip = {1}, port = {2}"""
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#    conn.commit()
#    # check standby cluster
#    for standby_cluster_info in standby_cluster_infos:
#      # connect
#      logging.info("""create connection : cluster_id = {0}, ip = {1}, port = {2}"""
#                   .format(standby_cluster_info['cluster_id'],
#                           standby_cluster_info['ip'],
#                           standby_cluster_info['port']))
#
#      tmp_conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                         password =  standby_cluster_info['pwd'],
#                                         host     =  standby_cluster_info['ip'],
#                                         port     =  standby_cluster_info['port'],
#                                         database =  'oceanbase')
#
#      tmp_cur = tmp_conn.cursor(buffered=True)
#      tmp_conn.autocommit = True
#      tmp_query_cur = Cursor(tmp_cur)
#      is_primary = check_current_cluster_is_primary(tmp_query_cur)
#      if is_primary:
#        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                          .format(standby_cluster_info['cluster_id'],
#                                  standby_cluster_info['ip'],
#                                  standby_cluster_info['port']))
#        raise e
#      # close
#      tmp_cur.close()
#      tmp_conn.close()
#
#    return standby_cluster_infos
#  except Exception, e:
#    logging.exception('fail to fetch standby cluster info')
#    raise e
#
#def check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_ids):
#  try:
#    conn.commit()
#    # check if need check ddl and dml sync
#    is_primary = check_current_cluster_is_primary(query_cur)
#    if not is_primary:
#      logging.exception("""should be primary cluster""")
#      raise e
#
#    # fetch sys stats
#    sys_infos = []
#    sql = """SELECT tenant_id,
#                    refreshed_schema_version,
#                    min_sys_table_scn,
#                    min_user_table_scn
#             FROM oceanbase.v$ob_cluster_stats
#             ORDER BY tenant_id desc"""
#    (desc, results) = query_cur.exec_query(sql)
#    if len(tenant_ids) != len(results):
#      logging.exception("result not match")
#      raise e
#    else:
#      for i in range(len(results)):
#        if len(results[i]) != 4:
#          logging.exception("length not match")
#          raise e
#        elif results[i][0] != tenant_ids[i]:
#          logging.exception("tenant_id not match")
#          raise e
#        else:
#          sys_info = {}
#          sys_info['tenant_id'] = results[i][0]
#          sys_info['refreshed_schema_version'] = results[i][1]
#          sys_info['min_sys_table_scn'] = results[i][2]
#          sys_info['min_user_table_scn'] = results[i][3]
#          logging.info("sys info : {0}".format(sys_info))
#          sys_infos.append(sys_info)
#    conn.commit()
#
#    # check ddl and dml by cluster
#    for standby_cluster_info in standby_cluster_infos:
#      check_ddl_and_dml_sync_by_cluster(standby_cluster_info, sys_infos)
#
#  except Exception, e:
#    logging.exception("fail to check ddl and dml sync")
#    raise e
#
#def check_ddl_and_dml_sync_by_tenant(query_cur, sys_info):
#  try:
#    times = 1800 # 30min
#    logging.info("start to check ddl and dml sync by tenant : {0}".format(sys_info))
#    start_time = time.time()
#    sql = ""
#    if 1 == sys_info['tenant_id'] :
#      # 备库系统租户DML不走物理同步，需要升级脚本负责写入，系统租户仅校验DDL同步
#      sql = """SELECT count(*)
#               FROM oceanbase.v$ob_cluster_stats
#               WHERE tenant_id = {0}
#                     AND refreshed_schema_version >= {1}
#            """.format(sys_info['tenant_id'],
#                       sys_info['refreshed_schema_version'])
#    else:
#      sql = """SELECT count(*)
#               FROM oceanbase.v$ob_cluster_stats
#               WHERE tenant_id = {0}
#                     AND refreshed_schema_version >= {1}
#                     AND min_sys_table_scn >= {2}
#                     AND min_user_table_scn >= {3}
#            """.format(sys_info['tenant_id'],
#                       sys_info['refreshed_schema_version'],
#                       sys_info['min_sys_table_scn'],
#                       sys_info['min_user_table_scn'])
#    while times > 0 :
#      (desc, results) = query_cur.exec_query(sql)
#      if len(results) == 1 and results[0][0] == 1:
#        break;
#      time.sleep(1)
#      times -= 1
#    if times == 0:
#      logging.exception("check ddl and dml sync timeout! : {0}, cost = {1}"
#                    .format(sys_info, time.time() - start_time))
#      raise e
#    else:
#      logging.info("check ddl and dml sync success! : {0}, cost = {1}"
#                   .format(sys_info, time.time() - start_time))
#
#  except Exception, e:
#    logging.exception("fail to check ddl and dml sync : {0}".format(sys_info))
#    raise e
#
## 开始升级后的检查
#def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
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
#      try:
#        check_cluster_version(query_cur)
#        upgrade_table_schema_version(conn, query_cur)
#        check_root_inspection(query_cur)
#        enable_ddl(cur)
#        enable_rebalance(cur)
#        enable_rereplication(cur)
#        enable_major_freeze(cur)
#        enable_px_inner_sql(query_cur, cur)
#      except Exception, e:
#        logging.exception('run error')
#        raise e
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, log_filename)
#      do_check(host, port, user, password, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_rolling_post.py
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
#  log_filename = 'upgrade_rolling_post.log'
##### --------------start : my_error.py --------------
#class MyError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
#
##### --------------start : actions.py--------------
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
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
#def run(my_host, my_port, my_user, my_passwd, upgrade_params):
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
#      query_cur = QueryCursor(cur)
#      (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#      if len(results) != 1:
#          raise MyError('distinct observer version not exist')
#      #rolling upgrade 在2.2.50版本后支持
#      elif cmp(results[0][0], "2.2.50") >= 0:
#          query_cur.exec_sql("""ALTER SYSTEM END ROLLING UPGRADE""")
#          logging.info("END ROLLING UPGRADE success")
#      else:
#          logging.info("cluster version ({0}) less than 2.2.50, skip".format(results[0][0]))
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, log_filename)
#      run(host, port, user, password, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_rolling_pre.py
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
#  log_filename = 'upgrade_rolling_pre.log'
##### --------------start : my_error.py --------------
#class MyError(Exception):
#  def __init__(self, value):
#    self.value = value
#  def __str__(self):
#    return repr(self.value)
#
##### --------------start : actions.py--------------
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
#'\n\n' +\
#'Maybe you want to run cmd like that:\n' +\
#sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'
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
#def run(my_host, my_port, my_user, my_passwd, upgrade_params):
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
#      query_cur = QueryCursor(cur)
#      (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#      if len(results) != 1:
#          raise MyError('distinct observer version not exist')
#      #rolling upgrade 在2.2.50版本后支持
#      elif cmp(results[0][0], "2.2.50") >= 0:
#          query_cur.exec_sql("""ALTER SYSTEM BEGIN ROLLING UPGRADE""")
#          logging.info("BEGIN ROLLING UPGRADE success")
#      else:
#          logging.info("cluster version ({0}) less than 2.2.50, skip".format(results[0][0]))
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
#      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
#          host, port, user, password, log_filename)
#      run(host, port, user, password, upgrade_params)
#    except mysql.connector.Error, e:
#      logging.exception('mysql connctor error')
#      raise e
#    except Exception, e:
#      logging.exception('normal error')
#      raise e
#
####====XXXX======######==== I am a splitter ====######======XXXX====####
#filename:upgrade_sys_vars.py
##!/usr/bin/env python
## -*- coding: utf-8 -*-
#
#import new
#import time
#import re
#import json
#import traceback
#import sys
#import mysql.connector
#from mysql.connector import errorcode
#import logging
#from my_error import MyError
#import actions
#from actions import DMLCursor
#from actions import QueryCursor
#from sys_vars_dict import sys_var_dict
#import my_utils
#
#
## 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户
#
#def calc_diff_sys_var(cur, tenant_id):
#  try:
#    change_tenant(cur, tenant_id)
#    actual_tenant_id = get_actual_tenant_id(tenant_id)
#    cur.execute("""select name, data_type, value, info, flags, min_val, max_val from __all_sys_variable_history where tenant_id=%s and (tenant_id, zone, name, schema_version) in (select tenant_id, zone, name, max(schema_version) from __all_sys_variable_history where tenant_id=%s group by tenant_id, zone, name);"""%(actual_tenant_id, actual_tenant_id))
#    results = cur.fetchall()
#    logging.info('there has %s system variable of tenant id %d', len(results), tenant_id)
#    update_sys_var_list = []
#    update_sys_var_ori_list = []
#    add_sys_var_list = []
#    for r in results:
#      if sys_var_dict.has_key(r[0]):
#        sys_var = sys_var_dict[r[0]]
#        if long(sys_var["data_type"]) != long(r[1]) or sys_var["info"].strip() != r[3].strip() or long(sys_var["flags"]) != long(r[4]) or ("min_val" in sys_var.keys() and sys_var["min_val"] != r[5]) or ("max_val" in sys_var.keys() and sys_var["max_val"] != r[6]):
#          update_sys_var_list.append(sys_var)
#          update_sys_var_ori_list.append(r)
#    for (name, sys_var) in sys_var_dict.items():
#      sys_var_exist = 0
#      for r in results:
#        if r[0] == sys_var["name"]:
#          sys_var_exist = 1
#          break
#      if 0 == sys_var_exist:
#        add_sys_var_list.append(sys_var)
#    # reset
#    sys_tenant_id = 1
#    change_tenant(cur, sys_tenant_id)
#    return (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list)
#  except Exception, e:
#    logging.exception('fail to calc diff sys var')
#    raise e
#
#def gen_update_sys_var_sql_for_tenant(tenant_id, sys_var):
#  actual_tenant_id = get_actual_tenant_id(tenant_id)
#  update_sql = 'update oceanbase.__all_sys_variable set data_type = ' + str(sys_var["data_type"])\
#      + ', info = \'' + sys_var["info"].strip() + '\', flags = ' + str(sys_var["flags"])
#  update_sql = update_sql\
#      + ((', min_val = \'' + sys_var["min_val"] + '\'') if "min_val" in sys_var.keys() else '')\
#      + ((', max_val = \'' + sys_var["max_val"] + '\'') if "max_val" in sys_var.keys() else '')
#  update_sql = update_sql + ' where tenant_id = ' + str(actual_tenant_id) + ' and name = \'' + sys_var["name"] + '\''
#  return update_sql
#
#def gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
#  try:
#    actual_tenant_id = get_actual_tenant_id(tenant_id)
#    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
#                                            where tenant_id = {0} and name = '{1}'
#                                            order by schema_version desc limit 1"""
#                                            .format(actual_tenant_id, sys_var["name"]))
#    schema_version = results[0][0]
#    (desc, results) = dml_cur.exec_query("""select value from __all_sys_variable where tenant_id={0} and name='{1}' limit 1"""
#                                         .format(actual_tenant_id, sys_var["name"]))
#    res_len = len(results)
#    if res_len != 1:
#      logging.error('fail to get value from __all_sys_variable, result count:'+ str(res_len))
#      raise MyError('fail to get value from __all_sys_variable')
#    value = results[0][0]
#    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
#    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
#    replace_sql = """replace into oceanbase.__all_sys_variable_history(
#                          tenant_id,
#                          zone,
#                          name,
#                          schema_version,
#                          is_deleted,
#                          data_type,
#                          value,
#                          info,
#                          flags,
#                          min_val,
#                          max_val)
#                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
#                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], value, sys_var["info"], sys_var["flags"], min_val, max_val)
#    return replace_sql
#  except Exception, e:
#    logging.exception('fail to gen replace sys var history sql')
#    raise e
#
#def gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
#  try:
#    actual_tenant_id = get_actual_tenant_id(tenant_id)
#    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
#                                            where tenant_id={0} order by schema_version asc limit 1""".format(actual_tenant_id))
#    schema_version = results[0][0]
#    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
#    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
#    replace_sql = """replace into oceanbase.__all_sys_variable_history(
#                          tenant_id,
#                          zone,
#                          name,
#                          schema_version,
#                          is_deleted,
#                          data_type,
#                          value,
#                          info,
#                          flags,
#                          min_val,
#                          max_val)
#                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
#                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], sys_var["value"], sys_var["info"], sys_var["flags"], min_val, max_val)
#    return replace_sql
#  except Exception, e:
#    logging.exception('fail to gen replace sys var history sql')
#    raise e
#
#
#def gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list):
#  update_sqls = ''
#  for i in range(0, len(update_sys_var_list)):
#    sys_var = update_sys_var_list[i]
#    if i > 0:
#      update_sqls += '\n'
#    update_sqls += gen_update_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
#    update_sqls += gen_update_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
#  return update_sqls
#
#def update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list):
#  try:
#    for i in range(0, len(update_sys_var_list)):
#      sys_var = update_sys_var_list[i]
#      update_sql = gen_update_sys_var_sql_for_tenant(tenant_id, sys_var)
#      rowcount = dml_cur.exec_update(update_sql)
#      if 1 != rowcount:
#        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
#        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
#      else:
#        logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
##replace update sys var to __all_sys_variable_history
#      replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
#      rowcount = dml_cur.exec_update(replace_sql)
#      if 1 != rowcount and 2 != rowcount:
#        logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
#        raise MyError('fail to repalce sysvar')
#      else:
#        logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
#  except Exception, e:
#    logging.exception('fail to update for tenant, tenant_id: %d', tenant_id)
#    raise e
#
#def gen_add_sys_var_sql_for_tenant(tenant_id, sys_var):
#  actual_tenant_id = get_actual_tenant_id(tenant_id)
#  add_sql = 'replace into oceanbase.__all_sys_variable(tenant_id, zone, name, data_type, value, info, flags, min_val, max_val) values('\
#      + str(actual_tenant_id) +', \'\', \'' + sys_var["name"] + '\', ' + str(sys_var["data_type"]) + ', \'' + sys_var["value"] + '\', \''\
#      + sys_var["info"].strip() + '\', ' + str(sys_var["flags"]) + ', \''
#  add_sql = add_sql + (sys_var["min_val"] if "min_val" in sys_var.keys() else '') + '\', \''\
#      + (sys_var["max_val"] if "max_val" in sys_var.keys() else '') + '\')'
#  return add_sql
#
#def gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list):
#  add_sqls = ''
#  for i in range(0, len(add_sys_var_list)):
#    sys_var = add_sys_var_list[i]
#    if i > 0:
#      add_sqls += '\n'
#    add_sqls += gen_add_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
#    add_sqls += gen_replace_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
#  return add_sqls
#
#def add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list):
#  try:
#    for i in range(0, len(add_sys_var_list)):
#      sys_var = add_sys_var_list[i]
#      add_sql = gen_add_sys_var_sql_for_tenant(tenant_id, sys_var)
#      rowcount = dml_cur.exec_update(add_sql)
#      if 1 != rowcount:
#        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
#        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
#      else:
#        logging.info('succeed to insert sys var for tenant, sql: %s, tenant_id: %d', add_sql, tenant_id)
#      replace_sql = gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
#      rowcount = dml_cur.exec_update(replace_sql)
#      if 1 != rowcount:
#        logging.error('fail to replace system variable history, sql:%s'%replace_sql)
#        raise MyError('fail to replace system variable history')
#      else:
#        logging.info('succeed to replace sys var for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
#  except Exception, e:
#    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
#    raise e
#
#
#def gen_sys_var_special_update_sqls_for_tenant(tenant_id):
#  special_update_sqls = ''
#  return special_update_sqls
#
#def special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, sys_var_name, sys_var_value):
#  try:
#    sys_var = None
#    for i in range(0, len(add_sys_var_list)):
#      if (sys_var_name == add_sys_var_list[i]["name"]):
#        sys_var = add_sys_var_list[i]
#        break;
#
#    if None == sys_var:
#      logging.info('%s is not new, no need special update again', sys_var_name)
#      return
#
#    sys_var["value"] = sys_var_value;
#    update_sql = gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var)
#    rowcount = dml_cur.exec_update(update_sql)
#    if 1 != rowcount:
#      # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
#      logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
#    else:
#      logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
#    #replace update sys var to __all_sys_variable_history
#    replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
#    rowcount = dml_cur.exec_update(replace_sql)
#    if 1 != rowcount and 2 != rowcount:
#      logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
#      raise MyError('fail to repalce sysvar')
#    else:
#      logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
#  except Exception, e:
#    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
#    raise e
#
#def get_sys_vars_upgrade_dmls_str(cur, query_cur, tenant_id_list, update_sys_var_list, add_sys_var_list):
#  ret_str = ''
#  if len(tenant_id_list) <= 0:
#    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
#    raise MyError('invalid arg')
#  for i in range(0, len(tenant_id_list)):
#    tenant_id = tenant_id_list[i]
#    change_tenant(cur, tenant_id)
#    if i > 0:
#      ret_str += '\n'
#    ret_str += gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list)
#  if ret_str != '' and len(add_sys_var_list) > 0:
#    ret_str += '\n'
#  for i in range(0, len(tenant_id_list)):
#    tenant_id = tenant_id_list[i]
#    change_tenant(cur, tenant_id)
#    if i > 0:
#      ret_str += '\n'
#    ret_str += gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list)
#  if ret_str != '' and gen_sys_var_special_update_sqls_for_tenant(tenant_id_list[0]) != '':
#    ret_str += '\n'
#  for i in range(0, len(tenant_id_list)):
#    tenant_id = tenant_id_list[i]
#    change_tenant(cur, tenant_id)
#    if i > 0:
#      ret_str += '\n'
#    ret_str += gen_sys_var_special_update_sqls_for_tenant(tenant_id)
#  sys_tenant_id= 1
#  change_tenant(cur, sys_tenant_id)
#  return ret_str
#
#def gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var):
#  update_sql = ('update oceanbase.__all_sys_variable set value = \'' + str(sys_var["value"])
#      + '\' where tenant_id = ' + str(tenant_id) + ' and name = \'' + sys_var["name"] + '\'')
#  return update_sql
#
## 修改相关实现需要调整ObUpgradeUtils::upgrade_sys_variable()
#def exec_sys_vars_upgrade_dml(cur, tenant_id_list):
#  if len(tenant_id_list) <= 0:
#    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
#    raise MyError('invalid arg')
#  dml_cur = DMLCursor(cur)
#  # 操作前先dump出oceanbase.__all_sys_variable表的所有数据
#  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable""")
#  # 操作前先dump出oceanbase.__all_sys_variable_history表的所有数据
#  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable_history""")
#
#  for i in range(0, len(tenant_id_list)):
#    tenant_id = tenant_id_list[i]
#    # calc diff
#    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
#    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
#    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
#    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
#    # update
#    change_tenant(cur, tenant_id)
#    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
#    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');
#  # reset
#  sys_tenant_id = 1
#  change_tenant(cur, sys_tenant_id)
#
#def exec_sys_vars_upgrade_dml_in_standby_cluster(standby_cluster_infos):
#  try:
#    for standby_cluster_info in standby_cluster_infos:
#      exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info)
#  except Exception, e:
#    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed""")
#    raise e
#
#def exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info):
#  try:
#
#    logging.info("exec_sys_vars_upgrade_dml_by_cluster : cluster_id = {0}, ip = {1}, port = {2}"
#                 .format(standby_cluster_info['cluster_id'],
#                         standby_cluster_info['ip'],
#                         standby_cluster_info['port']))
#    logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
#                 .format(standby_cluster_info['cluster_id'],
#                         standby_cluster_info['ip'],
#                         standby_cluster_info['port']))
#    conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
#                                   password =  standby_cluster_info['pwd'],
#                                   host     =  standby_cluster_info['ip'],
#                                   port     =  standby_cluster_info['port'],
#                                   database =  'oceanbase',
#                                   raise_on_warnings = True)
#    cur = conn.cursor(buffered=True)
#    conn.autocommit = True
#    dml_cur = DMLCursor(cur)
#    query_cur = QueryCursor(cur)
#    is_primary = actions.check_current_cluster_is_primary(query_cur)
#    if is_primary:
#      logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
#                        .format(standby_cluster_info['cluster_id'],
#                                standby_cluster_info['ip'],
#                                standby_cluster_info['port']))
#      raise e
#
#    # only update sys tenant in standby cluster
#    tenant_id = 1
#    # calc diff
#    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
#    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
#    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
#    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
#    # update
#    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
#    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
#    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');
#
#    cur.close()
#    conn.close()
#
#  except Exception, e:
#    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed :
#                         cluster_id = {0}, ip = {1}, port = {2}"""
#                         .format(standby_cluster_info['cluster_id'],
#                                 standby_cluster_info['ip'],
#                                 standby_cluster_info['port']))
#    raise e
#
#
#def get_actual_tenant_id(tenant_id):
#  return tenant_id if (1 == tenant_id) else 0;
#
#def change_tenant(cur, tenant_id):
#  # change tenant
#  sql = "alter system change tenant tenant_id = {0};".format(tenant_id)
#  logging.info(sql);
#  cur.execute(sql);
#  # check
#  sql = "select effective_tenant_id();"
#  cur.execute(sql)
#  result = cur.fetchall()
#  if (1 != len(result) or 1 != len(result[0])):
#    raise MyError("invalid result cnt")
#  elif (tenant_id != result[0][0]):
#    raise MyError("effective_tenant_id:{0} , tenant_id:{1}".format(result[0][0], tenant_id))
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
