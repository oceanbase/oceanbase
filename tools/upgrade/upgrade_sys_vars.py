#!/usr/bin/env python
# -*- coding: utf-8 -*-

import new
import time
import re
import json
import traceback
import sys
import mysql.connector
from mysql.connector import errorcode
import logging
from my_error import MyError
import actions
from actions import DMLCursor
from actions import QueryCursor
from sys_vars_dict import sys_var_dict
import my_utils


# 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户

def calc_diff_sys_var(cur, tenant_id):
  try:
    change_tenant(cur, tenant_id)
    actual_tenant_id = get_actual_tenant_id(tenant_id)
    cur.execute("""select name, data_type, value, info, flags, min_val, max_val from __all_sys_variable_history where tenant_id=%s and (tenant_id, zone, name, schema_version) in (select tenant_id, zone, name, max(schema_version) from __all_sys_variable_history where tenant_id=%s group by tenant_id, zone, name);"""%(actual_tenant_id, actual_tenant_id))
    results = cur.fetchall()
    logging.info('there has %s system variable of tenant id %d', len(results), tenant_id)
    update_sys_var_list = []
    update_sys_var_ori_list = []
    add_sys_var_list = []
    for r in results:
      if sys_var_dict.has_key(r[0]):
        sys_var = sys_var_dict[r[0]]
        if long(sys_var["data_type"]) != long(r[1]) or sys_var["info"].strip() != r[3].strip() or long(sys_var["flags"]) != long(r[4]) or ("min_val" in sys_var.keys() and sys_var["min_val"] != r[5]) or ("max_val" in sys_var.keys() and sys_var["max_val"] != r[6]):
          update_sys_var_list.append(sys_var)
          update_sys_var_ori_list.append(r)
    for (name, sys_var) in sys_var_dict.items():
      sys_var_exist = 0
      for r in results:
        if r[0] == sys_var["name"]:
          sys_var_exist = 1
          break
      if 0 == sys_var_exist:
        add_sys_var_list.append(sys_var)
    # reset
    sys_tenant_id = 1
    change_tenant(cur, sys_tenant_id)
    return (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list)
  except Exception, e:
    logging.exception('fail to calc diff sys var')
    raise e

def gen_update_sys_var_sql_for_tenant(tenant_id, sys_var):
  actual_tenant_id = get_actual_tenant_id(tenant_id)
  update_sql = 'update oceanbase.__all_sys_variable set data_type = ' + str(sys_var["data_type"])\
      + ', info = \'' + sys_var["info"].strip() + '\', flags = ' + str(sys_var["flags"])
  update_sql = update_sql\
      + ((', min_val = \'' + sys_var["min_val"] + '\'') if "min_val" in sys_var.keys() else '')\
      + ((', max_val = \'' + sys_var["max_val"] + '\'') if "max_val" in sys_var.keys() else '')
  update_sql = update_sql + ' where tenant_id = ' + str(actual_tenant_id) + ' and name = \'' + sys_var["name"] + '\''
  return update_sql

def gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
  try:
    actual_tenant_id = get_actual_tenant_id(tenant_id)
    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
                                            where tenant_id = {0} and name = '{1}'
                                            order by schema_version desc limit 1"""
                                            .format(actual_tenant_id, sys_var["name"]))
    schema_version = results[0][0]
    (desc, results) = dml_cur.exec_query("""select value from __all_sys_variable where tenant_id={0} and name='{1}' limit 1"""
                                         .format(actual_tenant_id, sys_var["name"]))
    res_len = len(results)
    if res_len != 1:
      logging.error('fail to get value from __all_sys_variable, result count:'+ str(res_len))
      raise MyError('fail to get value from __all_sys_variable')
    value = results[0][0]
    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
    replace_sql = """replace into oceanbase.__all_sys_variable_history(
                          tenant_id,
                          zone,
                          name,
                          schema_version,
                          is_deleted,
                          data_type,
                          value,
                          info,
                          flags,
                          min_val,
                          max_val)
                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], value, sys_var["info"], sys_var["flags"], min_val, max_val)
    return replace_sql
  except Exception, e:
    logging.exception('fail to gen replace sys var history sql')
    raise e

def gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var):
  try:
    actual_tenant_id = get_actual_tenant_id(tenant_id)
    (desc, results) = dml_cur.exec_query("""select schema_version from oceanbase.__all_sys_variable_history
                                            where tenant_id={0} order by schema_version asc limit 1""".format(actual_tenant_id))
    schema_version = results[0][0]
    min_val = sys_var["min_val"] if "min_val" in sys_var.keys() else ''
    max_val = sys_var["max_val"] if "max_val" in sys_var.keys() else ''
    replace_sql = """replace into oceanbase.__all_sys_variable_history(
                          tenant_id,
                          zone,
                          name,
                          schema_version,
                          is_deleted,
                          data_type,
                          value,
                          info,
                          flags,
                          min_val,
                          max_val)
                      values(%d, '', '%s', %d, 0, %d, '%s', '%s', %d, '%s', '%s')
                  """%(actual_tenant_id, sys_var["name"], schema_version, sys_var["data_type"], sys_var["value"], sys_var["info"], sys_var["flags"], min_val, max_val)
    return replace_sql
  except Exception, e:
    logging.exception('fail to gen replace sys var history sql')
    raise e


def gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list):
  update_sqls = ''
  for i in range(0, len(update_sys_var_list)):
    sys_var = update_sys_var_list[i]
    if i > 0:
      update_sqls += '\n'
    update_sqls += gen_update_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
    update_sqls += gen_update_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
  return update_sqls

def update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list):
  try:
    for i in range(0, len(update_sys_var_list)):
      sys_var = update_sys_var_list[i]
      update_sql = gen_update_sys_var_sql_for_tenant(tenant_id, sys_var)
      rowcount = dml_cur.exec_update(update_sql)
      if 1 != rowcount:
        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
      else:
        logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
#replace update sys var to __all_sys_variable_history
      replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
      rowcount = dml_cur.exec_update(replace_sql)
      if 1 != rowcount and 2 != rowcount:
        logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
        raise MyError('fail to repalce sysvar')
      else:
        logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
  except Exception, e:
    logging.exception('fail to update for tenant, tenant_id: %d', tenant_id)
    raise e

def gen_add_sys_var_sql_for_tenant(tenant_id, sys_var):
  actual_tenant_id = get_actual_tenant_id(tenant_id)
  add_sql = 'replace into oceanbase.__all_sys_variable(tenant_id, zone, name, data_type, value, info, flags, min_val, max_val) values('\
      + str(actual_tenant_id) +', \'\', \'' + sys_var["name"] + '\', ' + str(sys_var["data_type"]) + ', \'' + sys_var["value"] + '\', \''\
      + sys_var["info"].strip() + '\', ' + str(sys_var["flags"]) + ', \''
  add_sql = add_sql + (sys_var["min_val"] if "min_val" in sys_var.keys() else '') + '\', \''\
      + (sys_var["max_val"] if "max_val" in sys_var.keys() else '') + '\')'
  return add_sql

def gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list):
  add_sqls = ''
  for i in range(0, len(add_sys_var_list)):
    sys_var = add_sys_var_list[i]
    if i > 0:
      add_sqls += '\n'
    add_sqls += gen_add_sys_var_sql_for_tenant(tenant_id, sys_var) + ';\n'
    add_sqls += gen_replace_sys_var_history_sql_for_tenant(query_cur, tenant_id, sys_var) + ';'
  return add_sqls

def add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list):
  try:
    for i in range(0, len(add_sys_var_list)):
      sys_var = add_sys_var_list[i]
      add_sql = gen_add_sys_var_sql_for_tenant(tenant_id, sys_var)
      rowcount = dml_cur.exec_update(add_sql)
      if 1 != rowcount:
        # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
        logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
      else:
        logging.info('succeed to insert sys var for tenant, sql: %s, tenant_id: %d', add_sql, tenant_id)
      replace_sql = gen_replace_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
      rowcount = dml_cur.exec_update(replace_sql)
      if 1 != rowcount:
        logging.error('fail to replace system variable history, sql:%s'%replace_sql)
        raise MyError('fail to replace system variable history')
      else:
        logging.info('succeed to replace sys var for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
  except Exception, e:
    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
    raise e


def gen_sys_var_special_update_sqls_for_tenant(tenant_id):
  special_update_sqls = ''
  return special_update_sqls

def special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, sys_var_name, sys_var_value):
  try:
    sys_var = None
    for i in range(0, len(add_sys_var_list)):
      if (sys_var_name == add_sys_var_list[i]["name"]):
        sys_var = add_sys_var_list[i]
        break;

    if None == sys_var:
      logging.info('%s is not new, no need special update again', sys_var_name)
      return

    sys_var["value"] = sys_var_value;
    update_sql = gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var)
    rowcount = dml_cur.exec_update(update_sql)
    if 1 != rowcount:
      # 以history为准，考虑可重入，此处不校验__all_sys_variable的更新结果
      logging.info('sys var not change, just skip, sql: %s, tenant_id: %d', update_sql, tenant_id)
    else:
      logging.info('succeed to update sys var for tenant, sql: %s, tenant_id: %d', update_sql, tenant_id)
    #replace update sys var to __all_sys_variable_history
    replace_sql = gen_update_sys_var_history_sql_for_tenant(dml_cur, tenant_id, sys_var)
    rowcount = dml_cur.exec_update(replace_sql)
    if 1 != rowcount and 2 != rowcount:
      logging.error('fail to replace sysvar, replace_sql:%s'%replace_sql)
      raise MyError('fail to repalce sysvar')
    else:
      logging.info('succeed to replace sys var history for tenant, sql: %s, tenant_id: %d', replace_sql, tenant_id)
  except Exception, e:
    logging.exception('fail to add for tenant, tenant_id: %d', tenant_id)
    raise e

def get_sys_vars_upgrade_dmls_str(cur, query_cur, tenant_id_list, update_sys_var_list, add_sys_var_list):
  ret_str = ''
  if len(tenant_id_list) <= 0:
    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
    raise MyError('invalid arg')
  for i in range(0, len(tenant_id_list)):
    tenant_id = tenant_id_list[i]
    change_tenant(cur, tenant_id)
    if i > 0:
      ret_str += '\n'
    ret_str += gen_sys_var_update_sqls_for_tenant(query_cur, tenant_id, update_sys_var_list)
  if ret_str != '' and len(add_sys_var_list) > 0:
    ret_str += '\n'
  for i in range(0, len(tenant_id_list)):
    tenant_id = tenant_id_list[i]
    change_tenant(cur, tenant_id)
    if i > 0:
      ret_str += '\n'
    ret_str += gen_sys_var_add_sqls_for_tenant(query_cur, tenant_id, add_sys_var_list)
  if ret_str != '' and gen_sys_var_special_update_sqls_for_tenant(tenant_id_list[0]) != '':
    ret_str += '\n'
  for i in range(0, len(tenant_id_list)):
    tenant_id = tenant_id_list[i]
    change_tenant(cur, tenant_id)
    if i > 0:
      ret_str += '\n'
    ret_str += gen_sys_var_special_update_sqls_for_tenant(tenant_id)
  sys_tenant_id= 1
  change_tenant(cur, sys_tenant_id)
  return ret_str

def gen_update_sys_var_value_sql_for_tenant(tenant_id, sys_var):
  update_sql = ('update oceanbase.__all_sys_variable set value = \'' + str(sys_var["value"])
      + '\' where tenant_id = ' + str(tenant_id) + ' and name = \'' + sys_var["name"] + '\'')
  return update_sql

# 修改相关实现需要调整ObUpgradeUtils::upgrade_sys_variable()
def exec_sys_vars_upgrade_dml(cur, tenant_id_list):
  if len(tenant_id_list) <= 0:
    logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
    raise MyError('invalid arg')
  dml_cur = DMLCursor(cur)
  # 操作前先dump出oceanbase.__all_sys_variable表的所有数据
  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable""")
  # 操作前先dump出oceanbase.__all_sys_variable_history表的所有数据
  my_utils.query_and_dump_results(dml_cur, """select * from oceanbase.__all_virtual_sys_variable_history""")

  for i in range(0, len(tenant_id_list)):
    tenant_id = tenant_id_list[i]
    # calc diff
    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
    # update
    change_tenant(cur, tenant_id)
    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');
  # reset
  sys_tenant_id = 1
  change_tenant(cur, sys_tenant_id)

def exec_sys_vars_upgrade_dml_in_standby_cluster(standby_cluster_infos):
  try:
    for standby_cluster_info in standby_cluster_infos:
      exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info)
  except Exception, e:
    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed""")
    raise e

def exec_sys_vars_upgrade_dml_by_cluster(standby_cluster_info):
  try:

    logging.info("exec_sys_vars_upgrade_dml_by_cluster : cluster_id = {0}, ip = {1}, port = {2}"
                 .format(standby_cluster_info['cluster_id'],
                         standby_cluster_info['ip'],
                         standby_cluster_info['port']))
    logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
                 .format(standby_cluster_info['cluster_id'],
                         standby_cluster_info['ip'],
                         standby_cluster_info['port']))
    conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
                                   password =  standby_cluster_info['pwd'],
                                   host     =  standby_cluster_info['ip'],
                                   port     =  standby_cluster_info['port'],
                                   database =  'oceanbase',
                                   raise_on_warnings = True)
    cur = conn.cursor(buffered=True)
    conn.autocommit = True
    dml_cur = DMLCursor(cur)
    query_cur = QueryCursor(cur)
    is_primary = actions.check_current_cluster_is_primary(query_cur)
    if is_primary:
      logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                        .format(standby_cluster_info['cluster_id'],
                                standby_cluster_info['ip'],
                                standby_cluster_info['port']))
      raise e

    # only update sys tenant in standby cluster
    tenant_id = 1
    # calc diff
    (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = calc_diff_sys_var(cur, tenant_id)
    logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
    logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
    logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
    # update
    update_sys_vars_for_tenant(dml_cur, tenant_id, update_sys_var_list)
    add_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list)
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_date_format', 'YYYY-MM-DD HH24:MI:SS');
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.FF');
    special_update_sys_vars_for_tenant(dml_cur, tenant_id, add_sys_var_list, 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD');

    cur.close()
    conn.close()

  except Exception, e:
    logging.exception("""exec_sys_vars_upgrade_dml_in_standby_cluster failed :
                         cluster_id = {0}, ip = {1}, port = {2}"""
                         .format(standby_cluster_info['cluster_id'],
                                 standby_cluster_info['ip'],
                                 standby_cluster_info['port']))
    raise e


def get_actual_tenant_id(tenant_id):
  return tenant_id if (1 == tenant_id) else 0;

def change_tenant(cur, tenant_id):
  # change tenant
  sql = "alter system change tenant tenant_id = {0};".format(tenant_id)
  logging.info(sql);
  cur.execute(sql);
  # check
  sql = "select effective_tenant_id();"
  cur.execute(sql)
  result = cur.fetchall()
  if (1 != len(result) or 1 != len(result[0])):
    raise MyError("invalid result cnt")
  elif (tenant_id != result[0][0]):
    raise MyError("effective_tenant_id:{0} , tenant_id:{1}".format(result[0][0], tenant_id))
