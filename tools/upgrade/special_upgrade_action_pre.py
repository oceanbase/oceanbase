#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import re
import string
from random import Random
from actions import DMLCursor
from actions import QueryCursor
from actions import check_current_cluster_is_primary
import binascii
import my_utils
import actions
import sys

#def modify_schema_history(conn, cur, tenant_ids):
#  try:
#    conn.autocommit = True
#
#    # disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#    log('tenant_ids: {0}'.format(tenant_ids))
#    for tenant_id in tenant_ids:
#      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
#      log(sql)
#      cur.execute(sql)

#    #####implement#####
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


# 主库需要执行的升级动作
def do_special_upgrade(conn, cur, tenant_id_list, user, passwd):
  # special upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
# 主库升级流程没加滚动升级步骤，或混部阶段DDL测试有相关case覆盖前，混部开始禁DDL
  actions.set_parameter(cur, 'enable_ddl', 'False')
####========******####======== actions begin ========####******========####
  return
####========******####========= actions end =========####******========####

def do_add_recovery_status_to_all_zone(conn, cur):
  try:
    logging.info('add recovery status row to __all_zone for each zone')
    zones = [];
    recovery_status = [];

    # pre-check, may skip
    check_updated_sql = "select * from oceanbase.__all_zone where zone !='' AND name='recovery_status'"
    cur.execute(check_updated_sql)
    recovery_status = cur.fetchall()
    if 0 < len(recovery_status):
      logging.info('[recovery_status] row already exists, no need to add')

    # get zones
    if 0 >= len(recovery_status):
      all_zone_sql = "select distinct(zone) zone from oceanbase.__all_zone where zone !=''"
      cur.execute(all_zone_sql)
      zone_results = cur.fetchall()
      for r in zone_results:
        zones.append("('" + r[0] + "', 'recovery_status', 0, 'NORMAL')")

    # add rows
    if 0 < len(zones):
      upgrade_sql = "insert into oceanbase.__all_zone(zone, name, value, info) values " + ','.join(zones)
      logging.info(upgrade_sql)
      cur.execute(upgrade_sql)
      conn.commit()

    # check result
    if 0 < len(zones):
      cur.execute(check_updated_sql)
      check_results = cur.fetchall()
      if len(check_results) != len(zones):
        raise MyError('fail insert [recovery_status] row into __all_zone')

  except Exception, e:
    logging.exception('do_add_recovery_status_to_all_zone error')
    raise e

def do_add_storage_type_to_all_zone(conn, cur):
  try:
    logging.info('add storage type row to __all_zone for each zone')
    zones = [];
    storage_types = [];

    # pre-check, may skip
    check_updated_sql = "select * from oceanbase.__all_zone where zone !='' AND name='storage_type'"
    cur.execute(check_updated_sql)
    storage_types = cur.fetchall()
    if 0 < len(storage_types):
      logging.info('[storage_types] row already exists, no need to add')

    # get zones
    if 0 >= len(storage_types):
      all_zone_sql = "select distinct(zone) zone from oceanbase.__all_zone where zone !=''"
      cur.execute(all_zone_sql)
      zone_results = cur.fetchall()
      for r in zone_results:
        zones.append("('" + r[0] + "', 'storage_type', 0, 'LOCAL')")

    # add rows
    if 0 < len(zones):
      upgrade_sql = "insert into oceanbase.__all_zone(zone, name, value, info) values " + ','.join(zones)
      logging.info(upgrade_sql)
      cur.execute(upgrade_sql)
      conn.commit()

    # check result
    if 0 < len(zones):
      cur.execute(check_updated_sql)
      check_results = cur.fetchall()
      if len(check_results) != len(zones):
        raise MyError('fail insert [storage_type] row into __all_zone')

  except Exception, e:
    logging.exception('do_add_storage_type_to_all_zone error')
    raise e

def modify_trigger(conn, cur, tenant_ids):
  try:
    conn.autocommit = True
    # disable ddl
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'False')
    log('tenant_ids: {0}'.format(tenant_ids))
    for tenant_id in tenant_ids:
      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
      log(sql)
      cur.execute(sql)
      #####implement#####
      trigger_sql = """
update __all_tenant_trigger
set
package_spec_source = replace(
package_spec_source,
'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL\;',
'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL\;'
),
package_body_source = replace(replace(
package_body_source,
'
PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
BEGIN
  NULL\;
END\;
',
'
PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
BEGIN
  update_columns_ := STRINGARRAY()\;
  update_columns_.EXTEND(update_columns.COUNT)\;
  FOR i IN 1 .. update_columns.COUNT LOOP
    update_columns_(i) := update_columns(i)\;
  END LOOP\;
END\;
'),
'
FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS
BEGIN
  RETURN (dml_event_ = 4)\;
END\;
',
'
FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS
  is_updating BOOL\;
BEGIN
  is_updating := (dml_event_ = 4)\;
  IF (is_updating AND column_name IS NOT NULL) THEN
    is_updating := FALSE\;
    FOR i IN 1 .. update_columns_.COUNT LOOP
      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE\; EXIT\; END IF\;
    END LOOP\;
  END IF\;
  RETURN is_updating\;
END\;
'); """

      log(trigger_sql)
      cur.execute(trigger_sql)
      log("update rows = " + str(cur.rowcount))

      trigger_history_sql = """
update __all_tenant_trigger_history
set
package_spec_source = replace(
package_spec_source,
'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL\;',
'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL\;'
),
package_body_source = replace(replace(
package_body_source,
'
PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
BEGIN
  NULL\;
END\;
',
'
PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS
BEGIN
  update_columns_ := STRINGARRAY()\;
  update_columns_.EXTEND(update_columns.COUNT)\;
  FOR i IN 1 .. update_columns.COUNT LOOP
    update_columns_(i) := update_columns(i)\;
  END LOOP\;
END\;
'),
'
FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS
BEGIN
  RETURN (dml_event_ = 4)\;
END\;
',
'
FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS
  is_updating BOOL\;
BEGIN
  is_updating := (dml_event_ = 4)\;
  IF (is_updating AND column_name IS NOT NULL) THEN
    is_updating := FALSE\;
    FOR i IN 1 .. update_columns_.COUNT LOOP
      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE\; EXIT\; END IF\;
    END LOOP\;
  END IF\;
  RETURN is_updating\;
END\;
')
where is_deleted = 0; """

      log(trigger_history_sql)
      cur.execute(trigger_history_sql)
      log("update rows = " + str(cur.rowcount))

      #####implement end#####

    # 还原默认 tenant
    sys_tenant_id = 1
    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
    log(sql)
    cur.execute(sql)

    # enable ddl
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'True')
  except Exception, e:
    logging.warn("exec modify trigger failed")
    raise e
  logging.info('exec modify trigger finish')

def fill_priv_file_column_for_all_user(conn, cur):
  try:
    conn.autocommit = True
    # disable ddl
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'False')
    tenant_ids = get_tenant_ids(cur)
    log('tenant_ids: {0}'.format(tenant_ids))
    for tenant_id in tenant_ids:
      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
      log(sql)
      cur.execute(sql)
      tenant_id_in_sql = 0
      if 1 == tenant_id:
        tenant_id_in_sql = 1

      begin_user_id = 0
      begin_schema_version = 0
      fetch_num = 1000

      while (True):
        query_limit = """
                  where tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
                  order by tenant_id, user_id, schema_version
                  limit {3}""".format(tenant_id_in_sql, begin_user_id, begin_schema_version, fetch_num)

        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ user_id, schema_version
                 from oceanbase.__all_user_history""" + query_limit
        log(sql)
        result_rows = query(cur, sql)
        log("select rows = " + str(cur.rowcount))

        if len(result_rows) <= 0:
          break
        else:
          last_schema_version = result_rows[-1][1]
          last_user_id = result_rows[-1][0]
          condition = """
                  where priv_alter = 1 and priv_create = 1 and priv_create_user = 1 and priv_delete = 1
                         and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1
                         and priv_index = 1 and priv_create_view = 1 and priv_show_view = 1 and priv_show_db = 1
                         and priv_super = 1 and priv_create_synonym = 1
                         and tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
                         and (user_id, schema_version) <= ({3}, {4})
                  """.format(tenant_id_in_sql, begin_user_id, begin_schema_version, last_user_id, last_schema_version)

          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user_history
                   set priv_file = 1""" + condition
          log(sql)
          cur.execute(sql)
          log("update rows = " + str(cur.rowcount))

          condition = """
                  where priv_super = 1 and tenant_id = {0} and (user_id, schema_version) > ({1}, {2})
                         and (user_id, schema_version) <= ({3}, {4})
                  """.format(tenant_id_in_sql, begin_user_id, begin_schema_version, last_user_id, last_schema_version)

          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user_history
                   set priv_alter_tenant = 1,
                       priv_alter_system = 1,
                       priv_create_resource_unit = 1,
                       priv_create_resource_pool = 1 """ + condition
          log(sql)
          cur.execute(sql)

          begin_schema_version = last_schema_version
          begin_user_id = last_user_id

      begin_user_id = 0
      while (True):
        query_limit = """
                   where tenant_id = {0} and user_id > {1}
                   order by tenant_id, user_id
                   limit {2}""".format(tenant_id_in_sql, begin_user_id, fetch_num)
        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ user_id
                 from oceanbase.__all_user""" + query_limit
        log(sql)
        result_rows = query(cur, sql)
        log("select rows = " + str(cur.rowcount))

        if len(result_rows) <= 0:
          break
        else:
          end_user_id = result_rows[-1][0]
          condition = """
                   where priv_alter = 1 and priv_create = 1 and priv_create_user = 1 and priv_delete = 1
                         and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1
                         and priv_index = 1 and priv_create_view = 1 and priv_show_view = 1 and priv_show_db = 1
                         and priv_super = 1 and priv_create_synonym = 1
                         and tenant_id = {0} and user_id > {1} and user_id <= {2}
                  """.format(tenant_id_in_sql, begin_user_id, end_user_id)
          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user
                   set priv_file = 1 """ + condition
          log(sql)
          cur.execute(sql)
          log("update rows = " + str(cur.rowcount))

          condition = """
                   where priv_super = 1
                         and tenant_id = {0} and user_id > {1} and user_id <= {2}
                  """.format(tenant_id_in_sql, begin_user_id, end_user_id)
          sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_user
                   set priv_alter_tenant = 1,
                       priv_alter_system = 1,
                       priv_create_resource_unit = 1,
                       priv_create_resource_pool = 1 """ + condition
          log(sql)
          cur.execute(sql)

          begin_user_id = end_user_id

    # 还原默认 tenant
    sys_tenant_id = 1
    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
    log(sql)
    cur.execute(sql)

    # enable ddl
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'True')
  except Exception, e:
    logging.warn("exec fill priv_file to all_user failed")
    raise e
  logging.info('exec fill priv_file to all_user finish')

def insert_split_schema_version_v2(conn, cur, user, pwd):
  try:
    query_cur = actions.QueryCursor(cur)
    is_primary = actions.check_current_cluster_is_primary(query_cur)
    if not is_primary:
      logging.warn("should run in primary cluster")
      raise e

    # primary cluster
    dml_cur = actions.DMLCursor(cur)
    sql = """replace into __all_core_table(table_name, row_id, column_name, column_value)
              values ('__all_global_stat', 1, 'split_schema_version_v2', '-1');"""
    rowcount = dml_cur.exec_update(sql)
    if rowcount <= 0:
      logging.warn("invalid rowcount : {0}".format(rowcount))
      raise e

    # standby cluster
    standby_cluster_list = actions.fetch_standby_cluster_infos(conn, query_cur, user, pwd)
    for standby_cluster in standby_cluster_list:
      # connect
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
      tmp_query_cur = actions.QueryCursor(tmp_cur)
      # check if stanby cluster
      is_primary = actions.check_current_cluster_is_primary(tmp_query_cur)
      if is_primary:
        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                          .format(standby_cluster['cluster_id'],
                                  standby_cluster['ip'],
                                  standby_cluster['port']))
        raise e
      # replace
      tmp_dml_cur = actions.DMLCursor(tmp_cur)
      sql = """replace into __all_core_table(table_name, row_id, column_name, column_value)
                values ('__all_global_stat', 1, 'split_schema_version_v2', '-1');"""
      rowcount = tmp_dml_cur.exec_update(sql)
      if rowcount <= 0:
        logging.warn("invalid rowcount : {0}".format(rowcount))
        raise e
      # close
      tmp_cur.close()
      tmp_conn.close()
  except Exception, e:
    logging.warn("init split_schema_version_v2 failed")
    raise e

def query(cur, sql):
  log(sql)
  cur.execute(sql)
  results = cur.fetchall()
  return results

def log(msg):
  logging.info(msg)

def get_oracle_tenant_ids(cur):
  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant where compatibility_mode = 1')]

def get_tenant_ids(cur):
  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant')]

# 修正升级上来的旧外键在 __all_foreign_key_column 和 __all_foreign_key_column_history 中 position 列的数据
def modify_foreign_key_column_position_info(conn, cur):
  try:
    conn.autocommit = True
    # disable ddl
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'False')
    tenant_ids = get_tenant_ids(cur)
    log('tenant_ids: {0}'.format(tenant_ids))
    for tenant_id in tenant_ids:
      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
      log(sql)
      cur.execute(sql)
      tenant_id_in_sql = 0
      if 1 == tenant_id:
        tenant_id_in_sql = 1
      # 查出租户下所有未被删除的外键
      sql = """select /*+ QUERY_TIMEOUT(1500000000) */ foreign_key_id from oceanbase.__all_foreign_key where tenant_id = {0}""".format(tenant_id_in_sql)
      log(sql)
      foreign_key_id_rows = query(cur, sql)
      fk_num = len(foreign_key_id_rows)
      cnt = 0
      # 遍历每个外键，检查 oceanbase.__all_foreign_key_column 中记录的 position 信息是否为 0，如果为 0，需要更新为正确的值
      while cnt < fk_num:
        foreign_key_id = foreign_key_id_rows[cnt][0]
        sql = """select /*+ QUERY_TIMEOUT(1500000000) */ child_column_id, parent_column_id from oceanbase.__all_foreign_key_column where foreign_key_id = {0} and position = 0 and tenant_id = {1} order by gmt_create asc""".format(foreign_key_id, tenant_id_in_sql)
        log(sql)
        need_update_rows = query(cur, sql)
        fk_col_num = len(need_update_rows)
        if fk_col_num > 0:
          position = 1
          # 遍历特定外键里的每个 position 信息为 0 的列
          while position <= fk_col_num:
            child_column_id = need_update_rows[position - 1][0]
            parent_column_id = need_update_rows[position - 1][1]
            # 在 oceanbase.__all_foreign_key_column_history 里面更新 position 的值
            sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_foreign_key_column_history set position = {0} where foreign_key_id = {1} and child_column_id = {2} and parent_column_id = {3} and tenant_id = {4}""".format(position, foreign_key_id, child_column_id, parent_column_id, tenant_id_in_sql)
            log(sql)
            cur.execute(sql)
            if cur.rowcount == 0:
              logging.warn("affected rows is 0 when update oceanbase.__all_foreign_key_column_history")
              raise e
            # 在 oceanbase.__all_foreign_key_column 里面更新 position 的值
            sql = """update /*+ QUERY_TIMEOUT(150000000) */ oceanbase.__all_foreign_key_column set position = {0} where foreign_key_id = {1} and child_column_id = {2} and parent_column_id = {3} and tenant_id = {4}""".format(position, foreign_key_id, child_column_id, parent_column_id, tenant_id_in_sql)
            log(sql)
            cur.execute(sql)
            if cur.rowcount != 1:
              logging.warn("affected rows is not 1 when update oceanbase.__all_foreign_key_column")
              raise e
            position = position + 1
        cnt = cnt + 1
    # 还原默认 tenant
    sys_tenant_id = 1
    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
    log(sql)
    cur.execute(sql)

    # enable ddl
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'True')
  except Exception, e:
    logging.warn("modify foreign key column position failed")
    raise e
  logging.info('modify foreign key column position finish')

