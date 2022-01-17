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
from my_error import MyError
import sys
import mysql.connector
from mysql.connector import errorcode
import logging
import json
import config
import opts
import run_modules
import actions
import string


def dump_sql_to_file(dump_filename, tenant_id_list):
#  normal_ddls_str = normal_ddl_actions_post.get_normal_ddl_actions_sqls_str()
#  normal_dmls_str = normal_dml_actions_post.get_normal_dml_actions_sqls_str()
  #each_tenant_dmls_str = each_tenant_dml_actions_post.get_each_tenant_dml_actions_sqls_str(tenant_id_list)
  dump_file = open(dump_filename, 'w')
  dump_file.write('# 以下是priv_checker.py脚本中的步骤\n')
  dump_file.write('# 仅供priv_checker.py脚本运行失败需要人肉的时候参考\n')
  dump_file.close()

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
#      # 获取standby_cluster_info列表
#      standby_cluster_infos = actions.fetch_standby_cluster_infos(conn, query_cur, my_user, my_passwd)
#      # check ddl and dml sync
#      actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
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
#        # check ddl and dml sync
#        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
#
#      if run_modules.MODULE_EACH_TENANT_DDL in my_module_set:
#        has_run_ddl = True
#        logging.info('================begin to run each tenant ddl===============')
#        conn.autocommit = True
#        each_tenant_ddl_actions_post.do_each_tenant_ddl_actions(cur, tenant_id_list)
#        logging.info('================succeed to run each tenant ddl===============')
#        conn.autocommit = False
#        # check ddl and dml sync
#        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
#
#      if run_modules.MODULE_NORMAL_DML in my_module_set:
#        logging.info('================begin to run normal dml===============')
#        normal_dml_actions_post.do_normal_dml_actions(cur)
#        logging.info('================succeed to run normal dml===============')
#        conn.commit()
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit dml===============')
#        # check ddl and dml sync
#        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
#
#      if run_modules.MODULE_EACH_TENANT_DML in my_module_set:
#        logging.info('================begin to run each tenant dml===============')
#        conn.autocommit = True
#        each_tenant_dml_actions_post.do_each_tenant_dml_actions(cur, tenant_id_list)
#        conn.autocommit = False
#        logging.info('================succeed to run each tenant dml===============')
#        # check ddl and dml sync
#        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
#
#      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
#        logging.info('================begin to run special action===============')
#        conn.autocommit = True
#        special_upgrade_action_post.do_special_upgrade(conn, cur, tenant_id_list)
#        conn.autocommit = False
#        actions.refresh_commit_sql_list()
#        logging.info('================succeed to commit special action===============')
#        # check ddl and dml sync
#        actions.check_ddl_and_dml_sync(conn, query_cur, standby_cluster_infos, tenant_id_list)
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
#class EachTenantDDLActionPostCreateAllTenantSecurityRecordAudit(BaseEachTenantDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 15
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
#    if len(results) > 0:
#      raise MyError('__all_tenant_security_audit_record already created')
#  def dump_before_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
#  def check_before_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
#    if len(results) > 0:
#      raise MyError('tenant_id:{0} has already create table __all_tenant_security_audit_record'.format(tenant_id))
#  @staticmethod
#  def get_each_tenant_action_ddl(tenant_id):
#    pure_table_id = 259
#    table_id = (tenant_id << 40) | pure_table_id
#    return """
#      CREATE TABLE `__all_tenant_security_audit_record` (
#          `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#          `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#          `tenant_id` bigint(20) NOT NULL,
#          `svr_ip` varchar(46) NOT NULL,
#          `svr_port` bigint(20) NOT NULL,
#          `record_timestamp_us` timestamp(6) NOT NULL,
#          `user_id` bigint(20) unsigned NOT NULL,
#          `user_name` varchar(64) NOT NULL,
#          `effective_user_id` bigint(20) unsigned NOT NULL,
#          `effective_user_name` varchar(64) NOT NULL,
#          `client_ip` varchar(46) NOT NULL,
#          `user_client_ip` varchar(46) NOT NULL,
#          `proxy_session_id` bigint(20) unsigned NOT NULL,
#          `session_id` bigint(20) unsigned NOT NULL,
#          `entry_id` bigint(20) unsigned NOT NULL,
#          `statement_id` bigint(20) unsigned NOT NULL,
#          `trans_id` varchar(512) NOT NULL,
#          `commit_version` bigint(20) NOT NULL,
#          `trace_id` varchar(64) NOT NULL,
#          `db_id` bigint(20) unsigned NOT NULL,
#          `cur_db_id` bigint(20) unsigned NOT NULL,
#          `sql_timestamp_us` timestamp(6) NOT NULL,
#          `audit_id` bigint(20) unsigned NOT NULL,
#          `audit_type` bigint(20) unsigned NOT NULL,
#          `operation_type` bigint(20) unsigned NOT NULL,
#          `action_id` bigint(20) unsigned NOT NULL,
#          `return_code` bigint(20) NOT NULL,
#          `obj_owner_name` varchar(64) DEFAULT NULL,
#          `obj_name` varchar(256) DEFAULT NULL,
#          `new_obj_owner_name` varchar(64) DEFAULT NULL,
#          `new_obj_name` varchar(256) DEFAULT NULL,
#          `auth_privileges` varchar(256) DEFAULT NULL,
#          `auth_grantee` varchar(256) DEFAULT NULL,
#          `logoff_logical_read` bigint(20) unsigned NOT NULL,
#          `logoff_physical_read` bigint(20) unsigned NOT NULL,
#          `logoff_logical_write` bigint(20) unsigned NOT NULL,
#          `logoff_lock_count` bigint(20) unsigned NOT NULL,
#          `logoff_dead_lock` varchar(40) DEFAULT NULL,
#          `logoff_cpu_time_us` bigint(20) unsigned NOT NULL,
#          `logoff_exec_time_us` bigint(20) unsigned NOT NULL,
#          `logoff_alive_time_us` bigint(20) unsigned NOT NULL,
#          `comment_text` varchar(65536) DEFAULT NULL,
#          `sql_bind` varchar(65536) DEFAULT NULL,
#          `sql_text` varchar(65536) DEFAULT NULL,
#          PRIMARY KEY (`tenant_id`, `svr_ip`, `svr_port`, `record_timestamp_us`)          
#      ) TABLE_ID = {0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1
#        BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
#    """.format(table_id)
#  @staticmethod
#  def get_each_tenant_rollback_sql(tenant_id):
#    return """select 1"""
#  def dump_after_do_each_tenant_action(self, tenant_id):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
#  def check_after_do_each_tenant_action(self, tenant_id):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record' and tenant_id = {0}""".format(tenant_id))
#    if len(results) != 1:
#      raise MyError('tenant_id:{0} create table __all_tenant_security_audit_record failed'.format(tenant_id))
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from __all_table where table_name = '__all_tenant_security_audit_record'""")
#    if len(results) != len(self.get_tenant_id_list()):
#      raise MyError('there should be {0} rows in __all_table whose table_name is __all_tenant_security_audit_record, but there has {1} rows like that'.format(len(self.get_tenant_id_list()), len(results)))
#
#class NormalDDLActionPreAddAllUserType(BaseDDLAction):
#    @staticmethod
#    def get_seq_num():
#      return 2
#    def dump_before_do_action(self):
#      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
#    def check_before_do_action(self):
#      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'type'""")
#      if len(results) > 0:
#        raise MyError('column oceanbase.__all_user.type already exists')
#    @staticmethod
#    def get_action_ddl():
#      return """alter table oceanbase.__all_user add column `type` bigint(20) DEFAULT '0'"""
#    @staticmethod
#    def get_rollback_sql():
#      return """alter table oceanbase.__all_user drop column type"""
#    def dump_after_do_action(self):
#      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
#    def check_after_do_action(self):
#      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'type'""")
#      if len(results) != 1:
#        raise MyError('failed to add column status for oceanbase.__all_user')
#
#class NormalDDLActionPreAddAllUserHistoryType(BaseDDLAction):
#    @staticmethod
#    def get_seq_num():
#      return 3
#    def dump_before_do_action(self):
#      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
#    def check_before_do_action(self):
#      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'type'""")
#      if len(results) > 0:
#        raise MyError('column oceanbase.__all_user_history.type already exists')
#    @staticmethod
#    def get_action_ddl():
#      return """alter table oceanbase.__all_user_history add column `type` bigint(20) DEFAULT '0'"""
#    @staticmethod
#    def get_rollback_sql():
#      return """alter table oceanbase.__all_user_history drop column type"""
#    def dump_after_do_action(self):
#      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
#    def check_after_do_action(self):
#      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'type'""")
#      if len(results) != 1:
#        raise MyError('failed to add column status for oceanbase.__all_user_history')
#
#
#class NormalDDLActionPreAddProfileIdAllUser(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 4
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'profile_id'""")
#    if len(results) != 0:
#      raise MyError('profile_id column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_user add column `profile_id` bigint(20) NOT NULL DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_user drop column profile_id"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'profile_id'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_user')
#
#class NormalDDLActionPreAddProfileIdAllUserHistory(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 5
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'profile_id'""")
#    if len(results) != 0:
#      raise MyError('profile_id column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_user_history add column `profile_id` bigint(20) DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_user_history drop column profile_id"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'profile_id'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_user_history')
#
#class NormalDDLActionPreModifyAllRootServiceEventHistoryIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 6
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_rootservice_event_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_rootservice_event_history like 'rs_svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_rootservice_event_history column rs_svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_rootservice_event_history modify column rs_svr_ip varchar(46) default ''"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_rootservice_event_history modify column rs_svr_ip varchar(32) default ''"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_rootservice_event_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_rootservice_event_history like 'rs_svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column rs_svr_ip for oceanbase.__all_rootservice_event_history')
#
#class NormalDDLActionPreModifyAllSStableChecksumIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 7
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sstable_checksum""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sstable_checksum like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_sstable_checksum column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_sstable_checksum modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_sstable_checksum modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sstable_checksum""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sstable_checksum like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_sstable_checksum')
#
#class NormalDDLActionPreModifyAllClogHistoryInfoV2IpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 8
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_clog_history_info_v2""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_clog_history_info_v2 like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_clog_history_info_v2 column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_clog_history_info_v2 modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_clog_history_info_v2 modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_clog_history_info_v2""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_clog_history_info_v2 like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_clog_history_info_v2')
#
#class NormalDDLActionPreModifyAllSysParameterIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 9
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sys_parameter""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sys_parameter like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_sys_parameter column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_sys_parameter modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_sys_parameter modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sys_parameter""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sys_parameter like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_sys_parameter')
#
#class NormalDDLActionPreModifyAllLocalIndexStatusIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 10
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_local_index_status""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_local_index_status like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_local_index_status column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_local_index_status modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_local_index_status modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_local_index_status""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_local_index_status like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_local_index_status')
#
#class NormalDDLActionPreModifyAllTenantResourceUsageIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 11
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_resource_usage""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_resource_usage like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_tenant_resource_usage column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_tenant_resource_usage modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_tenant_resource_usage modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_resource_usage""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_resource_usage like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_tenant_resource_usage')
#
#class NormalDDLActionPreModifyAllRootTableIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 12
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_root_table""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_root_table like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_root_table column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_root_table modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_root_table modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_root_table""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_root_table like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_root_table')
#
#class NormalDDLActionPreModifyAllServerEventHistoryIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 13
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_server_event_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_server_event_history like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_server_event_history column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_server_event_history modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_server_event_history modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_server_event_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_server_event_history like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_server_event_history')
#
#class NormalDDLActionPreModifyAllIndexScheduleTaskIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 14
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_index_schedule_task""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_index_schedule_task like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_index_schedule_task column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_index_schedule_task modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_index_schedule_task modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_index_schedule_task""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_index_schedule_task like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_index_schedule_task')
#
#class NormalDDLActionPreModifyAllMetaTableIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 15
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_meta_table""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_meta_table like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_meta_table column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_meta_table modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_meta_table modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_meta_table""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_meta_table like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_meta_table')
#
#class NormalDDLActionPreModifyAllSqlExecuteTaskIpColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 16
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sql_execute_task""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sql_execute_task like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_sql_execute_task column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_sql_execute_task modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_sql_execute_task modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sql_execute_task""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sql_execute_task like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_sql_execute_task')
#

#
#class NormalDDLActionPreModifyAllGlobalIndexDataSrcDataPartitionIDColumn(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 39
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_global_index_data_src""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_global_index_data_src like 'data_partition_id,'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_global_index_data_src column data_partition_id, not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_global_index_data_src change `data_partition_id,`  `data_partition_id` bigint(20) NOT NULL"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_global_index_data_src change `data_partition_id`  `data_partition_id,` bigint(20) NOT NULL"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_global_index_data_src""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_global_index_data_src like 'data_partition_id'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column data_partition_id, for oceanbase.__all_global_index_data_src')
#
#class NormalDDLActionPreAddAllForeignKeyEnableFlag(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 40
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'enable_flag'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key.enable_flag already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key add column `enable_flag` tinyint(4) NOT NULL DEFAULT '1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key drop column enable_flag"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'enable_flag'""")
#    if len(results) != 1:
#      raise MyError('failed to add column enable_flag for oceanbase.__all_foreign_key')
#
#class NormalDDLActionPreAddAllForeignKeyHistoryEnableFlag(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 41
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'enable_flag'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key_history.enable_flag already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key_history add column `enable_flag` tinyint(4) DEFAULT '1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key_history drop column enable_flag"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'enable_flag'""")
#    if len(results) != 1:
#      raise MyError('failed to add column enable_flag for oceanbase.__all_foreign_key_history')
#
#class NormalDDLActionPreAddAllForeignKeyRefCstType(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 42
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_type'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key.ref_cst_type already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key add column `ref_cst_type` bigint(20) NOT NULL DEFAULT '0'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key drop column ref_cst_type"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_type'""")
#    if len(results) != 1:
#      raise MyError('failed to add column ref_cst_type for oceanbase.__all_foreign_key')
#
#class NormalDDLActionPreAddAllForeignKeyHistoryRefCstType(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 43
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_type'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key_history.ref_cst_type already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key_history add column `ref_cst_type` bigint(20) DEFAULT '0'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key_history drop column ref_cst_type"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_type'""")
#    if len(results) != 1:
#      raise MyError('failed to add column ref_cst_type for oceanbase.__all_foreign_key_history')
#
#class NormalDDLActionPreAddAllForeignKeyRefCstId(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 44
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_id'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key.ref_cst_id already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key add column `ref_cst_id` bigint(20) NOT NULL DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key drop column ref_cst_id"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'ref_cst_id'""")
#    if len(results) != 1:
#      raise MyError('failed to add column ref_cst_id for oceanbase.__all_foreign_key')
#
#class NormalDDLActionPreAddAllForeignKeyHistoryRefCstId(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 45
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_id'""")
#    if len(results) > 0:
#      raise MyError('column oceanbase.__all_foreign_key_history.ref_cst_id already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_foreign_key_history add column `ref_cst_id` bigint(20) DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_foreign_key_history drop column ref_cst_id"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key_history like 'ref_cst_id'""")
#    if len(results) != 1:
#      raise MyError('failed to add column ref_cst_id for oceanbase.__all_foreign_key_history')
#
#class NormalDDLActionPreModifyAllSeedParameterSvrIp(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 46
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_seed_parameter""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_seed_parameter like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_seed_parameter column svr_ip not exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_seed_parameter modify column svr_ip varchar(46) not null"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_seed_parameter modify column svr_ip varchar(32) not null"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_seed_parameter""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_seed_parameter like 'svr_ip'""")
#    if len(results) != 1:
#      raise MyError('fail to modify column svr_ip for oceanbase.__all_seed_parameter')
#
#class NormalDDLActionPreAddClusterStatus(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 47
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_cluster""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_cluster like 'cluster_status'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_cluster add column cluster_status bigint(20) NOT NULL default 1"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_cluster drop column cluster_status"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_cluster""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_cluster like 'cluster_status'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_cluster')
#
#class NormalDDLActionPreAllTableAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 48
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_table add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_table drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_table')
#
#class NormalDDLActionPreAllTablegroupAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 49
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_tablegroup add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_tablegroup drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_tablegroup')
#
#class NormalDDLActionPreAllPartAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 50
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_part add column `drop_schema_version` bigint(20) NOT NULL DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_part drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_part')
#
#class NormalDDLActionPreAllTableHistoryAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 51
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_table_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_table_history drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_table_history')
#
#class NormalDDLActionPreAllTablegroupHistoryAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 52
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup_history like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_tablegroup_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_tablegroup_history drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tablegroup_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tablegroup_history like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_tablegroup_history')
#
#class NormalDDLActionPreAllPartHistoryAddDropSchemaVersion(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 53
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'drop_schema_version'""")
#    if len(results) != 0:
#      raise MyError('cluster_status column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_part_history add column `drop_schema_version` bigint(20) DEFAULT '-1'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_part_history drop column `drop_schema_version`"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'drop_schema_version'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_part_history')
#
#class NormalDDLActionPreAddBuildingSnapshot(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 54
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_ori_schema_version""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_ori_schema_version like 'building_snapshot'""")
#    if len(results) != 0:
#      raise MyError('building_snapshot column alread exists')
#  @staticmethod
#  def get_action_ddl():
#    return """alter table oceanbase.__all_ori_schema_version add column building_snapshot bigint(20) NOT NULL default '0'"""
#  @staticmethod
#  def get_rollback_sql():
#    return """alter table oceanbase.__all_ori_schema_version drop column building_snapshot"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_ori_schema_version""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_ori_schema_version like 'building_snapshot'""")
#    if len(results) != 1:
#      raise MyError('failed to add column for oceanbase.__all_ori_schema_version')
#
#class NormalDDLActionPreAddAllRestoreInfo(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 55
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_restore_info'""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_restore_info'""")
#    if len(results) > 0:
#      raise MyError('table oceanbase.__all_restore_info already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """ CREATE TABLE `__all_restore_info` (
#                 `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#                 `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#                 `job_id` bigint(20) NOT NULL,
#                 `name` varchar(1024) NOT NULL,
#                 `value` varchar(4096) NOT NULL,
#                 PRIMARY KEY (`job_id`, `name`)
#               ) TABLE_ID = 1099511628041 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
#  """
#  @staticmethod
#  def get_rollback_sql():
#    return """drop table oceanbase.__all_restore_info"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_restore_info'""")
#    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.__all_restore_info""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_restore_info'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_restore_info not exists')
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info""")
#    if len(results) != 5:
#      raise MyError('table oceanbase.__all_restore_info has invalid column descs')
#
#class NormalDDLActionPreAddAllFailoverInfo(BaseDDLAction):
#  @staticmethod
#  def get_seq_num():
#    return 56
#  def dump_before_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_failover_info'""")
#  def check_before_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_failover_info'""")
#    if len(results) > 0:
#      raise MyError('table oceanbase.__all_failover_info already exists')
#  @staticmethod
#  def get_action_ddl():
#    return """ CREATE TABLE `__all_failover_info` (
#                 `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
#                 `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
#                 `failover_epoch` bigint(20) NOT NULL,
#                 `tenant_id` bigint(20) NOT NULL,
#                 `sys_table_scn` bigint(20) NOT NULL,
#                 `user_table_scn` bigint(20) NOT NULL,
#                 `schema_version` bigint(20) NOT NULL,
#                   PRIMARY KEY (`failover_epoch`, `tenant_id`)
#               ) TABLE_ID = 1099511628047 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
#  """
#  @staticmethod
#  def get_rollback_sql():
#    return """drop table oceanbase.__all_failover_info"""
#  def dump_after_do_action(self):
#    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_failover_info'""")
#    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.__all_failover_info""")
#  def check_after_do_action(self):
#    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_failover_info'""")
#    if len(results) != 1:
#      raise MyError('table oceanbase.__all_failover_info not exists')
#    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_failover_info""")
#    if len(results) != 7:
#      raise MyError('table oceanbase.__all_failover_info has invalid column descs')
#
#class NormalDDLActionPreAddAllForeignKeyValidateFlag(BaseDDLAction):
# @staticmethod
# def get_seq_num():
#   return 57
# def dump_before_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
# def check_before_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'validate_flag'""")
#   if len(results) > 0:
#     raise MyError('column oceanbase.__all_foreign_key.validate_flag already exists')
# @staticmethod
# def get_action_ddl():
#   return """alter table oceanbase.__all_foreign_key add column `validate_flag` tinyint(4) NOT NULL DEFAULT '1'"""
# @staticmethod
# def get_rollback_sql():
#   return """alter table oceanbase.__all_foreign_key drop column validate_flag"""
# def dump_after_do_action(self):
#   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_foreign_key""")
# def check_after_do_action(self):
#   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_foreign_key like 'validate_flag'""")
#   if len(results) != 1:
#     raise MyError('failed to add column validate_flag for oceanbase.__all_foreign_key')
#
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
## 7.检查所有的表格的sstable checksum method都升级到2
#def check_sstable_column_checksum_method_new(query_cur):
#  query_cur.exec_sql("""set ob_query_timeout=1800000000""")
#  row_count = query_cur.exec_sql("""select * from __all_table where table_id in (select index_id from __all_sstable_column_checksum where checksum_method = 1 and data_table_id != 0) limit 1""")
#  if row_count != 0:
#    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade column checksum method using progressive compaction')
#  row_count = query_cur.exec_sql("""select column_value from __all_core_table where table_name = '__all_table' and column_name = 'table_id' and column_value in (select index_id from __all_sstable_column_checksum where checksum_method = 1 and data_table_id != 0) limit 1""")
#  if row_count != 0:
#    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade system table column checksum method using progressive compaction')
#  row_count = query_cur.exec_sql("""select index_id from __all_sstable_column_checksum where checksum_method = 1 and index_id = 1099511627777 and data_table_id != 0 limit 1""")
#  if row_count != 0:
#    raise MyError('check_sstable_column_checksum_method_new failed, please upgrade system table column checksum method using progressive compaction')
#  logging.info('check_sstable_column_checksum_method_new success')
#
## 8. 检查租户的resource_pool内存规格, 要求F类型unit最小内存大于5G，L类型unit最小内存大于2G.
#def check_tenant_resource_pool(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=0 and b.min_memory < 5368709120""")
#  if results[0][0] > 0 :
#    raise MyError('{0} tenant resource pool unit config is less than 5G, please check'.format(results[0][0]))
#  (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=5 and b.min_memory < 2147483648""")
#  if results[0][0] > 0 :
#    raise MyError('{0} tenant logonly resource pool unit config is less than 2G, please check'.format(results[0][0]))
#
## 9. 检查是否有日志型副本分布在Full类型unit中
#def check_logonly_replica_unit(query_cur):
#  # 统计all_tnant表中每个tenant的L类型的zone个数
#  (desc, res_tenant_log_zone_cnt) = query_cur.exec_query("""select  tenant_id, (char_length(locality) - char_length(replace(locality, 'LOGONLY', ''))) / char_length('LOGONLY') as log_zone_cnt from __all_tenant""")
#  # 统计all_resource_pool中每个tenant的L类型的resource_pool中包含的zone个数
#  (desc, res_respool_log_zone_cnt) = query_cur.exec_query("""select tenant_id, sum(lzone_cnt) as rp_log_zone_cnt from (select  tenant_id, ((char_length(zone_list) - char_length(replace(zone_list, ';', ''))) + 1) as lzone_cnt from __all_resource_pool where replica_type=5) as vt group by tenant_id""")
#  # 比较两个数量是否一致，不一致则报错
#  for line in res_tenant_log_zone_cnt:
#    tenant_id = line[0]
#    tenant_log_zone_cnt = line[1]
#    if tenant_log_zone_cnt > 0:
#      is_found = False
#      for rpline in res_respool_log_zone_cnt:
#        rp_tenant_id = rpline[0]
#        rp_log_zone_cnt = rpline[1]
#        if tenant_id == rp_tenant_id:
#          is_found = True
#          if tenant_log_zone_cnt > rp_log_zone_cnt:
#            raise MyError('{0} {1} check_logonly_replica_unit failed, log_type zone count not match with resource_pool'.format(line, rpline))
#          break
#      if is_found == False:
#        raise MyError('{0} check_logonly_replica_unit failed, not found log_type resource_pool for this tenant'.format(line))
#  logging.info('check_logonly_replica_unit success')
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
#def modify_server_permanent_offline_time(query_cur):
#  query_cur.exec_sql("""alter system set server_permanent_offline_time='72h'""")
#  logging.info("modify server_permanent_offline_time success")
#
## 17. 修改安全删除副本时间
#def modify_replica_safe_remove_time(query_cur):
#  query_cur.exec_sql("""alter system set replica_safe_remove_time='72h'""")
#  logging.info("modify modify_replica_safe_remove_time success")
#
## 18. 检查progressive_merge_round都升到1
#def check_progressive_merge_round(query_cur):
#  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_virtual_table where progressive_merge_round = 0 and table_type not in (1,2,4)""")
#  if results[0][0] != 0:
#      raise MyError("""progressive_merge_round should all be 1""")
#  logging.info("""check progressive_merge_round status success""")
#
##19. 检查是否有内置用户，角色
#def check_inner_user_role_exists(query_cur):
#   (desc, results) = query_cur.exec_query(
#     """select count(*) 
#          from oceanbase.__all_tenant t, oceanbase.__all_virtual_user u 
#        where t.tenant_id = u.tenant_id and 
#              t.compatibility_mode=1 and 
#              u.user_name in ('DBA','CONNECT','RESOURCE','LBACSYS','ORAAUDITOR');""")
#   if results[0][0] != 0:
#       raise MyError("""check_inner_user_role_exists failed, ob2.2 not support user_name in 'dba' etc... """)
#   logging.info("""check_inner_user_role_exists success""")
#
## 19. 从小于224的版本升级上来时，需要确认high_priority_net_thread_count配置项值为0 (224版本开始该值默认为1)
#def check_high_priority_net_thread_count_before_224(query_cur):
#  # 获取最小版本
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('distinct observer version exist')
#  elif cmp(results[0][0], "2.2.40") >= 0 :
#    # 最小版本大于等于2.2.40，忽略检查
#    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check standby cluster'.format(results[0][0]))
#  else:
#    # 低于224版本的需要确认配置项值为0
#    logging.info('cluster version is ({0}), need check high_priority_net_thread_count'.format(results[0][0]))
#    (desc, results) = query_cur.exec_query("""select value from __all_sys_parameter where name like 'high_priority_net_thread_count'""")
#    for line in results:
#        thread_cnt = line[0]
#        if thread_cnt > 0:
#            raise MyError('high_priority_net_thread_count is greater than 0, unexpected')
#    logging.info('check high_priority_net_thread_count success')
#
## 20. 从小于224的版本升级上来时，要求不能有备库存在
#def check_standby_cluster(query_cur):
#  # 获取最小版本
#  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
#  if len(results) != 1:
#    raise MyError('distinct observer version exist')
#  elif cmp(results[0][0], "2.2.40") >= 0 :
#    # 最小版本大于等于2.2.40，忽略检查
#    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check standby cluster'.format(results[0][0]))
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
#      query_cur = QueryCursor(cur)
#      check_observer_version(query_cur, upgrade_params)
#      check_paxos_replica(query_cur)
#      check_rebalance_task(query_cur)
#      check_cluster_status(query_cur)
#      check_disable_separate_sys_clog(query_cur)
#      check_macro_block_data_seq(query_cur)
#      check_sstable_column_checksum_method_new(query_cur)
#      check_tenant_resource_pool(query_cur)
#      check_logonly_replica_unit(query_cur)
#      check_tenant_part_num(query_cur)
#      check_tenant_resource(query_cur)
#      check_readonly_zone(query_cur)
#      modify_server_permanent_offline_time(query_cur)
#      modify_replica_safe_remove_time(query_cur)
#      check_progressive_merge_round(query_cur)
#      check_inner_user_role_exists(query_cur)
#      check_high_priority_net_thread_count_before_224(query_cur)
#      check_standby_cluster(query_cur)
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
#
#class UpgradeParams:
#  log_filename = 'upgrade_post_checker.log'
#  new_version = '3.0.0'
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
#    raise MyError("check cluster version timeout!")
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
#    raise MyError("check storage format version timeout! Expected version {0}".format(expect_version))
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
#    sql = "alter system set enable_ddl = true;"
#    logging.info(sql)
#    cur.execute(sql)
#    time.sleep(10)
#
#    # run job
#    sql = "alter system run job 'UPGRADE_STORAGE_FORMAT_VERSION';"
#    logging.info(sql)
#    cur.execute(sql)
#
#  except Exception, e:
#    logging.warn("upgrade storage format version failed")
#    raise MyError("upgrade storage format version failed")
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
#    raise MyError('check root inspection failed!')
#  logging.info('check root inspection success')
#
## 3 开ddl
#def enable_ddl(query_cur):
#  query_cur.exec_sql("""alter system set enable_ddl = true""")
#  logging.info("enable_ddl success")
#
## 4 打开rebalance
#def enable_rebalance(query_cur):
#  query_cur.exec_sql("""alter system set enable_rebalance = true""")
#  logging.info("enable_rebalance success")
#
## 5 打开rereplication
#def enable_rereplication(query_cur):
#  query_cur.exec_sql("""alter system set enable_rereplication = true""")
#  logging.info("enable_rereplication success")
#
## 6 打开major freeze
#def enable_major_freeze(query_cur):
#  query_cur.exec_sql("""alter system set enable_major_freeze=true""")
#  logging.info("enable_major_freeze success")
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
#      query_cur = QueryCursor(cur)
#      try:
#        check_cluster_version(query_cur)
#        #upgrade_storage_format_version(conn, cur)
#        #check_storage_format_version(query_cur)
#        upgrade_table_schema_version(conn, query_cur)
#        check_root_inspection(query_cur)
#        enable_ddl(query_cur)
#        enable_rebalance(query_cur)
#        enable_rereplication(query_cur )
#        enable_major_freeze(query_cur)
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
#    is_primary = actions.check_current_cluster_is_primary(conn, query_cur)
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


import os
import sys
import datetime
from random import Random

#def get_actual_tenant_id(tenant_id):
#  return tenant_id if (1 == tenant_id) else 0;

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


def fetch_tenant_ids(query_cur):
  try:
    tenant_id_list = []
    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant where compatibility_mode=1 order by tenant_id desc""")
    for r in results:
      tenant_id_list.append(r[0])
    return tenant_id_list
  except Exception, e:
    logging.exception('fail to fetch distinct tenant ids')
    raise e


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

class UpgradeParams:
  log_filename = config.post_upgrade_log_filename
  sql_dump_filename = config.post_upgrade_sql_filename
  rollback_sql_filename =  config.post_upgrade_rollback_sql_filename

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

def fetch_user_names(query_cur):
  try:
    user_name_list = []
    (desc, results) = query_cur.exec_query("""select user_name from oceanbase.__all_user where type=0""")
    for r in results:
      user_name_list.append(r[0])
    return user_name_list
  except Exception, e:
    logging.exception('fail to fetch user names')
    raise e

def get_priv_info(m_p):
  priv = []
  priv_str = ''
  obj_name=''
  words = m_p.split(" ")
  # build priv list
  i = 1
  while words[i].upper() != 'ON':
    if (words[i].upper() == 'ALL'):
      priv.append('ALL')
    else:
      if (words[i].upper() != 'PRIVILEGES'):
        priv.append(words[i])
    i= i+1
  # Jump 'ON'
  i=i+1
  #print words
  if (words[i] == '*.*'):
    priv_level = 'USER'
  else:
    ind = string.find(words[i], '.*')
    if (ind != -1):
      priv_level = 'DB'
      obj_name = words[i][0: ind]
    else:
      priv_level = 'OBJ'
      obj_name = words[i]
  #jump 'TO'
  i = i + 2
  user_name = words[i]

  return (priv_level, user_name, priv, obj_name)

def owner_priv_to_str(priv):
  priv_str = '';
  i = 0
  while i <  len(priv):
    if (string.find(priv[i].uppper(), 'CREATE') == -1):
      if (priv[i].upper() == 'SHOW VIEW'):
        priv_str = 'SELECT ,'
      else:
        priv_str = priv[i] + ' ,'
    i = i + 1
  priv_str = priv_str[0: len(priv_str) -1]
  return priv_str

def owner_db_priv_to_str(priv):
  priv_str = ''
  i = 0
  while i <  len(priv):
    if (string.find(priv[i].uppper(), 'CREATE') != -1):
      priv_str = priv[i] + ' ,'
    i = i + 1
  priv_str = priv_str[0: len(priv_str) -1]
  return priv_str

def other_db_priv_to_str(priv):
  priv_str = ''
  i = 0
  while i <  len(priv):
    if (string.find(priv[i].uppper(), 'CREATE') != -1):
      priv_str = 'CREATE TABLE ,'
    elif (string.find(priv[i].upper(), 'CREATE VIEW') != -1):
      priv_str = 'CREATE VIEW ,'
    i = i + 1
  priv_str = priv_str[0: len(priv_str) -1]
  return priv_str

def user_priv_to_str(priv):
  priv_str = ''
  i = 0
  while i <  len(priv):
    if (string.find(priv[i].upper(), 'SUPER') != -1):
      priv_str = 'ALTER SYSTEM ,'
    elif (string.find(priv[i].upper(), 'CREATE USER') != -1):
      priv_str = 'CREATE USER, ALTER USER, DROP USER,'
    elif (string.find(priv[i].upper(), 'PROCESS') != -1):
      priv_str = 'SHOW PROCESS,'
    i = i + 1
  priv_str = priv_str[0: len(priv_str) -1]

owner_db_all_privs = 'CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE SYNONYM, CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE '

other_db_all_privs = 'create any table, create any view, create any  procedure,\
create any synonym, create any sequence, create any trigger, create any type,\
alter any table,  alter any procedure, alter any sequence,\
alter any trigger, alter any type, drop any table, drop any view,\
drop any procedure, drop any synonym, drop any sequence, drop any trigger,\
drop any type, select any table, insert any table, update any table, delete any table,\
flashback any table, create any index, alter any index, drop any index,\
execute any procedure, create public synonym, drop public synonym,\
select any sequence, execute any type, create tablespace,\
alter tablespace, drop tablespace '

other_obj_all_privs = 'alter, index, select, insert, update, delete '

def map_priv(m_p):
   (priv_level, user_name, priv, obj_name)  = get_priv_info(m_p)
   #print priv_level, user_name, priv, obj_name
   if priv_level == 'DB':
     if user_name == obj_name:
       if priv[0] == 'ALL':
         return 'GRANT ' + owner_db_all_privs + ' TO ' + user_name
       else:
         priv_str = owner_db_priv_to_str(priv)
         return 'GRANT ' +  priv_str + ' TO ' + user_name
     else:
       if priv[0] == 'ALL':
         return 'GRANT ' + other_db_all_privs + ' TO ' + user_name
       else:
         priv_str = other_db_priv_to_str(priv)
         if (priv_str != ''):
           return 'GRANT ' + priv_str + 'TO ' + user_name
         else:
           return '';
   else:
     if priv_level == 'USER':
       if priv[0] == 'ALL':
         return 'GRANT DBA TO ' + user_name
       else:
         priv_str = user_priv_to_str(priv)
         return 'GRANT ' + priv_set + 'TO ' + user_name
     else:
       if user_name == obj_name:
         return ''
       else:
         if priv[0] == 'ALL':
           return 'GRANT ' + other_obj_all_privs + ' TO ' + user_name
         else:
           priv_str = other_priv_to_str(priv)
           return 'GRANT ' + priv_str + 'ON ' + obj_name + 'TO ' + user_name

##################################################################
# check的条件：
# 1. 存在库级grant并且用户名和库名不一致，并且用户不是SYS, 则check不通过
# 
#
#
##################################################################
def check_grant_sql(grant_sql):
  (priv_level, user_name, priv, obj_name)  = get_priv_info(grant_sql)
  if priv_level == 'DB':
     if user_name != obj_name and user_name.upper() != 'SYS' and user_name.upper() != '\'SYS\'':
       return False
  return True

def fetch_user_privs(query_cur, user_name):
  try:
    grant_sql_list = []
    (desc, results) = query_cur.exec_query("""show grants for """ + user_name)
    for r in results:
      grant_sql_list.append(r[0])
    return grant_sql_list
  except Exception, e:
    logging.exception('fail to fetch user privs')
    raise e

def check_is_dcl_sql(sql):
  word_list = sql.split()
  if len(word_list) < 1:
    raise MyError('sql is empty, sql="{0}"'.format(sql))
  key_word = word_list[0].lower()
  if 'grant' != key_word and 'revoke' != key_word:
    raise MyError('sql must be dcl, key_word="{0}", sql="{1}"'.format(key_word, sql))

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

class DCLCursor:
  _cursor = None
  def __init__(self, cursor):
    self._cursor = Cursor(cursor)
  def exec_dcl(self, sql, print_when_succ = True):
    try:
      # 这里检查是不是ddl，不是ddl就抛错
      check_is_dcl_sql(sql)
      return self._cursor.exec_sql(sql, print_when_succ)
    except Exception, e:
      logging.exception('fail to execute dcl: %s', sql)
      raise e

def do_priv_check_by_tenant_id(cur, tenant_id_list):
  if len(tenant_id_list) <= 0:
    logging.info('This is no oraclemode tenant in cluster')
  
  simple_priv = True

  query_cur = actions.QueryCursor(cur)
  dcl_cur   = DCLCursor(cur)
  for i in range(len(tenant_id_list)): 
    if simple_priv == False:
      break
    else:
      tenant_id = tenant_id_list[i]
      change_tenant(cur, tenant_id)
      user_name_list = fetch_user_names(query_cur)
      for j in range(len(user_name_list)):
        if simple_priv == False:
          break
        else:
          user_name = user_name_list[j]
          grant_sql_list = fetch_user_privs(query_cur, user_name)
          for k in range(len(grant_sql_list)):
            grant_sql = grant_sql_list[k]
            is_ok = check_grant_sql(grant_sql)
            if (False == is_ok):
              simple_priv = False
              logging.info("--------------------------------------------------------------------")
              logging.info("--------------------------------------------------------------------")
              logging.warning('Database privs exists:' + grant_sql)
              logging.info("--------------------------------------------------------------------")
              logging.info("--------------------------------------------------------------------")
              break
              
  return simple_priv

def do_priv_check(my_host, my_port, my_user, my_passwd, upgrade_params):
  try:
    conn = mysql.connector.connect(user = my_user,
                                   password = my_passwd,
                                   host = my_host,
                                   port = my_port,
                                   database = 'oceanbase',
                                   raise_on_warnings = True)
    cur = conn.cursor(buffered=True)
    try:
      query_cur = actions.QueryCursor(cur)
      # 获取租户id列表
      tenant_id_list = fetch_tenant_ids(query_cur)
      if len(tenant_id_list) <= 0:
        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
        raise MyError('no tenant id')
      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
      conn.commit()

      simple_priv = do_priv_check_by_tenant_id(cur, tenant_id_list)
      conn.commit()

      if simple_priv == True:
        logging.info("****************************************************************************")
        logging.info("****************************************************************************")
        logging.info("")
        logging.info("No Database Privs Exists.")
        logging.info(tenant_id_list)
        logging.info("")
        logging.info("****************************************************************************")
        logging.info("****************************************************************************")
      
      # reset
      sys_tenant_id = 1
      change_tenant(cur, sys_tenant_id)

    except Exception, e:
      logging.exception('run error')
      raise e
    finally:
      # 将回滚sql写到文件中
      cur.close()
      conn.close()
  except mysql.connector.Error, e:
    logging.exception('connection error')
    raise e
  except Exception, e:
    logging.exception('normal error')
    raise e

def do_priv_check_by_argv(argv):
  upgrade_params = UpgradeParams()
  opts.change_opt_defult_value('log-file', upgrade_params.log_filename)
  opts.parse_options(argv)
  if not opts.has_no_local_opts():
    opts.deal_with_local_opts()
  else:
    opts.check_db_client_opts()
    log_filename = opts.get_opt_log_file()
    upgrade_params.log_filename = log_filename
    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
    config_logging_module(upgrade_params.log_filename)
    try:
      host = opts.get_opt_host()
      port = int(opts.get_opt_port())
      user = opts.get_opt_user()
      password = opts.get_opt_password()
      
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
          host, port, user, password, log_filename)
      do_priv_check(host, port, user, password, upgrade_params)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
      raise e
    except Exception, e:
      logging.exception('normal error')
      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
      raise e


if __name__ == '__main__':
  #cur_filename = sys.argv[0][sys.argv[0].rfind(os.sep)+1:]
  #(cur_file_short_name,cur_file_ext_name1) = os.path.splitext(sys.argv[0])
  #(cur_file_real_name,cur_file_ext_name2) = os.path.splitext(cur_filename)
  #sub_files_dir_suffix = '_extract_files_' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '_' + random_str()
  #sub_files_dir = cur_file_short_name + sub_files_dir_suffix
  #sub_files_short_dir = cur_file_real_name + sub_files_dir_suffix
  #split_py_files(sub_files_dir)
  #print sub_files_dir, sub_files_short_dir
  #print sys.argv[1:]
  do_priv_check_by_argv(sys.argv[1:])
  #exec('from ' + sub_files_short_dir + '.do_upgrade_post import do_upgrade_by_argv')
  #do_upgrade_by_argv(sys.argv[1:])
