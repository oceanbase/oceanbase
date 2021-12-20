#!/usr/bin/env python
# -*- coding: utf-8 -*-
from my_error import MyError
from actions import BaseDDLAction
from actions import reflect_action_cls_list
from actions import fetch_observer_version
from actions import QueryCursor
import logging
import time
import my_utils
import actions
import re

class UpgradeParams:
  low_version = '1.4.73'
  high_version = '2.0.0'

'''
添加一条normal ddl的方法:

在本文件中，添加一个类名以"NormalDDLActionPre"开头并且继承自BaseDDLAction的类，
然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
(1)@staticmethod get_seq_num():
返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
(2)dump_before_do_action(self):
执行action sql之前把一些相关数据dump到日志中。
(3)check_before_do_action(self):
执行action sql之前的检查。
(4)@staticmethod get_action_ddl():
返回action sql，并且该sql必须为ddl。
(5)@staticmethod get_rollback_sql():
返回回滚该action的sql。
(6)dump_after_do_action(self):
执行action sql之后把一些相关数据dump到日志中。
(7)check_after_do_action(self):
执行action sql之后的检查。
(8)skip_action(self):
check if check_before_do_action() and do_action() can be skipped

加表示例：
class NormalDDLActionPreAddAllBackupBackupLogArchiveStatusHistory(BaseDDLAction):
  table_name = '__all_backup_backup_log_archive_stat'
  @staticmethod
  def get_seq_num():
    return 102
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '{0}'""".format(self.table_name))
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
    return (len(results) > 0)
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
    if len(results) > 0:
      raise MyError("""table oceanabse.{0} already exists""".format(self.table_name))
  @staticmethod
  def get_action_ddl():
    return """
    CREATE TABLE `__all_backup_backup_log_archive_status_history` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `tenant_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `log_archive_round` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `min_first_time` timestamp(6) NOT NULL,
  `max_next_time` timestamp(6) NOT NULL,
  `input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `pg_count` bigint(20) NOT NULL DEFAULT '0',
  `backup_dest` varchar(2048) DEFAULT NULL,
  `is_mark_deleted` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`tenant_id`, `incarnation`, `log_archive_round`, `copy_id`)
) TABLE_ID = 1099511628080 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
           """
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '{0}'""".format(self.table_name))
    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.{0}""".format(self.table_name))
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '{0}'""".format(self.table_name))
    if len(results) != 1:
      raise MyError("""table oceanbase.{0} not exists""".format(self.table_name))
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.{0}""".format(self.table_name))
    if len(results) != 15:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))

改列示例：
class NormalDDLActionPreModifyAllRestoreInfoValue(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 12
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info like 'value'""")
    if len(results) != 1:
      raise MyError('table oceanbase.__all_rootservice_event_history column value not exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_info modify column `value` longtext NOT NULL"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
    if len(results) != 1:
      raise MyError('fail to modify column value for oceanbase.__all_restore_info')

加列示例：
class NormalDDLActionPreAllTenantProfileAddVerifyFunction(BaseDDLAction):
 @staticmethod
 def get_seq_num():
   return 0
 def dump_before_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
 def skip_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
   return len(results) > 0;
 def check_before_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
   if len(results) != 0:
     raise MyError('password_verify_function column alread exists')
 @staticmethod
 def get_action_ddl():
   return """alter table oceanbase.__all_tenant_profile add column `password_verify_function` varchar(30) DEFAULT NULL id 23"""
 @staticmethod
 def get_rollback_sql():
   return """alter table oceanbase.__all_tenant_profile drop column password_verify_function"""
 def dump_after_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_profile""")
 def check_after_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_profile like 'password_verify_function'""")
   if len(results) != 1:
     raise MyError('failed to add column password_verify_function for oceanbase.__all_tenant_profile')

'''

#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。

####========******####======== actions begin ========####******========####        
####========******####========= actions end =========####******========####
def do_normal_ddl_actions(cur):
  import normal_ddl_actions_pre
  upgrade_params = UpgradeParams()
  cls_list = reflect_action_cls_list(normal_ddl_actions_pre, 'NormalDDLActionPre')

  # check if pre upgrade script can run reentrantly
  query_cur = QueryCursor(cur)
  version = fetch_observer_version(query_cur)
  can_skip = False
  if (cmp(version, "2.2.77") >= 0 and cmp(version, "3.0.0") < 0):
    can_skip = True
  elif (cmp(version, "3.1.1") >= 0):
    can_skip = True
  else:
    can_skip = False

  # set parameter
  if len(cls_list) > 0:
    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'True')
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'True')

  for cls in cls_list:
    logging.info('do normal ddl acion, seq_num: %d', cls.get_seq_num())
    action = cls(cur)
    action.dump_before_do_action()
    if False == can_skip or False == action.skip_action():
      action.check_before_do_action()
      action.do_action()
    else:
      logging.info("skip ddl action, seq_num: %d", cls.get_seq_num())
    action.dump_after_do_action()
    action.check_after_do_action()

  # reset parameter
  if len(cls_list) > 0:
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl' , 'False')
    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'False')

def get_normal_ddl_actions_sqls_str(query_cur):
  import normal_ddl_actions_pre
  ret_str = ''
  cls_list = reflect_action_cls_list(normal_ddl_actions_pre, 'NormalDDLActionPre')
  for i in range(0, len(cls_list)):
    if i > 0:
      ret_str += '\n'
    ret_str += cls_list[i].get_action_ddl() + ';'
  return ret_str

