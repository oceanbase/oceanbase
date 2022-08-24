#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
from actions import BaseEachTenantDDLAction
from actions import reflect_action_cls_list
from actions import fetch_observer_version
from actions import QueryCursor
import logging
import time
import my_utils
import actions

'''
添加一条each tenant ddl的方法:

在本文件中，添加一个类名以"EachTenantDDLActionPost"开头并且继承自BaseEachTenantDDLAction的类，
然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
(1)@staticmethod get_seq_num():
返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
(2)dump_before_do_action(self):
执行action sql之前把一些相关数据dump到日志中。
(3)check_before_do_action(self):
执行action sql之前的检查。
(4)dump_before_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之前把一些相关数据dump到日志中。
(5)check_before_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之前的检查。
(6)@staticmethod get_each_tenant_action_ddl(tenant_id):
返回用参数tenant_id拼成的一条action sql，并且该sql必须为ddl。
(7)@staticmethod get_each_tenant_rollback_sql(tenant_id):
返回一条sql，用于回滚get_each_tenant_action_ddl(tenant_id)返回的sql。
(8)dump_after_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之后把一些相关数据dump到日志中。
(9)check_after_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之后的检查。
(10)dump_after_do_action(self):
执行action sql之后把一些相关数据dump到日志中。
(11)check_after_do_action(self):
执行action sql之后的检查。
(12)skip_pre_check(self):
check if check_before_do_action() can be skipped
(13)skip_each_tenant_action(self):
check if check_before_do_each_tenant_action() and do_each_tenant_action() can be skipped

举例: 以下为schema拆分后加租户级系统表的示例
class EachTenantDDLActionPostCreateAllTenantBackupBackupLogArchiveStatus(BaseEachTenantDDLAction):
  table_name = '__all_tenant_backup_backup_log_archive_status'
  @staticmethod
  def get_seq_num():
    return 24
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
  def skip_pre_check(self):
    return True
  def skip_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
    return (1 == len(results))
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
    if len(results) > 0:
      raise MyError("""{0} already created""".format(self.table_name))
  def dump_before_do_each_tenant_action(self, tenant_id):
    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
  def check_before_do_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
    if len(results) > 0:
      raise MyError("""tenant_id:{0} has already create table {1}""".format(tenant_id, self.table_name))
  @staticmethod
  def get_each_tenant_action_ddl(tenant_id):
    pure_table_id = 303
    table_id = (tenant_id << 40) | pure_table_id
    return """CREATE TABLE `__all_tenant_backup_backup_log_archive_status` (
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
  `status` varchar(64) NOT NULL DEFAULT '',
  PRIMARY KEY (`tenant_id`, `incarnation`, `log_archive_round`, `copy_id`)
) TABLE_ID={0} DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
    """.format(table_id)
  @staticmethod
  def get_each_tenant_rollback_sql(tenant_id):
    return """select 1"""
  def dump_after_do_each_tenant_action(self, tenant_id):
    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
  def check_after_do_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}' and tenant_id = {2}""".format(self.get_all_table_name(), self.table_name, tenant_id))
    if len(results) != 1:
      raise MyError("""tenant_id:{0} create table {1} failed""".format(tenant_id, self.table_name))
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select tenant_id, table_id, table_name from {0} where table_name = '{1}'""".format(self.get_all_table_name(), self.table_name))
    if len(results) != len(self.get_tenant_id_list()):
      raise MyError("""there should be {0} rows in {1} whose table_name is {2}, but there has {3} rows like that""".format(len(self.get_tenant_id_list()), self.get_all_table_name(), self.table_name, len(results)))
'''


#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。

####========******####======== actions begin ========####******========####
####========******####========= actions end =========####******========####

def do_each_tenant_ddl_actions(cur, tenant_id_list):
  import each_tenant_ddl_actions_post
  # 组户级系统表没法通过虚拟表暴露，需要根据版本决定查哪张实体表
  query_cur = QueryCursor(cur)
  version = fetch_observer_version(query_cur)
  all_table_name = "__all_table"
  if (cmp(version, "2.2.60") >= 0) :
    all_table_name = "__all_table_v2"

  cls_list = reflect_action_cls_list(each_tenant_ddl_actions_post, 'EachTenantDDLActionPost')

  # set parameter
  if len(cls_list) > 0:
    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'True')
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'True')

  for cls in cls_list:
    logging.info('do each tenant ddl acion, seq_num: %d', cls.get_seq_num())
    action = cls(cur, tenant_id_list)
    action.set_all_table_name(all_table_name)
    action.dump_before_do_action()
    if False == action.skip_pre_check():
      action.check_before_do_action()
    else:
      logging.info("skip pre check. seq_num: %d", cls.get_seq_num())
    # 系统租户组户级系统表创建成功会覆盖普通租户系统表，所以系统租户要最后建表
    for tenant_id in action.get_tenant_id_list():
      action.dump_before_do_each_tenant_action(tenant_id)
      if False == action.skip_each_tenant_action(tenant_id):
        action.check_before_do_each_tenant_action(tenant_id)
        action.do_each_tenant_action(tenant_id)
      else:
        logging.info("skip each tenant ddl action, seq_num: %d, tenant_id: %d", cls.get_seq_num(), tenant_id)
      action.dump_after_do_each_tenant_action(tenant_id)
      action.check_after_do_each_tenant_action(tenant_id)
    action.dump_after_do_action()
    action.check_after_do_action()

  # reset parameter
  if len(cls_list) > 0:
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl' , 'False')
    actions.set_parameter(cur, 'enable_sys_table_ddl' , 'False')

def get_each_tenant_ddl_actions_sqls_str(tenant_id_list):
  import each_tenant_ddl_actions_post
  ret_str = ''
  cls_list = reflect_action_cls_list(each_tenant_ddl_actions_post, 'EachTenantDDLActionPost')
  for i in range(0, len(cls_list)):
    for j in range(0, len(tenant_id_list)):
      if i > 0 or j > 0:
        ret_str += '\n'
      ret_str += cls_list[i].get_each_tenant_action_ddl(tenant_id_list[j]) + ';'
  return ret_str
