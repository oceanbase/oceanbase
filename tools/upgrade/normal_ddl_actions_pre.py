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
class NormalDDLActionPreCreateAllRegionNetworkBandwidthLimit(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 1
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_region_network_bandwidth_limit'""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_region_network_bandwidth_limit'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_region_network_bandwidth_limit'""")
    if len(results) > 0:
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit already exists')
  @staticmethod
  def get_action_ddl():
    return  """CREATE TABLE `__all_region_network_bandwidth_limit` (
                `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
                `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                `src_region` varchar(128) NOT NULL,
                `dst_region` varchar(128) NOT NULL,
                `max_bw` bigint(20) NOT NULL,
                PRIMARY KEY (`src_region`, `dst_region`)
               ) TABLE_ID = 1099511628096 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
           """
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """show tables from oceanbase like '__all_region_network_bandwidth_limit'""")
    my_utils.query_and_dump_results(self._query_cursor, """show columns from oceanbase.__all_region_network_bandwidth_limit""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show tables from oceanbase like '__all_region_network_bandwidth_limit'""")
    if len(results) != 1:
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit not exists')
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_region_network_bandwidth_limit""")
    if len(results) != 5:
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column descs')
    elif results[0][0] != 'gmt_create' or results[0][1] != 'timestamp(6)' or results[0][2] != 'YES':
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column gmt_create')
    elif results[1][0] != 'gmt_modified' or results[1][1] != 'timestamp(6)' or results[1][2] != 'YES':
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column gmt_modified')
    elif results[2][0] != 'src_region' or results[2][1] != 'varchar(128)' or results[2][2] != 'NO' or results[2][3] != 'PRI':
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column src_region')
    elif results[3][0] != 'dst_region' or results[3][1] != 'varchar(128)' or results[3][2] != 'NO' or results[3][3] != 'PRI':
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column dst_region')
    elif results[4][0] != 'max_bw' or results[4][1] != 'bigint(20)' or results[4][2] != 'NO':
      raise MyError('table oceanbase.__all_region_network_bandwidth_limit has invalid column max_bw')

class NormalDDLActionPreAddColumnAllRoutine(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 2
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_routine""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine like 'type_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine like 'type_id'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_routine.type_id already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_routine add column `type_id` bigint(20) DEFAULT '-1' id 35"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_routine""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine like 'type_id'""")
    if len(results) != 1:
      raise MyError('failed to add column type_id for oceanbase.__all_routine')

class NormalDDLActionPreAddColumnAllRoutineHistory(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 3
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_routine_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine_history like 'type_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine_history like 'type_id'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_routine_history.type_id already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_routine_history add column `type_id` bigint(20) DEFAULT '-1' id 36"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_routine_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_routine_history like 'type_id'""")
    if len(results) != 1:
      raise MyError('failed to add column type_id for oceanbase.__all_routine_history')

class NormalDDLActionPreAddColumnAssociationTableIdAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 4
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'association_table_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'association_table_id'""")
    if len(results) != 0:
      raise MyError('association_table_id column alread exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table add column `association_table_id` bigint(20) NOT NULL DEFAULT '-1' id 90"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_table drop column association_table_id"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'association_table_id'""")
    if len(results) != 1:
      raise MyError('failed to add column association_table_id for oceanbase.__all_table')

class NormalDDLActionPreAddColumnAssociationTableIdAllTableHistory(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 5
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'association_table_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'association_table_id'""")
    if len(results) != 0:
      raise MyError('association_table_id column alread exists')
  @staticmethod
  def get_action_ddl():
   return """alter table oceanbase.__all_table_history add column `association_table_id` bigint(20) DEFAULT '-1' id 91"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_table_history drop column association_table_id"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'association_table_id'""")
    if len(results) != 1:
      raise MyError('failed to add column association_table_id for oceanbase.__all_table_history')


class NormalDDLActionPreAllPartAddPartitionType(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 6
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'partition_type'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'partition_type'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_part.partition_type already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_part add column `partition_type` bigint(20) NOT NULL DEFAULT '0' id 49"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part like 'partition_type'""")
    if len(results) != 1:
      raise MyError('failed to add column partition_type for oceanbase.__all_part')

class NormalDDLActionPreAllPartHistoryAddPartitionType(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 7
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'partition_type'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'partition_type'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_part_history.partition_type already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_part_history add column `partition_type` bigint(20) DEFAULT '0' id 50"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_part_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_part_history like 'partition_type'""")
    if len(results) != 1:
      raise MyError('failed to add column partition_type for oceanbase.__all_part_history')

class NormalDDLActionPreAllSubPartAddPartitionType(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 8
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sub_part""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part like 'partition_type'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part like 'partition_type'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_sub_part.partition_type already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_sub_part add column `partition_type` bigint(20) NOT NULL DEFAULT '0' id 41"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sub_part""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part like 'partition_type'""")
    if len(results) != 1:
      raise MyError('failed to add column partition_type for oceanbase.__all_sub_part')

class NormalDDLActionPreAllSubPartHistoryAddPartitionType(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 9
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sub_part_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part_history like 'partition_type'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part_history like 'partition_type'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_sub_part_history.partition_type already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_sub_part_history add column `partition_type` bigint(20) DEFAULT '0' id 42"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_sub_part_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_sub_part_history like 'partition_type'""")
    if len(results) != 1:
      raise MyError('failed to add column partition_type for oceanbase.__all_sub_part_history')

class NormalDDLActionPreAddBMemberListArg(BaseDDLAction):
 @staticmethod
 def get_seq_num():
   return 11
 def dump_before_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_partition_member_list""")
 def skip_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list like 'b_member_list_arg'""")
   return (len(results) > 0)
 def check_before_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list like 'b_member_list_arg'""")
   if len(results) != 0:
     raise MyError('b_member_list_arg column alread exists')
 @staticmethod
 def get_action_ddl():
   return """alter table oceanbase.__all_partition_member_list add column `b_member_list_arg` longtext id 24"""
 @staticmethod
 def get_rollback_sql():
   return """"""
 def dump_after_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_partition_member_list""")
 def check_after_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list like 'b_member_list_arg'""")
   if len(results) != 1:
     raise MyError('failed to add column b_member_list_arg for oceanbase.__all_partition_member_list')

class NormalDDLActionPreModifyAllPartitionMemberList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 12
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_partition_member_list""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list where field = 'member_list' and type = 'longtext'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list like 'member_list'""")
    if len(results) != 1:
      raise MyError('table oceanbase.__all_rootservice_event_history column member_list not exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_partition_member_list modify column `member_list` longtext"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_partition_member_list""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_partition_member_list where field = 'member_list' and type = 'longtext'""")
    if len(results) != 1:
      raise MyError('fail to modify column member_list for oceanbase.__all_partition_member_list')

class NormalDDLActionPreAddAllBackupBackupPieceJob(BaseDDLAction):
  table_name = '__all_backup_backuppiece_job'
  @staticmethod
  def get_seq_num():
    return 13
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
    return """ CREATE TABLE `__all_backup_backuppiece_job` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `job_id` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `backup_piece_id` bigint(20) NOT NULL,
  `max_backup_times` bigint(20) NOT NULL,
  `result` bigint(20) NOT NULL,
  `status` varchar(64) NOT NULL,
  `backup_dest` varchar(2048) DEFAULT NULL,
  `comment` varchar(4096) NOT NULL,
  `type` bigint(20) NOT NULL,
  PRIMARY KEY (`job_id`, `tenant_id`, `incarnation`, `backup_piece_id`)
) TABLE_ID = 1099511628086 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' 
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
    if len(results) != 12:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
        
class NormalDDLActionPreAddAllBackupBackupPieceJobHistory(BaseDDLAction):
  table_name = '__all_backup_backuppiece_job_history'
  @staticmethod
  def get_seq_num():
    return 14
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
    return """  CREATE TABLE `__all_backup_backuppiece_job_history` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `job_id` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `backup_piece_id` bigint(20) NOT NULL,
  `max_backup_times` bigint(20) NOT NULL,
  `result` bigint(20) NOT NULL,
  `status` varchar(64) NOT NULL,
  `backup_dest` varchar(2048) DEFAULT NULL,
  `comment` varchar(4096) NOT NULL,
  `type` bigint(20) NOT NULL,
  PRIMARY KEY (`job_id`, `tenant_id`, `incarnation`, `backup_piece_id`)
) TABLE_ID = 1099511628087 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' 
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
    if len(results) != 12:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
        
class NormalDDLActionPreAddAllBackupBackupPieceTask(BaseDDLAction):
  table_name = '__all_backup_backuppiece_task'
  @staticmethod
  def get_seq_num():
    return 15
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
      CREATE TABLE `__all_backup_backuppiece_task` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `job_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `round_id` bigint(20) NOT NULL,
  `backup_piece_id` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `start_time` timestamp(6) NOT NULL,
  `end_time` timestamp(6) NOT NULL,
  `status` varchar(64) NOT NULL,
  `backup_dest` varchar(2048) DEFAULT NULL,
  `result` bigint(20) NOT NULL,
  `comment` varchar(4096) NOT NULL,
  PRIMARY KEY (`job_id`, `incarnation`, `tenant_id`, `round_id`, `backup_piece_id`, `copy_id`)
) TABLE_ID = 1099511628088 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' 
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
    if len(results) != 14:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
class NormalDDLActionPreAddAllBackupBackupPieceTaskHistory(BaseDDLAction):
  table_name = '__all_backup_backuppiece_task_history'
  @staticmethod
  def get_seq_num():
    return 16
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
    CREATE TABLE `__all_backup_backuppiece_task_history` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `job_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `round_id` bigint(20) NOT NULL,
  `backup_piece_id` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `start_time` timestamp(6) NOT NULL,
  `end_time` timestamp(6) NOT NULL,
  `status` varchar(64) NOT NULL,
  `backup_dest` varchar(2048) DEFAULT NULL,
  `result` bigint(20) NOT NULL,
  `comment` varchar(4096) NOT NULL,
  PRIMARY KEY (`job_id`, `incarnation`, `tenant_id`, `round_id`, `backup_piece_id`, `copy_id`)
) TABLE_ID = 1099511628089 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'   
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
    if len(results) != 14:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
class NormalDDLActionPreAddAllBackupSetFiles(BaseDDLAction):
  table_name = '__all_backup_set_files'
  @staticmethod
  def get_seq_num():
    return 17
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
     CREATE TABLE `__all_backup_set_files` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `incarnation` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `backup_set_id` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `backup_type` varchar(1) NOT NULL,
  `snapshot_version` bigint(20) NOT NULL,
  `prev_full_backup_set_id` bigint(20) NOT NULL,
  `prev_inc_backup_set_id` bigint(20) NOT NULL,
  `prev_backup_data_version` bigint(20) NOT NULL,
  `pg_count` bigint(20) NOT NULL,
  `macro_block_count` bigint(20) NOT NULL,
  `finish_pg_count` bigint(20) NOT NULL,
  `finish_macro_block_count` bigint(20) NOT NULL,
  `input_bytes` bigint(20) NOT NULL,
  `output_bytes` bigint(20) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `end_time` bigint(20) NOT NULL,
  `compatible` bigint(20) NOT NULL,
  `cluster_version` bigint(20) NOT NULL,
  `status` varchar(64) NOT NULL,
  `result` bigint(20) NOT NULL,
  `cluster_id` bigint(20) NOT NULL,
  `backup_data_version` bigint(20) NOT NULL,
  `backup_schema_version` bigint(20) NOT NULL,
  `cluster_version_display` varchar(64) NOT NULL,
  `partition_count` bigint(20) NOT NULL,
  `finish_partition_count` bigint(20) NOT NULL,
  `encryption_mode` varchar(64) NOT NULL DEFAULT 'None',
  `passwd` varchar(128) NOT NULL DEFAULT '',
  `file_status` varchar(64) NOT NULL,
  `backup_dest` varchar(2048) NOT NULL,
  `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0',
  `date` bigint(20) NOT NULL DEFAULT '0',
  `backup_level` varchar(64) NOT NULL DEFAULT 'CLUSTER',
  PRIMARY KEY (`incarnation`, `tenant_id`, `backup_set_id`, `copy_id`)
) TABLE_ID = 1099511628091 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
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
    if len(results) != 36:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
class NormalDDLActionPreAddAllBackupInfo(BaseDDLAction):
  table_name = '__all_backup_info'
  @staticmethod
  def get_seq_num():
    return 18
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
     CREATE TABLE `__all_backup_info` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `name` varchar(1024) NOT NULL,
  `value` longtext NOT NULL,
  PRIMARY KEY (`name`)
) TABLE_ID = 1099511628093 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' 
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
    if len(results) != 4:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
class NormalDDLActionPreAddAllBackupLogArchiveStatusV2(BaseDDLAction):
  table_name = '__all_backup_log_archive_status_v2'
  @staticmethod
  def get_seq_num():
    return 19
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
     CREATE TABLE `__all_backup_log_archive_status_v2` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `tenant_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `log_archive_round` bigint(20) NOT NULL,
  `min_first_time` timestamp(6) NOT NULL,
  `max_next_time` timestamp(6) NOT NULL,
  `input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `pg_count` bigint(20) NOT NULL DEFAULT '0',
  `status` varchar(64) NOT NULL,
  `is_mount_file_created` bigint(20) NOT NULL DEFAULT '0',
  `compatible` bigint(20) NOT NULL DEFAULT '0',
  `start_piece_id` bigint(20) NOT NULL DEFAULT '0',
  `backup_piece_id` bigint(20) NOT NULL DEFAULT '0',
  `backup_dest` varchar(2048) NOT NULL DEFAULT '',
  PRIMARY KEY (`tenant_id`)
) TABLE_ID = 1099511628094 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase' 
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
    if len(results) != 18:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))
class NormalDDLActionPreAddAllBackupBackupLogArchiveStatusV2(BaseDDLAction):
  table_name = '__all_backup_backup_log_archive_status_v2'
  @staticmethod
  def get_seq_num():
    return 20
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
     CREATE TABLE `__all_backup_backup_log_archive_status_v2` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `tenant_id` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `incarnation` bigint(20) NOT NULL,
  `log_archive_round` bigint(20) NOT NULL,
  `min_first_time` timestamp(6) NOT NULL,
  `max_next_time` timestamp(6) NOT NULL,
  `input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_input_bytes` bigint(20) NOT NULL DEFAULT '0',
  `deleted_output_bytes` bigint(20) NOT NULL DEFAULT '0',
  `pg_count` bigint(20) NOT NULL DEFAULT '0',
  `status` varchar(64) NOT NULL,
  `is_mount_file_created` bigint(20) NOT NULL DEFAULT '0',
  `compatible` bigint(20) NOT NULL DEFAULT '0',
  `start_piece_id` bigint(20) NOT NULL DEFAULT '0',
  `backup_piece_id` bigint(20) NOT NULL DEFAULT '0',
  `backup_dest` varchar(2048) NOT NULL DEFAULT '',
  PRIMARY KEY (`tenant_id`)
) TABLE_ID =  1099511628097 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'  
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
    if len(results) != 19:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))

class NormalDDLActionPreAddColumnAllUserMaxConnections(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 21
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_connections'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_connections'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_user.max_connections already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_user add column `max_connections` bigint(20) NOT NULL DEFAULT '0' id 53"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_connections'""")
    if len(results) != 1:
      raise MyError('failed to add column max_connections for oceanbase.__all_user')

class NormalDDLActionPreAddColumnAllUserMaxUserConnections(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 22
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_user_connections'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_user_connections'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_user.max_user_connections already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_user add column `max_user_connections` bigint(20) NOT NULL DEFAULT '0' id 54"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user like 'max_user_connections'""")
    if len(results) != 1:
      raise MyError('failed to add column max_user_connections for oceanbase.__all_user')

class NormalDDLActionPreAddColumnAllUserHistoryMaxConnections(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 23
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_connections'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_connections'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_user_history.max_connections already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_user_history add column `max_connections` bigint(20) DEFAULT '0' id 55"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_connections'""")
    if len(results) != 1:
      raise MyError('failed to add column max_connections for oceanbase.__all_user_history')

class NormalDDLActionPreAddColumnAllUserHistoryMaxUserConnections(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 24
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_user_connections'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_user_connections'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_user_history.max_user_connections already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_user_history add column `max_user_connections` bigint(20) DEFAULT '0' id 56"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_user_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_user_history like 'max_user_connections'""")
    if len(results) != 1:
      raise MyError('failed to add column max_user_connections for oceanbase.__all_user_history')

class NormalDDLActionPreAllDblinkAddDriveProto(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 25
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'driver_proto'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'driver_proto'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column driver_proto exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `driver_proto` bigint(20) NOT NULL DEFAULT '0' id 28"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column driver_proto"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'driver_proto'""")
    if len(results) != 1:
      raise MyError('fail to modify column driver_proto for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddFlag(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 26
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'flag'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'flag'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column flag exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `flag` bigint(20) NOT NULL DEFAULT '0' id 29"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column flag"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'flag'""")
    if len(results) != 1:
      raise MyError('fail to modify column flag for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddConnString(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 27
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'conn_string'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'conn_string'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column conn_string exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `conn_string` varchar(4096) DEFAULT '' id 30"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column conn_string"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'conn_string'""")
    if len(results) != 1:
      raise MyError('fail to modify column conn_string for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddServiceName(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 28
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'service_name'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'service_name'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column service_name exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `service_name` varchar(128) DEFAULT '' id 31"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column service_name"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'service_name'""")
    if len(results) != 1:
      raise MyError('fail to modify column service_name for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddAuthusr(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 29
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authusr'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authusr'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column authusr exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `authusr` varchar(128) DEFAULT '' id 32"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column authusr"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authusr'""")
    if len(results) != 1:
      raise MyError('fail to modify column authusr for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddAuthpwd(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 30
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwd'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwd'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column authpwd exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `authpwd` varchar(128) DEFAULT '' id 33"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column authpwd"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwd'""")
    if len(results) != 1:
      raise MyError('fail to modify column authpwd for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddPasswordx(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 31
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'passwordx'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'passwordx'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column passwordx exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `passwordx` varbinary(128) DEFAULT '' id 34"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column passwordx"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'passwordx'""")
    if len(results) != 1:
      raise MyError('fail to modify column passwordx for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkAddAuthpwdx(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 32
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwdx'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwdx'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink column authpwdx exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink add column `authpwdx` varbinary(128) DEFAULT '' id 35"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink drop column authpwdx"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink like 'authpwdx'""")
    if len(results) != 1:
      raise MyError('fail to modify column authpwdx for oceanbase.__all_dblink')

class NormalDDLActionPreAllDblinkHistoryAddDriveProto(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 33
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'driver_proto'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'driver_proto'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column driver_proto exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `driver_proto` bigint(20) DEFAULT '0' id 30"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column driver_proto"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'driver_proto'""")
    if len(results) != 1:
      raise MyError('fail to modify column driver_proto for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddFlag(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 34
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'flag'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'flag'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column flag exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `flag` bigint(20) DEFAULT '0' id 31"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column flag"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'flag'""")
    if len(results) != 1:
      raise MyError('fail to modify column flag for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddConnString(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 35
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'conn_string'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'conn_string'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column conn_string exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `conn_string` varchar(4096) DEFAULT '' id 32"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column conn_string"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'conn_string'""")
    if len(results) != 1:
      raise MyError('fail to modify column conn_string for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddServiceName(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 36
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'service_name'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'service_name'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column service_name exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `service_name` varchar(128) DEFAULT '' id 33"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column service_name"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'service_name'""")
    if len(results) != 1:
      raise MyError('fail to modify column service_name for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddAuthusr(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 37
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authusr'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authusr'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column authusr exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `authusr` varchar(128) DEFAULT '' id 34"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column authusr"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authusr'""")
    if len(results) != 1:
      raise MyError('fail to modify column authusr for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddAuthpwd(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 38
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwd'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwd'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column authpwd exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `authpwd` varchar(128) DEFAULT '' id 35"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column authpwd"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwd'""")
    if len(results) != 1:
      raise MyError('fail to modify column authpwd for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddPasswordx(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 39
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'passwordx'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'passwordx'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column passwordx exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `passwordx` varbinary(128) DEFAULT '' id 36"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column passwordx"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'passwordx'""")
    if len(results) != 1:
      raise MyError('fail to modify column passwordx for oceanbase.__all_dblink_history')

class NormalDDLActionPreAllDblinkHistoryAddAuthpwdx(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 40
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwdx'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwdx'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_dblink_history column authpwdx exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_dblink_history add column `authpwdx` varbinary(128) DEFAULT '' id 37"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_dblink_history drop column authpwdx"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_dblink_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_dblink_history like 'authpwdx'""")
    if len(results) != 1:
      raise MyError('fail to modify column authpwdx for oceanbase.__all_dblink_history')

class NormalDDLActionPreAddColumnTransitionPointAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 41
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table.transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table add column `transition_point` varchar(4096) DEFAULT NULL id 91"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column transition_point for oceanbase.__all_table')

class NormalDDLActionPreAddColumnTransitionPointAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 42
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_history.transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_history add column `transition_point` varchar(4096) DEFAULT NULL id 92"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column transition_point for oceanbase.__all_table_history')

class NormalDDLActionPreAddColumnBTransitionPointAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 43
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table.b_transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table add column `b_transition_point` varchar(8192) DEFAULT NULL id 92"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column b_transition_point for oceanbase.__all_table')

class NormalDDLActionPreAddColumnBTransitionPointAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 44
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_history.b_transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_history add column `b_transition_point` varchar(8192) DEFAULT NULL NULL id 93"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column b_transition_point for oceanbase.__all_table_history')

class NormalDDLActionPreAddColumnIntervalRangeAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 45
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table.interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table add column `interval_range` varchar(4096) DEFAULT NULL id 93"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column interval_range for oceanbase.__all_table')

class NormalDDLActionPreAddColumnIntervalRangeAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 46
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_history.interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_history add column `interval_range` varchar(4096) DEFAULT NULL id 94"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column interval_range for oceanbase.__all_table_history')

class NormalDDLActionPreAddColumnBIntervalRangeAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 47
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table.b_interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table add column `b_interval_range` varchar(8192) DEFAULT NULL id 94"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table like 'b_interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column b_interval_range for oceanbase.__all_table')

class NormalDDLActionPreAddColumnBIntervalRangeAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 48
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_history.b_interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_history add column `b_interval_range` varchar(8192) DEFAULT NULL id 95"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_history like 'b_interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column b_interval_range for oceanbase.__all_table_history')

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

