#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
from actions import BaseDDLAction
from actions import reflect_action_cls_list
import logging
import time
import my_utils
import actions

'''
添加一条normal ddl的方法:

在本文件中，添加一个类名以"NormalDDLActionPost"开头并且继承自BaseDDLAction的类，
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

改列示例：
class NormalDDLActionPostModifyAllRestoreInfoValue(BaseDDLAction):
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
class NormalDDLActionPostAllTenantProfileAddVerifyFunction(BaseDDLAction):
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
class NormalDDLActionPostAllTableV2AddAssociationTableId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 0
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'association_table_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'association_table_id'""")
    if len(results) != 0:
      raise MyError('association_table_id column alread exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2 add column `association_table_id` bigint(20) NOT NULL DEFAULT '-1' id 90"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_table_v2 drop column association_table_id"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'association_table_id'""")
    if len(results) != 1:
      raise MyError('failed to add column association_table_id for oceanbase.__all_table_v2')

class NormalDDLActionPostAllTableV2HistoryAddAssociationTableId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 1
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'association_table_id'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'association_table_id'""")
    if len(results) != 0:
      raise MyError('association_table_id column alread exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2_history add column `association_table_id` bigint(20) DEFAULT '-1' id 91"""
  @staticmethod
  def get_rollback_sql():
    return """alter table oceanbase.__all_table_v2_history drop column association_table_id"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'association_table_id'""")
    if len(results) != 1:
      raise MyError('failed to add column association_table_id for oceanbase.__all_table_v2_history')
##FIXME: to remove
class NormalDDLActionPostModifyAllRestoreInfoValue(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 2
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_info""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info where field = 'value' and type = 'longtext'""")
    return len(results) > 0;
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
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_info like 'value'""")
    if len(results) != 1:
      raise MyError('fail to modify column value for oceanbase.__all_restore_info')

class NormalDDLActionPostAddAllRestoreProgressWhiteList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 3
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'white_list'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'white_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_progress column white_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_progress add column `white_list` longtext NULL id 39"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'white_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column white_list for oceanbase.__all_restore_progress')

class NormalDDLActionPostAddAllRestoreHistoryWhiteList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 4
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'white_list'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'white_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_history column white_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_history add column `white_list` longtext NULL id 42"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'white_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column white_list for oceanbase.__all_restore_history')

class NormalDDLActionPostAddAllRestoreProgressBackupSetList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 6
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_set_list'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_set_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_progress column backup_set_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_progress add column `backup_set_list` longtext NULL id 40"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_set_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_set_list for oceanbase.__all_restore_progress')

class NormalDDLActionPostAddAllRestoreProgressBackupPieceList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 7
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_piece_list'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_piece_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_progress column backup_piece_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_progress add column `backup_piece_list` longtext NULL id 41"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_progress""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_progress like 'backup_piece_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_piece_list for oceanbase.__all_restore_progress')
class NormalDDLActionPostAddAllRestoreHistoryBackupSetList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 8
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_set_list'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_set_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_history column backup_set_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_history add column `backup_set_list` longtext NULL id 43"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_set_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_set_list for oceanbase.__all_restore_history')


class NormalDDLActionPostAddAllRestoreHistoryBackupPieceList(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 9
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_piece_list'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_piece_list'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_restore_history column backup_piece_list exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_restore_history add column `backup_piece_list` longtext NULL id 44"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_restore_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_restore_history like 'backup_piece_list'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_piece_list for oceanbase.__all_restore_history')

class NormalDDLActionPostAddAllBackupBackupSetJobBackupDest(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 10
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'backup_dest'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'backup_dest'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job column backup_dest exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job add column `backup_dest` varchar(2048) DEFAULT NULL id 26"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'backup_dest'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_dest for oceanbase.__all_backup_backupset_job')



class NormalDDLActionPostAddAllBackupBackupSetJobMaxBackupTimes(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 11
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'max_backup_times'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'max_backup_times'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job column max_backup_times exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job add column `max_backup_times` bigint(20) NOT NULL id 27"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'max_backup_times'""")
    if len(results) != 1:
      raise MyError('fail to modify column max_backup_times for oceanbase.__all_backup_backupset_job')
class NormalDDLActionPostAddAllBackupBackupSetJobResult(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 12
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'result'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'result'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job column result exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job add column `result` bigint(20) NOT NULL id 28"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'result'""")
    if len(results) != 1:
      raise MyError('fail to modify column result for oceanbase.__all_backup_backupset_job')
class NormalDDLActionPostAddAllBackupBackupSetJobComment(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 13
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'comment'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'comment'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job column comment exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job add column `comment` varchar(4096) NOT NULL id 29"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job like 'comment'""")
    if len(results) != 1:
      raise MyError('fail to modify column comment for oceanbase.__all_backup_backupset_job')
class NormalDDLActionPostAddAllBackupBackupSetJobHistoryBackupDest(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 14
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'backup_dest'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'backup_dest'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job_history column backup_dest exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job_history add column `backup_dest` varchar(2048) DEFAULT NULL id 26"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'backup_dest'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_dest for oceanbase.__all_backup_backupset_job_history')



class NormalDDLActionPostAddAllBackupBackupSetJobHistoryMaxBackupTimes(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 15
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'max_backup_times'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'max_backup_times'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job_history column max_backup_times exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job_history add column `max_backup_times` bigint(20) NOT NULL id 27"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'max_backup_times'""")
    if len(results) != 1:
      raise MyError('fail to modify column max_backup_times for oceanbase.__all_backup_backupset_job_history')
class NormalDDLActionPostAddAllBackupBackupSetJobHistoryResult(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 16
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'result'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'result'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job_history column result exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job_history add column `result` bigint(20) NOT NULL id 28"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'result'""")
    if len(results) != 1:
      raise MyError('fail to modify column result for oceanbase.__all_backup_backupset_job_history')
class NormalDDLActionPostAddAllBackupBackupSetJobHistoryComment(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 17
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'comment'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'comment'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_job_history column comment exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_job_history add column `comment` varchar(4096) NOT NULL id 29"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_job_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_job_history like 'comment'""")
    if len(results) != 1:
      raise MyError('fail to modify column comment for oceanbase.__all_backup_backupset_job_history')
class NormalDDLActionPostAddAllBackupBackupSetTaskHistoryStartReplayLogTs(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 18
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_task_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'start_replay_log_ts'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'start_replay_log_ts'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_task_history column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_task_history add column `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0' id 53"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_task_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'start_replay_log_ts'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_replay_log_ts for oceanbase.__all_backup_backupset_task_history')

class NormalDDLActionPostAddAllBackupBackupSetTaskHistoryDate(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 19
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_task_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'date'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'date'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backupset_task_history column date exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backupset_task_history add column `date` bigint(20) NOT NULL DEFAULT '0' id 54"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backupset_task_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backupset_task_history like 'date'""")
    if len(results) != 1:
      raise MyError('fail to modify column date for oceanbase.__all_backup_backupset_task_history')

class NormalDDLActionPostAddBackupTaskStartReplayLogTs(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 20
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_task""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'start_replay_log_ts'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'start_replay_log_ts'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_tenant_backup_task column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_tenant_backup_task add column `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0' id 48"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_task""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'start_replay_log_ts'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_replay_log_ts for oceanbase.__all_tenant_backup_task')


class NormalDDLActionPostAddBackupTaskStartDate(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 21
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_task""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'date'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'date'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_tenant_backup_task column date exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_tenant_backup_task add column `date` bigint(20) NOT NULL DEFAULT '0' id  49"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_task""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_task like 'date'""")
    if len(results) != 1:
      raise MyError('fail to modify column date for oceanbase.__all_tenant_backup_task')

class NormalDDLActionPostAddBackupBackupsetTaskStartReplayLogTs(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 22
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_backupset_task""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task like 'start_replay_log_ts'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task like 'start_replay_log_ts'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_tenant_backup_backupset_task column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_tenant_backup_backupset_task add column `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0' id  52"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_backupset_task""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task like 'start_replay_log_ts'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_replay_log_ts for oceanbase.__all_tenant_backup_backupset_task')
class NormalDDLActionPostAddBackupBackupsetTaskDate(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 23
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_backupset_task """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task like 'date'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task  like 'date'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_tenant_backup_backupset_task column date exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_tenant_backup_backupset_task add column `date` bigint(20) NOT NULL DEFAULT '0'  id  53"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_tenant_backup_backupset_task""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_tenant_backup_backupset_task like 'date'""")
    if len(results) != 1:
      raise MyError('fail to modify column date for oceanbase.__all_tenant_backup_backupset_task')
class NormalDDLActionPostAddBackupLogArchiveStatusHistoryStartPieceId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 24
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_log_archive_status_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history like 'start_piece_id'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history  like 'start_piece_id'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_log_archive_status_history column start_piece_id exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_log_archive_status_history add column `start_piece_id` bigint(20) NOT NULL DEFAULT '0' id 31"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_log_archive_status_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history like 'start_piece_id'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_piece_id for oceanbase.__all_backup_log_archive_status_history')
class NormalDDLActionPostAddBackupLogArchiveStatusHistoryBackupPieceId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 25
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_log_archive_status_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history like 'backup_piece_id'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history  like 'backup_piece_id'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_log_archive_status_history column backup_piece_id exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_log_archive_status_history add column `backup_piece_id` bigint(20) NOT NULL DEFAULT '0' id 32"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_log_archive_status_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_log_archive_status_history like 'backup_piece_id'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_piece_id for oceanbase.__all_backup_log_archive_status_history')

class NormalDDLActionPostAddBackupTaskHistoryStartReplayLogTs(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 26
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history like 'start_replay_log_ts'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history  like 'start_replay_log_ts'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_task_history column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_task_history add column `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0' id 49"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history like 'start_replay_log_ts'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_replay_log_ts for oceanbase.__all_backup_task_history')
class NormalDDLActionPostAddBackupTaskHistoryDate(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 27
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history like 'date'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history  like 'date'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_task_history column date exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_task_history add column `date` bigint(20) NOT NULL DEFAULT '0' id 50"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_history like 'date'""")
    if len(results) != 1:
      raise MyError('fail to modify column date for oceanbase.__all_backup_task_history')

class NormalDDLActionPostAddBackupTaskCleanHistoryStartReplayLogTs(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 28
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'start_replay_log_ts'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history  like 'start_replay_log_ts'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_task_clean_history column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_task_clean_history add column `start_replay_log_ts` bigint(20) NOT NULL DEFAULT '0' id 48"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'start_replay_log_ts'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_replay_log_ts for oceanbase.__all_backup_task_clean_history')

class NormalDDLActionPostAddBackupTaskCleanHistoryDate(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 29
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'date'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history  like 'date'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_task_clean_history column start_replay_log_ts exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_task_clean_history add column `date` bigint(20) NOT NULL DEFAULT '0' id 49"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'date'""")
    if len(results) != 1:
      raise MyError('fail to modify column date for oceanbase.__all_backup_task_clean_history')

class NormalDDLActionPostAddBackupBackupLogArchiveStatusHistoryCompatible(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 30
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'compatible'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history  like 'compatible'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backup_log_archive_status_history column compatible exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backup_log_archive_status_history add column `compatible` bigint(20) NOT NULL DEFAULT '0' id 31"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'compatible'""")
    if len(results) != 1:
      raise MyError('fail to modify column compatible for oceanbase.__all_backup_backup_log_archive_status_history')

class NormalDDLActionPostAddBackupBackupLogArchiveStatusHistoryStartPieceId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 31
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'start_piece_id'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history  like 'start_piece_id'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backup_log_archive_status_history column start_piece_id exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backup_log_archive_status_history add column `start_piece_id` bigint(20) NOT NULL DEFAULT '0' id 32"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'start_piece_id'""")
    if len(results) != 1:
      raise MyError('fail to modify column start_piece_id for oceanbase.__all_backup_backup_log_archive_status_history')

class NormalDDLActionPostAddBackupBackupLogArchiveStatusHistoryBackupPieceId(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 32
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history """)
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'backup_piece_id'""")
    return len(results) > 0
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history  like 'backup_piece_id'""")
    if len(results) != 0:
      raise MyError('table oceanbase.__all_backup_backup_log_archive_status_history column backup_piece_id exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_backup_backup_log_archive_status_history add column `backup_piece_id` bigint(20) NOT NULL DEFAULT '0' id 33"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_backup_log_archive_status_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_backup_log_archive_status_history like 'backup_piece_id'""")
    if len(results) != 1:
      raise MyError('fail to modify column backup_piece_id for oceanbase.__all_backup_backup_log_archive_status_history')

class NormalDDLActionPostAddAllBackupPieceFiles(BaseDDLAction):
  table_name = '__all_backup_piece_files'
  @staticmethod
  def get_seq_num():
    return 33
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
      CREATE TABLE `__all_backup_piece_files` (
  `gmt_create` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `incarnation` bigint(20) NOT NULL,
  `tenant_id` bigint(20) NOT NULL,
  `round_id` bigint(20) NOT NULL,
  `backup_piece_id` bigint(20) NOT NULL,
  `copy_id` bigint(20) NOT NULL,
  `create_date` bigint(20) NOT NULL DEFAULT '0',
  `start_ts` bigint(20) NOT NULL DEFAULT '0',
  `checkpoint_ts` bigint(20) NOT NULL DEFAULT '0',
  `max_ts` bigint(20) NOT NULL DEFAULT '0',
  `status` varchar(64) NOT NULL DEFAULT '',
  `file_status` varchar(64) NOT NULL DEFAULT '',
  `backup_dest` varchar(2048) DEFAULT NULL,
  `compatible` bigint(20) NOT NULL DEFAULT '0',
  `start_piece_id` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`incarnation`, `tenant_id`, `round_id`, `backup_piece_id`, `copy_id`)
) TABLE_ID = 1099511628090 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'none' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10 TABLEGROUP = 'oceanbase'
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
    if len(results) != 16:
      raise MyError("""table oceanbase.{0} has invalid column descs""".format(self.table_name))

class NormalDDLActionPostAllBackupTaskCleanHistoryAddBackupLevel(BaseDDLAction):
 @staticmethod
 def get_seq_num():
   return 34
 def dump_before_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history""")
 def skip_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'backup_level'""")
   return len(results) > 0;
 def check_before_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'backup_level'""")
   if len(results) != 0:
     raise MyError('backup_level column alread exists')
 @staticmethod
 def get_action_ddl():
   return """alter table oceanbase.__all_backup_task_clean_history add column `backup_level` varchar(64) NOT NULL DEFAULT 'CLUSTER' id 50"""
 @staticmethod
 def get_rollback_sql():
   return """alter table oceanbase.__all_backup_task_clean_history drop column backup_level"""
 def dump_after_do_action(self):
   my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_backup_task_clean_history""")
 def check_after_do_action(self):
   (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_backup_task_clean_history like 'backup_level'""")
   if len(results) != 1:
     raise MyError('failed to add column backup_level for oceanbase.__all_backup_task_clean_history')

class NormalDDLActionPostAddColumnTransitionPointAllTable(BaseDDLAction):
    @staticmethod
    def get_seq_num():
      return 35
    def dump_before_do_action(self):
      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
    def skip_action(self):
      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'transition_point'""")
      return len(results) > 0;
    def check_before_do_action(self):
      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'transition_point'""")
      if len(results) > 0:
        raise MyError('column oceanbase.__all_table_v2.transition_point already exists')
    @staticmethod
    def get_action_ddl():
      return """alter table oceanbase.__all_table_v2 add column `transition_point` varchar(4096) DEFAULT NULL id 91"""
    @staticmethod
    def get_rollback_sql():
      return """"""
    def dump_after_do_action(self):
      my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
    def check_after_do_action(self):
      (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'transition_point'""")
      if len(results) != 1:
        raise MyError('failed to add column transition_point for oceanbase.__all_table_v2')

class NormalDDLActionPostAddColumnTransitionPointAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 36
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2_history.transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2_history add column `transition_point` varchar(4096) DEFAULT NULL id 92"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column transition_point for oceanbase.__all_table_v2_history')

class NormalDDLActionPostAddColumnBTransitionPointAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 37
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2.b_transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2 add column `b_transition_point` varchar(8192) DEFAULT NULL id 92"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column b_transition_point for oceanbase.__all_table_v2')

class NormalDDLActionPostAddColumnBTransitionPointAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 38
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_transition_point'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_transition_point'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2_history.b_transition_point already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2_history add column `b_transition_point` varchar(8192) DEFAULT NULL id 93"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_transition_point'""")
    if len(results) != 1:
      raise MyError('failed to add column b_transition_point for oceanbase.__all_table_v2_history')

class NormalDDLActionPostAddColumnIntervalRangeAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 39
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2.interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2 add column `interval_range` varchar(4096) DEFAULT NULL id 93"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column interval_range for oceanbase.__all_table_v2')

class NormalDDLActionPostAddColumnIntervalRangeAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 40
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2_history.interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2_history add column `interval_range` varchar(4096) DEFAULT NULL id 94"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column interval_range for oceanbase.__all_table_v2_history')

class NormalDDLActionPostAddColumnBIntervalRangeAllTable(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 41
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2.b_interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2 add column `b_interval_range` varchar(8192) DEFAULT NULL id 94"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2 like 'b_interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column b_interval_range for oceanbase.__all_table_v2')

class NormalDDLActionPostAddColumnBIntervalRangeAllTableHistroy(BaseDDLAction):
  @staticmethod
  def get_seq_num():
    return 42
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_interval_range'""")
    return len(results) > 0;
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_interval_range'""")
    if len(results) > 0:
      raise MyError('column oceanbase.__all_table_v2_history.b_interval_range already exists')
  @staticmethod
  def get_action_ddl():
    return """alter table oceanbase.__all_table_v2_history add column `b_interval_range` varchar(8192) DEFAULT NULL id 95"""
  @staticmethod
  def get_rollback_sql():
    return """"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """desc oceanbase.__all_table_v2_history""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""show columns from oceanbase.__all_table_v2_history like 'b_interval_range'""")
    if len(results) != 1:
      raise MyError('failed to add column b_interval_range for oceanbase.__all_table_v2_history')

####========******####========= actions end =========####******========####

def do_normal_ddl_actions(cur):
  import normal_ddl_actions_post
  cls_list = reflect_action_cls_list(normal_ddl_actions_post, 'NormalDDLActionPost')

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
    if False == action.skip_action():
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

def get_normal_ddl_actions_sqls_str():
  import normal_ddl_actions_post
  ret_str = ''
  cls_list = reflect_action_cls_list(normal_ddl_actions_post, 'NormalDDLActionPost')
  for i in range(0, len(cls_list)):
    if i > 0:
      ret_str += '\n'
    ret_str += cls_list[i].get_action_ddl() + ';'
  return ret_str
