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
class NormalDDLActionPostReportInvisibleSysUnitResource(BaseDDLAction):

  @staticmethod
  def get_bytes_from_readable_string(s):
    unit = s[-1]
    value = s[0 : -1]

    unit = unit.upper()
    if len(unit) == 0:
      unit = 1
    elif unit == 'G':
      unit = 1 * 1024 * 1024 * 1024
    elif unit == 'M':
      unit = 1 * 1024 * 1024
    elif unit == 'K':
      unit = 1 * 1024
    else:
      raise MyError('unknown UNIT of {}'.format(s))

    try:
      return int(value) * unit
    except ValueError as ex:
      raise MyError('cannot convert {} to integer'.format(value))

  def get_config(self, config_name):
    (desc, results) = self._query_cursor.exec_query(
      '''
      select distinct value from oceanbase.__all_virtual_sys_parameter_stat
      where name="{}"
      '''.format(config_name))
    
    return results if results else []

  def get_unique_config(self, config_name):
    '''
    get one config of `config_name`
    expect the result exists and only one
    '''
    values = self.get_config(config_name)
    if len(values) != 1:
      logging.error('difference config of %s, values=%s', config_name, values)
      raise MyError('difference config of {}, values={}'.format(config_name, values))

    return values[0][0]

  def get_float_config(self, config_name):
    '''
    get one config of `config_name`
    should one config values exists which is a float
    '''
    value = self.get_unique_config(config_name)
    try:
      return float(value)
    except Exception as ex:
      raise MyError('cannot convert {} to float. value={}'.format(config_name, value))

  def get_bytes_config(self, config_name):
    '''
    get one config of `config_name`
    should one config values exists which is a readable bytes value like '32G', '16M'
    '''
    value = self.get_unique_config(config_name)
    return self.get_bytes_from_readable_string(value)

  def get_server_cpu_quota(self):
    min_config_name = 'server_cpu_quota_min'
    max_config_name = 'server_cpu_quota_max'
    return (self.get_float_config(min_config_name), self.get_float_config(max_config_name))

  def get_sys_tenant_memory(self):
    '''
    refer to ObServerConfig::get_max_sys_tenant_memory
    '''
    memory_limit = self.get_bytes_config('memory_limit')
    system_memory = self.get_bytes_config('system_memory')

    if memory_limit != 0:
      server_memory_limit = memory_limit
    else:
      memory_limit_percentage = self.get_float_config('memory_limit_percentage')
      phy_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
      server_memory_limit = phy_memory * memory_limit_percentage / 100

    reserved_server_memory = system_memory
    server_memory_available = server_memory_limit - reserved_server_memory

    min_sys_tenant_mem_factor = 0.25
    max_sys_tenant_mem_factor = 0.3
    default_min_sys_memory = 12 << 30
    default_max_sys_memory = 16 << 30

    min_memory = min(int(server_memory_available * min_sys_tenant_mem_factor), default_min_sys_memory)
    max_memory = min(int(server_memory_available * max_sys_tenant_mem_factor), default_max_sys_memory)
    return (min_memory, max_memory)

  def get_sys_unit_config(self):
    (desc, results) = self._query_cursor.exec_query(
      '''
      select max_cpu, min_cpu, max_memory, min_memory
      from oceanbase.__all_unit_config
      where name="sys_unit_config"
      ''')
    if not results or len(results) != 1:
      logging.error('failed to fetch sys unit config. results=%s', str(results))
      raise MyError('faield to fetch sys unit config')

    result = results[0]
    max_cpu = float(result[0])
    min_cpu = float(result[1])
    max_memory = int(result[2])
    min_memory = int(result[3])

    return {"max_cpu": max_cpu, "min_cpu": min_cpu, 
            "max_memory": max_memory, "min_memory": min_memory}

  def get_observers_with_sys_unit(self):
    (desc, results) = self._query_cursor.exec_query(
      '''
      select svr_ip,svr_port 
      from oceanbase.__all_resource_pool as a, oceanbase.__all_unit as b 
      where a.resource_pool_id=b.resource_pool_id and a.unit_config_id=1
      '''
    )

    return results if results else []

  @staticmethod
  def get_seq_num():
    return 0

  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, 
      '''
      select 
         svr_ip, svr_port, zone, 
         cpu_total, cpu_assigned, cpu_assigned_percent, cpu_capacity, cpu_max_assigned, 
         mem_total, mem_assigned, mem_assigned_percent, mem_capacity, mem_max_assigned,
         unit_num, `load`, 
         build_version,
         with_rootserver
      from oceanbase.__all_virtual_server_stat
      ''')

  def skip_action(self):

    # read config items: server_cpu_quota_max & max_sys_tenant_memory
    server_cpu_quota_min, server_cpu_quota_max = self.get_server_cpu_quota()
    min_sys_tenant_memory, max_sys_tenant_memory = self.get_sys_tenant_memory()

    # get sys unit config
    sys_unit_config = self.get_sys_unit_config()

    # get all observers with sys unit
    observers_with_sys_unit = self.get_observers_with_sys_unit()

    CPU_EPSILON = 0.00001 # const value, copy from ob_unit_info.h

    # check whether the observer can hold the invisible sys unit
    (desc, all_observer_resource_results) = self._query_cursor.exec_query(
      '''
      select 
         svr_ip, svr_port, zone, 
         cpu_total, cpu_assigned, cpu_assigned_percent, cpu_capacity, cpu_max_assigned, 
         mem_total, mem_assigned, mem_assigned_percent, mem_capacity, mem_max_assigned,
         unit_num, `load`, 
         build_version,
         with_rootserver
      from oceanbase.__all_virtual_server_stat
      ''')

    if not all_observer_resource_results:
      raise MyError('cannot fetch results of __all_virtual_server_stat')

    for observer_resource_result in all_observer_resource_results:
      cpu_total = float(observer_resource_result[3])
      cpu_assigned = float(observer_resource_result[4])
      cpu_max_assigned = float(observer_resource_result[7])
      mem_total = int(observer_resource_result[8])
      mem_assigned = int(observer_resource_result[9])
      mem_max_assigned = int(observer_resource_result[12])
      # with_rootserver = True if str(observer_resource_results[16]) == '1' else False
      has_sys_unit = False
      for observer_with_sys_unit in observers_with_sys_unit:
        if (observer_with_sys_unit[0] == observer_resource_result[0] and
            observer_with_sys_unit[1] == observer_resource_result[1]):
          has_sys_unit = True
          break

      if not has_sys_unit:

        if (cpu_total - cpu_max_assigned - server_cpu_quota_max < -CPU_EPSILON or
            cpu_total - cpu_assigned < server_cpu_quota_min < -CPU_EPSILON or
            mem_total - mem_max_assigned < max_sys_tenant_memory or
            mem_total - mem_assigned < min_sys_tenant_memory):

          logging.warn('there is one observer cannot hold the invisible sys unit. ' \
                       'observer=%s, invisible sys unit=cpu (min=%s,max=%s),memory (min=%s,max=%s)',
                      str(observer_resource_result), str(server_cpu_quota_min), str(server_cpu_quota_max), 
                      str(min_sys_tenant_memory), str(max_sys_tenant_memory))
          return False # skip the action

      else: # the observer with sys unit
        if (cpu_total - cpu_max_assigned + sys_unit_config['max_cpu'] - server_cpu_quota_max < -CPU_EPSILON or
            cpu_total - cpu_assigned + sys_unit_config['min_cpu'] - server_cpu_quota_min < -CPU_EPSILON or
            mem_total - mem_max_assigned + sys_unit_config['max_memory'] < max_sys_tenant_memory or
            mem_total - mem_assigned + sys_unit_config['min_memory'] < min_sys_tenant_memory):

          logging.warn('there is on observer with real sys unit cannot hold the invisible sys unit. ' \
                       'observer=%s, invisible sys unit=cpu (min=%s,max=%s),memory (min=%s,max=%s), ' \
                       'real sys unit config=%s',
                       str(observer_resource_result), str(server_cpu_quota_min), str(server_cpu_quota_max), 
                       str(min_sys_tenant_memory), str(max_sys_tenant_memory), str(sys_unit_config))
          return False

    logging.info('very good. all observer can success hold the invisible sys unit')
    return True # do the action

  def check_before_do_action(self):
    pass

  @staticmethod
  def get_action_ddl():
    return 'alter system set _report_invisible_sys_unit_resource = "False"'

  @staticmethod
  def get_rollback_sql():
    return 'alter system set _report_invisible_sys_unit_resource = "True"'

  def dump_after_do_action(self):
    self.dump_before_do_action()

  def check_after_do_action(self):
    #from actions import wait_parameter_sync
    #wait_parameter_sync(self._cursor, '_report_invisible_sys_unit_resource', 'True')
    pass

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
