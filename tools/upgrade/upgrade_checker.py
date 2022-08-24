#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import time

class UpgradeParams:
  log_filename = 'upgrade_checker.log'
  old_version = '3.1.0'
#### --------------start : my_error.py --------------
class MyError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)
#### --------------start : actions.py------------
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

def set_parameter(cur, parameter, value):
  sql = """alter system set {0} = '{1}'""".format(parameter, value)
  logging.info(sql)
  cur.execute(sql)
  wait_parameter_sync(cur, parameter, value)

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

#### --------------start :  opt.py --------------
help_str = \
"""
Help:
""" +\
sys.argv[0] + """ [OPTIONS]""" +\
'\n\n' +\
'-I, --help          Display this help and exit.\n' +\
'-V, --version       Output version information and exit.\n' +\
'-h, --host=name     Connect to host.\n' +\
'-P, --port=name     Port number to use for connection.\n' +\
'-u, --user=name     User for login.\n' +\
'-p, --password=name Password to use when connecting to server. If password is\n' +\
'                    not given it\'s empty string "".\n' +\
'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
'                    system_variable_dml, special_action, all. "all" represents\n' +\
'                    that all modules should be run. They are splitted by ",".\n' +\
'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
'\n\n' +\
'Maybe you want to run cmd like that:\n' +\
sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u xxx -p xxx\n'

version_str = """version 1.0.0"""

class Option:
  __g_short_name_set = set([])
  __g_long_name_set = set([])
  __short_name = None
  __long_name = None
  __is_with_param = None
  __is_local_opt = None
  __has_value = None
  __value = None
  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
    if short_name in Option.__g_short_name_set:
      raise MyError('duplicate option short name: {0}'.format(short_name))
    elif long_name in Option.__g_long_name_set:
      raise MyError('duplicate option long name: {0}'.format(long_name))
    Option.__g_short_name_set.add(short_name)
    Option.__g_long_name_set.add(long_name)
    self.__short_name = short_name
    self.__long_name = long_name
    self.__is_with_param = is_with_param
    self.__is_local_opt = is_local_opt
    self.__has_value = False
    if None != default_value:
      self.set_value(default_value)
  def is_with_param(self):
    return self.__is_with_param
  def get_short_name(self):
    return self.__short_name
  def get_long_name(self):
    return self.__long_name
  def has_value(self):
    return self.__has_value
  def get_value(self):
    return self.__value
  def set_value(self, value):
    self.__value = value
    self.__has_value = True
  def is_local_opt(self):
    return self.__is_local_opt
  def is_valid(self):
    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value

g_opts =\
[\
Option('I', 'help', False, True),\
Option('V', 'version', False, True),\
Option('h', 'host', True, False),\
Option('P', 'port', True, False),\
Option('u', 'user', True, False),\
Option('p', 'password', True, False, ''),\
# 要跑哪个模块，默认全跑
Option('m', 'module', True, False, 'all'),\
# 日志文件路径，不同脚本的main函数中中会改成不同的默认值
Option('l', 'log-file', True, False)
]\

def change_opt_defult_value(opt_long_name, opt_default_val):
  global g_opts
  for opt in g_opts:
    if opt.get_long_name() == opt_long_name:
      opt.set_value(opt_default_val)
      return

def has_no_local_opts():
  global g_opts
  no_local_opts = True
  for opt in g_opts:
    if opt.is_local_opt() and opt.has_value():
      no_local_opts = False
  return no_local_opts

def check_db_client_opts():
  global g_opts
  for opt in g_opts:
    if not opt.is_local_opt() and not opt.has_value():
      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
          .format(opt.get_short_name(), sys.argv[0]))

def parse_option(opt_name, opt_val):
  global g_opts
  for opt in g_opts:
    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
      opt.set_value(opt_val)

def parse_options(argv):
  global g_opts
  short_opt_str = ''
  long_opt_list = []
  for opt in g_opts:
    if opt.is_with_param():
      short_opt_str += opt.get_short_name() + ':'
    else:
      short_opt_str += opt.get_short_name()
  for opt in g_opts:
    if opt.is_with_param():
      long_opt_list.append(opt.get_long_name() + '=')
    else:
      long_opt_list.append(opt.get_long_name())
  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
  for (opt_name, opt_val) in opts:
    parse_option(opt_name, opt_val)
  if has_no_local_opts():
    check_db_client_opts()

def deal_with_local_opt(opt):
  if 'help' == opt.get_long_name():
    global help_str
    print help_str
  elif 'version' == opt.get_long_name():
    global version_str
    print version_str

def deal_with_local_opts():
  global g_opts
  if has_no_local_opts():
    raise MyError('no local options, can not deal with local options')
  else:
    for opt in g_opts:
      if opt.is_local_opt() and opt.has_value():
        deal_with_local_opt(opt)
        # 只处理一个
        return

def get_opt_host():
  global g_opts
  for opt in g_opts:
    if 'host' == opt.get_long_name():
      return opt.get_value()

def get_opt_port():
  global g_opts
  for opt in g_opts:
    if 'port' == opt.get_long_name():
      return opt.get_value()

def get_opt_user():
  global g_opts
  for opt in g_opts:
    if 'user' == opt.get_long_name():
      return opt.get_value()

def get_opt_password():
  global g_opts
  for opt in g_opts:
    if 'password' == opt.get_long_name():
      return opt.get_value()

def get_opt_module():
  global g_opts
  for opt in g_opts:
    if 'module' == opt.get_long_name():
      return opt.get_value()

def get_opt_log_file():
  global g_opts
  for opt in g_opts:
    if 'log-file' == opt.get_long_name():
      return opt.get_value()
#### ---------------end----------------------

#### --------------start :  do_upgrade_pre.py--------------
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
#### ---------------end----------------------




#### START ####
# 1. 检查前置版本
def check_observer_version(query_cur, upgrade_params):
  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
  if len(results) != 1:
    raise MyError('query results count is not 1')
  elif cmp(results[0][0], upgrade_params.old_version) < 0 :
    raise MyError('old observer version is expected equal or higher then: {0}, actual version:{1}'.format(upgrade_params.old_version, results[0][0]))
  logging.info('check observer version success, version = {0}'.format(results[0][0]))

# 2. 检查paxos副本是否同步, paxos副本是否缺失
def check_paxos_replica(query_cur):
  # 2.1 检查paxos副本是否同步
  (desc, results) = query_cur.exec_query("""select count(1) as unsync_cnt from __all_virtual_clog_stat where is_in_sync = 0 and is_offline = 0 and replica_type != 16""")
  if results[0][0] > 0 :
    raise MyError('{0} replicas unsync, please check'.format(results[0][0]))
  # 2.2 检查paxos副本是否有缺失 TODO
  logging.info('check paxos replica success')

# 3. 检查是否有做balance, locality变更
def check_rebalance_task(query_cur):
  # 3.1 检查是否有做locality变更
  (desc, results) = query_cur.exec_query("""select count(1) as cnt from __all_rootservice_job where job_status='INPROGRESS' and return_code is null""")
  if results[0][0] > 0 :
    raise MyError('{0} locality tasks is doing, please check'.format(results[0][0]))
  # 3.2 检查是否有做balance
  (desc, results) = query_cur.exec_query("""select count(1) as rebalance_task_cnt from __all_virtual_rebalance_task_stat""")
  if results[0][0] > 0 :
    raise MyError('{0} rebalance tasks is doing, please check'.format(results[0][0]))
  logging.info('check rebalance task success')

# 4. 检查集群状态
def check_cluster_status(query_cur):
  # 4.1 检查是否非合并状态
  (desc, results) = query_cur.exec_query("""select info from __all_zone where zone='' and name='merge_status'""")
  if cmp(results[0][0], 'IDLE')  != 0 :
    raise MyError('global status expected = {0}, actual = {1}'.format('IDLE', results[0][0]))
  logging.info('check cluster status success')
  # 4.2 检查合并版本是否>=3
  (desc, results) = query_cur.exec_query("""select cast(value as unsigned) value from __all_zone where zone='' and name='last_merged_version'""")
  if results[0][0] < 2 :
    raise MyError('global last_merged_version expected >= 2 actual = {0}'.format(results[0][0]))
  logging.info('check global last_merged_version success')

# 5. 检查没有打开enable_separate_sys_clog
def check_disable_separate_sys_clog(query_cur):
  (desc, results) = query_cur.exec_query("""select count(1) from __all_sys_parameter where name like 'enable_separate_sys_clog'""")
  if results[0][0] > 0 :
    raise MyError('enable_separate_sys_clog is true, unexpected')
  logging.info('check separate_sys_clog success')

# 6. 检查配置宏块的data_seq
def check_macro_block_data_seq(query_cur):
  query_cur.exec_sql("""set ob_query_timeout=1800000000""")
  row_count = query_cur.exec_sql("""select * from __all_virtual_partition_sstable_macro_info where data_seq < 0 limit 1""")
  if row_count != 0:
    raise MyError('check_macro_block_data_seq failed, too old macro block needs full merge')
  logging.info('check_macro_block_data_seq success')

# 8. 检查租户的resource_pool内存规格, 要求F类型unit最小内存大于5G，L类型unit最小内存大于2G.
def check_tenant_resource_pool(query_cur):
  (desc, results) = query_cur.exec_query("""select 1 from v$sysstat where name = 'is mini mode' and value = '1' and con_id = 1 limit 1""")
  if len(results) > 0:
     # mini部署的集群，租户规格可以很小，这里跳过检查
     pass
  else:
    (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=0 and b.min_memory < 5368709120""")
    if results[0][0] > 0 :
      raise MyError('{0} tenant resource pool unit config is less than 5G, please check'.format(results[0][0]))
    (desc, results) = query_cur.exec_query("""select count(*) from oceanbase.__all_resource_pool a, oceanbase.__all_unit_config b where a.unit_config_id = b.unit_config_id and b.unit_config_id != 100 and a.replica_type=5 and b.min_memory < 2147483648""")
    if results[0][0] > 0 :
      raise MyError('{0} tenant logonly resource pool unit config is less than 2G, please check'.format(results[0][0]))

# 9. 检查是否有日志型副本分布在Full类型unit中
# 2020-12-31 根据外部使用L副本且L型unit功能不成熟的需求，将这个检查去掉.

# 10. 检查租户分区数是否超出内存限制
def check_tenant_part_num(query_cur):
  # 统计每个租户在各个server上的分区数量
  (desc, res_part_num) = query_cur.exec_query("""select svr_ip, svr_port, table_id >> 40 as tenant_id, count(*) as part_num from  __all_virtual_clog_stat  group by 1,2,3  order by 1,2,3""")
  # 计算每个租户在每个server上的max_memory
  (desc, res_unit_memory) = query_cur.exec_query("""select u.svr_ip, u.svr_port, t.tenant_id, uc.max_memory, p.replica_type  from __all_unit u, __All_resource_pool p, __all_tenant t, __all_unit_config uc where p.resource_pool_id = u.resource_pool_id and t.tenant_id = p.tenant_id and p.unit_config_id = uc.unit_config_id""")
  # 查询每个server的memstore_limit_percentage
  (desc, res_svr_memstore_percent) = query_cur.exec_query("""select svr_ip, svr_port, name, value  from __all_virtual_sys_parameter_stat where name = 'memstore_limit_percentage'""")
  part_static_cost = 128 * 1024
  part_dynamic_cost = 400 * 1024
  # 考虑到升级过程中可能有建表的需求，因此预留512个分区
  part_num_reserved = 512
  for line in res_part_num:
    svr_ip = line[0]
    svr_port = line[1]
    tenant_id = line[2]
    part_num = line[3]
    for uline in res_unit_memory:
      uip = uline[0]
      uport = uline[1]
      utid = uline[2]
      umem = uline[3]
      utype = uline[4]
      if svr_ip == uip and svr_port == uport and tenant_id == utid:
        for mpline in res_svr_memstore_percent:
          mpip = mpline[0]
          mpport = mpline[1]
          if mpip == uip and mpport == uport:
            mspercent = int(mpline[3])
            mem_limit = umem
            if 0 == utype:
              # full类型的unit需要为memstore预留内存
              mem_limit = umem * (100 - mspercent) / 100
            part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost / 10);
            if part_num_limit <= 1000:
              part_num_limit = mem_limit / (part_static_cost + part_dynamic_cost)
            if part_num >= (part_num_limit - part_num_reserved):
              raise MyError('{0} {1} {2} exceed tenant partition num limit, please check'.format(line, uline, mpline))
            break
  logging.info('check tenant partition num success')

# 11. 检查存在租户partition，但是不存在unit的observer
def check_tenant_resource(query_cur):
  (desc, res_unit) = query_cur.exec_query("""select tenant_id, svr_ip, svr_port from __all_virtual_partition_info where (tenant_id, svr_ip, svr_port) not in (select tenant_id, svr_ip, svr_port from __all_unit, __all_resource_pool where __all_unit.resource_pool_id = __all_resource_pool.resource_pool_id group by tenant_id, svr_ip, svr_port) group by tenant_id, svr_ip, svr_port""")
  for line in res_unit:
    raise MyError('{0} tenant unit not exist but partition exist'.format(line))
  logging.info("check tenant resource success")

# 12. 检查系统表(__all_table_history)索引生效情况
def check_sys_index_status(query_cur):
  (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where data_table_id = 1099511627890 and table_id = 1099511637775 and index_type = 1 and index_status = 2""")
  if len(results) != 1 or results[0][0] != 1:
    raise MyError("""__all_table_history's index status not valid""")
  logging.info("""check __all_table_history's index status success""")

# 14. 检查升级前是否有只读zone
def check_readonly_zone(query_cur):
   (desc, results) = query_cur.exec_query("""select count(*) from __all_zone where name='zone_type' and info='ReadOnly'""")
   if results[0][0] != 0:
       raise MyError("""check_readonly_zone failed, ob2.2 not support readonly_zone""")
   logging.info("""check_readonly_zone success""")

# 16. 修改永久下线的时间，避免升级过程中缺副本
def modify_server_permanent_offline_time(cur):
  set_parameter(cur, 'server_permanent_offline_time', '72h')

# 17. 修改安全删除副本时间
def modify_replica_safe_remove_time(cur):
  set_parameter(cur, 'replica_safe_remove_time', '72h')

# 18. 检查progressive_merge_round都升到1

# 19. 从小于224的版本升级上来时，需要确认high_priority_net_thread_count配置项值为0 (224版本开始该值默认为1)
def check_high_priority_net_thread_count_before_224(query_cur):
  # 获取最小版本
  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
  if len(results) != 1:
    raise MyError('distinct observer version exist')
  elif cmp(results[0][0], "2.2.40") >= 0 :
    # 最小版本大于等于2.2.40，忽略检查
    logging.info('cluster version ({0}) is greate than or equal to 2.2.40, need not check high_priority_net_thread_count'.format(results[0][0]))
  else:
    # 低于224版本的需要确认配置项值为0
    logging.info('cluster version is ({0}), need check high_priority_net_thread_count'.format(results[0][0]))
    (desc, results) = query_cur.exec_query("""select count(*) from __all_sys_parameter where name like 'high_priority_net_thread_count' and value not like '0'""")
    if results[0][0] > 0:
        raise MyError('high_priority_net_thread_count is greater than 0, unexpected')
    logging.info('check high_priority_net_thread_count finished')

# 20. 从小于226的版本升级上来时，要求不能有备库存在
def check_standby_cluster(query_cur):
  # 获取最小版本
  (desc, results) = query_cur.exec_query("""select distinct value from __all_virtual_sys_parameter_stat where name='min_observer_version'""")
  if len(results) != 1:
    raise MyError('distinct observer version exist')
  elif cmp(results[0][0], "2.2.60") >= 0 :
    # 最小版本大于等于2.2.60，忽略检查
    logging.info('cluster version ({0}) is greate than or equal to 2.2.60, need not check standby cluster'.format(results[0][0]))
  else:
    logging.info('cluster version is ({0}), need check standby cluster'.format(results[0][0]))
    (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_table where table_name = '__all_cluster'""")
    if results[0][0] == 0:
      logging.info('cluster ({0}) has no __all_cluster table, no standby cluster'.format(results[0][0]))
    else:
      (desc, results) = query_cur.exec_query("""select count(*) as cnt from __all_cluster""")
      if results[0][0] > 1:
        raise MyError("""multiple cluster exist in __all_cluster, maybe standby clusters added, not supported""")
  logging.info('check standby cluster from __all_cluster success')

# 21. 3.0是barrier版本，要求列column_id修正的升级任务做完
def check_schema_split_v2_finish(query_cur):
  # 获取最小版本
  sql = """select cast(column_value as signed) as version from __all_core_table
           where table_name='__all_global_stat' and column_name = 'split_schema_version_v2'"""
  (desc, results) = query_cur.exec_query(sql)
  if len(results) != 1 or len(results[0]) != 1:
    raise MyError('row or column cnt not match')
  elif results[0][0] < 0:
    raise MyError('schema split v2 not finished yet')
  else:
    logging.info('check schema split v2 finish success')



# 23. 检查是否有异常租户(creating，延迟删除，恢复中)
def check_tenant_status(query_cur):
  (desc, results) = query_cur.exec_query("""select count(*) as count from __all_tenant where status != 'TENANT_STATUS_NORMAL'""")
  if len(results) != 1 or len(results[0]) != 1:
    raise MyError('results len not match')
  elif 0 != results[0][0]:
    raise MyError('has abnormal tenant, should stop')
  else:
    logging.info('check tenant status success')

# 24. 所有版本升级都要检查micro_block_merge_verify_level
def check_micro_block_verify_level(query_cur):
  (desc, results) = query_cur.exec_query("""select count(1) from __all_virtual_sys_parameter_stat where name='micro_block_merge_verify_level' and value < 2""")
  if results[0][0] != 0:
      raise MyError("""unexpected micro_block_merge_verify_level detected, upgrade is not allowed temporarily""")
  logging.info('check micro_block_merge_verify_level success')

#25. 需要使用最大性能模式升级，227版本修改了模式切换相关的内部表
def check_cluster_protection_mode(query_cur):
  (desc, results) = query_cur.exec_query("""select count(*) from __all_core_table where table_name = '__all_cluster' and column_name = 'protection_mode'""");
  if len(results) != 1:
    raise MyError('failed to get protection mode')
  elif results[0][0] == 0:
    logging.info('no need to check protection mode')
  else:
    (desc, results) = query_cur.exec_query("""select column_value from __all_core_table where table_name = '__all_cluster' and column_name = 'protection_mode'""");
    if len(results) != 1:
      raise MyError('failed to get protection mode')
    elif cmp(results[0][0], '0') != 0:
      raise MyError('cluster not maximum performance protection mode before update not allowed, protecion_mode={0}'.format(results[0][0]))
    else:
      logging.info('cluster protection mode legal before update!')

# 27. 检查无恢复任务
def check_restore_job_exist(query_cur):
  (desc, results) = query_cur.exec_query("""select count(1) from __all_restore_job""")
  if len(results) != 1 or len(results[0]) != 1:
    raise MyError('failed to restore job cnt')
  elif results[0][0] != 0:
      raise MyError("""still has restore job, upgrade is not allowed temporarily""")
  logging.info('check restore job success')

# 28. 检查系统租户系统表leader是否打散
def check_sys_table_leader(query_cur):
  (desc, results) = query_cur.exec_query("""select svr_ip, svr_port from oceanbase.__all_virtual_core_meta_table where role = 1""")
  if len(results) != 1 or len(results[0]) != 2:
    raise MyError('failed to rs leader')
  else:
    svr_ip = results[0][0]
    svr_port = results[0][1]
    # check __all_root_table's leader
    (desc, results) = query_cur.exec_query("""select count(1) from oceanbase.__all_virtual_core_root_table
                                           where role = 1 and svr_ip = '{0}' and svr_port = '{1}'""".format(svr_ip, svr_port))
    if len(results) != 1 or len(results[0]) != 1:
      raise MyError('failed to __all_root_table leader')
    elif results[0][0] != 1:
      raise MyError("""__all_root_table should be {0}:{1}""".format(svr_ip, svr_port))

    # check sys tables' leader
    (desc, results) = query_cur.exec_query("""select count(1) from oceanbase.__all_virtual_core_root_table
                                           where tenant_id = 1 and role = 1 and (svr_ip != '{0}' or svr_port != '{1}')""" .format(svr_ip, svr_port))
    if len(results) != 1 or len(results[0]) != 1:
      raise MyError('failed to __all_root_table leader')
    elif results[0][0] != 0:
      raise MyError("""sys tables'leader should be {0}:{1}""".format(svr_ip, svr_port))

# 29. 检查版本号设置inner sql不走px.
def check_and_modify_px_query(query_cur, cur):
  (desc, results) = query_cur.exec_query("""select distinct value from oceanbase.__all_virtual_sys_parameter_stat where name = 'min_observer_version'""")
  if (len(results) != 1) :
    raise MyError('distinct observer version not exist')
  elif cmp(results[0][0], "3.1.1") == 0 or cmp(results[0][0], "3.1.0") == 0:
    if cmp(results[0][0], "3.1.1") == 0:
      cur.execute("alter system set _ob_enable_px_for_inner_sql = false")
    (desc, results) = query_cur.exec_query("""select cluster_role from oceanbase.v$ob_cluster""")
    if (len(results) != 1) :
      raise MyError('cluster role results is not valid')
    elif (cmp(results[0][0], "PRIMARY") == 0):
      tenant_id_list = []
      (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
      for r in results:
        tenant_id_list.append(r[0])
      for tenant_id in tenant_id_list:
        cur.execute("alter system change tenant tenant_id = {0}".format(tenant_id))
        sql = """set global _ob_use_parallel_execution = false"""
        logging.info("tenant_id : %d , %s", tenant_id, sql)
        cur.execute(sql)
    elif (cmp(results[0][0], "PHYSICAL STANDBY") == 0):
      sql = """set global _ob_use_parallel_execution = false"""
      cur.execute(sql)
      logging.info("execute sql in standby: %s", sql)
    cur.execute("alter system change tenant tenant_id = 1")
    time.sleep(5)
    cur.execute("alter system flush plan cache global")
    logging.info("execute: alter system flush plan cache global")
  else:
    logging.info('cluster version is not equal 3.1.1, skip px operate'.format(results[0][0]))

# 30. check duplicate index name in mysql
def check_duplicate_index_name_in_mysql(query_cur, cur):
  (desc, results) = query_cur.exec_query(
                    """
                    select /*+ OB_QUERY_TIMEOUT(100000000) */
                    a.database_id, a.data_table_id, lower(substr(table_name, length(substring_index(a.table_name, "_", 4)) + 2)) as index_name
                    from oceanbase.__all_virtual_table as a join oceanbase.__all_tenant as b on a.tenant_id = b.tenant_id
                    where a.table_type = 5 and b.compatibility_mode = 0 and lower(table_name) like "__idx%" group by 1,2,3 having count(*) > 1
                    """)
  if (len(results) != 0) :
    raise MyError("Duplicate index name exist in mysql tenant")

# 31. check the _max_trx_size
def check_max_trx_size_config(query_cur, cur):
  set_parameter(cur, '_max_trx_size', '100G')
  logging.info('set _max_trx_size to default value 100G')

# 开始升级前的检查
def do_check(my_host, my_port, my_user, my_passwd, upgrade_params):
  try:
    conn = mysql.connector.connect(user = my_user,
                                   password = my_passwd,
                                   host = my_host,
                                   port = my_port,
                                   database = 'oceanbase',
                                   raise_on_warnings = True)
    conn.autocommit = True
    cur = conn.cursor(buffered=True)
    try:
      query_cur = Cursor(cur)
      check_observer_version(query_cur, upgrade_params)
      check_paxos_replica(query_cur)
      check_rebalance_task(query_cur)
      check_cluster_status(query_cur)
      check_disable_separate_sys_clog(query_cur)
      check_macro_block_data_seq(query_cur)
      check_tenant_resource_pool(query_cur)
      check_tenant_status(query_cur)
      check_tenant_part_num(query_cur)
      check_tenant_resource(query_cur)
      check_readonly_zone(query_cur)
      modify_server_permanent_offline_time(cur)
      modify_replica_safe_remove_time(cur)
      check_high_priority_net_thread_count_before_224(query_cur)
      check_standby_cluster(query_cur)
      check_schema_split_v2_finish(query_cur)
      check_micro_block_verify_level(query_cur)
      check_restore_job_exist(query_cur)
      check_sys_table_leader(query_cur)
      check_and_modify_px_query(query_cur, cur)
      check_duplicate_index_name_in_mysql(query_cur, cur)
      check_max_trx_size_config(query_cur, cur)
    except Exception, e:
      logging.exception('run error')
      raise e
    finally:
      cur.close()
      conn.close()
  except mysql.connector.Error, e:
    logging.exception('connection error')
    raise e
  except Exception, e:
    logging.exception('normal error')
    raise e

if __name__ == '__main__':
  upgrade_params = UpgradeParams()
  change_opt_defult_value('log-file', upgrade_params.log_filename)
  parse_options(sys.argv[1:])
  if not has_no_local_opts():
    deal_with_local_opts()
  else:
    check_db_client_opts()
    log_filename = get_opt_log_file()
    upgrade_params.log_filename = log_filename
    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
    config_logging_module(upgrade_params.log_filename)
    try:
      host = get_opt_host()
      port = int(get_opt_port())
      user = get_opt_user()
      password = get_opt_password()
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\"',\
          host, port, user, password, log_filename)
      do_check(host, port, user, password, upgrade_params)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      raise e
    except Exception, e:
      logging.exception('normal error')
      raise e
