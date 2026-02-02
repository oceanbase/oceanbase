#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import re

class UpgradeParams:
  log_filename = 'upgrade_cluster_health_checker.log'

class PasswordMaskingFormatter(logging.Formatter):
  def format(self, record):
    s = super(PasswordMaskingFormatter, self).format(record)
    return re.sub(r'password="(?:[^"\\]|\\.)*"', 'password="******"', s)

#### --------------start : my_error.py --------------
class MyError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

#### --------------start : actions.py 只允许执行查询语句--------------
class QueryCursor:
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
    except mysql.connector.Error as e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise
    except Exception as e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise
  def exec_query(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      results = self.__cursor.fetchall()
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.info('succeed to execute query: %s, rowcount = %d', sql, rowcount)
      return (self.__cursor.description, results)
    except mysql.connector.Error as e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise
    except Exception as e:
      logging.exception('normal error, fail to execute sql: %s', sql)
      raise
#### ---------------end----------------------

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
'-t, --timeout=name  check timeout.\n' + \
'-z, --zone=name     If zone is not specified, check all servers status in cluster. \n' +\
'                    Otherwise, only check servers status in specified zone. \n' + \
'\n\n' +\
'Maybe you want to run cmd like that:\n' +\
sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'

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
Option('l', 'log-file', True, False),\
Option('t', 'timeout', True, False, 0),\
Option('z', 'zone', True, False, ''),\
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
    print(help_str)
  elif 'version' == opt.get_long_name():
    global version_str
    print(version_str)

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

def get_opt_timeout():
  global g_opts
  for opt in g_opts:
    if 'timeout' == opt.get_long_name():
      return opt.get_value()

def get_opt_zone():
  global g_opts
  for opt in g_opts:
    if 'zone' == opt.get_long_name():
      return opt.get_value()
#### ---------------end----------------------

#### --------------start :  do_upgrade_pre.py--------------
def config_logging_module(log_filenamme):
  logger = logging.getLogger('')
  logger.setLevel(logging.INFO)
  # 定义日志打印格式
  formatter = PasswordMaskingFormatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
  #######################################
  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.setLevel(logging.INFO)
  stdout_handler.setFormatter(formatter)
  # 定义一个Handler处理文件输出
  file_handler = logging.FileHandler(log_filenamme, mode='w')
  file_handler.setLevel(logging.INFO)
  file_handler.setFormatter(formatter)
  logging.getLogger('').addHandler(stdout_handler)
  logging.getLogger('').addHandler(file_handler)
#### ---------------end----------------------

def check_zone_valid(query_cur, zone):
  if zone != '':
    sql = """select count(*) from oceanbase.DBA_OB_ZONES where zone = '{0}'""".format(zone)
    (desc, results) = query_cur.exec_query(sql);
    if len(results) != 1 or len(results[0]) != 1:
      raise MyError("unmatched row/column cnt")
    elif results[0][0] == 0:
      raise MyError("zone:{0} doesn't exist".format(zone))
    else:
      logging.info("zone:{0} is valid".format(zone))
  else:
    logging.info("zone is empty, check all servers in cluster")

def fetch_tenant_ids(query_cur):
  try:
    tenant_id_list = []
    (desc, results) = query_cur.exec_query("""select distinct tenant_id from oceanbase.__all_tenant order by tenant_id desc""")
    for r in results:
      tenant_id_list.append(r[0])
    return tenant_id_list
  except Exception as e:
    logging.exception('fail to fetch distinct tenant ids')
    raise

def set_default_timeout_by_tenant(query_cur, timeout, timeout_per_tenant, min_timeout):
  if timeout > 0:
    logging.info("use timeout from opt, timeout(s):{0}".format(timeout))
  else:
    tenant_id_list = fetch_tenant_ids(query_cur)
    cal_timeout = len(tenant_id_list) * timeout_per_tenant
    timeout = (cal_timeout if cal_timeout > min_timeout else min_timeout)
    logging.info("use default timeout caculated by tenants, "
                 "timeout(s):{0}, tenant_count:{1}, "
                 "timeout_per_tenant(s):{2}, min_timeout(s):{3}"
                 .format(timeout, len(tenant_id_list), timeout_per_tenant, min_timeout))

  return timeout

#### START ####
# 0. 检查server版本是否严格一致
def check_server_version_by_zone(query_cur, zone):
  if zone == '':
    logging.info("skip check server version by cluster")
  else:
    sql = """select distinct(substring_index(build_version, '_', 1)) from oceanbase.__all_server where zone = '{0}'""".format(zone);
    (desc, results) = query_cur.exec_query(sql);
    if len(results) != 1:
      raise MyError("servers build_version not match")
    else:
      logging.info("check server version success")

# 1. 检查paxos副本是否同步, paxos副本是否缺失
def check_paxos_replica(query_cur, timeout):
  # 1.1 检查paxos副本是否同步
  sql = """select count(*) from oceanbase.GV$OB_LOG_STAT where in_sync = 'NO'"""
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 10, 600)
  check_until_timeout(query_cur, sql, 0, wait_timeout)

  # 1.2 检查paxos副本是否有缺失 TODO
  logging.info('check paxos replica success')

# 2. 检查observer是否可服务
def check_observer_status(query_cur, zone, timeout):
  sql = """select count(*) from oceanbase.__all_server where (start_service_time <= 0 or status='inactive')"""
  if zone != '':
    sql += """ and zone = '{0}'""".format(zone)
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 10, 600)
  check_until_timeout(query_cur, sql, 0, wait_timeout)

# 3. 检查schema是否刷新成功
def check_schema_status(query_cur, timeout):
  sql = """select if (a.cnt = b.cnt, 1, 0) as passed from (select count(*) as cnt from oceanbase.__all_virtual_server_schema_info where refreshed_schema_version > 1 and refreshed_schema_version % 8 = 0) as a join (select count(*) as cnt from oceanbase.__all_server join oceanbase.__all_tenant) as b"""
  wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 30, 600)
  check_until_timeout(query_cur, sql, 1, wait_timeout)

# 4. check major finish
def check_major_merge(query_cur, timeout):
  need_check = 0
  (desc, results) = query_cur.exec_query("""select distinct value from oceanbase.GV$OB_PARAMETERS where name = 'enable_major_freeze';""")
  if len(results) != 1:
    need_check = 1
  elif results[0][0] != 'True':
    need_check = 1
  if need_check == 1:
    wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 30, 600)
    sql = """select count(1) from oceanbase.CDB_OB_MAJOR_COMPACTION where (GLOBAL_BROADCAST_SCN > LAST_SCN or STATUS != 'IDLE')"""
    check_until_timeout(query_cur, sql, 0, wait_timeout)
    sql2 = """select /*+ query_timeout(1000000000) */ count(1) from oceanbase.__all_virtual_tablet_compaction_info where max_received_scn > finished_scn and max_received_scn > 0"""
    check_until_timeout(query_cur, sql2, 0, wait_timeout)

def check_until_timeout(query_cur, sql, value, timeout):
  times = timeout / 10
  while times >= 0:
    (desc, results) = query_cur.exec_query(sql)

    if len(results) != 1 or len(results[0]) != 1:
      raise MyError("unmatched row/column cnt")
    elif results[0][0] == value:
      logging.info("check value is {0} success".format(value))
      break
    else:
      logging.info("value is {0}, expected value is {1}, not matched".format(results[0][0], value))

    times -= 1
    if times == -1:
      logging.warn("""result not expected, sql: '{0}', expected: '{1}', current: '{2}'""".format(sql, value, results[0][0]))
      raise MyError("""result not expected, sql: '{0}', expected: '{1}', current: '{2}'""".format(sql, value, results[0][0]))
    time.sleep(10)

# ====== 向量索引加载检查相关函数 ======
def get_version(version_str):
  versions = version_str.split(".")
  if len(versions) != 4:
    raise MyError("version:{0} is invalid".format(version_str))
  
  major = int(versions[0])
  minor = int(versions[1])
  major_patch = int(versions[2])
  minor_patch = int(versions[3])
  
  if major > 0xffffffff or minor > 0xffff or major_patch > 0xff or minor_patch > 0xff:
    raise MyError("version:{0} is invalid".format(version_str))
  
  version = (major << 32) | (minor << 16) | (major_patch << 8) | minor_patch
  return version

def get_max_zone_count(query_cur):
  sql = "select count(*) from oceanbase.DBA_OB_ZONES"
  try:
    (desc, results) = query_cur.exec_query(sql, print_when_succ=False)
    if len(results) == 0:
      return 0
    return results[0][0]
  except Exception as e:
    logging.exception('fail to query DBA_OB_ZONES')
    raise

# 检查集群版本是否支持向量索引（>= 4.3.3.0）
def check_cluster_version_for_vector_index(query_cur):
  try:
    # oceanbase.__all_virtual_vector_index_info 视图在 4.3.3.0 之后才存在
    sql = "select min(value) from gv$ob_parameters where TENANT_ID=1 and name='compatible'"
    (desc, results) = query_cur.exec_query(sql, print_when_succ=False)
    if len(results) > 0 and len(results[0]) > 0 and results[0][0] is not None:
      min_version_str = results[0][0]
      min_version = get_version(min_version_str)
      required_version = get_version("4.3.3.0")
      
      # 比较版本号
      if min_version >= required_version:
        return True
      else:
        logging.info("Cluster min compatible version is %s, less than required 4.3.3.0", min_version_str)
        return False
    else:
      logging.warn("Failed to get cluster compatible version, will continue vector index check")
      return True
  except Exception as e:
    # 查询失败，保守策略：返回 True，继续检查
    logging.warn("Failed to check cluster version for vector index: %s, will continue vector index check", str(e))
    return True

def has_vector_index(query_cur):
  # 先检查集群版本是否支持向量索引
  if not check_cluster_version_for_vector_index(query_cur):
    logging.info("Cluster version < 4.3.3.0, skip vector index check")
    return False
  
  # 版本满足要求，查询向量虚拟表
  sql = "select 1 from oceanbase.__all_virtual_vector_index_info limit 1"
  (desc, results) = query_cur.exec_query(sql, print_when_succ=False)
  return len(results) > 0

def query_vector_index_info_all(query_cur):
  sql = """select /*+ query_timeout(120000000) */ tenant_id, snapshot_index_tablet_id, svr_ip, svr_port, sync_info, data_table_id, snapshot_index_table_id, ls_id
           from oceanbase.__all_virtual_vector_index_info
           order by tenant_id, snapshot_index_tablet_id, svr_ip, svr_port"""
  (desc, results) = query_cur.exec_query(sql, print_when_succ=False)
  return results

# 解析 sync_info
def parse_sync_info(sync_info_str):
  result = {
    'snap_cnt': None,  # 使用None表示字段不存在，与0区分
    'incr_cnt': None,
    'vbitmap_cnt': None,
    'has_snap_cnt': False,
    'has_incr_cnt': False,
    'has_vbitmap_cnt': False
  }
  try:
    # sync_info字段格式在不同版本格式不一致
    # 4.4.1之后版本: 标准JSON {"snap_cnt":10,"incr_cnt":20}
    # 之前版本: {snap_cnt=10,incr_cnt=20}
    # IVF索引: 没有snap_cnt和incr_cnt字段
    snap_match = re.search(r'\"?snap_cnt\"?[:=]\s*(\d+)', sync_info_str)
    incr_match = re.search(r'\"?incr_cnt\"?[:=]\s*(\d+)', sync_info_str)
    vbitmap_match = re.search(r'\"?vbitmap_cnt\"?[:=]\s*(\d+)', sync_info_str)
    
    if snap_match:
      result['snap_cnt'] = int(snap_match.group(1))
      result['has_snap_cnt'] = True
    if incr_match:
      result['incr_cnt'] = int(incr_match.group(1))
      result['has_incr_cnt'] = True
    if vbitmap_match:
      result['vbitmap_cnt'] = int(vbitmap_match.group(1))
      result['has_vbitmap_cnt'] = True
  except Exception as e:
    logging.exception('Failed to parse sync_info string: %s', sync_info_str)
    raise MyError('Failed to parse sync_info, this may indicate data corruption or version incompatibility: {0}'.format(str(e)))
  
  return result

# 获得指定租户的副本数
def get_expected_replica_count(query_cur):
  sql = """select tenant_id, ls_id, count(*) as replica_count from oceanbase.CDB_OB_LS_LOCATIONS
           where ls_id != 1
           group by tenant_id, ls_id"""
  try:
    (desc, results) = query_cur.exec_query(sql, print_when_succ=False)
    if len(results) == 0:
      logging.error("No data found in CDB_OB_LS_LOCATIONS")
      raise MyError("No data found in CDB_OB_LS_LOCATIONS")
  except Exception as e:
    logging.exception('fail to query CDB_OB_LS_LOCATIONS')
    raise

  tenant_replica_count = {}
  for tenant_id, ls_id, count in results:
    if tenant_id not in tenant_replica_count:
      tenant_replica_count[tenant_id] = count
    elif count != tenant_replica_count[tenant_id]:
      logging.error("Inconsistent replica count: tenant_id=%d, ls_id=%s, expected=%d, actual=%d",
                    tenant_id, ls_id, tenant_replica_count[tenant_id], count)
      raise MyError("Inconsistent replica count: tenant_id={0}, ls_id={1}, expected={2}, actual={3}".format(
        tenant_id, ls_id, tenant_replica_count[tenant_id], count))

  logging.info("Got expected replica count from CDB_OB_LS_LOCATIONS: %d tenants, details: %s",
               len(tenant_replica_count), tenant_replica_count)
  return tenant_replica_count

# 按租户和tablet分组，同时区分HNSW/HGRAPH和IVF索引
def group_by_tenant_and_tablet(results):
  grouped = {'hnsw': {}, 'ivf': {}}
  for row in results:
    tenant_id = row[0]
    tablet_id = row[1]
    svr_ip = row[2]
    svr_port = row[3]
    sync_info_str = row[4]
    data_table_id = row[5]
    snapshot_index_table_id = row[6]
    ls_id = row[7]

    sync_info = parse_sync_info(sync_info_str)
    is_hnsw = sync_info.get('has_snap_cnt', False) and sync_info.get('has_incr_cnt', False)
    index_type = 'hnsw' if is_hnsw else 'ivf'

    if tenant_id not in grouped[index_type]:
      grouped[index_type][tenant_id] = {}
    if tablet_id not in grouped[index_type][tenant_id]:
      grouped[index_type][tenant_id][tablet_id] = {
        'data_table_id': data_table_id,
        'snapshot_index_table_id': snapshot_index_table_id,
        'ls_id': ls_id,
        'replicas': []
      }
    grouped[index_type][tenant_id][tablet_id]['replicas'].append({
      'svr_ip': svr_ip,
      'svr_port': svr_port,
      'sync_info': sync_info
    })

  return grouped

def check_tenant_hnsw_index_loaded(tenant_id, tablets, incr_history, expected_replica_count, max_zone_count):
  # snap_cnt放宽条件
  SNAP_MAX_PER_TABLET = 100000       # 不一致快照索引单个tablet不超过10W向量
  SNAP_MAX_TABLETS = 30              # 不一致快照索引tablet数量低于30个
  SNAP_MAX_SUM = 300000              # 不一致快照索引向量总数不超过30W

  # incr_cnt条件
  INCR_DIFF_THRESHOLD1 = 500         # 条件1: 差距小于500
  # incr_cnt放宽条件
  INCR_DIFF_THRESHOLD2 = 1000        # 条件2: 连续3轮差距都 < 1000
  INCR_MAX_EXPANSION = 200           # 条件3: 每轮扩大不超过200
  INCR_MAX_TABLETS = 20              # 条件4: 不一致tablet数量低于20个
  INCR_MAX_SUM = 100000              # 条件5: 各个副本的增量差距在 <= 10W

  loaded_tablets = 0
  pending_list = []
  snap_diff_tablets = 0
  snap_diff_sum = 0
  incr_diff_tablets = 0
  incr_diff_sum = 0

  if tenant_id not in incr_history:
    incr_history[tenant_id] = {}

  for tablet_id, tablet_info in tablets.items():
    replicas = tablet_info['replicas']
    data_table_id = tablet_info['data_table_id']
    actual_count = len(replicas)

    # 检查1: 副本数是否匹配
    if actual_count > max_zone_count or actual_count < expected_replica_count:
      pending_list.append({
        'tablet_id': tablet_id,
        'data_table_id': data_table_id,
        'reason': 'replica_count_mismatch',
        'expected': expected_replica_count,
        'actual': actual_count
      })
      continue

    # 检查2: snap_cnt是否一致
    snap_cnts = [r['sync_info']['snap_cnt'] for r in replicas]
    # 不一致则检查放宽条件
    if len(set(snap_cnts)) > 1:
      max_snap_cnt = max(snap_cnts) if snap_cnts else 0
      if max_snap_cnt <= SNAP_MAX_PER_TABLET:
        if snap_diff_tablets < SNAP_MAX_TABLETS and snap_diff_sum + max_snap_cnt <= SNAP_MAX_SUM:
          snap_diff_tablets += 1
          snap_diff_sum += max_snap_cnt
          logging.warn("Tenant %d Tablet %d: snap_cnt inconsistent but allowed (max=%d, count=%d, sum=%d), continue to check incr_cnt",
                       tenant_id, tablet_id, max_snap_cnt, snap_diff_tablets, snap_diff_sum)
          # 继续检查 incr_cnt
        else:
          # 超过放宽条件限制, 不通过加入pending_list, 继续检查下一个tablet
          pending_list.append({
            'tablet_id': tablet_id,
            'data_table_id': data_table_id,
            'reason': 'snap_cnt_inconsistent',
            'snap_cnts': snap_cnts
          })
          continue
      else:
        # 不通过加入pending_list, 继续检查下一个tablet
        pending_list.append({
          'tablet_id': tablet_id,
          'data_table_id': data_table_id,
          'reason': 'snap_cnt_inconsistent',
          'snap_cnts': snap_cnts
        })
        continue

    # 检查3: incr_cnt差距检查
    incr_cnts = [r['sync_info']['incr_cnt'] for r in replicas]
    max_incr = max(incr_cnts)
    min_incr = min(incr_cnts)
    incr_diff = max_incr - min_incr

    # 检查是否所有副本都有 vbitmap_cnt 字段
    has_vbitmap = all(r['sync_info'].get('has_vbitmap_cnt', False) for r in replicas)
    if has_vbitmap:
      vbitmap_cnts = [r['sync_info']['vbitmap_cnt'] for r in replicas]
      max_vbitmap = max(vbitmap_cnts) if vbitmap_cnts else 0
    else:
      vbitmap_cnts = []
      max_vbitmap = 0

    # incr条件1: 差距小于阈值，直接通过
    if incr_diff < INCR_DIFF_THRESHOLD1:
      if incr_diff_sum + incr_diff <= INCR_MAX_SUM:
        incr_diff_sum += incr_diff
        loaded_tablets += 1
        continue

    # 不一致则检查放宽条件：
    # 初始化历史记录
    if tablet_id not in incr_history[tenant_id]:
      incr_history[tenant_id][tablet_id] = {
        'incr_cnts_history': [],  # 记录每轮所有副本的incr_cnt值
        'diff_history': [],        # 记录每轮的差距
        'vbitmap_history': []      # 记录每轮所有副本的vbitmap_cnt值
      }

    history = incr_history[tenant_id][tablet_id]

    # 添加当前轮次的数据，只保留最近3轮数据
    history['incr_cnts_history'].append(tuple(incr_cnts))
    history['diff_history'].append(incr_diff)
    if has_vbitmap:
      history['vbitmap_history'].append(tuple(vbitmap_cnts))

    if len(history['incr_cnts_history']) > 3:
      history['incr_cnts_history'] = history['incr_cnts_history'][-3:]
    if len(history['diff_history']) > 3:
      history['diff_history'] = history['diff_history'][-3:]
    if len(history['vbitmap_history']) > 6:
      history['vbitmap_history'] = history['vbitmap_history'][-6:]

    # 增量不再快速扩大：连续两轮，每轮扩大都不超过阈值
    if len(history['diff_history']) >= 3:
      recent_diffs = history['diff_history'][-3:]
      d1, d2, d3 = recent_diffs
      not_expanding = d2 <= d1 + INCR_MAX_EXPANSION and d3 <= d2 + INCR_MAX_EXPANSION

      # incr条件: 差距 < 阈值 且增量不再快速扩大
      incr_condition = d3 < INCR_DIFF_THRESHOLD2 and not_expanding
      # bitmap条件: 每轮内所有副本的vbitmap_cnt都相等 >= 6轮 且增量不再快速扩大
      bitmap_condition = False
      if has_vbitmap and max_vbitmap > 0 and len(history['vbitmap_history']) >= 6 and not_expanding:
        if all(len(set(tuple_val)) == 1 for tuple_val in history['vbitmap_history']):
          bitmap_condition = True

      if incr_condition:
        if incr_diff_tablets < INCR_MAX_TABLETS and incr_diff_sum + incr_diff <= INCR_MAX_SUM:
          incr_diff_tablets += 1
          incr_diff_sum += incr_diff
          logging.info("Tenant %d Tablet %d: incr_cnt diff < %d for 3 rounds and not expanding too fast (diff_history=%s), mark as loaded",
                       tenant_id, tablet_id, INCR_DIFF_THRESHOLD2, recent_diffs)
          loaded_tablets += 1
          continue
      elif bitmap_condition:
        logging.info("Tenant %d Tablet %d: vbitmap_cnt equal for 6 rounds and not expanding too fast, mark as loaded",
                     tenant_id, tablet_id)
        loaded_tablets += 1
        continue

    #检查不通过加入pending_list，继续检查下一个tablet
    pending_list.append({
      'tablet_id': tablet_id,
      'data_table_id': data_table_id,
      'reason': 'incr_cnt_inconsistent',
      'max_incr': max_incr,
      'min_incr': min_incr,
      'diff': incr_diff,
      'history_rounds': len(history['diff_history'])
    })

  return loaded_tablets, pending_list

def format_tenant_status(tenant_stats, elapsed=None):
  lines = []
  if elapsed is not None:
    lines.append("Vector index loading timeout after {0}s".format(int(elapsed)))
    lines.append("Please check index loading status manually or skip")

  for tenant_id, stats in tenant_stats.items():
    total = stats['total']
    loaded = stats['loaded']
    if total == 0:
      lines.append("  Tenant {0}: no HNSW/HGRAPH indexes to check".format(tenant_id))
    else:
      progress = loaded * 100.0 / total
      lines.append("  Tenant {0}: {1}/{2} tablets loaded ({3:.1f}%), expected {4} replicas".format(
        tenant_id, loaded, total, progress, stats['expected_replica_count']))
      pending_list = stats.get('pending_list', [])
      if len(pending_list) > 0:
        lines.append("    Top {0} pending tablets:".format(min(10, len(pending_list))))
        for item in pending_list[:10]:
          lines.append(str(item))

  return "\n".join(lines)


# 检查向量索引是否加载完成
# 按租户分组检查, 确保同一租户的所有tablet副本数一致，并比较同一tablet在不同副本上的sync_info是否一致
# 轮询等待(10秒间隔)直到所有索引加载完成或超时
def check_vector_index_loaded(query_cur, zone, timeout):
  need_check = 0
  if has_vector_index(query_cur):
    need_check = 1
    logging.info('Cluster has vector indexes, start checking loading status (scope: %s)', zone if zone else 'all')
  else:
    logging.info('No vector index found, skip vector index loading check')
  
  if need_check == 1:
    wait_timeout = set_default_timeout_by_tenant(query_cur, timeout, 300, 900)
    max_times = int(wait_timeout / 10)
    times = max_times
    start_time = time.time()
    check_done = 0
    incr_history = {}
    hnsw_tenant_stats = {}

    while times >= 0 and check_done == 0:
      results = query_vector_index_info_all(query_cur)

      if len(results) == 0:
        logging.warn("Query returned empty, may be query unstable, will retry")
      else:
        grouped = group_by_tenant_and_tablet(results)

        if not grouped['hnsw'] and not grouped['ivf']:
          logging.warn("Query returned empty, may be query unstable, will retry")
        else:
          tenant_expected_replica_count = get_expected_replica_count(query_cur)
          max_zone_count = get_max_zone_count(query_cur)
          logging.info("Zone count: %d", max_zone_count)

          result, tenant_stats = check_vector_index_loaded_one_round(grouped, tenant_expected_replica_count, incr_history, max_zone_count, max_times - times, max_times)
          if result == 'success':
            check_done = 1
          else:
            hnsw_tenant_stats = tenant_stats

      times -= 1
      if times == -1 and check_done == 0:
        elapsed = time.time() - start_time
        error_msg = format_tenant_status(hnsw_tenant_stats, elapsed)
        logging.warn(error_msg)
        raise MyError(error_msg)

      if check_done == 0:
        time.sleep(10)

def check_vector_index_loaded_one_round(grouped, tenant_expected_replica_count, incr_history, max_zone_count, current_round, max_round):
  # 收集HNSW索引的tablet信息
  # 检查HNSW/HGRAPH索引
  all_loaded = 1
  hnsw_tenant_stats = {}

  for tenant_id, tablets in grouped['hnsw'].items():
    expected_count = tenant_expected_replica_count.get(tenant_id, 0)
    if expected_count == 0:
      logging.warn("No replica count found for tenant %d, will retry", tenant_id)
      return 'retry', {}

    loaded, pending_list = check_tenant_hnsw_index_loaded(tenant_id, tablets, incr_history, expected_count, max_zone_count)

    hnsw_tenant_stats[tenant_id] = {
      'loaded': loaded,
      'total': len(tablets),
      'pending': len(pending_list),
      'pending_list': pending_list,
      'expected_replica_count': expected_count
    }

    if len(pending_list) > 0:
      all_loaded = 0

  ivf_count = sum(len(tablets) for tablets in grouped['ivf'].values())
  if ivf_count > 0:
    logging.info("Found %d IVF index tablets (skipped, IVF does not require loading check)", ivf_count)

  # 打印检查进度
  logging.info("Checking vector index loading status (attempt %d/%d)", current_round, max_round)
  logging.info(format_tenant_status(hnsw_tenant_stats))

  # 检查是否全部加载完成
  if all_loaded == 1:
    logging.info("All vector indexes loaded successfully")
    logging.info("Checked vector indexes:")
    logging.info(format_tenant_status(hnsw_tenant_stats))
    return 'success', hnsw_tenant_stats

  return 'retry', hnsw_tenant_stats

# ====== 向量索引加载检查相关函数结束 ======

# 开始健康检查
def do_check(my_host, my_port, my_user, my_passwd, upgrade_params, timeout, need_check_major_status, zone = ''):
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
      query_cur = QueryCursor(cur)
      check_zone_valid(query_cur, zone)
      check_observer_status(query_cur, zone, timeout)
      check_paxos_replica(query_cur, timeout)
      check_schema_status(query_cur, timeout)
      check_server_version_by_zone(query_cur, zone)
      if True == need_check_major_status:
        check_major_merge(query_cur, timeout)
      check_vector_index_loaded(query_cur, zone, timeout)
    except Exception as e:
      logging.exception('run error')
      raise
    finally:
      cur.close()
      conn.close()
  except mysql.connector.Error as e:
    logging.exception('connection error')
    raise
  except Exception as e:
    logging.exception('normal error')
    raise

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
      timeout = int(get_opt_timeout())
      zone = get_opt_zone()
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", log-file=\"%s\", timeout=%s, zone=\"%s\"', \
          host, port, user, password.replace('"', '\\"'), log_filename, timeout, zone)
      do_check(host, port, user, password, upgrade_params, timeout, False, zone) # need_check_major_status = False
    except mysql.connector.Error as e:
      logging.exception('mysql connctor error')
      raise
    except Exception as e:
      logging.exception('normal error')
      raise

