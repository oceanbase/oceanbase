#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
from actions import Cursor
from actions import DMLCursor
from actions import QueryCursor
from actions import check_current_cluster_is_primary
import mysql.connector
from mysql.connector import errorcode
import actions

def do_special_upgrade(conn, cur, tenant_id_list, user, pwd):
  # special upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
####========******####======== actions begin ========####******========####
  run_upgrade_job(conn, cur, "3.1.2")

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

  def get_config(cur, config_name):
    results = query(cur,
      '''
      select distinct value from oceanbase.__all_virtual_sys_parameter_stat
      where name="{}"
      '''.format(config_name))
    
    return results if results else []

  def get_unique_config(cur, config_name):
    '''
    get one config of `config_name`
    expect the result exists and only one
    '''
    values = get_config(cur, config_name)
    if len(values) != 1:
      logging.error('difference config of %s, values=%s', config_name, values)
      raise MyError('difference config of {}, values={}'.format(config_name, values))

    return values[0][0]

  def get_float_config(cur, config_name):
    '''
    get one config of `config_name`
    should one config values exists which is a float
    '''
    value = get_unique_config(cur, config_name)
    try:
      return float(value)
    except Exception as ex:
      raise MyError('cannot convert {} to float. value={}'.format(config_name, value))

  def get_bytes_config(cur, config_name):
    '''
    get one config of `config_name`
    should one config values exists which is a readable bytes value like '32G', '16M'
    '''
    value = get_unique_config(cur, config_name)
    return get_bytes_from_readable_string(value)

  def get_server_cpu_quota(cur):
    min_config_name = 'server_cpu_quota_min'
    max_config_name = 'server_cpu_quota_max'
    return (get_float_config(cur, min_config_name), get_float_config(cur, max_config_name))

  def get_sys_tenant_memory(cur):
    '''
    refer to ObServerConfig::get_max_sys_tenant_memory
    '''
    memory_limit = get_bytes_config(cur, 'memory_limit')
    system_memory = get_bytes_config(cur, 'system_memory')

    if memory_limit != 0:
      server_memory_limit = memory_limit
    else:
      memory_limit_percentage = get_float_config(cur, 'memory_limit_percentage')
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

  def get_sys_unit_config(cur):
    results = query(cur,
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

  def get_observers_with_sys_unit(cur):
    results = query(cur,
      '''
      select svr_ip,svr_port 
      from oceanbase.__all_resource_pool as a, oceanbase.__all_unit as b 
      where a.resource_pool_id=b.resource_pool_id and a.unit_config_id=1
      '''
    )

    return results if results else []

  def skip_action(cur):

    # read config items: server_cpu_quota_max & max_sys_tenant_memory
    server_cpu_quota_min, server_cpu_quota_max = get_server_cpu_quota(cur)
    min_sys_tenant_memory, max_sys_tenant_memory = get_sys_tenant_memory(cur)

    # get sys unit config
    sys_unit_config = get_sys_unit_config(cur)

    # get all observers with sys unit
    observers_with_sys_unit = get_observers_with_sys_unit(cur)

    CPU_EPSILON = 0.00001 # const value, copy from ob_unit_info.h

    # check whether the observer can hold the invisible sys unit
    all_observer_resource_results = query(cur,
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

        if (cpu_total - cpu_max_assigned < -CPU_EPSILON or
            cpu_total - cpu_assigned < -CPU_EPSILON or
            mem_total < mem_max_assigned or
            mem_total < mem_assigned ):

          logging.warn('there is one observer cannot hold the invisible sys unit. ' \
                       'observer=%s, invisible sys unit=cpu (min=%s,max=%s),memory (min=%s,max=%s)',
                      str(observer_resource_result), str(server_cpu_quota_min), str(server_cpu_quota_max), 
                      str(min_sys_tenant_memory), str(max_sys_tenant_memory))
          return False

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

    logging.info('very good. all observers can hold the invisible sys unit')
    return True

  def check_before_do_action(cur):
    from upgrade_checker import set_parameter
    set_parameter(cur, 'enable_rebalance', 'False')
    # 升级前关闭rebalance，升级后在 upgrade_post_checker 中会打开enable_rebalance

  def get_action_ddl():
    return 'alter system set _report_invisible_sys_unit_resource = "False"'

  # 检查当前系统状态是否可以打开开关：不带实体sys unit的节点上报sys unit 相关的资源
  if not skip_action(cur):
    check_before_do_action(cur)
    sql = get_action_ddl()
    try:
      logging.info('before running: %s', sql)
      cur.execute(sql)
    except Exception, ex:
      logging.error('failed to run sql: %s. ', sql)
      raise ex

  return
####========******####========= actions end =========####******========####

def trigger_schema_split_job(conn, cur, user, pwd):
  try:
    query_cur = actions.QueryCursor(cur)
    is_primary = actions.check_current_cluster_is_primary(query_cur)
    if not is_primary:
      logging.warn("current cluster should by primary")
      raise e

    # primary cluster
    trigger_schema_split_job_by_cluster(conn, cur)

    # stanby cluster
    standby_cluster_list = actions.fetch_standby_cluster_infos(conn, query_cur, user, pwd)
    for standby_cluster in standby_cluster_list:
      # connect
      logging.info("start to trigger schema split by cluster: cluster_id = {0}"
                   .format(standby_cluster['cluster_id']))
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
      # trigger schema split
      trigger_schema_split_job_by_cluster(tmp_conn, tmp_cur)
      # close
      tmp_cur.close()
      tmp_conn.close()
      logging.info("""trigger schema split success : cluster_id = {0}, ip = {1}, port = {2}"""
                      .format(standby_cluster['cluster_id'],
                              standby_cluster['ip'],
                              standby_cluster['port']))

  except Exception, e:
    logging.warn("trigger schema split failed")
    raise e
  logging.info("trigger schema split success")

def trigger_schema_split_job_by_cluster(conn, cur):
  try:
    # check record in rs_job
    sql = "select count(*) from oceanbase.__all_rootservice_job where job_type = 'SCHEMA_SPLIT_V2';"
    logging.info(sql)
    cur.execute(sql)
    result = cur.fetchall()
    if 1 != len(result) or 1 != len(result[0]):
      logging.warn("unexpected result cnt")
      raise e
    elif 0 == result[0][0]:
      # insert fail record to start job
      sql = "replace into oceanbase.__all_rootservice_job(job_id, job_type, job_status, progress, rs_svr_ip, rs_svr_port) values (0, 'SCHEMA_SPLIT_V2', 'FAILED', 100, '0.0.0.0', '0');"
      logging.info(sql)
      cur.execute(sql)
      # check record in rs_job
      sql = "select count(*) from oceanbase.__all_rootservice_job where job_type = 'SCHEMA_SPLIT_V2' and job_status = 'FAILED';"
      logging.info(sql)
      cur.execute(sql)
      result = cur.fetchall()
      if 1 != len(result) or 1 != len(result[0]) or 1 != result[0][0]:
        logging.warn("schema split record should be 1")
        raise e

  except Exception, e:
    logging.warn("start schema split task failed")
    raise e
  logging.info("start schema split task success")

def query(cur, sql):
  cur.execute(sql)
  results = cur.fetchall()
  return results

def get_tenant_names(cur):
  return [_[0] for _ in query(cur, 'select tenant_name from oceanbase.__all_tenant')]

def update_cluster_update_table_schema_version(conn, cur):
  time.sleep(30)
  try:
    query_timeout_sql = "set ob_query_timeout = 30000000;"
    logging.info(query_timeout_sql)
    cur.execute(query_timeout_sql)
    sql = "alter system run job 'UPDATE_TABLE_SCHEMA_VERSION';"
    logging.info(sql)
    cur.execute(sql)
  except Exception, e:
    logging.warn("run update table schema version job failed")
    raise e
  logging.info("run update table schema version job success")


def run_create_inner_schema_job(conn, cur):
  try:
    ###### enable ddl
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'True')

    # check record in rs_job
    count_sql = """select count(*) from oceanbase.__all_rootservice_job
            where job_type = 'CREATE_INNER_SCHEMA';"""
    result = query(cur, count_sql)
    job_count = 0
    if (1 != len(result)) :
      logging.warn("unexpected sql output")
      raise e
    else :
      job_count = result[0][0]
      # run job
      sql = "alter system run job 'CREATE_INNER_SCHEMA';"
      logging.info(sql)
      cur.execute(sql)

      # wait job finish
      times = 180
      ## 先检查job count变化
      count_check = False
      while times > 0:
        result = query(cur, count_sql)
        if (1 != len(result)):
          logging.warn("unexpected sql output")
          raise e
        elif (result[0][0] > job_count):
          count_check = True
          logging.info('create_inner_schema job detected')
          break
        time.sleep(10)
        times -= 1
        if times == 0:
          raise MyError('check create_inner_schema job failed!')

      ## 继续检查job status状态
      status_sql = """select job_status from oceanbase.__all_rootservice_job
                      where job_type = 'CREATE_INNER_SCHEMA' order by job_id desc limit 1;"""
      status_check = False
      while times > 0 and count_check == True:
        result = query(cur, status_sql)
        if (0 == len(result)):
          logging.warn("unexpected sql output")
          raise e
        elif (1 != len(result) or 1 != len(result[0])):
          logging.warn("result len not match")
          raise e
        elif result[0][0] == "FAILED":
          logging.warn("run create_inner_schema job faild")
          raise e
        elif result[0][0] == "INPROGRESS":
          logging.info('create_inner_schema job is still running')
        elif result[0][0] == "SUCCESS":
          status_check = True
          break;
        else:
          logging.warn("invalid result: {0}" % (result[0][0]))
          raise e
        time.sleep(10)
        times -= 1
        if times == 0:
          raise MyError('check create_inner_schema job failed!')

      if (status_check == True and count_check == True):
        logging.info('check create_inner_schema job success')
      else:
        logging.warn("run create_inner_schema job faild")
        raise e

    # disable ddl
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'False')
  except Exception, e:
    logging.warn("run create_inner_schema job failed")
    raise e
  logging.info("run create_inner_schema job success")

def statistic_primary_zone_count(conn, cur):
  try:
    ###### disable ddl
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'False')

    # check record in rs_job
    count_sql = """select count(*) from oceanbase.__all_rootservice_job
            where job_type = 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT';"""
    result = query(cur, count_sql)
    job_count = 0
    if (1 != len(result)) :
      logging.warn("unexpected sql output")
      raise e
    else :
      job_count = result[0][0]
      # run job
      sql = "alter system run job 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT';"
      logging.info(sql)
      cur.execute(sql)
      # wait job finish
      times = 180
      ## 先检查job count变化
      count_check = False
      while times > 0:
        result = query(cur, count_sql)
        if (1 != len(result)):
          logging.warn("unexpected sql output")
          raise e
        elif (result[0][0] > job_count):
          count_check = True
          logging.info('statistic_primary_zone_entity_count job detected')
          break
        time.sleep(10)
        times -= 1
        if times == 0:
          raise MyError('statistic_primary_zone_entity_count job failed!')

      ## 继续检查job status状态
      status_sql = """select job_status from oceanbase.__all_rootservice_job
                      where job_type = 'STATISTIC_PRIMARY_ZONE_ENTITY_COUNT' order by job_id desc limit 1;"""
      status_check = False
      while times > 0 and count_check == True:
        result = query(cur, status_sql)
        if (0 == len(result)):
          logging.warn("unexpected sql output")
          raise e
        elif (1 != len(result) or 1 != len(result[0])):
          logging.warn("result len not match")
          raise e
        elif result[0][0] == "FAILED":
          logging.warn("run statistic_primary_zone_entity_count job faild")
          raise e
        elif result[0][0] == "INPROGRESS":
          logging.info('statistic_primary_zone_entity_count job is still running')
        elif result[0][0] == "SUCCESS":
          status_check = True
          break;
        else:
          logging.warn("invalid result: {0}" % (result[0][0]))
          raise e
        time.sleep(10)
        times -= 1
        if times == 0:
          raise MyError('check statistic_primary_zone_entity_count job failed!')

      if (status_check == True and count_check == True):
        logging.info('check statistic_primary_zone_entity_count job success')
      else:
        logging.warn("run statistic_primary_zone_entity_count job faild")
        raise e

    # enable ddl
    if ori_enable_ddl == 1:
      actions.set_parameter(cur, 'enable_ddl', 'True')
  except Exception, e:
    logging.warn("run statistic_primary_zone_entity_count job failed")
    raise e
  logging.info("run statistic_primary_zone_entity_count job success")

def disable_major_freeze(conn, cur):
  try:
    actions.set_parameter(cur, "enable_major_freeze", 'False')
  except Exception, e:
    logging.warn("disable enable_major_freeze failed")
    raise e
  logging.info("disable enable_major_freeze finish")

def get_max_used_job_id(cur):
  try:
    max_job_id = 0
    sql = "select job_id from oceanbase.__all_rootservice_job order by job_id desc limit 1"
    results = query(cur, sql)

    if (len(results) == 0):
      max_job_id = 0
    elif (len(results) != 1 or len(results[0]) != 1):
      logging.warn("row cnt not match")
      raise e
    else:
      max_job_id = results[0][0]

    logging.info("get max_used_job_id:{0}".format(max_job_id))

    return max_job_id
  except Exception, e:
    logging.warn("failed to get max_used_job_id")
    raise e

def check_can_run_upgrade_job(cur, version):
  try:
    sql = """select job_status from oceanbase.__all_rootservice_job
             where job_type = 'RUN_UPGRADE_POST_JOB' and extra_info = '{0}'
             order by job_id desc limit 1""".format(version)
    results = query(cur, sql)

    bret = True
    if (len(results) == 0):
      bret = True
      logging.info("upgrade job not created yet, should run upgrade job")
    elif (len(results) != 1 or len(results[0]) != 1):
      logging.warn("row cnt not match")
      raise e
    elif ("INPROGRESS" == results[0][0]):
      logging.warn("upgrade job still running, should wait")
      raise e
    elif ("SUCCESS" == results[0][0]):
      bret = False
      logging.info("execute upgrade job successfully, skip run upgrade job")
    elif ("FAILED" == results[0][0]):
      bret = True
      logging.info("execute upgrade job failed, should run again")
    else:
      logging.warn("invalid job status: {0}".format(results[0][0]))
      raise e

    return bret
  except Exception, e:
    logging.warn("failed to check if upgrade job can run")
    raise e


def check_upgrade_job_result(cur, version, max_used_job_id):
  try:
    times = 0
    while (times <= 180):
      sql = """select job_status from oceanbase.__all_rootservice_job
               where job_type = 'RUN_UPGRADE_POST_JOB' and extra_info = '{0}'
               and job_id > {1} order by job_id desc limit 1""".format(version, max_used_job_id)
      results = query(cur, sql)

      if (len(results) == 0):
        logging.info("upgrade job not created yet")
      elif (len(results) != 1 or len(results[0]) != 1):
        logging.warn("row cnt not match")
        raise e
      elif ("INPROGRESS" == results[0][0]):
        logging.info("upgrade job is still running")
      elif ("SUCCESS" == results[0][0]):
        logging.info("execute upgrade job successfully")
        break;
      elif ("FAILED" == results[0][0]):
        logging.warn("execute upgrade job failed")
        raise e
      else:
        logging.warn("invalid job status: {0}".format(results[0][0]))
        raise e

      times = times + 1
      time.sleep(10)
  except Exception, e:
    logging.warn("failed to check upgrade job result")
    raise e


def run_upgrade_job(conn, cur, version):
  try:
    logging.info("start to run upgrade job, version:{0}".format(version))
    # pre check
    if (check_can_run_upgrade_job(cur, version) == True):
      conn.autocommit = True
      # disable enable_ddl
      ori_enable_ddl = actions.get_ori_enable_ddl(cur)
      if ori_enable_ddl == 1:
        actions.set_parameter(cur, 'enable_ddl', 'False')
      # get max_used_job_id
      max_used_job_id = get_max_used_job_id(cur)
      # run upgrade job
      sql = """alter system run upgrade job '{0}'""".format(version)
      logging.info(sql)
      cur.execute(sql)
      # check upgrade job result
      check_upgrade_job_result(cur, version, max_used_job_id)
      # reset enable_ddl
      if ori_enable_ddl == 1:
        actions.set_parameter(cur, 'enable_ddl', 'True')
  except Exception, e:
    logging.warn("run upgrade job failed, version:{0}".format(version))
    raise e
  logging.info("run upgrade job success, version:{0}".format(version))
