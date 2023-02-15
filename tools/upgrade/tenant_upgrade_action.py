#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
from actions import Cursor
from actions import DMLCursor
from actions import QueryCursor
import mysql.connector
from mysql.connector import errorcode
import actions

def do_upgrade(conn, cur, timeout, user, pwd):
  # upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
  across_version = upgrade_across_version(cur)
  if across_version:
    run_upgrade_job(conn, cur, "UPGRADE_ALL", timeout)
  else:
    run_upgrade_job(conn, cur, "UPGRADE_VIRTUAL_SCHEMA", timeout)

  # just to make __all_virtual_upgrade_inspection avaliable
  timeout_ts = (timeout if timeout > 0 else 600) * 1000 * 1000
  sql = "set @@session.ob_query_timeout = {0}".format(timeout_ts)
  logging.info(sql)
  cur.execute(sql)
  sql = "alter system run job 'root_inspection'"
  logging.info(sql)
  cur.execute(sql)
  sql = "set @@session.ob_query_timeout = 10000000"
  logging.info(sql)
  cur.execute(sql)
####========******####======== actions begin ========####******========####
  upgrade_syslog_level(conn, cur)
  return

def upgrade_syslog_level(conn, cur):
  try:
    cur.execute("""select svr_ip, svr_port, value from oceanbase.__all_virtual_sys_parameter_stat where name = 'syslog_level'""")
    result = cur.fetchall()
    for r in result:
      logging.info("syslog level before upgrade: ip: {0}, port: {1}, value: {2}".format(r[0], r[1], r[2]))
    cur.execute("""select count(*) cnt from oceanbase.__all_virtual_sys_parameter_stat where name = 'syslog_level' and value = 'INFO'""")
    result = cur.fetchall()
    info_cnt = result[0][0]
    if info_cnt > 0:
      actions.set_parameter(cur, "syslog_level", "WDIAG")

  except Exception, e:
    logging.warn("upgrade syslog level failed!")
    raise e
####========******####========= actions end =========####******========####

def query(cur, sql):
  cur.execute(sql)
  logging.info(sql)
  results = cur.fetchall()
  return results

def get_tenant_ids(cur):
  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant')]

def upgrade_across_version(cur):
  current_data_version = actions.get_current_data_version()
  int_current_data_version = actions.get_version(current_data_version)

  across_version = False
  sys_tenant_id = 1

  # 1. check if target_data_version/current_data_version match with current_data_version
  sql = "select count(*) from oceanbase.__all_table where table_name = '__all_virtual_core_table'"
  results = query(cur, sql)
  if len(results) < 1 or len(results[0]) < 1:
    logging.warn("row/column cnt not match")
    raise e
  elif results[0][0] <= 0:
    # __all_virtual_core_table doesn't exist, this cluster is upgraded from 4.0.0.0
    across_version = True
  else:
    # check
    tenant_ids = get_tenant_ids(cur)
    if len(tenant_ids) <= 0:
      logging.warn("tenant_ids count is unexpected")
      raise e
    tenant_count = len(tenant_ids)

    sql = "select count(*) from __all_virtual_core_table where column_name in ('target_data_version', 'current_data_version') and column_value = {0}".format(int_current_data_version)
    results = query(cur, sql)
    if len(results) != 1 or len(results[0]) != 1:
      logging.warn('result cnt not match')
      raise e
    elif 2 * tenant_count != results[0][0]:
      logging.info('target_data_version/current_data_version not match with {0}, tenant_cnt:{1}, result_cnt:{2}'.format(current_data_version, tenant_count, results[0][0]))
      across_version = True
    else:
      logging.info("all tenant's target_data_version/current_data_version are match with {0}".format(current_data_version))
      across_version = False

  # 2. check if compatible match with current_data_version
  if not across_version:
    sql = "select count(*) from oceanbase.__all_virtual_tenant_parameter_info where name = 'compatible' and value != '{0}'".format(current_data_version)
    results = query(cur, sql)
    if len(results) < 1 or len(results[0]) < 1:
      logging.warn("row/column cnt not match")
      raise e
    elif results[0][0] == 0:
      logging.info("compatible are all matched")
    else:
      logging.info("compatible unmatched")
      across_version = True

  return across_version

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

def check_can_run_upgrade_job(cur, job_name):
  try:
    sql = """select job_status from oceanbase.__all_rootservice_job
             where job_type = '{0}' order by job_id desc limit 1""".format(job_name)
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
      bret = True
      logging.info("maybe upgrade job remained, can run again")
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

def check_upgrade_job_result(cur, job_name, timeout, max_used_job_id):
  try:
    times = (timeout if timeout > 0 else 3600) / 10
    while (times >= 0):
      sql = """select job_status, rs_svr_ip, rs_svr_port, gmt_create from oceanbase.__all_rootservice_job
               where job_type = '{0}' and job_id > {1} order by job_id desc limit 1
            """.format(job_name, max_used_job_id)
      results = query(cur, sql)

      if (len(results) == 0):
        logging.info("upgrade job not created yet")
      elif (len(results) != 1 or len(results[0]) != 4):
        logging.warn("row cnt not match")
        raise e
      elif ("INPROGRESS" == results[0][0]):
        logging.info("upgrade job is still running")
        # check if rs change
        if times % 10 == 0:
          ip = results[0][1]
          port = results[0][2]
          gmt_create = results[0][3]
          sql = """select count(*) from oceanbase.__all_virtual_core_meta_table where role = 1 and svr_ip = '{0}' and svr_port = {1}""".format(ip, port)
          results = query(cur, sql)
          if (len(results) != 1 or len(results[0]) != 1):
            logging.warn("row/column cnt not match")
            raise e
          elif results[0][0] == 1:
            sql = """select count(*) from oceanbase.__all_rootservice_event_history where gmt_create > '{0}' and event = 'full_rootservice'""".format(gmt_create)
            results = query(cur, sql)
            if (len(results) != 1 or len(results[0]) != 1):
              logging.warn("row/column cnt not match")
              raise e
            elif results[0][0] > 0:
              logging.warn("rs changed, should check if upgrade job is still running")
              raise e
            else:
              logging.info("rs[{0}:{1}] still exist, keep waiting".format(ip, port))
          else:
            logging.warn("rs changed or not exist, should check if upgrade job is still running")
            raise e
      elif ("SUCCESS" == results[0][0]):
        logging.info("execute upgrade job successfully")
        break;
      elif ("FAILED" == results[0][0]):
        logging.warn("execute upgrade job failed")
        raise e
      else:
        logging.warn("invalid job status: {0}".format(results[0][0]))
        raise e

      times = times - 1
      if times == -1:
        logging.warn("""check {0} job timeout""".format(job_name))
        raise e
      time.sleep(10)
  except Exception, e:
    logging.warn("failed to check upgrade job result")
    raise e

def run_upgrade_job(conn, cur, job_name, timeout):
  try:
    logging.info("start to run upgrade job, job_name:{0}".format(job_name))
    # pre check
    if check_can_run_upgrade_job(cur, job_name):
      conn.autocommit = True
      # disable enable_ddl
      ori_enable_ddl = actions.get_ori_enable_ddl(cur, timeout)
      if ori_enable_ddl == 0:
        actions.set_parameter(cur, 'enable_ddl', 'True', timeout)
      # enable_sys_table_ddl
      actions.set_parameter(cur, 'enable_sys_table_ddl', 'True', timeout)
      # get max_used_job_id
      max_used_job_id = get_max_used_job_id(cur)
      # run upgrade job
      sql = """alter system run upgrade job '{0}'""".format(job_name)
      logging.info(sql)
      cur.execute(sql)
      # check upgrade job result
      check_upgrade_job_result(cur, job_name, timeout, max_used_job_id)
      # reset enable_sys_table_ddl
      actions.set_parameter(cur, 'enable_sys_table_ddl', 'False', timeout)
      # reset enable_ddl
      if ori_enable_ddl == 0:
        actions.set_parameter(cur, 'enable_ddl', 'False', timeout)
  except Exception, e:
    logging.warn("run upgrade job failed, :{0}".format(job_name))
    raise e
  logging.info("run upgrade job success, job_name:{0}".format(job_name))
