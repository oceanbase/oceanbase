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
  run_upgrade_job(conn, cur, "3.1.5")
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
