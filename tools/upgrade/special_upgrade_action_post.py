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

def do_special_upgrade(conn, cur, tenant_id_list, user, pwd):
  # special upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
  upgrade_system_package(conn, cur)
####========******####======== actions begin ========####******========####
  return
####========******####========= actions end =========####******========####

def query(cur, sql):
  cur.execute(sql)
  results = cur.fetchall()
  return results

def get_tenant_names(cur):
  return [_[0] for _ in query(cur, 'select tenant_name from oceanbase.__all_tenant')]

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
             where job_type = 'UPGRADE_POST_ACTION' and extra_info = '{0}'
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


def upgrade_system_package(conn, cur):
  try:
    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'True')

    sql = "set ob_query_timeout = 300000000;"
    logging.info(sql)
    cur.execute(sql)

    sql = """
      CREATE OR REPLACE PACKAGE __DBMS_UPGRADE
        PROCEDURE UPGRADE(package_name VARCHAR(1024));
        PROCEDURE UPGRADE_ALL();
      END;
    """
    logging.info(sql)
    cur.execute(sql)

    sql = """
      CREATE OR REPLACE PACKAGE BODY __DBMS_UPGRADE
        PROCEDURE UPGRADE(package_name VARCHAR(1024));
          PRAGMA INTERFACE(c, UPGRADE_SINGLE);
        PROCEDURE UPGRADE_ALL();
          PRAGMA INTERFACE(c, UPGRADE_ALL);
      END;
    """
    logging.info(sql)
    cur.execute(sql)

    sql = """
      CALL __DBMS_UPGRADE.UPGRADE_ALL();
    """
    logging.info(sql)
    cur.execute(sql)

    ###### alter session compatibility mode
    sql = "set ob_compatibility_mode='oracle';"
    logging.info(sql)
    cur.execute(sql)

    sql = "set ob_query_timeout = 300000000;"
    logging.info(sql)
    cur.execute(sql)

    sql = """
      CREATE OR REPLACE PACKAGE "__DBMS_UPGRADE" IS
        PROCEDURE UPGRADE(package_name VARCHAR2);
        PROCEDURE UPGRADE_ALL;
      END;
    """
    logging.info(sql)
    cur.execute(sql)

    sql = """
      CREATE OR REPLACE PACKAGE BODY "__DBMS_UPGRADE" IS
        PROCEDURE UPGRADE(package_name VARCHAR2);
          PRAGMA INTERFACE(c, UPGRADE_SINGLE);
        PROCEDURE UPGRADE_ALL;
          PRAGMA INTERFACE(c, UPGRADE_ALL);
      END;
    """
    logging.info(sql)
    cur.execute(sql)

    ########## update system package ##########
    sql = """
      CALL "__DBMS_UPGRADE".UPGRADE_ALL();
    """
    logging.info(sql)
    cur.execute(sql)

    ###### alter session compatibility mode
    sql = "set ob_compatibility_mode='mysql';"
    logging.info(sql)
    cur.execute(sql)
    time.sleep(10)

    if ori_enable_ddl == 0:
      actions.set_parameter(cur, 'enable_ddl', 'False')

    logging.info("upgrade package finished!")
  except Exception, e:
    logging.warn("upgrade package failed!")
    raise e

