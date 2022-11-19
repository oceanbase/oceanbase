#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
import time
import mysql.connector
from mysql.connector import errorcode
import logging
import re
import string
from random import Random
from actions import DMLCursor
from actions import QueryCursor
import binascii
import my_utils
import actions
import sys

#def modify_schema_history(conn, cur, tenant_ids):
#  try:
#    conn.autocommit = True
#
#    # disable ddl
#    ori_enable_ddl = actions.get_ori_enable_ddl(cur)
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'False')
#    log('tenant_ids: {0}'.format(tenant_ids))
#    for tenant_id in tenant_ids:
#      sql = """alter system change tenant tenant_id = {0}""".format(tenant_id)
#      log(sql)
#      cur.execute(sql)

#    #####implement#####
#
#    # 还原默认 tenant
#    sys_tenant_id = 1
#    sql = """alter system change tenant tenant_id = {0}""".format(sys_tenant_id)
#    log(sql)
#    cur.execute(sql)
#
#    # enable ddl
#    if ori_enable_ddl == 1:
#      actions.set_parameter(cur, 'enable_ddl', 'True')
#  except Exception, e:
#    logging.warn("exec modify trigger failed")
#    raise e
#  logging.info('exec modify trigger finish')

# 主库需要执行的升级动作
def do_special_upgrade(conn, cur, tenant_id_list, user, passwd):
  # special upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
# 主库升级流程没加滚动升级步骤，或混部阶段DDL测试有相关case覆盖前，混部开始禁DDL
  actions.set_parameter(cur, 'enable_ddl', 'False')
####========******####======== actions begin ========####******========####
  return
####========******####========= actions end =========####******========####

def query(cur, sql):
  log(sql)
  cur.execute(sql)
  results = cur.fetchall()
  return results

def log(msg):
  logging.info(msg)

def get_oracle_tenant_ids(cur):
  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant where compatibility_mode = 1')]

def get_tenant_ids(cur):
  return [_[0] for _ in query(cur, 'select tenant_id from oceanbase.__all_tenant')]

