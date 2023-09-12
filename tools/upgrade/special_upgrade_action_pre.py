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
import upgrade_health_checker

# 主库需要执行的升级动作
def do_special_upgrade(conn, cur, timeout, user, passwd):
  # special upgrade action
#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些代码，如果不写在这两行之间的话会导致清空不掉相应的代码。
  current_version = actions.fetch_observer_version(cur)
  target_version = actions.get_current_cluster_version()
  # when upgrade across version, disable enable_ddl/major_freeze
  if current_version != target_version:
    actions.set_parameter(cur, 'enable_ddl', 'False', timeout)
    actions.set_parameter(cur, 'enable_major_freeze', 'False', timeout)
    actions.set_tenant_parameter(cur, '_enable_adaptive_compaction', 'False', timeout)
    # wait scheduler in storage to notice adaptive_compaction is switched to false
    time.sleep(60 * 2)
    query_cur = actions.QueryCursor(cur)
    wait_major_timeout = 600
    upgrade_health_checker.check_major_merge(query_cur, wait_major_timeout)
    actions.do_suspend_merge(cur, timeout)
  # When upgrading from a version prior to 4.2 to version 4.2, the bloom_filter should be disabled.
  # The param _bloom_filter_enabled is no longer in use as of version 4.2, there is no need to enable it again.
  if actions.get_version(current_version) < actions.get_version('4.2.0.0')\
      and actions.get_version(target_version) >= actions.get_version('4.2.0.0'):
    actions.set_tenant_parameter(cur, '_bloom_filter_enabled', 'False', timeout)

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

