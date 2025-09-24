#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import errorcode
from my_error import MyError
import logging

def results_to_str(desc, results):
  ret_str = ''
  max_width_list = []
  for col_desc in desc:
    max_width_list.append(len(str(col_desc[0])))
  col_count = len(max_width_list)
  for result in results:
    if col_count != len(result):
      raise MyError('column count is not equal, desc column count: {0}, data column count: {1}'.format(col_count, len(result)))
    for i in range(0, col_count):
      result_col_width = len(str(result[i]))
      if max_width_list[i] < result_col_width:
        max_width_list[i] = result_col_width
  # 打印列名
  for i in range(0, col_count):
    if i > 0:
      ret_str += '    ' # 空四格
    ret_str += str(desc[i][0])
    # 补足空白
    for j in range(0, max_width_list[i] - len(str(desc[i][0]))):
      ret_str += ' '
  # 打印数据
  for result in results:
    ret_str += '\n' # 先换行
    for i in range(0, col_count):
      if i > 0:
        ret_str += '    ' # 空四格
      ret_str += str(result[i])
      # 补足空白
      for j in range(0, max_width_list[i] - len(str(result[i]))):
        ret_str += ' '
  return ret_str

def query_and_dump_results(query_cur, sql):
  (desc, results) = query_cur.exec_query(sql)
  result_str = results_to_str(desc, results)
  logging.info('dump query results, sql: %s, results:\n%s', sql, result_str)

upgrade_session_timeout_set = False

# set_session_timeout_for_upgrade这个函数只能被调用一次，设置当前升级流程里所有SQL的timeout，单个步骤需要改timeout请使用SetSessionTimeout
# 用法：
# with SetSessionTimeout(cur, timeout):
#   # some code here
#   pass
def set_session_timeout_for_upgrade(cur, time):
  global upgrade_session_timeout_set
  if upgrade_session_timeout_set:
    raise MyError('error in upgrade script, set_session_timeout_for_upgrade should only be called once')
  upgrade_session_timeout_set = True
  sql = "set @@session.ob_query_timeout = {0}".format(time * 1000 * 1000)
  logging.info(sql)
  cur.execute(sql)

class SetSessionTimeout:
  def set_session_timeout(self, cur, time):
    sql = "set @@session.ob_query_timeout = {0}".format(time)
    logging.info(sql)
    cur.execute(sql)

  def get_session_timeout(self, cur):
    sql = "select @@session.ob_query_timeout"
    cur.execute(sql)
    results = cur.fetchall()
    if len(results) != 1 or len(results[0]) != 1:
      logging.exception('results unexpected')
    else:
      return int(results[0][0])

  def __init__(self, cur, seconds):
    self.cur = cur
    self.query_timeout_before = self.get_session_timeout(cur)
    self.timeout_overall = seconds * 1000 * 1000

  # 类似于C++里的RAII，会在with语句入口处调用__enter__，出口处调用__exit__
  def __enter__(self):
    if self.timeout_overall != 0:
      self.set_session_timeout(self.cur, self.timeout_overall)

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.timeout_overall != 0:
      self.set_session_timeout(self.cur, self.query_timeout_before)
