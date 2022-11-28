#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
import mysql.connector
from mysql.connector import errorcode
from actions import BaseDMLAction
from actions import reflect_action_cls_list
from actions import QueryCursor
from actions import check_current_cluster_is_primary
import logging
import my_utils

'''
添加一条normal dml的方法:

在本文件中，添加一个类名以"NormalDMLActionPost"开头并且继承自BaseDMLAction的类，
然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
(1)@staticmethod get_seq_num():
返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
(2)dump_before_do_action(self):
执行action sql之前把一些相关数据dump到日志中。
(3)check_before_do_action(self):
执行action sql之前的检查。
(4)@staticmethod get_action_dml():
返回action sql，并且该sql必须为dml。
(5)@staticmethod get_rollback_sql():
返回回滚该action的sql。
(6)dump_after_do_action(self):
执行action sql之后把一些相关数据dump到日志中。
(7)check_after_do_action(self):
执行action sql之后的检查。
(8)skip_action(self):
check if check_before_do_action() and do_action() can be skipped

举例:
class NormalDMLActionPost1(BaseDMLAction):
  @staticmethod
  def get_seq_num():
    return 0
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
  def skip_action(self):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
    return (len(results) > 0)
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
    if len(results) > 0:
      raise MyError('some row in table test.for_test whose primary key is 9 already exists')
  @staticmethod
  def get_action_dml():
    return """insert into test.for_test values (9, 'haha', 99)"""
  @staticmethod
  def get_rollback_sql():
    return """delete from test.for_test where pk = 9"""
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test where pk = 9""")
    if len(results) != 1:
      raise MyError('there should be only one row whose primary key is 9 in table test.for_test, but there has {0} rows like that'.format(len(results)))
    elif results[0][0] != 9 or results[0][1] != 'haha' or results[0][2] != 99:
      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
'''

#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。

####========******####======== actions begin ========####******========####
####========******####========= actions end =========####******========####

def do_normal_dml_actions_by_standby_cluster(standby_cluster_infos):
  try:
    for standby_cluster_info in standby_cluster_infos:
      logging.info("do_normal_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
                   .format(standby_cluster_info['cluster_id'],
                           standby_cluster_info['ip'],
                           standby_cluster_info['port']))
      logging.info("create connection : cluster_id = {0}, ip = {1}, port = {2}"
                   .format(standby_cluster_info['cluster_id'],
                           standby_cluster_info['ip'],
                           standby_cluster_info['port']))
      conn = mysql.connector.connect(user     =  standby_cluster_info['user'],
                                     password =  standby_cluster_info['pwd'],
                                     host     =  standby_cluster_info['ip'],
                                     port     =  standby_cluster_info['port'],
                                     database =  'oceanbase',
                                     raise_on_warnings = True)

      cur = conn.cursor(buffered=True)
      conn.autocommit = True
      query_cur = QueryCursor(cur)
      is_primary = check_current_cluster_is_primary(query_cur)
      if is_primary:
        logging.exception("""primary cluster changed : cluster_id = {0}, ip = {1}, port = {2}"""
                          .format(standby_cluster_info['cluster_id'],
                                  standby_cluster_info['ip'],
                                  standby_cluster_info['port']))
        raise e

      ## process
      do_normal_dml_actions(cur)

      cur.close()
      conn.close()
  except Exception, e:
    logging.exception("""do_normal_dml_actions_by_standby_cluster failed""")
    raise e

def do_normal_dml_actions(cur):
  import normal_dml_actions_post
  cls_list = reflect_action_cls_list(normal_dml_actions_post, 'NormalDMLActionPost')
  for cls in cls_list:
    logging.info('do normal dml acion, seq_num: %d', cls.get_seq_num())
    action = cls(cur)
    action.dump_before_do_action()
    if False == action.skip_action():
      action.check_before_do_action()
      action.do_action()
    else:
      logging.info("skip dml action, seq_num: %d", cls.get_seq_num())
    action.dump_after_do_action()
    action.check_after_do_action()

def get_normal_dml_actions_sqls_str():
  import normal_dml_actions_post
  ret_str = ''
  cls_list = reflect_action_cls_list(normal_dml_actions_post, 'NormalDMLActionPost')
  for i in range(0, len(cls_list)):
    if i > 0:
      ret_str += '\n'
    ret_str += cls_list[i].get_action_dml() + ';'
  return ret_str
