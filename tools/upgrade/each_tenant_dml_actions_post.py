#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
import mysql.connector
from mysql.connector import errorcode
from actions import BaseEachTenantDMLAction
from actions import reflect_action_cls_list
from actions import QueryCursor
from actions import check_current_cluster_is_primary
import logging
import my_utils

'''
添加一条each tenant dml的方法:

在本文件中，添加一个类名以"EachTenantDMLActionPost"开头并且继承自BaseEachTenantDMLAction的类，
然后在这个类中实现以下成员函数，并且每个函数执行出错都要抛错：
(1)@staticmethod get_seq_num():
返回一个代表着执行顺序的序列号，该序列号在本文件中不允许重复，若有重复则会报错。
(2)dump_before_do_action(self):
执行action sql之前把一些相关数据dump到日志中。
(3)check_before_do_action(self):
执行action sql之前的检查。
(4)dump_before_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之前把一些相关数据dump到日志中。
(5)check_before_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之前的检查。
(6)@staticmethod get_each_tenant_action_dml(tenant_id):
返回用参数tenant_id拼成的一条action sql，并且该sql必须为dml。
(7)@staticmethod get_each_tenant_rollback_sql(tenant_id):
返回一条sql，用于回滚get_each_tenant_action_dml(tenant_id)返回的sql。
(8)dump_after_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之后把一些相关数据dump到日志中。
(9)check_after_do_each_tenant_action(self, tenant_id):
执行用参数tenant_id拼成的这条action sql之后的检查。
(10)dump_after_do_action(self):
执行action sql之后把一些相关数据dump到日志中。
(11)check_after_do_action(self):
执行action sql之后的检查。
(12)skip_pre_check(self):
check if check_before_do_action() can be skipped
(13)skip_each_tenant_action(self):
check if check_before_do_each_tenant_action() and do_each_tenant_action() can be skipped

举例:
class EachTenantDMLActionPost1(BaseEachTenantDMLAction):
  @staticmethod
  def get_seq_num():
    return 0
  def dump_before_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
  def skip_pre_check(self):
    return True
  def skip_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
    return (len(results) > 0)
  def check_before_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
    if len(results) > 0:
      raise MyError('some rows in table test.for_test1 whose c2 column is 9494 already exists')
  def dump_before_do_each_tenant_action(self, tenant_id):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
  def check_before_do_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
    if len(results) > 0:
      raise MyError('some rows in table test.for_test1 whose pk is {0} already exists'.format(tenant_id))
  @staticmethod
  def get_each_tenant_action_dml(tenant_id):
    return """insert into test.for_test1 value ({0}, 'for test 1', 9494)""".format(tenant_id)
  @staticmethod
  def get_each_tenant_rollback_sql(tenant_id):
    return """delete from test.for_test1 where pk = {0}""".format(tenant_id)
  def dump_after_do_each_tenant_action(self, tenant_id):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1 where c2 = 9494""")
  def check_after_do_each_tenant_action(self, tenant_id):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where pk = {0}""".format(tenant_id))
    if len(results) != 1:
      raise MyError('there should be only one row whose primary key is {0} in table test.for_test1, but there has {1} rows like that'.format(tenant_id, len(results)))
    elif results[0][0] != tenant_id or results[0][1] != 'for test 1' or results[0][2] != 9494:
      raise MyError('the row that has been inserted is not expected, it is: [{0}]'.format(','.join(str(r) for r in results[0])))
  def dump_after_do_action(self):
    my_utils.query_and_dump_results(self._query_cursor, """select * from test.for_test1""")
  def check_after_do_action(self):
    (desc, results) = self._query_cursor.exec_query("""select * from test.for_test1 where c2 = 9494""")
    if len(results) != len(self.get_tenant_id_list()):
      raise MyError('there should be {0} rows whose c2 column is 9494 in table test.for_test1, but there has {1} rows like that'.format(len(self.get_tenant_id_list()), len(results)))
'''

#升级语句对应的action要写在下面的actions begin和actions end这两行之间，
#因为基准版本更新的时候会调用reset_upgrade_scripts.py来清空actions begin和actions end
#这两行之间的这些action，如果不写在这两行之间的话会导致清空不掉相应的action。

####========******####======== actions begin ========####******========####
####========******####========= actions end =========####******========####
def get_actual_tenant_id(tenant_id):
  return tenant_id if (1 == tenant_id) else 0;

def do_each_tenant_dml_actions_by_standby_cluster(standby_cluster_infos):
  try:
    tenant_id_list = [1]
    for standby_cluster_info in standby_cluster_infos:
      logging.info("do_each_tenant_dml_actions_by_standby_cluster: cluster_id = {0}, ip = {1}, port = {2}"
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
      do_each_tenant_dml_actions(cur, tenant_id_list)

      cur.close()
      conn.close()
  except Exception, e:
    logging.exception("""do_each_tenant_dml_actions_by_standby_cluster failed""")
    raise e

def do_each_tenant_dml_actions(cur, tenant_id_list):
  import each_tenant_dml_actions_post
  cls_list = reflect_action_cls_list(each_tenant_dml_actions_post, 'EachTenantDMLActionPost')
  for cls in cls_list:
    logging.info('do each tenant dml acion, seq_num: %d', cls.get_seq_num())
    action = cls(cur, tenant_id_list)
    sys_tenant_id = 1
    action.change_tenant(sys_tenant_id)
    action.dump_before_do_action()
    if False == action.skip_pre_check():
      action.check_before_do_action()
    else:
      logging.info("skip pre check. seq_num: %d", cls.get_seq_num())
    for tenant_id in action.get_tenant_id_list():
      action.change_tenant(tenant_id)
      action.dump_before_do_each_tenant_action(tenant_id)
      if False == action.skip_each_tenant_action(tenant_id):
        action.check_before_do_each_tenant_action(tenant_id)
        action.do_each_tenant_action(tenant_id)
      else:
        logging.info("skip each tenant dml action, seq_num: %d, tenant_id: %d", cls.get_seq_num(), tenant_id)
      action.dump_after_do_each_tenant_action(tenant_id)
      action.check_after_do_each_tenant_action(tenant_id)
    action.change_tenant(sys_tenant_id)
    action.dump_after_do_action()
    action.check_after_do_action()

def get_each_tenant_dml_actions_sqls_str(tenant_id_list):
  import each_tenant_dml_actions_post
  ret_str = ''
  cls_list = reflect_action_cls_list(each_tenant_dml_actions_post, 'EachTenantDMLActionPost')
  for i in range(0, len(cls_list)):
    for j in range(0, len(tenant_id_list)):
      if i > 0 or j > 0:
        ret_str += '\n'
      ret_str += cls_list[i].get_each_tenant_action_dml(tenant_id_list[j]) + ';'
  return ret_str

