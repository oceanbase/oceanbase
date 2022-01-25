#!/usr/bin/env python
# -*- coding: utf-8 -*-

from my_error import MyError
import sys
import mysql.connector
from mysql.connector import errorcode
import logging

import config
import opts
import run_modules
import actions
import normal_ddl_actions_pre
import normal_dml_actions_pre
import each_tenant_dml_actions_pre
import upgrade_sys_vars
import special_upgrade_action_pre

# 由于用了/*+read_consistency(WEAK) */来查询，因此升级期间不能允许创建或删除租户

class UpgradeParams:
  log_filename = config.pre_upgrade_log_filename
  sql_dump_filename = config.pre_upgrade_sql_filename
  rollback_sql_filename = config.pre_upgrade_rollback_sql_filename

def config_logging_module(log_filenamme):
  logging.basicConfig(level=logging.INFO,\
      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
      datefmt='%Y-%m-%d %H:%M:%S',\
      filename=log_filenamme,\
      filemode='w')
  # 定义日志打印格式
  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
  #######################################
  # 定义一个Handler打印INFO及以上级别的日志到sys.stdout
  stdout_handler = logging.StreamHandler(sys.stdout)
  stdout_handler.setLevel(logging.INFO)
  # 设置日志打印格式
  stdout_handler.setFormatter(formatter)
  # 将定义好的stdout_handler日志handler添加到root logger
  logging.getLogger('').addHandler(stdout_handler)

def dump_sql_to_file(cur, query_cur, dump_filename, tenant_id_list, update_sys_var_list, add_sys_var_list):
  normal_ddls_str = normal_ddl_actions_pre.get_normal_ddl_actions_sqls_str(query_cur)
  normal_dmls_str = normal_dml_actions_pre.get_normal_dml_actions_sqls_str()
  each_tenant_dmls_str = each_tenant_dml_actions_pre.get_each_tenant_dml_actions_sqls_str(tenant_id_list)
  sys_vars_upgrade_dmls_str = upgrade_sys_vars.get_sys_vars_upgrade_dmls_str(cur, query_cur, tenant_id_list, update_sys_var_list, add_sys_var_list)
  dump_file = open(dump_filename, 'w')
  dump_file.write('# 以下是upgrade_pre.py脚本中的步骤\n')
  dump_file.write('# 仅供upgrade_pre.py脚本运行失败需要人肉的时候参考\n')
  dump_file.write('\n\n')
  dump_file.write('# normal ddl\n')
  dump_file.write(normal_ddls_str + '\n')
  dump_file.write('\n\n')
  dump_file.write('# normal dml\n')
  dump_file.write(normal_dmls_str + '\n')
  dump_file.write('\n\n')
  dump_file.write('# each tenant dml\n')
  dump_file.write(each_tenant_dmls_str + '\n')
  dump_file.write('\n\n')
  dump_file.write('# upgrade sys vars\n')
  dump_file.write(sys_vars_upgrade_dmls_str + '\n')
  dump_file.write('\n\n')
  dump_file.write('# do special upgrade actions\n')
  dump_file.write('# please run ./upgrade_pre.py -h [host] -P [port] -u [user] -p [password] -m special_action\n')
  dump_file.write('\n\n')
  dump_file.close()

def check_before_upgrade(query_cur, upgrade_params):
  return

def print_stats():
  logging.info('==================================================================================')
  logging.info('============================== STATISTICS BEGIN ==================================')
  logging.info('==================================================================================')
  logging.info('succeed run sql(except sql of special actions): \n\n%s\n', actions.get_succ_sql_list_str())
  logging.info('commited sql(except sql of special actions): \n\n%s\n', actions.get_commit_sql_list_str())
  logging.info('==================================================================================')
  logging.info('=============================== STATISTICS END ===================================')
  logging.info('==================================================================================')

def do_upgrade(my_host, my_port, my_user, my_passwd, my_module_set, upgrade_params):
  try:
    conn = mysql.connector.connect(user = my_user,
                                   password = my_passwd,
                                   host = my_host,
                                   port = my_port,
                                   database = 'oceanbase',
                                   raise_on_warnings = True)
    cur = conn.cursor(buffered=True)
    try:
      query_cur = actions.QueryCursor(cur)
      # 开始升级前的检查
      check_before_upgrade(query_cur, upgrade_params)
      # get min_observer_version
      version = actions.fetch_observer_version(query_cur)
      need_check_standby_cluster = cmp(version, '2.2.40') >= 0
      # 获取租户id列表
      tenant_id_list = actions.fetch_tenant_ids(query_cur)
      if len(tenant_id_list) <= 0:
        logging.error('distinct tenant id count is <= 0, tenant_id_count: %d', len(tenant_id_list))
        raise MyError('no tenant id')
      logging.info('there has %s distinct tenant ids: [%s]', len(tenant_id_list), ','.join(str(tenant_id) for tenant_id in tenant_id_list))
      # 计算需要添加或更新的系统变量
      conn.commit()

      conn.autocommit = True
      (update_sys_var_list, update_sys_var_ori_list, add_sys_var_list) = upgrade_sys_vars.calc_diff_sys_var(cur, tenant_id_list[0])
      dump_sql_to_file(cur, query_cur, upgrade_params.sql_dump_filename, tenant_id_list, update_sys_var_list, add_sys_var_list)
      conn.autocommit = False
      conn.commit()
      logging.info('update system variables list: [%s]', ', '.join(str(sv) for sv in update_sys_var_list))
      logging.info('update system variables original list: [%s]', ', '.join(str(sv) for sv in update_sys_var_ori_list))
      logging.info('add system variables list: [%s]', ', '.join(str(sv) for sv in add_sys_var_list))
      logging.info('================succeed to dump sql to file: {0}==============='.format(upgrade_params.sql_dump_filename))

      if run_modules.MODULE_DDL in my_module_set:
        logging.info('================begin to run ddl===============')
        conn.autocommit = True
        normal_ddl_actions_pre.do_normal_ddl_actions(cur)
        logging.info('================succeed to run ddl===============')
        conn.autocommit = False

      if run_modules.MODULE_NORMAL_DML in my_module_set:
        logging.info('================begin to run normal dml===============')
        normal_dml_actions_pre.do_normal_dml_actions(cur)
        logging.info('================succeed to run normal dml===============')
        conn.commit()
        actions.refresh_commit_sql_list()
        logging.info('================succeed to commit dml===============')

      if run_modules.MODULE_EACH_TENANT_DML in my_module_set:
        logging.info('================begin to run each tenant dml===============')
        conn.autocommit = True
        each_tenant_dml_actions_pre.do_each_tenant_dml_actions(cur, tenant_id_list)
        conn.autocommit = False
        logging.info('================succeed to run each tenant dml===============')

      # 更新系统变量
      if run_modules.MODULE_SYSTEM_VARIABLE_DML in my_module_set:
        logging.info('================begin to run system variable dml===============')
        conn.autocommit = True
        upgrade_sys_vars.exec_sys_vars_upgrade_dml(cur, tenant_id_list)
        conn.autocommit = False
        logging.info('================succeed to run system variable dml===============')

      if run_modules.MODULE_SPECIAL_ACTION in my_module_set:
        logging.info('================begin to run special action===============')
        conn.autocommit = True
        special_upgrade_action_pre.do_special_upgrade(conn, cur, tenant_id_list, my_user, my_passwd)
        conn.autocommit = False
        actions.refresh_commit_sql_list()
        logging.info('================succeed to commit special action===============')
    except Exception, e:
      logging.exception('run error')
      raise e
    finally:
      # 打印统计信息
      print_stats()
      # 将回滚sql写到文件中
      actions.dump_rollback_sql_to_file(upgrade_params.rollback_sql_filename)
      cur.close()
      conn.close()
  except mysql.connector.Error, e:
    logging.exception('connection error')
    raise e
  except Exception, e:
    logging.exception('normal error')
    raise e

def do_upgrade_by_argv(argv):
  upgrade_params = UpgradeParams()
  opts.change_opt_defult_value('log-file', upgrade_params.log_filename)
  opts.parse_options(argv)
  if not opts.has_no_local_opts():
    opts.deal_with_local_opts()
  else:
    opts.check_db_client_opts()
    log_filename = opts.get_opt_log_file()
    upgrade_params.log_filename = log_filename
    # 日志配置放在这里是为了前面的操作不要覆盖掉日志文件
    config_logging_module(upgrade_params.log_filename)
    try:
      host = opts.get_opt_host()
      port = int(opts.get_opt_port())
      user = opts.get_opt_user()
      password = opts.get_opt_password()
      cmd_module_str = opts.get_opt_module()
      module_set = set([])
      all_module_set = run_modules.get_all_module_set()
      cmd_module_list = cmd_module_str.split(',')
      for cmd_module in cmd_module_list:
        if run_modules.ALL_MODULE == cmd_module:
          module_set = module_set | all_module_set
        elif cmd_module in all_module_set:
          module_set.add(cmd_module)
        else:
          raise MyError('invalid module: {0}'.format(cmd_module))
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", module=\"%s\", log-file=\"%s\"',\
          host, port, user, password, module_set, log_filename)
      do_upgrade(host, port, user, password, module_set, upgrade_params)
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error')
      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
      raise e
    except Exception, e:
      logging.exception('normal error')
      logging.exception('run error, maybe you can reference ' + upgrade_params.rollback_sql_filename + ' to rollback it')
      raise e



