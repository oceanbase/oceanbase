/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_BACKUP_CLEAN_OPERATOR_H_
#define OCEANBASE_SHARE_OB_BACKUP_CLEAN_OPERATOR_H_
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_clean_struct.h"

namespace oceanbase
{
namespace share
{

class ObBackupCleanJobAttr;
class ObBackupCleanStatus;
class ObBackupCleanStats;
class ObBackupCleanTaskAttr;
class ObBackupCleanLSTaskAttr;

class ObBackupCleanJobOperator
{
public:
  static int insert_job(
      common::ObISQLClient &proxy,
      const ObBackupCleanJobAttr &job_attr);
  static int get_jobs(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id, 
      bool need_lock, 
      ObIArray<ObBackupCleanJobAttr> &job_attrs);
  static int get_job(
      common::ObISQLClient &proxy, 
      bool need_lock, 
      const uint64_t tenant_id,
      const int64_t job_id,
      const bool is_initiator,
      ObBackupCleanJobAttr &job_attr);
  static int cnt_jobs(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const uint64_t initiator_tenant_id,
      const int64_t initiator_job_id, 
      int64_t &cnt);
  static int advance_job_status(
      common::ObISQLClient &proxy,
      const ObBackupCleanJobAttr &job_attr,
      const ObBackupCleanStatus &next_status,
      const int result = OB_SUCCESS,
      const int64_t end_ts = 0);
  static int update_task_count(
      common::ObISQLClient &proxy, 
      const ObBackupCleanJobAttr &job_attr,
      const bool is_total);
  static int move_job_to_his(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t job_id);
  static int check_same_tenant_and_clean_type_job_exist(
      common::ObISQLClient &proxy,
      const ObBackupCleanJobAttr &job_attr,
      bool &is_exist);
  static int report_failed_to_sys_tenant(
      common::ObISQLClient &proxy,
      const ObBackupCleanJobAttr &job_attr);
  static int update_retry_count(
      common::ObISQLClient &proxy, 
      const ObBackupCleanJobAttr &job_attr);
private:
  static int fill_dml_with_job_(const ObBackupCleanJobAttr &job_attr, ObDMLSqlSplicer &dml);
  static int fill_select_job_sql_(ObSqlString &sql);
  static int parse_job_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupCleanJobAttr> &jobs);
  static int do_parse_job_result_(sqlclient::ObMySQLResult &result, ObBackupCleanJobAttr &job);
  static int parse_int_(const char *str, int64_t &val);
};

class ObBackupCleanTaskOperator
{
public:
  static int insert_backup_clean_task(
      common::ObISQLClient &proxy,
      const ObBackupCleanTaskAttr &task_attr);
  static int get_backup_clean_tasks(
      common::ObISQLClient &proxy, 
      const int64_t job_id, 
      const uint64_t tenant_id,
      bool need_lock,  
      ObArray<ObBackupCleanTaskAttr> &task_attrs);
  static int get_backup_clean_task(
      common::ObISQLClient &proxy, 
      const int64_t task_id, 
      const uint64_t tenant_id,
      bool need_lock,  
      ObBackupCleanTaskAttr &set_task);
  static int check_backup_clean_task_exist(
      common::ObISQLClient &proxy, 
      const uint64_t tenant_id,
      const ObBackupCleanTaskType::TYPE task_type,
      const int64_t id,
      const int64_t dest_id,
      bool need_lock,  
      bool &is_exist);
  static int advance_task_status(
      common::ObISQLClient &proxy, 
      const ObBackupCleanTaskAttr &task_attr,
      const ObBackupCleanStatus &next_status, 
      const int result = OB_SUCCESS,
      const int64_t end_ts = 0);
  static int move_task_to_history(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id);
  static int update_stats(
      common::ObISQLClient &proxy,
      const int64_t task_id, 
      const uint64_t tenant_id, 
      const ObBackupCleanStats &stats);
  static int check_current_task_exist(common::ObISQLClient &proxy, const uint64_t tenant_id, bool &is_exist);
  static int update_ls_count(
      common::ObISQLClient &proxy, 
      const ObBackupCleanTaskAttr &task_attr,
      const bool is_total);
private:
  static int fill_dml_with_backup_clean_task_(
      const ObBackupCleanTaskAttr &task_attr,
      ObDMLSqlSplicer &dml);
  static int parse_task_result_(
      sqlclient::ObMySQLResult &result, 
      common::ObIArray<ObBackupCleanTaskAttr> &task_attrs);
  static int do_parse_task_result_(
      sqlclient::ObMySQLResult &result, 
      ObBackupCleanTaskAttr &task_attr);
};

class ObBackupCleanLSTaskOperator
{
public:
  static int insert_ls_task(common::ObISQLClient &proxy, const ObBackupCleanLSTaskAttr &ls_attr);
  static int advance_ls_task_status(
      common::ObISQLClient &proxy, 
      const ObBackupCleanLSTaskAttr &ls_attr,
      const ObBackupTaskStatus &next_status, 
      const int result, 
      const int64_t end_ts);
  static int redo_ls_task(common::ObISQLClient &proxy, const ObBackupCleanLSTaskAttr &ls_attr, const int64_t retry_id);
  static int get_ls_task(
      common::ObISQLClient &proxy,
      bool need_lock, 
      const int64_t task_id,
      const uint64_t tenant_id, 
      const ObLSID &ls_id, 
      ObBackupCleanLSTaskAttr &ls_attr); 
  static int get_ls_tasks_from_task_id(
      common::ObISQLClient &proxy, 
      const int64_t task_id, 
      const uint64_t tenant_id,
      bool need_lock, 
      ObIArray<ObBackupCleanLSTaskAttr> &ls_attrs);
  static int update_dst_and_status(
      common::ObISQLClient &proxy, 
      const int64_t task_id,
      const uint64_t tenant_id, 
      const ObLSID &ls_id, 
      share::ObTaskId task_trace_id, 
      common::ObAddr &dst);
  static int update_result(common::ObISQLClient &proxy, const ObBackupCleanLSTaskAttr &ls_attr, const int result);
  static int move_ls_to_his(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id);
  static int update_stats_(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const ObBackupCleanStats &stats);
private:
  static int fill_dml_with_ls_task_(const ObBackupCleanLSTaskAttr &ls_attr, ObDMLSqlSplicer &dml);
  static int parse_ls_result_(sqlclient::ObMySQLResult &result, ObIArray<ObBackupCleanLSTaskAttr> &ls_attrs);
  static int do_parse_ls_result_(sqlclient::ObMySQLResult &result, ObBackupCleanLSTaskAttr &ls_attr);
};


class ObDeletePolicyOperator
{
public:
  static int insert_delete_policy(common::ObISQLClient &proxy, const ObDeletePolicyAttr &delete_policy);
  static int drop_delete_policy(common::ObISQLClient &proxy, const ObDeletePolicyAttr &delete_policy);
  static int get_default_delete_policy(common::ObISQLClient &proxy, const uint64_t tenant_id, ObDeletePolicyAttr &delete_policy);
private:
  static int fill_dml_with_delete_policy_(const ObDeletePolicyAttr &delete_policy, ObDMLSqlSplicer &dml);
};

}
}

#endif // OCEANBASE_SHARE_OB_BACKUP_CLEAN_OPERATOR_H_