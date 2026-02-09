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

#ifndef OCEANBASE_SHARE_OB_BACKUP_VALIDATE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_BACKUP_VALIDATE_OPERATOR_H_
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace share
{
class ObBackupValidateJobAttr;
class ObBackupValidateStatus;
class ObBackupValidateStats;
class ObBackupValidateTaskAttr;
class ObBackupValidateLSTaskAttr;

class ObBackupValidateBaseTableOperator
{
protected:
  static uint64_t get_exec_tenant_id(uint64_t tenant_id);
};

class ObBackupValidateJobOperator : public ObBackupValidateBaseTableOperator
{
public:
  static int insert_job(common::ObISQLClient &proxy, const ObBackupValidateJobAttr &job_attr);
  static int get_jobs(common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObIArray<ObBackupValidateJobAttr> &job_attr);
  static int cnt_jobs(common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const uint64_t initiator_tenant_id,
    const int64_t initiator_job_id,
    int64_t &cnt);
  static int get_job(common::ObISQLClient &proxy,
    bool need_lock,
    const uint64_t tenant_id,
    const int64_t job_id,
    const bool is_initiator,
    ObBackupValidateJobAttr &job_attr);
  static int move_to_history(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id);
  static int advance_job_status(common::ObISQLClient &proxy, const ObBackupValidateJobAttr &job_attr,
    const ObBackupValidateStatus &next_status, const int result, const int64_t end_ts);
  static int update_comment(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t job_id,
      const char *comment);
  static int append_comment(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t job_id,
      const char *comment);
  static int update_task_count(
    common::ObISQLClient &proxy,
    const ObBackupValidateJobAttr &job_attr,
    const bool is_total);
  static int update_retry_count(
    common::ObISQLClient &proxy,
    const ObBackupValidateJobAttr &job_attr);
  static int report_failed_to_sys_tenant(common::ObISQLClient &proxy, const ObBackupValidateJobAttr &job_attr);
  static int cancel_jobs(common::ObISQLClient &proxy, const uint64_t tenant_id);
private:
  static int fill_dml_with_job_(const ObBackupValidateJobAttr &job_attr, ObDMLSqlSplicer &dml);
  static int fill_select_job_sql_(ObSqlString &sql);
  static int parse_job_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupValidateJobAttr> &jobs);
  static int do_parse_job_result_(sqlclient::ObMySQLResult &result, ObBackupValidateJobAttr &job);
};

class ObBackupValidateTaskOperator : public ObBackupValidateBaseTableOperator
{
public:
  static int insert_task(common::ObISQLClient &proxy, const ObBackupValidateTaskAttr &task_attr);
  static int get_backup_validate_task(common::ObISQLClient &proxy, const int64_t task_id,
    const uint64_t tenant_id, ObBackupValidateTaskAttr &task_attr);
  static int get_tasks(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id,
    ObIArray<ObBackupValidateTaskAttr> &task_attrs);
  static int advance_task_status(common::ObMySQLTransaction &trans, const ObBackupValidateTaskAttr &task_attr,
    const ObBackupValidateStatus &next_status, const int result, const int64_t end_ts);
  static int add_comment(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const char *comment);
  static int move_task_to_history(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id);
  static int update_ls_count_and_bytes(
      common::ObISQLClient &proxy,
      const ObBackupValidateTaskAttr &task_attr,
      const bool is_total);
  static int add_finish_ls_count(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t task_id);
  static int update_validated_bytes(
      common::ObMySQLTransaction &trans,
      const int64_t task_id,
      const uint64_t tenant_id,
      const int64_t validated_bytes);
private:
  static int update_finish_ls_count_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t finish_ls_count);
  static int update_task_status_row_(
      common::ObMySQLTransaction &trans,
      const ObBackupValidateTaskAttr &task_attr,
      const ObBackupValidateStatus &next_status,
      const int result,
      const int64_t end_ts,
      const char *comment);
  static int update_validated_bytes_(
      common::ObMySQLTransaction &trans,
      const int64_t task_id,
      const uint64_t tenant_id,
      const int64_t validated_bytes);
  static int update_task_comment_row_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t task_id,
      const char *comment);
  static int fill_dml_with_task_(const ObBackupValidateTaskAttr &task_attr, ObDMLSqlSplicer &dml);
  static int fill_select_task_sql_(ObSqlString &sql);
  static int parse_task_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupValidateTaskAttr> &tasks);
  static int do_parse_task_result_(sqlclient::ObMySQLResult &result, ObBackupValidateTaskAttr &task);
};

class ObBackupValidateLSTaskOperator : public ObBackupValidateBaseTableOperator
{
public:
  // Iterator for iterating through LS tasks one by one to avoid loading all tasks at once
  class LSTaskIterator final
  {
  public:
    LSTaskIterator() : sql_proxy_(nullptr), tenant_id_(OB_INVALID_TENANT_ID),
                       result_(nullptr), is_inited_(false) {}
    ~LSTaskIterator() { reset(); }
    int init(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, const int64_t task_id);
    int next(ObBackupValidateLSTaskAttr &ls_task);
    void reset();
    bool is_inited() const { return is_inited_; }
  private:
    common::ObISQLClient *sql_proxy_;
    uint64_t tenant_id_;
    common::ObMySQLProxy::ReadResult res_;
    common::sqlclient::ObMySQLResult *result_;
    bool is_inited_;
  };

public:
  static int insert_ls_task(common::ObISQLClient &proxy, const ObBackupValidateLSTaskAttr &ls_attr);
  static int get_ls_task(common::ObISQLClient &proxy, bool need_lock, const int64_t task_id,
    const uint64_t tenant_id, const ObLSID &ls_id, ObBackupValidateLSTaskAttr &ls_attr);
  static int get_ls_tasks_from_task_id(common::ObISQLClient &proxy, const int64_t task_id,
    const uint64_t tenant_id, bool need_lock, ObIArray<ObBackupValidateLSTaskAttr> &ls_attrs);
  static int advance_ls_task_status(common::ObISQLClient &proxy, const ObBackupValidateLSTaskAttr &ls_attr,
    const ObBackupTaskStatus &next_status, const int result, const int64_t end_ts);
  static int update_dst_and_status(
      common::ObISQLClient &proxy,
      const int64_t task_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      share::ObTaskId task_trace_id,
      common::ObAddr &dst);
  static int redo_ls_task(
      common::ObISQLClient &proxy,
      const ObBackupValidateLSTaskAttr &ls_attr,
      const int64_t retry_id);
  static int init_stats(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const int64_t total_object);
  static int update_stats(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, const int64_t validated_bytes, const int64_t finish_object_count);
  static int get_validated_stats(common::ObISQLClient &proxy, const int64_t task_id, const uint64_t tenant_id,
      const ObLSID &ls_id, int64_t &finish_object_count, int64_t &validated_bytes);
  static int move_ls_task_to_history(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t task_id);
private:
  static int fill_dml_with_ls_task_(const ObBackupValidateLSTaskAttr &ls_attr, ObDMLSqlSplicer &dml);
  static int fill_select_ls_task_sql_(ObSqlString &sql);
  static int parse_ls_task_result_(sqlclient::ObMySQLResult &result, ObIArray<ObBackupValidateLSTaskAttr> &ls_attrs);
  static int do_parse_ls_task_result_(sqlclient::ObMySQLResult &result, ObBackupValidateLSTaskAttr &ls_attr);

};

}//share
}//oceanbase
#endif //OCEANBASE_SHARE_OB_BACKUP_VALIDATE_OPERATOR_H_