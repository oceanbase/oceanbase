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

#define USING_LOG_PREFIX SHARE

#include "ob_balance_task_table_operator.h"
#include "lib/mysqlclient/ob_isql_client.h"//ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_proxy.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "share/inner_table/ob_inner_table_schema.h"//ALL_BALANCE_TASK_TNAME
#include "share/ob_dml_sql_splicer.h"//ObDMLSqlSplicer
#include "share/balance/ob_balance_job_table_operator.h"//job_status
#include "share/balance/ob_balance_task_helper_operator.h"//ObBalanceTaskHelper
#include "storage/tx/ob_multi_data_source.h"
#include "observer/ob_inner_sql_connection.h"//register

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
static const char* BALANCE_TASK_STATUS_ARRAY[] =
{
  "INIT", "CREATE_LS", "ALTER_LS", "SET_LS_MERGING", "TRANSFER", "DROP_LS", "COMPLETED", "CANCELED",
};
static const char *BALANCE_TASK_TYPE[] =
{
  "LS_SPLIT", "LS_ALTER", "LS_MERGE", "LS_TRANSFER",
};

const char* ObBalanceTaskStatus::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(BALANCE_TASK_STATUS_ARRAY) == BALANCE_TASK_STATUS_MAX, "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(BALANCE_TASK_STATUS_ARRAY) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown balance task status", K_(val));
  } else {
    type_str = BALANCE_TASK_STATUS_ARRAY[val_];
  }
  return type_str;
}

ObBalanceTaskStatus::ObBalanceTaskStatus(const ObString &str)
{
  val_ = BALANCE_TASK_STATUS_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(BALANCE_TASK_STATUS_ARRAY); ++i) {
      if (0 == str.case_compare(BALANCE_TASK_STATUS_ARRAY[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (BALANCE_TASK_STATUS_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid balance task status", K(val_), K(str));
  }
}

const char* ObBalanceTaskType::to_str() const
{
  STATIC_ASSERT(
      ARRAYSIZEOF(BALANCE_TASK_TYPE) == BALANCE_TASK_MAX,
      "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(BALANCE_TASK_TYPE) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown balance task status", K_(val));
  } else {
    type_str = BALANCE_TASK_TYPE[val_];
  }
  return type_str;
}

ObBalanceTaskType::ObBalanceTaskType(const ObString &str)
{
  val_ = BALANCE_TASK_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(BALANCE_TASK_TYPE); ++i) {
      if (0 == str.case_compare(BALANCE_TASK_TYPE[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (BALANCE_TASK_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid balance task status", K(val_), K(str));
  }
}

ObBalanceTaskStatus ObBalanceTask::get_next_status(const ObBalanceJobStatus &job_status) const
{
  ObBalanceTaskStatus task_status;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "task is invalid", "task", *this);
  } else if (task_status_.is_completed() || task_status_.is_canceled()) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "task alredy end, no next status", "task", *this);
  } else if (job_status.is_canceling()) {
    task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_CANCELED);  // failed
  } else if (task_type_.is_split_task()) {
    if (task_status_.is_init()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_CREATE_LS);
    } else if (task_status_.is_create_ls()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_TRANSFER);
    } else if (task_status_.is_transfer()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_COMPLETED);
    } else {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "split task has no other status", "task", *this);
    }
  } else if (task_type_.is_alter_task()) {
    if (task_status_.is_init()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_ALTER_LS);
    } else if (task_status_.is_alter_ls()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_COMPLETED);
    } else {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "alter task has no other status", "task", *this);
    }
  } else if (task_type_.is_merge_task()) {
    if (task_status_.is_init()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_SET_LS_MERGING);
    } else if (task_status_.is_set_merge_ls()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_TRANSFER);
    } else if (task_status_.is_transfer()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_DROP_LS);
    } else if (task_status_.is_drop_ls()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_COMPLETED);
    } else {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "merge task has no other status", "task", *this);
    }
  } else if (task_type_.is_transfer_task()) {
    if (task_status_.is_init()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_TRANSFER);
    } else if (task_status_.is_transfer()) {
      task_status = ObBalanceTaskStatus(ObBalanceTaskStatus::BALANCE_TASK_STATUS_COMPLETED);
    } else {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "transfer task has no other status", "task", *this);
    }
  } else {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "task type is invalid", "task", *this);
  }
  return task_status;
}

int ObBalanceTask::init_(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObString &comment)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! job_id.is_valid()
                  || ! balance_task_id.is_valid()
                  || !task_type.is_valid() || !task_status.is_valid()
                  || !src_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id), K(balance_task_id),
    K(task_type), K(task_status), K(src_ls_id));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to assign comment", KR(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    balance_task_id_ = balance_task_id;
    src_ls_id_ = src_ls_id;
    dest_ls_id_ = dest_ls_id;
    current_transfer_task_id_ = curr_transfer_task_id;
    task_type_ = task_type;
    task_status_ = task_status;
    ls_group_id_ = ls_group_id;
  }
  return ret;
}

int ObBalanceTask::simple_init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObBalanceTaskStatus task_status(0);
  ObTransferTaskID current_transfer_task_id;
  ObString comment;
  if (OB_FAIL(init_(tenant_id, job_id, balance_task_id, task_type, task_status, ls_group_id,
                    src_ls_id, dest_ls_id, current_transfer_task_id, comment))) {
    LOG_WARN("failed to init", KR(ret), K(tenant_id), K(job_id), K(balance_task_id),
    K(task_type), K(task_status), K(src_ls_id), K(current_transfer_task_id));
  } else if (OB_FAIL(part_list_.assign(part_list))) {
    LOG_WARN("failed to assign commet", KR(ret), K(part_list));
  }
  return ret;
}

int ObBalanceTask::init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObString &part_list_str,
           const ObString &finished_part_list_str,
           const ObString &parent_list_str,
           const ObString &child_list_str,
           const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_(tenant_id, job_id, balance_task_id, task_type, task_status, ls_group_id,
                    src_ls_id, dest_ls_id, curr_transfer_task_id, comment))) {
    LOG_WARN("failed to init", KR(ret), K(tenant_id), K(job_id), K(balance_task_id),
    K(task_type), K(task_status), K(src_ls_id), K(curr_transfer_task_id), K(comment));
  } else if (!parent_list_str.empty() && OB_FAIL(parent_list_.parse_from_display_str(parent_list_str))) {
    LOG_WARN("failed to parse str into list", KR(ret), K(parent_list_str));
  } else if (!child_list_str.empty() && OB_FAIL(child_list_.parse_from_display_str(child_list_str))) {
    LOG_WARN("failed to parse str into list", KR(ret), K(child_list_str));
  } else if (!part_list_str.empty() && OB_FAIL(part_list_.parse_from_display_str(part_list_str))) {
    LOG_WARN("failed to parse str into list", KR(ret), K(part_list_str));
  } else if (!finished_part_list_str.empty() && OB_FAIL(finished_part_list_.parse_from_display_str(finished_part_list_str))) {
    LOG_WARN("failed to parse str into list", KR(ret), K(finished_part_list_str));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("balance task is invalid", KR(ret), "this", *this);
  }
  return ret;
}


int ObBalanceTask::init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceTaskID balance_task_id,
           const ObBalanceTaskType task_type,
           const ObBalanceTaskStatus task_status,
           const uint64_t ls_group_id,
           const ObLSID &src_ls_id,
           const ObLSID &dest_ls_id,
           const ObTransferTaskID curr_transfer_task_id,
           const ObTransferPartList &part_list,
           const ObTransferPartList &finished_part_list,
           const ObBalanceTaskIDList &parent_list,
           const ObBalanceTaskIDList &child_list,
           const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_(tenant_id, job_id, balance_task_id, task_type, task_status, ls_group_id,
                    src_ls_id, dest_ls_id, curr_transfer_task_id, comment))) {
    LOG_WARN("failed to init", KR(ret), K(tenant_id), K(job_id), K(balance_task_id),
    K(task_type), K(task_status), K(src_ls_id), K(curr_transfer_task_id), K(comment));
  } else if (OB_FAIL(part_list_.assign(part_list))) {
    LOG_WARN("failed to assign commet", KR(ret), K(part_list));
  } else if (OB_FAIL(finished_part_list_.assign(finished_part_list))) {
    LOG_WARN("failed to assign finish part list", KR(ret), K(finished_part_list));
  } else if (OB_FAIL(parent_list_.assign(parent_list))) {
    LOG_WARN("failed to assign parent list", KR(ret), K(parent_list));
  } else if (OB_FAIL(child_list_.assign(child_list))) {
    LOG_WARN("failed to assign child list", KR(ret), K(child_list));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("balance task is invalid", KR(ret), "this", *this);
  }
  return ret;
}

bool ObBalanceTask::is_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || !job_id_.is_valid() || !balance_task_id_.is_valid()
      || !task_type_.is_valid() || !task_status_.is_valid()
      || !src_ls_id_.is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("balance task is invalid", "this", *this);
  } else if ( 0 < part_list_.count()) {
    if (task_type_.is_alter_task()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("balance task is invalid", "this", *this);
    } else if (task_type_.is_merge_task()) {
      if (!task_status_.is_transfer() && !task_status_.is_canceled()) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("balance task is invalid", "this", *this);
      }
    }
  }
  return OB_SUCC(ret);
}

void ObBalanceTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  src_ls_id_.reset();
  dest_ls_id_.reset();
  job_id_.reset();
  balance_task_id_.reset();
  task_type_.reset();
  task_status_.reset();
  ls_group_id_ = OB_INVALID_ID;
  current_transfer_task_id_.reset();
  part_list_.reset();
  finished_part_list_.reset();
  parent_list_.reset();
  child_list_.reset();
  comment_.reset();
}

int ObBalanceTask::assign(const ObBalanceTask &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    reset();
    if (OB_FAIL(part_list_.assign(other.part_list_))) {
      LOG_WARN("failed to assign part list", KR(ret), K(other));
    } else if (OB_FAIL(finished_part_list_.assign(other.finished_part_list_))) {
      LOG_WARN("failed to assign finished part list", KR(ret), K(other));
    } else if (OB_FAIL(parent_list_.assign(other.parent_list_))) {
      LOG_WARN("failed to assign parent list", KR(ret), K(other));
    } else if (OB_FAIL(child_list_.assign(other.child_list_))) {
      LOG_WARN("failed to assign child list", KR(ret), K(other));
    } else if (other.comment_.is_valid() && OB_FAIL(comment_.assign(other.comment_))) {
      LOG_WARN("failed to assign comment", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      src_ls_id_ = other.src_ls_id_;
      dest_ls_id_ = other.dest_ls_id_;
      job_id_ = other.job_id_;
      balance_task_id_ = other.balance_task_id_;
      task_type_ = other.task_type_;
      task_status_ = other.task_status_;
      ls_group_id_ = other.ls_group_id_;
      current_transfer_task_id_ = other.current_transfer_task_id_;
    }
  }
  return ret;
}



int ObBalanceTaskTableOperator::insert_new_task(const ObBalanceTask &task,
                     ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(client, task.get_tenant_id());
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (OB_FAIL(fill_dml_spliter(dml, task))) {
    LOG_WARN("failed to assign sql", KR(ret), K(task));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_TASK_TNAME,
                                             dml, affected_rows))) {
    LOG_WARN("execute update failed", KR(ret), K(task));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObBalanceTaskTableOperator::get_balance_task(const uint64_t tenant_id,
                      const ObBalanceTaskID balance_task_id,
                      const bool for_update,
                      ObISQLClient &client,
                      ObBalanceTask &task,
                      int64_t &start_time,
                      int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  task.reset();
  finish_time = OB_INVALID_TIMESTAMP;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(balance_task_id));
  } else if (OB_FAIL(sql.assign_fmt("select time_to_usec(gmt_create) as start_time, "
          " time_to_usec(gmt_modified) as finish_time, * from %s where task_id = %ld",
                                    OB_ALL_BALANCE_TASK_TNAME, balance_task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(balance_task_id), K(sql));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("balance task not eixst", KR(ret), K(sql));
        } else {
          LOG_WARN("failed to get balance task", KR(ret), K(sql));
        }
      } else if (OB_FAIL(fill_cell(tenant_id, result, task))) {
        LOG_WARN("failed to fill cell", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "start_time", start_time, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_time", finish_time, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get finish time", KR(ret), K(start_time), K(finish_time), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::update_task_status(
                               const ObBalanceTask &balance_task,
                               const ObBalanceTaskStatus new_task_status,
                               ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  bool clear_comment = new_task_status.is_canceled() ? false : true;
  const ObBalanceTaskStatus old_task_status = balance_task.get_task_status();
  const uint64_t tenant_id = balance_task.get_tenant_id();
  const ObBalanceTaskID balance_task_id = balance_task.get_balance_task_id();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ! balance_task_id.is_valid()
                  || !old_task_status.is_valid() || !new_task_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_task_status), K(new_task_status), K(balance_task_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set status = '%s'",
                     OB_ALL_BALANCE_TASK_TNAME,
                     new_task_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(new_task_status), K(old_task_status));
  } else if (clear_comment && OB_FAIL(sql.append(", comment = ''"))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql));
  } else if (OB_FAIL(sql.append_fmt(" where status = '%s' and task_id = %ld",
          old_task_status.to_str(), balance_task_id.id()))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql), K(old_task_status), K(balance_task_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may status change", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  } else if (new_task_status.is_transfer() || balance_task.get_task_status().is_transfer()) {
    //sub trans
    ObBalanceTaskHelperMeta ls_balance_task;
    ObBalanceTaskHelperOp task_op;
    if (new_task_status.is_transfer()) {
      task_op = ObBalanceTaskHelperOp(ObBalanceTaskHelperOp::LS_BALANCE_TASK_OP_TRANSFER_BEGIN);
    } else {
      task_op = ObBalanceTaskHelperOp(ObBalanceTaskHelperOp::LS_BALANCE_TASK_OP_TRANSFER_END);
    }

    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    const int64_t length = ls_balance_task.get_serialize_size();
    char *buf = NULL;
    int64_t pos = 0;
    const transaction::ObTxDataSourceType type = transaction::ObTxDataSourceType::TRANSFER_TASK;
    ObArenaAllocator allocator("BalanceTask");
    if (OB_ISNULL(conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection or trans service is null", KR(ret), KP(conn), K(tenant_id));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret), K(length));
    } else if (OB_FAIL(ls_balance_task.init(task_op,
            balance_task.get_src_ls_id(), balance_task.get_dest_ls_id(),
            balance_task.get_ls_group_id()))) {
      LOG_WARN("failed to init balance task helper", KR(ret), K(tenant_id), K(balance_task));
    } else if (OB_FAIL(ls_balance_task.serialize(buf, length, pos))) {
      LOG_WARN("failed to serialize balance task helper", KR(ret), K(ls_balance_task),
          K(length), K(pos));
    } else if (OB_UNLIKELY(pos > length)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialize error", KR(ret), K(pos), K(length));
    } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, SYS_LS, type, buf, pos))) {
      LOG_WARN("failed to register multi data source", KR(ret), K(tenant_id), K(type), K(buf));
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::update_task_comment(
    const uint64_t tenant_id, const ObBalanceTaskID task_id,
    const common::ObString &new_comment,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(new_comment), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set comment = '%.*s' where "
                                    "task_id = %ld ",
                                    OB_ALL_BALANCE_TASK_TNAME,
                                    new_comment.length(), new_comment.ptr(),
                                    task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(new_comment), K(task_id));
  } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may status change", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  }
  return ret;
}

int ObBalanceTaskTableOperator::fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("task_id", task.get_balance_task_id().id()))
      || OB_FAIL(dml.add_column("job_id", task.get_job_id().id()))
      || OB_FAIL(dml.add_column("task_type", task.get_task_type().to_str()))
      || OB_FAIL(dml.add_column("status", task.get_task_status().to_str()))
      || OB_FAIL(dml.add_column("src_ls", task.get_src_ls_id().id()))
      || OB_FAIL(dml.add_column("dest_ls", task.get_dest_ls_id().id()))
      || OB_FAIL(dml.add_column("ls_group_id", task.get_ls_group_id()))
      || OB_FAIL(dml.add_column("current_transfer_task_id", task.get_current_transfer_task_id().id()))
      || OB_FAIL(dml.add_column("comment", task.get_comment()))) {
    LOG_WARN("failed to fill dml spliter", KR(ret), K(task));
  } else {
    common::ObArenaAllocator allocator;
    ObString part_list_str;
    ObString finished_part_list_str;
    ObString parent_list_str;
    ObString child_list_str;
    if (OB_FAIL(task.get_part_list().to_display_str(allocator, part_list_str))) {
      LOG_WARN("failed to get part list str", KR(ret), K(task));
    } else if (OB_FAIL(task.get_finished_part_list().to_display_str(allocator, finished_part_list_str))) {
      LOG_WARN("failed to get part list str", KR(ret), K(task));
    } else if (OB_FAIL(task.get_parent_task_list().to_display_str(allocator, parent_list_str))) {
      LOG_WARN("failed to get parent list", KR(ret), K(task));
    } else if (OB_FAIL(task.get_child_task_list().to_display_str(allocator, child_list_str))) {
      LOG_WARN("failed to get child list str", KR(ret), K(task));
    } else if (OB_FAIL(dml.add_column("part_list", part_list_str))
               || OB_FAIL(dml.add_column("finished_part_list", finished_part_list_str))
               || OB_FAIL(dml.add_column("part_count", task.get_part_list().count()))
               || OB_FAIL(dml.add_column("finished_part_count", task.get_finished_part_list().count()))
               || OB_FAIL(dml.add_column("parent_list", parent_list_str))
               || OB_FAIL(dml.add_column("child_list", child_list_str))) {
      LOG_WARN("failed to add column", KR(ret), K(part_list_str), K(finished_part_list_str),
                                      K(parent_list_str), K(child_list_str));
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::start_transfer_task(const uint64_t tenant_id,
                                 const ObBalanceTaskID balance_task_id,
                                 const ObTransferTaskID transfer_task_id,
                                 ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! balance_task_id.is_valid()
                  || ! transfer_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(balance_task_id), K(transfer_task_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set current_transfer_task_id = %ld where "
                                "task_id = %ld and current_transfer_task_id = %ld",
                                OB_ALL_BALANCE_TASK_TNAME, transfer_task_id.id(),
                                balance_task_id.id(), ObTransferTaskID::INVALID_ID))) {
    LOG_WARN("failed to assign sql", KR(ret), K(transfer_task_id), K(balance_task_id));
  } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may status change", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  }

  return ret;
}

ERRSIM_POINT_DEF(EN_FINISH_TRANSFER_TASK_COST_TOO_MUCH_TIME);

int ObBalanceTaskTableOperator::finish_transfer_task(
    const ObBalanceTask &balance_task,
    const ObTransferTaskID transfer_task_id,
    const ObTransferPartList &transfer_finished_part_list,
    ObISQLClient &client,
    ObTransferPartList &to_do_part_list,
    bool &all_part_transferred)
{
  int ret = OB_SUCCESS;
  all_part_transferred = false;
  to_do_part_list.reset();
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObArenaAllocator allocator;
  ObTransferPartList new_finished_part_list; // balance finished + transfer finished
  ObString to_do_part_list_str;
  ObString new_finished_part_list_str;
  const int64_t invalid_transfer_task_id = ObTransferTaskID::INVALID_ID;

  if (OB_UNLIKELY(!balance_task.is_valid()
                  || !transfer_task_id.is_valid()
                  || balance_task.get_part_list().empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(balance_task), K(transfer_task_id));
  } else if (OB_FAIL(common::append(new_finished_part_list, balance_task.get_finished_part_list()))) {
    LOG_WARN("assign failed", KR(ret), K(balance_task), K(new_finished_part_list));
  } else if (OB_FAIL(common::append(new_finished_part_list, transfer_finished_part_list))) {
    LOG_WARN("append failed", KR(ret), K(transfer_finished_part_list), K(new_finished_part_list));
  } else if (OB_FAIL(new_finished_part_list.to_display_str(allocator, new_finished_part_list_str))) {
    LOG_WARN("failed to transfer list to str", KR(ret), K(new_finished_part_list));
  } else if (OB_FAIL(common::get_difference(
      balance_task.get_part_list(),
      new_finished_part_list,
      to_do_part_list))) {
    LOG_WARN("get difference failed", KR(ret), K(balance_task), K(transfer_finished_part_list));
  } else if (OB_FAIL(to_do_part_list.to_display_str(allocator, to_do_part_list_str))) {
    LOG_WARN("failed to transfer list to str", KR(ret), K(to_do_part_list));
  } else if (OB_FAIL(sql.assign_fmt("update %s set current_transfer_task_id = %ld, "
      "finished_part_list = '%.*s', part_list = '%.*s', part_count = %ld, finished_part_count = %ld "
      "where task_id = %ld and current_transfer_task_id = %ld",
      OB_ALL_BALANCE_TASK_TNAME,
      invalid_transfer_task_id,
      new_finished_part_list_str.length(),
      new_finished_part_list_str.ptr(),
      to_do_part_list_str.length(),
      to_do_part_list_str.ptr(),
      to_do_part_list.count(),
      new_finished_part_list.count(),
      balance_task.get_balance_task_id().id(),
      transfer_task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(transfer_task_id),
        K(balance_task), K(new_finished_part_list_str), K(to_do_part_list_str));
  } else if (OB_FAIL(client.write(balance_task.get_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(balance_task), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may status change", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  }

  if (OB_SUCC(ret) && to_do_part_list.empty()) {
    all_part_transferred = true;
  }

  // only for test
  if (EN_FINISH_TRANSFER_TASK_COST_TOO_MUCH_TIME) {
    ob_usleep(31 * 1000 * 1000L); // default internal_sql_execute_timeout + 1s
  }

  return ret;
}
//maybe in trans TODO
//remove task from __all_balance_task to __all_balance_task_history
int ObBalanceTaskTableOperator::clean_task(const uint64_t tenant_id,
                                        const ObBalanceTaskID balance_task_id,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObBalanceTask task;
  int64_t affected_rows = 0;
  ObSqlString sql;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  int64_t start_time = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ! balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K(balance_task_id));
  } else if (OB_FAIL(get_balance_task(tenant_id, balance_task_id, true, trans, task, start_time, finish_time))) {
    LOG_WARN("failed to get task", KR(ret), K(tenant_id), K(balance_task_id));
  } else if (OB_UNLIKELY(! task.get_task_status().is_finish_status())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("clean a not finished task unexpected", KR(ret), K(task));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where task_id = %ld",
          OB_ALL_BALANCE_TASK_TNAME, balance_task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(balance_task_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id), K(sql));
  } else if(!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
  } else {
    // insert into history
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(trans, task.get_tenant_id());
    if (OB_FAIL(fill_dml_spliter(dml, task))) {
      LOG_WARN("failed to assign sql", KR(ret), K(task));
    } else if (OB_FAIL(dml.add_time_column("create_time", start_time))) {
      LOG_WARN("failed to add start time", KR(ret), K(start_time));
    } else if (OB_FAIL(dml.add_time_column("finish_time", finish_time))) {
      LOG_WARN("failed to add start time", KR(ret), K(task), K(finish_time));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_TASK_HISTORY_TNAME, dml,
                                        affected_rows))) {
      LOG_WARN("execute update failed", KR(ret), K(task));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::clean_parent_info(const uint64_t tenant_id,
                               const ObBalanceTaskID parent_task_id,
                               const ObBalanceJobID balance_job_id,
                               common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || ! parent_task_id.is_valid()
      || ! balance_job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(parent_task_id), K(balance_job_id));
  } else  if (OB_FAIL(sql.assign_fmt("update %s set parent_list = ", OB_ALL_BALANCE_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (OB_FAIL(sql.append_fmt(
      "replace(replace(replace(parent_list, ',%ld', ''), '%ld,', ''), '%ld', '')",
      parent_task_id.id(), parent_task_id.id(), parent_task_id.id()))) {
    LOG_WARN("failed to append fmt sql", KR(ret), K(parent_task_id), K(sql));
  } else if (OB_FAIL(sql.append_fmt(" where job_id = %ld", balance_job_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(balance_job_id), K(sql));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  }

  LOG_INFO("clean parent info finished", KR(ret), K(tenant_id), K(parent_task_id), K(balance_job_id),
      K(affected_rows), K(sql));

  return ret;
}

int ObBalanceTaskTableOperator::remove_parent_task(const uint64_t tenant_id,
                               const ObBalanceTaskID balance_task_id,
                               const ObBalanceTaskID parent_task_id,
                               ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObBalanceTask task;
  int64_t affected_rows = 0;
  common::ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t index = OB_INVALID_INDEX_INT64;
  int64_t finish_time = OB_INVALID_TIMESTAMP;//no used
  int64_t start_time = OB_INVALID_TIMESTAMP;//no used
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(&client, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_balance_task(tenant_id, balance_task_id, true, trans,
          task, start_time, finish_time))) {
    LOG_WARN("failed to get task", KR(ret), K(tenant_id), K(balance_task_id));
  } else if (!has_exist_in_array(task.get_parent_task_list(), parent_task_id, &index)) {
    LOG_INFO("has not in parent list", KR(ret), K(parent_task_id), K(task));
  } else {
    ObBalanceTaskIDList parent_list;
    common::ObArenaAllocator allocator;
    ObString parent_list_str;
    if (OB_FAIL(parent_list.assign(task.get_parent_task_list()))) {
      LOG_WARN("failed to assign array", KR(ret), K(task));
    } else if (OB_FAIL(parent_list.remove(index))) {
      LOG_WARN("failed to remove it from parent list", KR(ret), K(index), K(parent_list));
    } else if (OB_FAIL(parent_list.to_display_str(allocator, parent_list_str))) {
      LOG_WARN("failed to get str", KR(ret), K(parent_list));
    } else {
      ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.assign_fmt("update %s set parent_list = '%.*s' where "
                             "task_id = %ld", OB_ALL_BALANCE_TASK_TNAME, parent_list_str.length(),
                             parent_list_str.ptr(), balance_task_id.id()))) {
        LOG_WARN("failed to assign sql", KR(ret), K(parent_list_str),
                 K(balance_task_id));
      } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
      } else if (is_zero_row(affected_rows)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("expected one row, may status change", KR(ret), K(sql));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected single row", KR(ret), K(sql));
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::load_can_execute_task(const uint64_t tenant_id,
                                       ObBalanceTaskIArray &task_array,
                                       ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where parent_list is null or parent_list = ''",
                                    OB_ALL_BALANCE_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (OB_FAIL(read_tasks_(tenant_id, client, sql, task_array))) {
    LOG_WARN("failed to read task", KR(ret), K(tenant_id), K(sql));
  }
  LOG_INFO("load can-execute balance task", KR(ret), K(task_array), K(sql));
  return ret;
}

int ObBalanceTaskTableOperator::load_task(const uint64_t tenant_id,
                                       ObBalanceTaskIArray &task_array,
                                       ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s",
                                    OB_ALL_BALANCE_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (OB_FAIL(read_tasks_(tenant_id, client, sql, task_array))) {
    LOG_WARN("failed to read task", KR(ret), K(tenant_id), K(sql));
  }
  LOG_INFO("load all balance task", KR(ret), K(task_array), K(sql));
  return ret;
}


int ObBalanceTaskTableOperator::get_job_cannot_execute_task(
    const uint64_t tenant_id, const ObBalanceJobID balance_job_id,
    ObBalanceTaskIArray &task_array, ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !balance_job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(balance_job_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where job_id = %ld and "
                                    "parent_list != '' and parent_list is not null",
                                    OB_ALL_BALANCE_TASK_TNAME, balance_job_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql), K(balance_job_id));
  } else if (OB_FAIL(read_tasks_(tenant_id, client, sql, task_array))) {
    LOG_WARN("failed to read task", KR(ret), K(tenant_id), K(sql));
  }
  LOG_INFO("load job's can not execute balance tasks", KR(ret), K(balance_job_id), K(task_array), K(sql));
  return ret;
}

int ObBalanceTaskTableOperator::read_tasks_(const uint64_t tenant_id,
                                            ObISQLClient &client,
                                            const ObSqlString &sql,
                                            ObBalanceTaskIArray &task_array)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        ObBalanceTask task;
        while(OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (OB_FAIL(fill_cell(tenant_id, result, task))) {
            LOG_WARN("failed to fill cell", KR(ret), K(sql));
          } else if (OB_FAIL(task_array.push_back(task))) {
            LOG_WARN("failed to push back task", KR(ret), K(task));
          }
        }//end while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get cell", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::fill_cell(const uint64_t tenant_id,
                                          sqlclient::ObMySQLResult *result,
                                          ObBalanceTask &task) {
  int ret = OB_SUCCESS;
  task.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    int64_t job_id = ObBalanceJobID::INVALID_ID;
    int64_t balance_task_id = ObBalanceTaskID::INVALID_ID;
    uint64_t ls_group_id = OB_INVALID_ID;
    int64_t src_ls_id = 0;
    int64_t dest_ls_id = 0;
    int64_t transfer_task_id = ObTransferTaskID::INVALID_ID;
    ObString task_type, task_status;
    ObString part_list_str, finished_part_list_str;
    ObString parent_list_str, child_list_str;
    ObString comment;
    EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", balance_task_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "src_ls", src_ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "dest_ls", dest_ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "current_transfer_task_id", transfer_task_id, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_type", task_type);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", task_status);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "comment", comment);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "parent_list",
        parent_list_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "child_list",
        child_list_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "part_list",
        part_list_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "finished_part_list",
        finished_part_list_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get cell", KR(ret), K(job_id),
          K(task_type), K(task_status), K(ls_group_id),
          K(src_ls_id), K(dest_ls_id),
          K(transfer_task_id), K(part_list_str), K(finished_part_list_str),
          K(parent_list_str), K(child_list_str));
    } else if (OB_FAIL(task.init(
            tenant_id, ObBalanceJobID(job_id), ObBalanceTaskID(balance_task_id),
            ObBalanceTaskType(task_type),
            ObBalanceTaskStatus(task_status), ls_group_id,
            ObLSID(src_ls_id), ObLSID(dest_ls_id),
            ObTransferTaskID(transfer_task_id),
            part_list_str, finished_part_list_str,
            parent_list_str, child_list_str, comment))) {
      LOG_WARN("failed to init task", KR(ret), K(tenant_id), K(job_id),
          K(balance_task_id), K(task_type), K(task_status), K(ls_group_id),
          K(src_ls_id), K(dest_ls_id), K(transfer_task_id), K(comment),
          K(part_list_str), K(finished_part_list_str), K(parent_list_str), K(child_list_str));
    }
  }

  return ret;
}

int ObBalanceTaskTableOperator::get_job_task_cnt(const uint64_t tenant_id,
    const ObBalanceJobID job_id, int64_t &task_cnt, ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  task_cnt = 0;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("select count(*) as task_cnt from %s where job_id = %ld",
                                    OB_ALL_BALANCE_TASK_TNAME, job_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get balance task", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "task_cnt", task_cnt, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to task cnt", KR(ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::update_task_part_list(const uint64_t tenant_id,
                               const ObBalanceTaskID balance_task_id,
                               const ObTransferPartList &part_list,
                               common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObArenaAllocator allocator;
  ObString part_list_str;
  //maybe part_list empty
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(balance_task_id),
             K(part_list));
  } else if (OB_FAIL(part_list.to_display_str(allocator, part_list_str))) {
    LOG_WARN("failed to transfer list to str", KR(ret), K(part_list));
  } else if (OB_FAIL(sql.assign_fmt("update %s set part_list = '%.*s', part_count = %ld where "
                                "task_id = %ld",
                                OB_ALL_BALANCE_TASK_TNAME, part_list_str.length(),
                                part_list_str.ptr(), part_list.count(), balance_task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(balance_task_id), K(part_list_str));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    //调用点处理
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may part_list not change", KR(ret), K(sql), K(affected_rows));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql), K(affected_rows));
  }

  return ret;
}

int ObBalanceTaskTableOperator::get_merge_task_dest_ls_by_src_ls(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &src_ls,
    ObLSID &dest_ls)
{
  int ret = OB_SUCCESS;
  dest_ls.reset();
  if (OB_UNLIKELY(!src_ls.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(src_ls));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      int64_t int_ls_id;
      common::sqlclient::ObMySQLResult *res;
      if (OB_FAIL(sql.assign_fmt(
          "select dest_ls from %s where src_ls = %ld and task_type = '%s' limit 1",
          OB_ALL_BALANCE_TASK_TNAME,
          src_ls.id(),
          ObBalanceTaskType(ObBalanceTaskType::BALANCE_TASK_MERGE).to_str()))) {
        LOG_WARN("failed to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_client.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("balance task not found", KR(ret), K(tenant_id), K(sql));
        } else {
          LOG_WARN("next failed", KR(ret), K(tenant_id), K(sql));
        }
      } else if (OB_FAIL(res->get_int("dest_ls", int_ls_id))) {
        LOG_WARN("get int failed", KR(ret), K(tenant_id), K(sql));
      } else {
        dest_ls = int_ls_id;
      }
    }
  }
  return ret;
}

int ObBalanceTaskTableOperator::load_need_transfer_task(const uint64_t tenant_id,
                                       ObBalanceTaskIArray &task_array,
                                       ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  ObBalanceTaskStatus task_status(ObBalanceTaskStatus::BALANCE_TASK_STATUS_TRANSFER);
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where status = '%s'",
                                    OB_ALL_BALANCE_TASK_TNAME, task_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql), K(task_status));
  } else if (OB_FAIL(read_tasks_(tenant_id, client, sql, task_array))) {
    LOG_WARN("failed to read task", KR(ret), K(tenant_id), K(sql));
  }
  LOG_INFO("load need transfer balance task", KR(ret), K(task_array), K(sql));
  return ret;
}



int ObBalanceTaskMDSHelper::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  UNUSEDx(buf, len, ctx);
  return OB_SUCCESS;
}

int ObBalanceTaskMDSHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  UNUSEDx(buf, len, scn, ctx);
  return OB_SUCCESS;
}
}//end of share
}//end of ob
