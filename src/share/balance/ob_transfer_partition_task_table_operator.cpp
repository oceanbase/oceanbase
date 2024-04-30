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

#include "ob_transfer_partition_task_table_operator.h"
#include "lib/mysqlclient/ob_isql_client.h"//ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_proxy.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "share/inner_table/ob_inner_table_schema.h"//ALL_TRANSFER_PARTITION_TASK_TNAME
#include "share/ob_dml_sql_splicer.h"//ObDMLSqlSplicer
#include "share/ls/ob_ls_operator.h"//get_tenant_gts
#include "storage/tablelock/ob_lock_utils.h"//lock table

namespace oceanbase
{
using namespace transaction;
using namespace common;
namespace share
{
static const char* TRP_TASK_STATUS_ARRAY[] =
{
  "WAITING", "INIT", "DOING", "COMPLETED", "FAILED", "CANCELED"
};

const char* ObTransferPartitionTaskStatus::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(TRP_TASK_STATUS_ARRAY) == TRP_TASK_STATUS_MAX, "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(TRP_TASK_STATUS_ARRAY) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown transfer partition task status", K_(val));
  } else {
    type_str = TRP_TASK_STATUS_ARRAY[val_];
  }
  return type_str;
}

ObTransferPartitionTaskStatus::ObTransferPartitionTaskStatus(const ObString &str)
{
  val_ = TRP_TASK_STATUS_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TRP_TASK_STATUS_ARRAY); ++i) {
      if (0 == str.case_compare(TRP_TASK_STATUS_ARRAY[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (TRP_TASK_STATUS_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid transfer partition status", K(val_), K(str));
  }
}

int ObTransferPartitionTask::simple_init(const uint64_t tenant_id,
           const ObTransferPartInfo &part_info,
           const ObLSID &dest_ls, const ObTransferPartitionTaskID &task_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !part_info.is_valid()
        || !dest_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(part_info), K(dest_ls));
  } else {
    tenant_id_ = tenant_id;
    part_info_ = part_info;
    dest_ls_ = dest_ls;
    task_id_ = task_id;
    task_status_ = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING;
    balance_job_id_.reset();
    transfer_task_id_.reset();
    comment_.reset();
  }
  return ret;
}

int ObTransferPartitionTask::init(const uint64_t tenant_id,
           const ObTransferPartInfo &part_info,
           const ObLSID &dest_ls,
           const ObTransferPartitionTaskID &task_id,
           const ObBalanceJobID &balance_job_id,
           const ObTransferTaskID &transfer_task_id,
           const ObTransferPartitionTaskStatus &task_status,
           const ObString &comment)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !part_info.is_valid()
        || !dest_ls.is_valid() || !task_status.is_valid()
        || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(part_info), K(dest_ls),
    K(task_status), K(task_id));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to assign commet", KR(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    part_info_ = part_info;
    dest_ls_ = dest_ls;
    task_id_ = task_id;
    balance_job_id_ = balance_job_id;
    transfer_task_id_ = transfer_task_id;
    task_status_ = task_status;
  }
  return ret;
}

bool ObTransferPartitionTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && part_info_.is_valid()
    && dest_ls_.is_valid()
    && task_id_.is_valid()
    && task_status_.is_valid();
}
int ObTransferPartitionTask::assign(const ObTransferPartitionTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (!other.comment_.empty() && OB_FAIL(comment_.assign(other.comment_))) {
      LOG_WARN("failed to assign comment", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      part_info_ = other.part_info_;
      dest_ls_ = other.dest_ls_;
      task_id_ = other.task_id_;
      balance_job_id_ = other.balance_job_id_;
      transfer_task_id_ = other.transfer_task_id_;
      task_status_ = other.task_status_;
    }
  }
  return ret;
}

void ObTransferPartitionTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  part_info_.reset();
  dest_ls_.reset();
  task_id_.reset();
  balance_job_id_.reset();
  transfer_task_id_.reset();
  task_status_.reset();
  comment_.reset();
}

int ObTransferPartitionTaskTableOperator::insert_new_task(
    const uint64_t tenant_id,
    const ObTransferPartInfo &part_info,
    const ObLSID &dest_ls,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  //生成唯一并且递增的task_id, 先加表锁
  ObTransferPartitionTaskID task_id;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !part_info.is_valid() || !dest_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_info), K(dest_ls));
  } else if (OB_FAIL(tablelock::ObInnerTableLockUtil::lock_inner_table_in_trans(
      trans,
      tenant_id,
      OB_ALL_TRANSFER_PARTITION_TASK_TID,
      tablelock::EXCLUSIVE, true))) {
    LOG_WARN("lock inner table failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fetch_new_task_id_(tenant_id, trans, task_id))) {
    LOG_WARN("failed to get max task id", KR(ret), K(tenant_id));
  } else {
    ObTransferPartitionTask new_task;
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(trans, tenant_id);
    if (OB_FAIL(new_task.simple_init(tenant_id, part_info, dest_ls,
                task_id))) {
      LOG_WARN("failed to init new task", KR(ret), K(part_info), K(dest_ls), K(task_id));
    } else if (OB_UNLIKELY(!new_task.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is invalid", KR(ret), K(new_task));
    } else if (OB_FAIL(fill_dml_splicer_(dml, new_task))) {
      LOG_WARN("failed to assign sql", KR(ret), K(new_task));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
                                        dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret), K(new_task));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected single row", KR(ret), K(affected_rows));
    }
    LOG_INFO("insert new task", KR(ret), K(new_task));
  }
  return ret;
}

int ObTransferPartitionTaskTableOperator::load_all_wait_task_in_part_info_order(
    const uint64_t tenant_id, const bool for_update,
    const ObTransferPartitionTaskID &max_task_id,
    ObIArray<ObTransferPartitionTask> &task_array,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  ObTransferPartitionTaskStatus wait_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !max_task_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(max_task_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select * from %s where status = '%s' and task_id <= %ld "
                 " order by table_id, object_id",
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME, wait_status.to_str(),
                 max_task_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(for_update && OB_FAIL(sql.append(" for update")))) {
    LOG_WARN("failed to append for update", KR(ret), K(sql), K(for_update));
  } else if (OB_FAIL(get_tasks_(tenant_id, sql, task_array, sql_client))) {
    LOG_WARN("failed to get tasks", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObTransferPartitionTaskTableOperator::load_all_balance_job_task(const uint64_t tenant_id,
      const share::ObBalanceJobID &job_id,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !job_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where balance_job_id = %ld",
          OB_ALL_TRANSFER_PARTITION_TASK_TNAME, job_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(get_tasks_(tenant_id, sql, task_array, sql_client))) {
    LOG_WARN("failed to get tasks", KR(ret), K(tenant_id), K(sql));
  }
  return ret;

}

int ObTransferPartitionTaskTableOperator::load_all_task(const uint64_t tenant_id,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s",
          OB_ALL_TRANSFER_PARTITION_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(get_tasks_(tenant_id, sql, task_array, sql_client))) {
    LOG_WARN("failed to get tasks", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObTransferPartitionTaskTableOperator::get_tasks_(const uint64_t tenant_id,
      const ObSqlString &sql,
      ObIArray<ObTransferPartitionTask> &task_array,
      ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        ObTransferPartitionTask task;
        while(OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (OB_FAIL(fill_cell_(tenant_id, result, task))) {
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


int ObTransferPartitionTaskTableOperator::set_all_tasks_schedule(const uint64_t tenant_id,
      const ObTransferPartitionTaskID &max_task_id,
      const ObBalanceJobID &job_id,
      const int64_t &task_count,
      ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_row = 0;
  ObTransferPartitionTaskStatus wait_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING;
  ObTransferPartitionTaskStatus new_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_INIT;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
        || !max_task_id.is_valid() || !job_id.is_valid()
        || 0 >= task_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id),
    K(max_task_id), K(task_count));
  } else if (OB_FAIL(sql.assign_fmt("update %s set balance_job_id = %ld, "
                                    "status = '%s' where task_id <= %ld and status = '%s'",
                                    OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
                                    job_id.id(), new_status.to_str(), max_task_id.id(), wait_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(job_id), K(max_task_id), K(sql));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to exec sql", KR(ret), K(sql), K(tenant_id));
  } else if (affected_row != task_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task count not equal to affected row, need retry", KR(ret), K(sql),
    K(affected_row), K(task_count));
  }
  LOG_INFO("set task to init", KR(ret), K(affected_row), K(task_count),
       K(max_task_id), K(job_id), K(sql));
  return ret;
}

int ObTransferPartitionTaskTableOperator::rollback_all_to_waitting(const uint64_t tenant_id,
                           const ObBalanceJobID &job_id,
                           ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_row = 0;
  ObTransferPartitionTaskStatus old_status1 = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_DOING;
  ObTransferPartitionTaskStatus old_status2 = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_INIT;
  ObTransferPartitionTaskStatus new_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING;
  ObBalanceJobID invalid_job_id;
  ObTransferTaskID invalid_task_id;
  ObSqlString comment;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(comment.assign_fmt(
                 "Rollback due to balance job %ld being canceled",
                 job_id.id()))) {
    LOG_WARN("failed to assign comment", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "update %s set balance_job_id = %ld, transfer_task_id = %ld, "
                 "comment = '%s', status = '%s' where balance_job_id = %ld and "
                 "status in ('%s', '%s')",
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME, invalid_job_id.id(),
                 invalid_task_id.id(), comment.ptr(), new_status.to_str(),
                 job_id.id(), old_status1.to_str(), old_status2.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(job_id), K(sql));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to exec sql", KR(ret), K(sql), K(tenant_id));
  }
  LOG_INFO("rollback task to waiting", KR(ret), K(affected_row), K(job_id),
              K(sql));
  return ret;
}
int ObTransferPartitionTaskTableOperator::rollback_from_doing_to_waiting(const uint64_t tenant_id,
                         const ObBalanceJobID &job_id,
                         const ObTransferPartList &part_list,
                         const ObString &comment,
                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTransferPartitionTaskStatus old_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_DOING;
  ObTransferPartitionTaskStatus new_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_WAITING;
  ObBalanceJobID invalid_job_id;
  ObTransferTaskID invalid_task_id;
  ObSqlString sql;
  ObSqlString part_list_sql;
  int64_t affected_row = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !job_id.is_valid()
        || part_list.count() <= 0 || comment.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id),
        K(part_list), K(comment));
  } else if (OB_FAIL(append_sql_with_part_list_(part_list, part_list_sql))) {
    LOG_WARN("failed to append sql", KR(ret), K(part_list));
  } else if (OB_FAIL(sql.assign_fmt(
                 "update %s set transfer_task_id = %ld, balance_job_id = %ld, "
                 "status = '%s', comment = '%.*s'"
                 "where balance_job_id = %ld and status = '%s' "
                 "and (table_id, object_id) in (%s)",
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME, invalid_task_id.id(),
                 invalid_job_id.id(), new_status.to_str(), comment.length(),
                 comment.ptr(), job_id.id(), old_status.to_str(),
                 part_list_sql.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(job_id), K(sql), K(comment));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to exec sql", KR(ret), K(sql), K(tenant_id));
  } else if (affected_row > part_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    // 不能检查part_list的个数等于affected_row。
    // 对于中间状态的日志流，并没有限制新建表，所以transfer的列表是包含临时日志流上的新建表信息
    LOG_WARN("affected row not match with part list", KR(ret), K(part_list.count()), K(affected_row),
    K(sql), K(part_list));
  }
  LOG_INFO("rollback task from doing to waiting", KR(ret), K(affected_row), K(job_id),
          K(part_list), K(comment), K(sql));
  return ret;
}

int ObTransferPartitionTaskTableOperator::start_transfer_task(const uint64_t tenant_id,
                         const ObBalanceJobID &job_id,
                         const ObTransferPartList &part_list,
                         const ObTransferTaskID &transfer_task_id,
                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString part_list_sql;
  int64_t affected_row = 0;
  //由于一个transfer partition任务会经过多轮transfer，所以start_transfer_task会调度多轮
  //可能status为init或者doing
  ObTransferPartitionTaskStatus status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_DOING;
  ObTransferPartitionTaskStatus old_status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_INIT;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !job_id.is_valid()
   || !transfer_task_id.is_valid() || part_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(transfer_task_id), K(job_id),
        K(part_list));
  } else if (OB_FAIL(append_sql_with_part_list_(part_list, part_list_sql))) {
    LOG_WARN("failed to append sql", KR(ret), K(part_list));
  } else if (OB_FAIL(sql.assign_fmt("update %s set transfer_task_id = %ld, status = '%s' "
          "where balance_job_id = %ld and status in ('%s', '%s') "
          "and (table_id, object_id) in (%s)",
          OB_ALL_TRANSFER_PARTITION_TASK_TNAME, transfer_task_id.id(), status.to_str(),
          job_id.id(), old_status.to_str(), status.to_str(), part_list_sql.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(transfer_task_id), K(sql));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to exec sql", KR(ret), K(sql), K(tenant_id));
  } else if (affected_row > part_list.count()) {
     ret = OB_ERR_UNEXPECTED;
   // 不能检查part_list的个数等于affected_row。
   // 对于中间状态的日志流，并没有限制新建表，所以transfer的列表是包含临时日志流上的新建表信息
    LOG_WARN("affected row not match with part list", KR(ret), K(part_list.count()), K(affected_row),
    K(sql), K(part_list));
  }
  LOG_INFO("start transfer task", KR(ret), K(affected_row),
             K(job_id), K(part_list), K(sql));
  return ret;
}

int ObTransferPartitionTaskTableOperator::finish_task(const uint64_t tenant_id,
                         const ObTransferPartList &part_list,
                         const ObTransferPartitionTaskID &max_task_id,
                         const ObTransferPartitionTaskStatus &status,
                         const ObString &comment,
                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString delete_sql;
  ObSqlString condition_sql;
  ObSqlString part_list_sql;
  int64_t affected_row = 0;
  int64_t delete_affected_row = 0;
  const char* table_column = "table_id, object_id, task_id, dest_ls, balance_job_id, transfer_task_id";
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !max_task_id.is_valid()
   || !status.is_valid() || part_list.count() <= 0 || !status.is_finish_status())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(status),
    K(part_list), K(max_task_id));
  } else if (OB_FAIL(append_sql_with_part_list_(part_list, part_list_sql))) {
    LOG_WARN("failed to append sql", KR(ret), K(part_list));
  } else if (OB_FAIL(condition_sql.assign_fmt("(table_id, object_id) in (%s) and task_id <= %ld",
                  part_list_sql.ptr(), max_task_id.id()))) {
    LOG_WARN("failed to append sql", KR(ret), K(part_list_sql));
  } else if (OB_FAIL(sql.assign_fmt("insert into %s (%s, status, comment, create_time, "
                 "finish_time) select %s, '%s', '%.*s', gmt_create, "
                 "now() from %s where %s",
                 OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TNAME, table_column,
                 table_column, status.to_str(), comment.length(), comment.ptr(),
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME, condition_sql.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(status), K(comment), K(condition_sql));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id), K(sql));
  } else if (affected_row > part_list.count()) {
    //因为part_list可能不是这么准确，不能做准确
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected row not match with part list", KR(ret),
             K(part_list.count()), K(affected_row), K(sql));
  } else if (OB_FAIL(delete_sql.assign_fmt("delete from %s where %s",
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME, condition_sql.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(status), K(comment), K(condition_sql));
    } else if (OB_FAIL(trans.write(tenant_id, delete_sql.ptr(), delete_affected_row))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id), K(delete_sql));
  } else if (affected_row != delete_affected_row) {
    //插入和删除的行应该是匹配的
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected row not match with part list", KR(ret), K(delete_affected_row),
             K(part_list.count()), K(affected_row), K(delete_sql));
  }
  LOG_INFO("finish task", KR(ret), K(affected_row), K(delete_affected_row), K(status),
           K(max_task_id), K(part_list), K(sql), K(delete_sql));
  return ret;
}

int ObTransferPartitionTaskTableOperator::fill_dml_splicer_(share::ObDMLSqlSplicer &dml,
                              const ObTransferPartitionTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("task_id", task.get_task_id().id()))
      || OB_FAIL(dml.add_pk_column("table_id", task.get_part_info().table_id()))
      || OB_FAIL(dml.add_pk_column("object_id", task.get_part_info().part_object_id()))
      || OB_FAIL(dml.add_column("dest_ls", task.get_dest_ls().id()))
      || OB_FAIL(dml.add_column("balance_job_id", task.get_balance_job_id().id()))
      || OB_FAIL(dml.add_column("transfer_task_id", task.get_transfer_task_id().id()))
      || OB_FAIL(dml.add_column("status", task.get_task_status().to_str()))
      || (!task.get_comment().empty() && OB_FAIL(dml.add_column("comment", task.get_comment())))) {
    LOG_WARN("failed to fill dml spliter", KR(ret), K(task));
  }
  return ret;
}
int ObTransferPartitionTaskTableOperator::fill_cell_(const uint64_t tenant_id,
                                          sqlclient::ObMySQLResult *result,
                                          ObTransferPartitionTask &task) {
  int ret = OB_SUCCESS;
  task.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    int64_t balance_job_id = ObBalanceJobID::INVALID_ID;
    int64_t task_id = ObTransferPartitionTaskID::INVALID_ID;
    int64_t dest_ls_id = 0;
    int64_t transfer_task_id = ObTransferTaskID::INVALID_ID;
    ObString task_status;
    ObString comment;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t object_id = OB_INVALID_ID;
    EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "object_id", object_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "balance_job_id", balance_job_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "dest_ls", dest_ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "transfer_task_id", transfer_task_id, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", task_status);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "comment", comment, true, false, "");
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get cell", KR(ret), K(task_id), K(balance_job_id),
          K(task_status), K(dest_ls_id), K(table_id), K(object_id),
          K(transfer_task_id), K(comment));
    } else if (OB_FAIL(task.init(
            tenant_id, ObTransferPartInfo(table_id, object_id), ObLSID(dest_ls_id),
            ObTransferPartitionTaskID(task_id), ObBalanceJobID(balance_job_id),
            ObTransferTaskID(transfer_task_id),
            ObTransferPartitionTaskStatus(task_status), comment))) {
      LOG_WARN("failed to init task", KR(ret), K(task_id), K(balance_job_id),
               K(task_status), K(dest_ls_id), K(table_id),
               K(object_id), K(transfer_task_id), K(comment), K(tenant_id));
    }
  }
  return ret;
}

//TODO, transfer part is small than 100, no need take care of part_list count
int ObTransferPartitionTaskTableOperator::append_sql_with_part_list_(
    const ObTransferPartList &part_list, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= part_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sql), K(part_list));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_list.count(); ++i) {
    const ObTransferPartInfo& part = part_list.at(i);
    if (0 != i && OB_FAIL(sql.append(", "))) {
      LOG_WARN("failed to append sql", KR(ret), K(i));
    } else if (OB_FAIL(sql.append_fmt("(%ld, %ld)", part.table_id(),
                                      part.part_object_id()))) {
      LOG_WARN("failed to append sql", KR(ret), K(i), K(part), K(sql));
    }
  }
  return ret;
}

int ObTransferPartitionTaskTableOperator::fetch_new_task_id_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans,
    ObTransferPartitionTaskID &task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select max(task_id) as task_id from (select max(task_id) as task_id from %s "
                 "union select max(task_id) as task_id from %s) as a",
                 OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
                 OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_SUCC(result->next())) {
        int64_t id = ObTransferPartitionTaskID::INVALID_ID;
        //maybe null and default is zero
        EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "task_id", id, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get task id", KR(ret), K(sql));
        } else {
          task_id = (id + 1);
        }
      } else {
        LOG_WARN("failed to get cell", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObTransferPartitionTaskTableOperator::load_part_list_task(
    const uint64_t tenant_id, const ObBalanceJobID &job_id,
    const ObTransferPartList &part_list,ObIArray<ObTransferPartitionTask> &task_array,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString part_list_sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || part_list.count() <= 0
                  || !job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(part_list), K(job_id));
  } else if (OB_FAIL(append_sql_with_part_list_(part_list, part_list_sql))) {
    LOG_WARN("failed to append sql", KR(ret), K(part_list));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where balance_job_id = %ld "
                                "and (table_id, object_id) in (%s)",
                                OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
                                job_id.id(), part_list_sql.ptr()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(sql), K(job_id));
  } else if (OB_FAIL(get_tasks_(tenant_id, sql, task_array, trans))) {
    LOG_WARN("failed to get tasks", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}
int ObTransferPartitionTaskTableOperator::get_transfer_partition_task(
      const uint64_t tenant_id,
      const ObTransferPartInfo &part_info,
      ObTransferPartitionTask &task,
      ObISQLClient &sql_client)
{
   int ret = OB_SUCCESS;
   ObSEArray<ObTransferPartitionTask, 1> task_array;
   ObSqlString sql;
   if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !part_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(part_info));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where table_id = %ld "
                                "and object_id = %ld",
                                OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
                                part_info.table_id(), part_info.part_object_id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(sql), K(part_info));
  } else if (OB_FAIL(get_tasks_(tenant_id, sql, task_array, sql_client))) {
    LOG_WARN("failed to get tasks", KR(ret), K(tenant_id), K(sql));
  } else if (0 == task_array.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("part not in transfer partition", KR(ret), K(part_info), K(tenant_id));
  } else if (1 != task_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(part_info), K(task_array));
  } else if (OB_FAIL(task.assign(task_array.at(0)))) {
    LOG_WARN("failed to assin transfer partition task", KR(ret), K(task_array));
  }
  return ret;

}


}//end of share
}//end of ob
