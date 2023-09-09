/**
 * Copyright (c) 2022 OceanBase
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

#include "share/transfer/ob_transfer_task_operator.h"
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TRANSFER_TASK_TNAME

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace transaction::tablelock;

namespace share
{
int ObTransferTaskOperator::get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const bool for_update,
    ObTransferTask &task,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !task_id.is_valid() || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id), K(for_update), K(group_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE task_id = %ld%s",
          OB_ALL_TRANSFER_TASK_TNAME, task_id.id(), for_update ? " FOR UPDATE" : ""))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id), K(for_update));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_transfer_task_(*result.get_result(), task))) {
        LOG_WARN("construct transfer task failed", KR(ret), K(tenant_id), K(task_id), K(sql), K(task));
      }
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_task_with_time(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const bool for_update,
    ObTransferTask &task,
    int64_t &create_time,
    int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id), K(for_update));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      const bool with_time = true;
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) AS create_time, "
          "time_to_usec(gmt_modified) AS finish_time, * FROM %s WHERE task_id = %ld%s",
          OB_ALL_TRANSFER_TASK_TNAME, task_id.id(), for_update ? " FOR UPDATE" : ""))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id), K(for_update));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("get next result failed", KR(ret), K(sql), K(tenant_id), K(task_id));
        }
      } else if (OB_FAIL(parse_sql_result_(*res, with_time, task, create_time, finish_time))) {
        LOG_WARN("parse sql result failed", KR(ret), K(tenant_id), K(task_id), K(task));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(task_id));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read more than one row", KR(ret), K(tenant_id), K(task_id));
      }
    } // end SMART_VAR
  }
  return ret;
}

int ObTransferTaskOperator::get_by_status(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferStatus &status,
    common::ObIArray<ObTransferTask> &tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(status));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE status = '%s'",
          OB_ALL_TRANSFER_TASK_TNAME, status.str()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(status));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_transfer_tasks_(*result.get_result(), tasks))) {
        LOG_WARN("construct transfer task failed", KR(ret), K(tenant_id), K(sql), K(tasks));
      } else if (tasks.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("tasks in status not found", KR(ret), K(tenant_id), K(status), K(tasks));
      }
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_all_task_status(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    common::ObIArray<ObTransferTask::TaskStatus> &task_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT task_id, status FROM %s", OB_ALL_TRANSFER_TASK_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_task_status_(*result.get_result(), task_status))) {
        LOG_WARN("construct transfer task failed", KR(ret), K(tenant_id), K(sql), K(task_status));
      } else if (task_status.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("no task in table", KR(ret), K(tenant_id), K(task_status));
      }
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_by_src_ls(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObLSID &src_ls,
    ObTransferTask &task,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  const bool is_src_ls = true;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !src_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(src_ls));
  } else if (OB_FAIL(get_by_ls_id_(sql_proxy, tenant_id, src_ls, is_src_ls, group_id, task))) {
    LOG_WARN("failed to get by ls id", K(ret), K(tenant_id), K(src_ls));
  }
  return ret;
}

int ObTransferTaskOperator::get_by_dest_ls(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObLSID &dest_ls,
    ObTransferTask &task,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  const bool is_src_ls = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !dest_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dest_ls));
  } else if (OB_FAIL(get_by_ls_id_(sql_proxy, tenant_id, dest_ls, is_src_ls, group_id, task))) {
    LOG_WARN("failed to get by ls id", K(ret), K(tenant_id), K(dest_ls));
  }
  return ret;
}

int ObTransferTaskOperator::fill_dml_splicer_(
    ObDMLSqlSplicer &dml_splicer,
    common::ObArenaAllocator &allocator,
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  char *trace_id = NULL;
  ObString part_list_str;
  ObString not_exist_part_list_str;
  ObString lock_conflict_part_list_str;
  ObString table_lock_tablet_list_str;
  ObString tablet_list_str;

  if (OB_ISNULL(trace_id = static_cast<char *>(allocator.alloc(OB_MAX_TRACE_ID_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", KR(ret), "length", OB_MAX_TRACE_ID_BUFFER_SIZE);
  } else if (OB_FALSE_IT(task.get_trace_id().to_string(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(task.get_part_list().to_display_str(allocator, part_list_str))) {
    LOG_WARN("transfer list to str failed", KR(ret),
        "part_list", task.get_part_list(), K(part_list_str));
  } else if (OB_FAIL(task.get_not_exist_part_list().to_display_str(
      allocator,
      not_exist_part_list_str))) {
    LOG_WARN("transfer list to str failed", KR(ret), "not_exist_part_list",
        task.get_not_exist_part_list(), K(not_exist_part_list_str));
  } else if (OB_FAIL(task.get_lock_conflict_part_list().to_display_str(
      allocator,
      lock_conflict_part_list_str))) {
    LOG_WARN("transfer list to str failed", KR(ret), "lock_conflict_part_list",
        task.get_lock_conflict_part_list(), K(lock_conflict_part_list_str));
  } else if (OB_FAIL(task.get_table_lock_tablet_list().to_display_str(
      allocator,
      table_lock_tablet_list_str))) {
    LOG_WARN("transfer list to str failed", KR(ret), "table_lock_tablet_list",
        task.get_table_lock_tablet_list(), K(table_lock_tablet_list_str));
  } else if (OB_FAIL(task.get_tablet_list().to_display_str(allocator, tablet_list_str))) {
    LOG_WARN("transfer list to str failed", KR(ret), "tablet_list",
        task.get_tablet_list(), K(tablet_list_str));
  }

  if (FAILEDx(dml_splicer.add_pk_column("task_id", task.get_task_id().id()))
      || OB_FAIL(dml_splicer.add_column("src_ls", task.get_src_ls().id()))
      || OB_FAIL(dml_splicer.add_column("dest_ls", task.get_dest_ls().id()))
      || OB_FAIL(dml_splicer.add_column("part_list", part_list_str))
      || OB_FAIL(dml_splicer.add_column("part_count", task.get_part_list().count()))
      || OB_FAIL(dml_splicer.add_column("not_exist_part_list", not_exist_part_list_str))
      || OB_FAIL(dml_splicer.add_column("lock_conflict_part_list", lock_conflict_part_list_str))
      || OB_FAIL(dml_splicer.add_column("table_lock_tablet_list", table_lock_tablet_list_str))
      || OB_FAIL(dml_splicer.add_column("tablet_list", tablet_list_str))
      || OB_FAIL(dml_splicer.add_column("tablet_count", task.get_tablet_list().count()))
      || OB_FAIL(dml_splicer.add_column("start_scn", task.get_start_scn().get_val_for_inner_table_field()))
      || OB_FAIL(dml_splicer.add_column("finish_scn", task.get_finish_scn().get_val_for_inner_table_field()))
      || OB_FAIL(dml_splicer.add_column("status", task.get_status().str()))
      || OB_FAIL(dml_splicer.add_column("trace_id", trace_id))
      || OB_FAIL(dml_splicer.add_column("result", task.get_result()))
      || OB_FAIL(dml_splicer.add_column("comment", transfer_task_comment_to_str(task.get_comment())))
      || OB_FAIL(dml_splicer.add_column("balance_task_id", task.get_balance_task_id().id()))
      || OB_FAIL(dml_splicer.add_column("table_lock_owner_id", task.get_table_lock_owner_id().id()))) {
    LOG_WARN("fail to add column", KR(ret), K(task));
  }
  return ret;
}

int ObTransferTaskOperator::insert(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    ObArenaAllocator allocator;
    if (OB_FAIL(fill_dml_splicer_(dml_splicer, allocator, task))) {
      LOG_WARN("fail to fill dml splicer", KR(ret), K(tenant_id), K(task));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id), K(task));
    } else if (OB_FAIL(dml_splicer.splice_insert_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_ENTRY_EXIST;
      }
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows), K(task));
    } else {
      LOG_INFO("insert transfer task success", K(tenant_id), K(affected_rows), K(task));
    }
  }
  return ret;
}

int ObTransferTaskOperator::update_to_start_status(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const ObTransferPartList &new_part_list,
    const ObTransferPartList &new_not_exist_part_list,
    const ObTransferPartList &new_lock_conflict_part_list,
    const ObDisplayTabletList &new_table_lock_tablet_list,
    const ObTransferTabletList &new_tablet_list,
    const ObTransferStatus &new_status,
    const ObTableLockOwnerID &table_lock_owner_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid()
      || new_part_list.empty()
      || new_tablet_list.empty()
      || !table_lock_owner_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
        K(tenant_id), K(task_id), K(new_part_list), K(new_tablet_list), K(table_lock_owner_id));
  } else if (old_status.status() != ObTransferStatus::INIT
      || new_status.status() != ObTransferStatus::START) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", KR(ret),
        K(tenant_id), K(task_id), K(old_status), K(new_status));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    ObString part_list_str;
    ObString not_exist_part_list_str;
    ObString lock_conflict_part_list_str;
    ObString table_lock_tablet_list_str;
    ObString tablet_list_str;
    ObArenaAllocator allocator;

    if (OB_FAIL(new_part_list.to_display_str(allocator, part_list_str))) {
      LOG_WARN("transfer list to str failed", KR(ret), K(new_part_list), K(part_list_str));
    } else if (OB_FAIL(new_tablet_list.to_display_str(allocator, tablet_list_str))) {
      LOG_WARN("transfer list to str failed", KR(ret), K(new_tablet_list), K(tablet_list_str));
    } else if (OB_FAIL(new_not_exist_part_list.to_display_str(
        allocator,
        not_exist_part_list_str))) {
      LOG_WARN("transfer list to str failed", KR(ret),
          K(new_not_exist_part_list), K(not_exist_part_list_str));
    } else if (OB_FAIL(new_lock_conflict_part_list.to_display_str(
        allocator,
        lock_conflict_part_list_str))) {
      LOG_WARN("transfer list to str failed", KR(ret),
          K(new_lock_conflict_part_list), K(lock_conflict_part_list_str));
    } else if (OB_FAIL(new_table_lock_tablet_list.to_display_str(
        allocator,
        table_lock_tablet_list_str))) {
      LOG_WARN("transfer list to str failed", KR(ret),
          K(new_table_lock_tablet_list), K(table_lock_tablet_list_str));
    }

    if (FAILEDx(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("part_list", part_list_str))
        || OB_FAIL(dml_splicer.add_column("part_count", new_part_list.count()))
        || OB_FAIL(dml_splicer.add_column("not_exist_part_list", not_exist_part_list_str))
        || OB_FAIL(dml_splicer.add_column("lock_conflict_part_list", lock_conflict_part_list_str))
        || OB_FAIL(dml_splicer.add_column("table_lock_tablet_list", table_lock_tablet_list_str))
        || OB_FAIL(dml_splicer.add_column("tablet_list", tablet_list_str))
        || OB_FAIL(dml_splicer.add_column("tablet_count", new_tablet_list.count()))
        || OB_FAIL(dml_splicer.add_column("status", new_status.str()))
        || OB_FAIL(dml_splicer.add_column("comment", transfer_task_comment_to_str(ObTransferTaskComment::EMPTY_COMMENT))) // reset comment
        || OB_FAIL(dml_splicer.add_column("table_lock_owner_id", table_lock_owner_id.id()))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task_id), K(part_list_str),
          K(not_exist_part_list_str), K(lock_conflict_part_list_str), K(table_lock_tablet_list_str),
          K(tablet_list_str), K(new_status), K(table_lock_owner_id));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id), K(task_id));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update transfer_task to start success",
          K(tenant_id), K(task_id), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObTransferTaskOperator::finish_task(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const ObTransferStatus &new_status,
    const int result,
    const ObTransferTaskComment &comment,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (OB_UNLIKELY(!new_status.is_finish_status())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("incorrect new status", KR(ret), K(tenant_id), K(task_id), K(new_status));
  } else if (OB_FAIL(ObTransferStatusHelper::check_can_change_status(
      old_status,
      new_status,
      can_change))) {
    LOG_WARN("fail to check can change status", KR(ret),
        K(old_status), K(new_status), K(tenant_id), K(task_id));
  } else if (OB_UNLIKELY(!can_change)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not change status", KR(ret),
        K(old_status), K(new_status), K(tenant_id), K(task_id));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("status", new_status.str())
        || OB_FAIL(dml_splicer.add_column("result", result))
        || OB_FAIL(dml_splicer.add_column("comment", transfer_task_comment_to_str(comment))))) {
      LOG_WARN("fail to add column", KR(ret),
          K(tenant_id), K(task_id), K(new_status), K(result), K(comment));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), group_id, affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("finish transfer_task success",
          K(tenant_id), K(task_id), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObTransferTaskOperator::finish_task_from_init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const ObTransferPartList &new_part_list,
    const ObTransferPartList &new_not_exist_part_list,
    const ObTransferPartList &new_lock_conflict_part_list,
    const ObTransferStatus &new_status,
    const int result,
    const ObTransferTaskComment &comment)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (old_status.status() != ObTransferStatus::INIT
      && new_status.status() != ObTransferStatus::COMPLETED) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", KR(ret),
        K(tenant_id), K(task_id), K(old_status), K(new_status));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    ObArenaAllocator allocator;
    ObString part_list_str;
    ObString not_exist_part_list_str;
    ObString lock_conflict_part_list_str;
    if (OB_FAIL(new_part_list.to_display_str(allocator, part_list_str))) {
      LOG_WARN("transfer list to display str failed",
          KR(ret), K(part_list_str), K(new_part_list));
    } else if (OB_FAIL(new_not_exist_part_list.to_display_str(
        allocator,
        not_exist_part_list_str))) {
      LOG_WARN("transfer list to display str failed", KR(ret),
          K(new_not_exist_part_list), K(not_exist_part_list_str));
    } else if (OB_FAIL(new_lock_conflict_part_list.to_display_str(
        allocator,
        lock_conflict_part_list_str))) {
      LOG_WARN("transfer list to display str failed", KR(ret),
          K(new_lock_conflict_part_list), K(lock_conflict_part_list_str));
    } else if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("part_list", part_list_str))
        || OB_FAIL(dml_splicer.add_column("not_exist_part_list", not_exist_part_list_str))
        || OB_FAIL(dml_splicer.add_column("lock_conflict_part_list", lock_conflict_part_list_str))
        || OB_FAIL(dml_splicer.add_column("status", new_status.str()))
        || OB_FAIL(dml_splicer.add_column("result", result))
        || OB_FAIL(dml_splicer.add_column("comment", transfer_task_comment_to_str(comment)))) {
      LOG_WARN("fail to add column", KR(ret),
          K(tenant_id), K(task_id), K(new_status), K(result), K(comment));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("finish transfer_task from init success",
          K(tenant_id), K(task_id), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObTransferTaskOperator::remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE task_id = %ld",
      OB_ALL_TRANSFER_TASK_TNAME, task_id.id()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(task_id), K(affected_rows));
  } else if (OB_UNLIKELY(1 < affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete more than one row", KR(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
    LOG_INFO("remove transfer_task success", K(tenant_id), K(task_id));
  }
  return ret;
}

int ObTransferTaskOperator::update_status_and_result(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const ObTransferStatus &new_status,
    const int result,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid()
      || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id), K(group_id));
  } else if (OB_FAIL(ObTransferStatusHelper::check_can_change_status(
      old_status,
      new_status,
      can_change))) {
    LOG_WARN("fail to check can change status", KR(ret),
        K(old_status), K(new_status), K(tenant_id), K(task_id));
  } else if (OB_UNLIKELY(!can_change)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not change status", KR(ret),
        K(old_status), K(new_status), K(tenant_id), K(task_id));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("status", new_status.str()))
        || OB_FAIL(dml_splicer.add_column("result", result))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task_id), K(new_status));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), group_id, affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows && 0 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows",
          KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update status success", K(tenant_id), K(task_id),
          K(old_status), K(new_status), K(affected_rows));
    }
  }
  return ret;
}

int ObTransferTaskOperator::update_start_scn(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const share::SCN &start_scn,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid()
      || !old_status.is_valid()
      || !start_scn.is_valid()
      || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(tenant_id), K(task_id), K(old_status), K(start_scn), K(group_id));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;

    if (FAILEDx(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("start_scn", start_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task_id), K(start_scn));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", K(tenant_id), KR(ret));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), group_id, affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows && 0 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update start_scn success", K(tenant_id), K(task_id),
          K(old_status), K(start_scn), K(affected_rows));
    }
  }
  return ret;
}

int ObTransferTaskOperator::update_finish_scn(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &old_status,
    const share::SCN &finish_scn,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid()
      || !old_status.is_valid()
      || !finish_scn.is_valid()
      || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(tenant_id), K(task_id), K(old_status), K(finish_scn), K(group_id));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;

    if (FAILEDx(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("finish_scn", finish_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task_id), K(finish_scn));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", K(tenant_id), KR(ret));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql.append_fmt(" AND status='%s'", old_status.str()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql), K(old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), group_id, affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update finish_scn success", K(tenant_id), K(task_id),
          K(old_status), K(finish_scn), K(affected_rows));
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_by_ls_id_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool is_src_ls,
    const int32_t group_id,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(group_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (is_src_ls) {
        if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE src_ls = %ld",
            OB_ALL_TRANSFER_TASK_TNAME, ls_id.id()))) {
          LOG_WARN("fail to assign sql", KR(ret), K(ls_id));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE dest_ls = %ld",
            OB_ALL_TRANSFER_TASK_TNAME, ls_id.id()))) {
          LOG_WARN("fail to assign sql", KR(ret), K(ls_id));
        }
      }

      if (FAILEDx(sql_proxy.read(result, tenant_id, sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_transfer_task_(*result.get_result(), task))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("construct transfer task failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
        } else {
          LOG_WARN("dest ls transfer task not found", KR(ret), K(tenant_id), K(ls_id));
        }
      }
    }
  }

  return ret;
}

int ObTransferTaskOperator::construct_transfer_tasks_(
    common::sqlclient::ObMySQLResult &res,
    ObIArray<ObTransferTask> &tasks)
{
  int ret = OB_SUCCESS;
  tasks.reset();
  const bool with_time = false;
  int64_t create_time = OB_INVALID_TIMESTAMP; // unused
  int64_t finish_time = OB_INVALID_TIMESTAMP; // unused
  ObTransferTask task;
  while (OB_SUCC(ret)) {
    task.reset();
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
    } else if (OB_FAIL(parse_sql_result_(res, with_time, task, create_time, finish_time))) {
      LOG_WARN("parse sql result failed", KR(ret), K(task));
    } else if (OB_FAIL(tasks.push_back(task))) {
      LOG_WARN("fail to push back", KR(ret), K(task), K(tasks));
    }
  } // end while
  return ret;
}

int ObTransferTaskOperator::construct_transfer_task_(
    common::sqlclient::ObMySQLResult &res,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  const bool with_time = false;
  int64_t create_time = OB_INVALID_TIMESTAMP; // unused
  int64_t finish_time = OB_INVALID_TIMESTAMP; // unused
  if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("get next result failed", KR(ret));
    }
  } else if (OB_FAIL(parse_sql_result_(res, with_time, task, create_time, finish_time))) {
    LOG_WARN("parse sql result failed", KR(ret), K(task));
  } else if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get next result failed", KR(ret), K(task));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read more than one row", KR(ret), K(task));
  }
  return ret;
}

int ObTransferTaskOperator::parse_sql_result_(
    common::sqlclient::ObMySQLResult &res,
    const bool with_time,
    ObTransferTask &task,
    int64_t &create_time,
    int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  task.reset();
  create_time = OB_INVALID_TIMESTAMP;
  finish_time = OB_INVALID_TIMESTAMP;
  int64_t task_id = ObTransferTaskID::INVALID_ID;
  int64_t src_ls = ObLSID::INVALID_LS_ID;
  int64_t dest_ls = ObLSID::INVALID_LS_ID;
  ObString part_list_str;
  ObString not_exist_part_list_str;
  ObString lock_conflict_part_list_str;
  ObString table_lock_tablet_list_str;
  ObString tablet_list_str;
  uint64_t start_scn_val = OB_INVALID_SCN_VAL;
  uint64_t finish_scn_val = OB_INVALID_SCN_VAL;
  ObString status_str;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  int64_t real_length = 0;
  int64_t result = -1;
  ObString comment;
  int64_t balance_task_id = ObBalanceTaskID::INVALID_ID;
  int64_t table_lock_owner_id = ObTableLockOwnerID::INVALID_ID;
  ObTransferStatus status;
  SCN start_scn;
  SCN finish_scn;
  common::ObCurTraceId::TraceId trace_id;

  if (with_time) {
    (void)GET_COL_IGNORE_NULL(res.get_int, "create_time", create_time);
    (void)GET_COL_IGNORE_NULL(res.get_int, "finish_time", finish_time);
  }
  (void)GET_COL_IGNORE_NULL(res.get_int, "task_id", task_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "src_ls", src_ls);
  (void)GET_COL_IGNORE_NULL(res.get_int, "dest_ls", dest_ls);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "part_list", part_list_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "not_exist_part_list", not_exist_part_list_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "lock_conflict_part_list", lock_conflict_part_list_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "table_lock_tablet_list", table_lock_tablet_list_str);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "tablet_list", tablet_list_str);
  (void)GET_COL_IGNORE_NULL(res.get_uint, "start_scn", start_scn_val);
  (void)GET_COL_IGNORE_NULL(res.get_uint, "finish_scn", finish_scn_val);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "status", status_str);
  (void)GET_COL_IGNORE_NULL(res.get_int, "result", result);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "comment", comment);
  (void)GET_COL_IGNORE_NULL(res.get_int, "balance_task_id", balance_task_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "table_lock_owner_id", table_lock_owner_id);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "trace_id", trace_id_buf, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);

  if (OB_FAIL(ret)) {
  } else if (start_scn_val != OB_INVALID_SCN_VAL
      && OB_FAIL(start_scn.convert_for_inner_table_field(start_scn_val))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(start_scn_val));
  } else if (finish_scn_val != OB_INVALID_SCN_VAL
      && OB_FAIL(finish_scn.convert_for_inner_table_field(finish_scn_val))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(finish_scn_val));
  } else if (OB_FAIL(trace_id.parse_from_buf(trace_id_buf))) {
    LOG_WARN("failed to parse trace id from buf", KR(ret), K(trace_id_buf));
  } else if (OB_FAIL(status.parse_from_str(status_str))) {
    LOG_WARN("fail to parse from str", KR(ret), K(status_str));
  } else if (OB_FAIL(task.init(
      ObTransferTaskID(task_id),
      ObLSID(src_ls),
      ObLSID(dest_ls),
      part_list_str,
      not_exist_part_list_str,
      lock_conflict_part_list_str,
      table_lock_tablet_list_str,
      tablet_list_str,
      start_scn,
      finish_scn,
      status,
      trace_id,
      static_cast<int32_t>(result),
      str_to_transfer_task_comment(comment),
      ObBalanceTaskID(balance_task_id),
      ObTableLockOwnerID(table_lock_owner_id)))) {
    LOG_WARN("fail to init transfer task", KR(ret), K(task_id), K(src_ls), K(dest_ls), K(part_list_str),
        K(not_exist_part_list_str), K(lock_conflict_part_list_str), K(table_lock_tablet_list_str), K(tablet_list_str), K(start_scn),
        K(finish_scn), K(status), K(trace_id), K(result), K(comment), K(balance_task_id), K(table_lock_owner_id));
  }

  return ret;
}

int ObTransferTaskOperator::construct_task_status_(
    common::sqlclient::ObMySQLResult &res,
    ObIArray<ObTransferTask::TaskStatus> &task_status)
{
  int ret = OB_SUCCESS;
  task_status.reset();
  ObTransferTask::TaskStatus stat;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
    } else {
      stat.reset();
      int64_t task_id = ObTransferTaskID::INVALID_ID;
      ObString status_str;
      ObTransferStatus status;

      EXTRACT_INT_FIELD_MYSQL(res, "task_id", task_id, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(res, "status", status_str);

      if (FAILEDx(status.parse_from_str(status_str))) {
        LOG_WARN("fail to parse from str", KR(ret), K(status_str));
      } else if (OB_FAIL(stat.init(ObTransferTaskID(task_id), status))) {
        LOG_WARN("fail to init transfer task", KR(ret), K(task_id), K(status));
      } else if (OB_FAIL(task_status.push_back(stat))) {
        LOG_WARN("fail to push back", KR(ret), K(stat), K(task_status));
      }
    }
  } // end while
  return ret;
}

int ObTransferTaskOperator::insert_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTask &task,
    const int64_t create_time,
    const int64_t finish_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    ObArenaAllocator allocator;

    if (OB_FAIL(fill_dml_splicer_(dml_splicer, allocator, task))) {
      LOG_WARN("fail to fill dml splicer", KR(ret), K(tenant_id), K(task));
    } else if (OB_FAIL(dml_splicer.add_time_column("create_time", create_time))
        || OB_FAIL(dml_splicer.add_time_column("finish_time", finish_time))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task), K(create_time), K(finish_time));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(tenant_id), K(task), K(create_time), K(finish_time));
    } else if (OB_FAIL(dml_splicer.splice_insert_sql(OB_ALL_TRANSFER_TASK_HISTORY_TNAME, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_ENTRY_EXIST;
      }
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows), K(task));
    } else {
      LOG_INFO("insert transfer task history success",
          K(tenant_id), K(affected_rows), K(task), K(create_time), K(finish_time));
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_history_task(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    ObTransferTask &task,
    int64_t &create_time,
    int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  task.reset();
  create_time = OB_INVALID_TIMESTAMP;
  finish_time = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      bool with_time = true;
      common::sqlclient::ObMySQLResult *res = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) AS create_time, "
          "time_to_usec(gmt_modified) AS finish_time, * FROM %s WHERE task_id = %ld",
          OB_ALL_TRANSFER_TASK_HISTORY_TNAME, task_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not found", KR(ret), K(tenant_id), K(task_id));
        } else {
          LOG_WARN("next failed", KR(ret), K(sql));
        }
      } else if (OB_FAIL(parse_sql_result_(*res, with_time, task, create_time, finish_time))) {
        LOG_WARN("parse sql result failed", KR(ret));
      } else {
        LOG_TRACE("get history task status success", KR(ret), K(tenant_id), K(task_id), K(task));
      }
    }
  }
  return ret;
}

int ObTransferTaskOperator::get_max_task_id_from_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    ObTransferTaskID &max_task_id)
{
  int ret = OB_SUCCESS;
  max_task_id.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      int64_t max_task_id_int64 = ObTransferTaskID::INVALID_ID;
      if (OB_FAIL(sql.assign_fmt("SELECT max(task_id) as max_task_id FROM %s",
          OB_ALL_TRANSFER_TASK_HISTORY_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        LOG_WARN("next failed", KR(ret), K(sql));
      } else if (OB_FAIL(res->get_int("max_task_id", max_task_id_int64))) {
        if (OB_ERR_NULL_VALUE == ret) {
          max_task_id.reset(); // return INVALID_ID when history is empty
          LOG_TRACE("transfer history is empty", KR(ret), K(tenant_id), K(max_task_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get max task id failed", KR(ret), K(max_task_id), K(sql));
        }
      } else {
        max_task_id = max_task_id_int64;
        LOG_TRACE("get max transfer task_id from history success", KR(ret), K(tenant_id), K(max_task_id));
      }
    }
  }
  return ret;
}

int ObTransferTaskOperator::update_comment(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferTaskComment &comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id), K(comment));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;

    if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id.id()))
        || OB_FAIL(dml_splicer.add_column("comment", transfer_task_comment_to_str(comment)))) {
      LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(task_id), K(comment));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("fail to finish row", K(tenant_id), KR(ret));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TRANSFER_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows && 0 != affected_rows)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incorrect affected rows", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update comment successfully", K(tenant_id), K(task_id), K(comment), K(affected_rows));
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
