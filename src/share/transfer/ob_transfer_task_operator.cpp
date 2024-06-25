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
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TRANSFER_TASK_TNAME
#include "share/location_cache/ob_location_struct.h" // ObTabletLSCache

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
      } else if (OB_FAIL(construct_transfer_task_(tenant_id, *result.get_result(), task))) {
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
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) AS create_time_int64, "
          "time_to_usec(gmt_modified) AS finish_time_int64, * FROM %s WHERE task_id = %ld%s",
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
      } else if (OB_FAIL(parse_sql_result_(tenant_id, *res, with_time, task, create_time, finish_time))) {
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
      } else if (OB_FAIL(construct_transfer_tasks_(tenant_id, *result.get_result(), tasks))) {
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
      || OB_FAIL(dml_splicer.add_column("table_lock_owner_id", task.get_table_lock_owner_id().raw_value()))) {
    LOG_WARN("fail to add column", KR(ret), K(task));
  }
  if (OB_FAIL(ret)) {
  } else if (task.get_data_version() < MOCK_DATA_VERSION_4_2_3_0
      || (task.get_data_version() >= DATA_VERSION_4_3_0_0 && task.get_data_version() < DATA_VERSION_4_3_2_0)) {
    // do nothing
  } else {
    char version_buf[common::OB_CLUSTER_VERSION_LENGTH] = {'\0'};
    int64_t len = ObClusterVersion::print_version_str(
        version_buf,
        common::OB_CLUSTER_VERSION_LENGTH,
        task.get_data_version());
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(len < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid version", KR(ret), K(task));
    } else if (OB_FAIL(dml_splicer.add_column("data_version", version_buf))) {
      LOG_WARN("add column failed", KR(ret), K(task));
    }
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
        || OB_FAIL(dml_splicer.add_column("table_lock_owner_id", table_lock_owner_id.raw_value()))) {
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
      } else if (OB_FAIL(construct_transfer_task_(tenant_id, *result.get_result(), task))) {
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
    const uint64_t tenant_id,
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
    } else if (OB_FAIL(parse_sql_result_(tenant_id, res, with_time, task, create_time, finish_time))) {
      LOG_WARN("parse sql result failed", KR(ret), K(tenant_id), K(task));
    } else if (OB_FAIL(tasks.push_back(task))) {
      LOG_WARN("fail to push back", KR(ret), K(task), K(tasks));
    }
  } // end while
  return ret;
}

int ObTransferTaskOperator::construct_transfer_task_(
    const uint64_t tenant_id,
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
  } else if (OB_FAIL(parse_sql_result_(tenant_id, res, with_time, task, create_time, finish_time))) {
    LOG_WARN("parse sql result failed", KR(ret), K(tenant_id), K(task));
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
    const uint64_t tenant_id,
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
  int64_t lock_owner_val = ObTableLockOwnerID::INVALID_ID;
  ObTransferStatus status;
  SCN start_scn;
  SCN finish_scn;
  ObTableLockOwnerID owner_id;
  common::ObCurTraceId::TraceId trace_id;
  uint64_t data_version = 0;
  ObString data_version_str;
  bool data_version_is_null = false;
  bool data_version_not_exist = false;

  if (with_time) {
    (void)GET_COL_IGNORE_NULL(res.get_int, "create_time_int64", create_time);
    (void)GET_COL_IGNORE_NULL(res.get_int, "finish_time_int64", finish_time);
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
  (void)GET_COL_IGNORE_NULL(res.get_int, "table_lock_owner_id", lock_owner_val);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "trace_id", trace_id_buf, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET_WITH_COLUMN_INFO(res, "data_version", data_version_str,
      data_version_is_null, data_version_not_exist);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(data_version_is_null)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_version can not be null", KR(ret), K(data_version_is_null));
  } else if (OB_FAIL(convert_data_version_(
      tenant_id,
      with_time,
      data_version_not_exist,
      data_version_str,
      data_version))) {
    LOG_WARN("convert data_version failed", KR(ret), K(tenant_id),
        K(with_time), K(data_version_not_exist), K(data_version_str));
  }

  if (OB_FAIL(ret)) {
  } else if (start_scn_val != OB_INVALID_SCN_VAL
      && OB_FAIL(start_scn.convert_for_inner_table_field(start_scn_val))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(start_scn_val));
  } else if (finish_scn_val != OB_INVALID_SCN_VAL
      && OB_FAIL(finish_scn.convert_for_inner_table_field(finish_scn_val))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(finish_scn_val));
  } else if (OB_FAIL(owner_id.convert_from_value(lock_owner_val))) {
    LOG_WARN("fail to convert to owner id", K(ret), K(lock_owner_val));
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
      owner_id,
      data_version))) {
    LOG_WARN("fail to init transfer task", KR(ret), K(task_id), K(src_ls), K(dest_ls), K(part_list_str),
        K(not_exist_part_list_str), K(lock_conflict_part_list_str), K(table_lock_tablet_list_str), K(tablet_list_str), K(start_scn),
        K(finish_scn), K(status), K(trace_id), K(result), K(comment), K(balance_task_id), K(owner_id), K(data_version));
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
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(create_time) AS create_time_int64, "
          "time_to_usec(finish_time) AS finish_time_int64, * FROM %s WHERE task_id = %ld",
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
      } else if (OB_FAIL(parse_sql_result_(tenant_id, *res, with_time, task, create_time, finish_time))) {
        LOG_WARN("parse sql result failed", KR(ret), K(tenant_id));
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

int ObTransferTaskOperator::get_last_task_by_balance_task_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObBalanceTaskID &balance_task_id,
    ObTransferTask &last_task,
    int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  last_task.reset();
  finish_time = OB_INVALID_TIMESTAMP;
  int64_t create_time = OB_INVALID_TIMESTAMP;
  const bool with_time = true;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(balance_task_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(create_time) AS create_time_int64, "
          "time_to_usec(finish_time) AS finish_time_int64, * FROM %s "
          "WHERE balance_task_id = %ld order by task_id desc limit 1",
          OB_ALL_TRANSFER_TASK_HISTORY_TNAME,
          balance_task_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(balance_task_id));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_TRACE("task not found", KR(ret), K(tenant_id), K(balance_task_id));
        } else {
          LOG_WARN("next failed", KR(ret), K(sql));
        }
      } else if (OB_FAIL(parse_sql_result_(tenant_id, *res, with_time, last_task, create_time, finish_time))) {
        LOG_WARN("parse sql result failed", KR(ret), K(tenant_id), K(with_time), K(sql));
      } else {
        LOG_TRACE("get last task by balance task id from history", KR(ret),
            K(tenant_id), K(balance_task_id), K(last_task), K(create_time), K(finish_time));
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

int ObTransferTaskOperator::generate_transfer_task_id(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    ObTransferTaskID &new_task_id)
{
  int ret = OB_SUCCESS;
  new_task_id.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT MAX(task_id) AS task_id, 0 AS is_history FROM %s "
        "UNION SELECT MAX(task_id) AS task_id, 1 AS is_history FROM %s",
        OB_ALL_TRANSFER_TASK_TNAME, OB_ALL_TRANSFER_TASK_HISTORY_TNAME))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
    } else {
      int64_t row_count = 0;
      int64_t max_task_id = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          row_count++;
          int64_t task_id = OB_INVALID_ID; // NULL means empty, then convert to 0
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
            *result, "task_id", task_id, int64_t,
            true /*skip_null_error*/, false /*skip_column_error*/, 0 /*default_value*/);
          if (OB_SUCC(ret)) {
            max_task_id = max(max_task_id, task_id);
          }
        }
      } // end while

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(2 != row_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_count not match", KR(ret), K(tenant_id), K(row_count));
        } else {
          new_task_id = ObTransferTaskID(max_task_id + 1);
        }
      }
    }
    } // end SMART_VAR
  }
  return ret;
}

int ObTransferTaskOperator::fetch_initial_base_task_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    ObTransferTaskID &base_task_id)
{
  int ret = OB_SUCCESS;
  base_task_id.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT MIN(task_id) AS task_id, 0 AS is_history FROM %s "
        "UNION SELECT MAX(task_id) AS task_id, 1 AS is_history FROM %s",
        OB_ALL_TRANSFER_TASK_TNAME, OB_ALL_TRANSFER_TASK_HISTORY_TNAME))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
    } else {
      int64_t min_process_task_id = OB_INVALID_ID;
      int64_t max_history_task_id = OB_INVALID_ID;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          int64_t task_id = OB_INVALID_ID; // NULL means empty, then convert to 0
          int64_t is_history = 0;
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
            *result, "task_id", task_id, int64_t,
            true /*skip_null_error*/, false /*skip_column_error*/, 0 /*default_value*/);
          EXTRACT_INT_FIELD_MYSQL(*result, "is_history", is_history, int64_t);

          if (OB_FAIL(ret)) {
          } else if (0 == is_history) {
            min_process_task_id = task_id;
          } else {
            max_history_task_id = task_id;
          }
        }
      } // end while

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(min_process_task_id < 0 || max_history_task_id < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid result", KR(ret), K(tenant_id), K(min_process_task_id), K(max_history_task_id));
        } else if (0 == min_process_task_id && 0 == max_history_task_id) {
          // __all_transfer_task/__all_transfer_task_history are empty
          base_task_id = ObTransferTaskID(0);
        } else if (0 == min_process_task_id && 0 < max_history_task_id) {
          // only __all_transfer_task is empty
          base_task_id = ObTransferTaskID(max_history_task_id);
        } else if (0 < min_process_task_id && 0 == max_history_task_id) {
          // only __all_transfer_task_history is empty
          base_task_id = ObTransferTaskID(min_process_task_id - 1);
        } else {
          // __all_transfer_task/__all_transfer_task_history are not empty
          int64_t min_task_id = min(min_process_task_id - 1, max_history_task_id);
          base_task_id = ObTransferTaskID(min_task_id);
        }
      }
    }
    } // end SMART_VAR
  }
  return ret;
}

int ObTransferTaskOperator::fetch_inc_task_infos(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTransferTaskID &base_task_id,
    common::ObIArray<ObTransferRefreshInfo> &inc_task_infos)
{
  int ret = OB_SUCCESS;
  inc_task_infos.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || !base_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id/base_task_id", KR(ret), K(tenant_id), K(base_task_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = NULL;
    // transfer task which status is `completed` but tablet_list is invalid actually do nothing.
    ObTransferStatus complete_status(ObTransferStatus::COMPLETED);
    ObTransferStatus fail_status(ObTransferStatus::FAILED);

#define FETCH_INC_TRANSFER_TASKS_SQL "SELECT task_id, " \
    "(CASE WHEN status = '%s' AND (tablet_list is null OR tablet_list = '') THEN '%s' ELSE status END) AS STATUS " \
    "FROM %s WHERE task_id > %ld "

    if (OB_FAIL(sql.assign_fmt(
        FETCH_INC_TRANSFER_TASKS_SQL " UNION " FETCH_INC_TRANSFER_TASKS_SQL,
        complete_status.str(), fail_status.str(), OB_ALL_TRANSFER_TASK_TNAME, base_task_id.id(),
        complete_status.str(), fail_status.str(), OB_ALL_TRANSFER_TASK_HISTORY_TNAME, base_task_id.id()
        ))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
    } else {
      int64_t task_id_val = ObTransferTaskID::INVALID_ID;
      ObString status_str;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id_val, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "STATUS", status_str);

          ObTransferStatus status;
          if (FAILEDx(status.parse_from_str(status_str))) {
            LOG_WARN("fail to parse status", KR(ret), K(status_str));
          } else {
            ObTransferTaskID task_id(task_id_val);
            ObTransferRefreshStatus refresh_status;
            ObTransferRefreshInfo refresh_info;
            (void) refresh_status.convert_from(status);
            if (OB_UNLIKELY(!task_id.is_valid() || !refresh_status.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invaild task_id/refresh_status",
                       KR(ret), K(tenant_id), K(task_id), K(refresh_status), K(status));
            } else if (OB_FAIL(refresh_info.init(task_id, refresh_status))) {
              LOG_WARN("fail to init refresh info",
                       KR(ret), K(tenant_id), K(task_id), K(refresh_status));
            } else if (OB_FAIL(inc_task_infos.push_back(refresh_info))) {
              LOG_WARN("fail to push back refresh info",
                       KR(ret), K(tenant_id), K(refresh_info));
            }
          }
        }
      } // end while
    }
    } // end SMART_VAR
#undef TRANSFER_TASK_COLUMNS
  }
  return ret;
}

int ObTransferTaskOperator::batch_get_tablet_ls_cache(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObIArray<ObTransferTaskID> &task_ids,
    common::ObIArray<ObTabletLSCache> &tablet_ls_caches)
{
  int ret = OB_SUCCESS;
  tablet_ls_caches.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || task_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id/task_ids_cnt",
             KR(ret), K(tenant_id), "task_ids_cnt", task_ids.count());
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = NULL;

    ObSqlString task_ids_str;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_ids.count(); i++) {
      if (OB_FAIL(task_ids_str.append_fmt("%s%ld",
          0 == i ? "" : ", ", task_ids.at(i).id()))) {
        LOG_WARN("fail to append fmt", KR(ret), K(i), K(task_ids.at(i)));
      }
    } // end for

    if (FAILEDx(sql.assign_fmt(
        "SELECT tablet_list, dest_ls FROM %s WHERE task_id in (%s) "
        "UNION SELECT tablet_list, dest_ls FROM %s WHERE task_id in (%s)",
        OB_ALL_TRANSFER_TASK_TNAME, task_ids_str.ptr(),
        OB_ALL_TRANSFER_TASK_HISTORY_TNAME, task_ids_str.ptr()))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
    } else {
      const int64_t now = ObTimeUtility::fast_current_time();
      int64_t row_cnt = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else {
          row_cnt++;
          ObString tablet_list_str;
          int64_t dest_ls_id = ObLSID::INVALID_LS_ID;
          EXTRACT_INT_FIELD_MYSQL(*result, "dest_ls", dest_ls_id, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tablet_list", tablet_list_str);

          ObTransferTabletList tablet_list;
          ObLSID ls_id(dest_ls_id);
          if (FAILEDx(tablet_list.parse_from_display_str(tablet_list_str))) {
            LOG_WARN("fail to parse from str", KR(ret), K(tablet_list_str));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); i++) {
              const ObTransferTabletInfo &tablet_info = tablet_list.at(i);
              ObTabletLSCache tablet_ls_cache;
              // `transfer_seq` in __all_transfer_task/__all_transfer_task_history means
              // the original `transfer_seq` before transfer task execute.
              // We should use `transfer_seq` + 1 as the result of related transfer task.
              int64_t transfer_seq = tablet_info.transfer_seq() + 1;
              if (OB_FAIL(tablet_ls_cache.init(
                  tenant_id,
                  tablet_info.tablet_id(),
                  ls_id,
                  now,
                  transfer_seq))) {
                LOG_WARN("fail to init tablet-ls cache",
                         KR(ret), K(tenant_id), K(ls_id), K(tablet_info), K(transfer_seq));
              } else if (OB_FAIL(tablet_ls_caches.push_back(tablet_ls_cache))) {
                LOG_WARN("fail to push back tablet-ls cache", KR(ret), K(tablet_ls_cache));
              }
            } // end for
          }

        }
      } // end while

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(task_ids.count() != row_cnt
            || tablet_ls_caches.count() <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result not match", KR(ret), K(tenant_id), K(row_cnt),
                   K(task_ids.count()), K(tablet_ls_caches.count()), K(task_ids));
        }
      }
    }

    } // end SMART_VAR
  }
  return ret;
}

int ObTransferTaskOperator::convert_data_version_(
    const uint64_t tenant_id,
    const bool is_history,
    const bool column_not_exist,
    const ObString &data_version_str,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(is_history), K(data_version_str));
  } else if (column_not_exist) {
    bool column_exist_double_check = false;
    const ObString column_name("data_version");
    const uint64_t table_id = is_history ? OB_ALL_TRANSFER_TASK_HISTORY_TID : OB_ALL_TRANSFER_TASK_TID;
    if (OB_FAIL(ObSchemaUtils::check_whether_column_exist(
        tenant_id,
        table_id,
        column_name,
        column_exist_double_check))) { // contain an inner sql
      LOG_WARN("check column exist failed", KR(ret), K(tenant_id), K(table_id), K(column_name));
    } else if (!column_exist_double_check) {
      // 1. tenant data_version is old, column does not exist
      data_version = DEFAULT_MIN_DATA_VERSION;
    } else { // 2. column exists but schema is old
      ret = OB_NEED_RETRY;
      LOG_WARN("schema is old, can not read column data_version",
          KR(ret), K(tenant_id), K(table_id), K(column_name));
    }
  } else if (data_version_str.empty()) {
    data_version = DEFAULT_MIN_DATA_VERSION;
  } else if (OB_FAIL(ObClusterVersion::get_version(data_version_str, data_version))) {
    LOG_WARN("get version failed", KR(ret), K(data_version_str), K(data_version));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
