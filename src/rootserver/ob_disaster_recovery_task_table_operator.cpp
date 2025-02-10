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
#define USING_LOG_PREFIX RS
#include "ob_disaster_recovery_task_table_operator.h"
#include "ob_disaster_recovery_task_utils.h"                   // for DisasterRecoveryUtils
#include "lib/container/ob_se_array.h"                         // for ObSEArray

namespace oceanbase
{
namespace rootserver
{

int ObLSReplicaTaskTableOperator::delete_task(
    common::ObISQLClient &sql_proxy,
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (false == task.get_task_id().to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), K(task));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %lu "
                          "AND task_type = '%s' AND task_id = '%s'",
                          share::OB_ALL_LS_REPLICA_TASK_TNAME,
                          task.get_tenant_id(),
                          task.get_ls_id().id(),
                          ob_disaster_recovery_task_type_strs(task.get_disaster_recovery_task_type()),
                          task_id_to_set))) {
    LOG_WARN("assign sql string failed", KR(ret), K(task));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql), K(sql_tenant_id));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_EAGAIN;
    // may be delete row multiple times
    LOG_WARN("delete task not single row", KR(ret), K(affected_rows), K(task));
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::insert_task(
    common::ObISQLClient &sql_proxy,
    const ObDRTask &task,
    const bool record_history)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  const char *table_name = record_history
                         ? share::OB_ALL_LS_REPLICA_TASK_HISTORY_TNAME
                         : share::OB_ALL_LS_REPLICA_TASK_TNAME;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(task.fill_dml_splicer(dml, record_history))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(task));
  } else if (OB_FAIL(dml.splice_insert_sql(table_name, sql))) {
    LOG_WARN("fail to splice insert update sql", KR(ret), K(task));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(task.get_tenant_id()), K(sql_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert is not single row", KR(ret), K(affected_rows));
  }
  return ret;
}

#define BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(TASK_TYPE)                                                         \
  SMART_VAR(TASK_TYPE, task) {                                                                                            \
    void *raw_ptr = nullptr;                                                                                              \
    ObDRTask *new_task = nullptr;                                                                                         \
    if (OB_FAIL(task.build_task_from_sql_result(res))) {                                                                  \
      LOG_WARN("fail to build task info from res", KR(ret));                                                              \
    } else if (OB_ISNULL(raw_ptr = allocator.alloc(task.get_clone_size()))) {                                             \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                                                    \
      LOG_WARN("fail to alloc task", KR(ret), "size", task.get_clone_size());                                             \
    } else if (OB_FAIL(task.clone(raw_ptr, new_task))) {                                                                  \
      LOG_WARN("fail to clone task", KR(ret), K(task));                                                                   \
    } else if (OB_ISNULL(new_task)) {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                                            \
      LOG_WARN("new task ptr is null", KR(ret));                                                                          \
    } else if (OB_FAIL(dr_tasks.push_back(new_task))) {                                                                   \
      LOG_WARN("fail to load task into schedule list", KR(ret));                                                          \
    }                                                                                                                     \
  }                                                                                                                       \
// if failed, no need free here

int ObLSReplicaTaskTableOperator::read_task_from_result_(
    common::ObArenaAllocator& allocator,
    sqlclient::ObMySQLResult &res,
    ObIArray<ObDRTask*> &dr_tasks)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else {
      common::ObString task_type;
      (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_type", task_type);
      if (common::ObString("MIGRATE REPLICA") == task_type) {
        BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(ObMigrateLSReplicaTask)
      } else if (common::ObString("ADD REPLICA") == task_type) {
        BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(ObAddLSReplicaTask)
      } else if (common::ObString("TYPE TRANSFORM") == task_type) {
        BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(ObLSTypeTransformTask)
      } else if (common::ObString("REMOVE PAXOS REPLICA") == task_type
              || common::ObString("REMOVE NON PAXOS REPLICA") == task_type) {
        BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(ObRemoveLSReplicaTask)
      } else if (common::ObString("MODIFY PAXOS REPLICA NUMBER") == task_type) {
        BUILD_TASK_FROM_SQL_RESULT_AND_PUSH_INTO_ARRAY(ObLSModifyPaxosReplicaNumberTask)
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected task type", KR(ret), K(task_type));
      }
    }
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::load_task_from_inner_table(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    common::ObArenaAllocator& allocator,
    ObIArray<ObDRTask*> &dr_tasks)
{
  int ret = OB_SUCCESS;
  dr_tasks.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(sql));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(read_task_from_result_(allocator, *result.get_result(), dr_tasks))) {
        LOG_WARN("read task from result failed", KR(ret), K(tenant_id), K(sql_tenant_id));
      }
    }
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::read_task_key_from_result_(
    sqlclient::ObMySQLResult &res,
    ObIArray<ObDRTaskKey> &dr_task_keys)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else {
      ObDRTaskKey task_key;
      if (OB_FAIL(task_key.build_task_key_from_sql_result(res))) {
        LOG_WARN("build task_key from sql result failed", KR(ret), K(task_key));
      } else if (OB_FAIL(dr_task_keys.push_back(task_key))) {
        LOG_WARN("push back task_key failed", KR(ret), K(task_key));
      }
    }
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::load_task_key_from_inner_table(
    common::ObISQLClient &sql_proxy,
    const ObSqlString &sql,
    const uint64_t tenant_id,
    ObIArray<ObDRTaskKey> &dr_task_keys)
{
  int ret = OB_SUCCESS;
  dr_task_keys.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(sql));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(read_task_key_from_result_(*result.get_result(), dr_task_keys))) {
        LOG_WARN("read task from result failed", KR(ret), K(tenant_id), K(sql_tenant_id));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase