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

namespace oceanbase
{
namespace rootserver
{

int ObLSReplicaTaskTableOperator::delete_task(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const share::ObLSID& ls_id,
    const ObDRTaskType& task_type,
    const share::ObTaskId& task_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString sql;
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
               || !ls_id.is_valid()
               || ObDRTaskType::MAX_TYPE == task_type
               || task_id.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_type), K(task_id));
  } else if (false == task_id.to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %lu "
                          "AND task_type = '%s' AND task_id = '%s'",
                          share::OB_ALL_LS_REPLICA_TASK_TNAME,
                          tenant_id,
                          ls_id.id(),
                          ob_disaster_recovery_task_type_strs(task_type),
                          task_id_to_set))) {
    LOG_WARN("assign sql string failed", KR(ret), K(tenant_id),
            K(ls_id), K(task_type), K(task_id_to_set));
  } else if (OB_FAIL(trans.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql), K(sql_tenant_id));
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::insert_task(
    common::ObISQLClient &sql_proxy,
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(task.fill_dml_splicer(dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(task));
  } else if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_LS_REPLICA_TASK_TNAME, sql))) {
    LOG_WARN("fail to splice insert update sql", KR(ret), K(task));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(task.get_tenant_id()), K(sql_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert is not single row", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::finish_task(
    common::ObMySQLTransaction& trans,
    const ObDRTaskTableUpdateTask& task)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  int64_t insert_rows = 0;
  int64_t delete_rows = 0;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(task.get_tenant_id()), tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(task));
  } else if (is_manual_dr_task_data_version_match(tenant_data_version)) {
    char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
    int64_t schedule_time = 0;
    ObSqlString execute_result;
    ObSqlString condition_sql;
    ObSqlString sql;
    uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
    const char* table_column = "tenant_id, ls_id, task_type, task_id, priority, target_replica_svr_ip, target_replica_svr_port, target_paxos_replica_number,"
      "target_replica_type, source_replica_svr_ip, source_replica_svr_port, source_paxos_replica_number, source_replica_type, task_exec_svr_ip, task_exec_svr_port,"
      "generate_time, schedule_time, comment, data_source_svr_ip, data_source_svr_port, is_manual";
    // no task_status
    ObDRLSReplicaTaskStatus task_status(ObDRLSReplicaTaskStatus::COMPLETED);
    if (OB_CANCELED == task.get_ret_code()) {
      task_status = ObDRLSReplicaTaskStatus::CANCELED;
    } else if (OB_SUCCESS != task.get_ret_code()) {
      task_status = ObDRLSReplicaTaskStatus::FAILED;
    }
    if (false == task.get_task_id().to_string(task_id_to_set, sizeof(task_id_to_set))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert task id to string failed", KR(ret), K(task));
    } else if (OB_FAIL(get_task_schedule_time_(trans, task, schedule_time))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("task count is 0", KR(ret), K(task));
      } else {
        LOG_WARN("faild to get task schedule_time", KR(ret), K(task));
      }
    } else if (OB_FAIL(build_execute_result(task.get_ret_code(),
                                            task.get_ret_comment(),
                                            schedule_time,
                                            execute_result))) {
      LOG_WARN("build_execute_result failed", KR(ret), K(task), K(schedule_time));
    } else if (OB_FAIL(condition_sql.assign_fmt("tenant_id = %lu AND ls_id = %lu AND task_type = '%s' AND task_id = '%s'",
                        task.get_tenant_id(), task.get_ls_id().id(),
                        ob_disaster_recovery_task_type_strs(task.get_task_type()), task_id_to_set))) {
      LOG_WARN("failed to append sql", KR(ret), K(task), K(task_id_to_set));
    } else if (OB_FAIL(sql.assign_fmt("insert into %s (%s, task_status, execute_result, finish_time) "
                            " select %s, '%s', '%s', now() from %s where %s",
                            share::OB_ALL_LS_REPLICA_TASK_HISTORY_TNAME, table_column, table_column,
                            task_status.get_status_str(), execute_result.ptr(),
                            share::OB_ALL_LS_REPLICA_TASK_TNAME, condition_sql.ptr()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(task_status), K(execute_result), K(condition_sql));
    } else if (OB_FAIL(trans.write(sql_tenant_id, sql.ptr(), insert_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(sql));
    } else if (insert_rows != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql insert error", KR(ret), K(insert_rows), K(task));
    }
  }
  if (FAILEDx(delete_task(trans, task.get_tenant_id(), task.get_ls_id(), task.get_task_type(),
                          task.get_task_id(), delete_rows))) {
    LOG_WARN("delete_task failed", KR(ret), K(task));
  } else if (!is_single_row(delete_rows)) {
    // ignore affected row check for task not exist
    LOG_INFO("expected deleted single row", K(delete_rows), K(task));
  }// during the upgrade process, it is possible that insert_rows is 0 and delete_rows is 1.
  return ret;
}

int ObLSReplicaTaskTableOperator::get_task_schedule_time_(
    common::ObMySQLTransaction& trans,
    const ObDRTaskTableUpdateTask &task,
    int64_t &schedule_time)
{
  int ret = OB_SUCCESS;
  schedule_time = 0;
  uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (false == task.get_task_id().to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), K(task));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(schedule_time) AS schedule_time FROM %s WHERE "
             "tenant_id = %lu AND ls_id = %lu AND task_type = '%s' AND task_id = '%s'",
                        share::OB_ALL_LS_REPLICA_TASK_TNAME,
                        task.get_tenant_id(),
                        task.get_ls_id().id(),
                        ob_disaster_recovery_task_type_strs(task.get_task_type()),
                        task_id_to_set))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id_to_set), K(task));
      } else if (OB_FAIL(trans.read(res, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("fail to get next result", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "schedule_time", schedule_time, int64_t);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObLSReplicaTaskTableOperator::get_task_info_for_cancel(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const share::ObTaskId &task_id,
    common::ObAddr &task_execute_server,
    share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  task_execute_server.reset();
  ls_id.reset();
  int64_t ls_id_res = share::ObLSID::INVALID_LS_ID;
  common::ObString server_ip;
  int64_t server_port = OB_INVALID_INDEX;
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else if (false == task_id.to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), K(task_id));
  } else {
    uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT ls_id, task_exec_svr_ip, task_exec_svr_port FROM %s WHERE "
             "tenant_id = %lu AND task_id = '%s'", share::OB_ALL_LS_REPLICA_TASK_TNAME, tenant_id, task_id_to_set))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id_to_set), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(res, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("fail to get next result", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id_res, int64_t);
        (void)GET_COL_IGNORE_NULL(result->get_varchar, "task_exec_svr_ip", server_ip);
        (void)GET_COL_IGNORE_NULL(result->get_int, "task_exec_svr_port", server_port);
        if (OB_FAIL(ret)) {
        } else if (false == task_execute_server.set_ip_addr(server_ip, static_cast<uint32_t>(server_port))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid server address", K(server_ip), K(server_port));
        } else if (OB_UNLIKELY(!task_execute_server.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid task_execute_server", KR(ret), K(task_execute_server));
        } else {
          ls_id = ls_id_res;
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase