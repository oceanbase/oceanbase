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
#include "ob_all_virtual_ls_replica_task_plan.h"
#include "rootserver/ob_disaster_recovery_worker.h"
#include "share/ob_all_server_tracer.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver
{

ObAllVirtualLSReplicaTaskPlan::ObAllVirtualLSReplicaTaskPlan()
  :inited_(false),
   schema_service_(nullptr),
   task_worker_(nullptr),
   arena_allocator_()
{
}

ObAllVirtualLSReplicaTaskPlan::~ObAllVirtualLSReplicaTaskPlan()
{
}

int ObAllVirtualLSReplicaTaskPlan::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObDRWorker &task_worker)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    schema_service_ = &schema_service;
    task_worker_ = &task_worker;
    inited_ = true;
  }

  return ret;
}

int ObAllVirtualLSReplicaTaskPlan::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_id_array;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(task_worker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ or task_worker_ is nullptr", KR(ret), KP(schema_service_), KP(task_worker_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get sys tenant schema guard error", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_id_array))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else if (!start_to_read_) {
    int64_t tmp_ret = OB_SUCCESS;
    common::ObSArray<ObLSReplicaTaskDisplayInfo> task_stats;
    int64_t task_cnt = 0;
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID;
    if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), KR(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), KR(ret));
    } else if (is_sys_tenant(effective_tenant_id_)) {
      for (int64_t tenant_idx = 0; OB_SUCC(ret) && tenant_idx < tenant_id_array.count(); tenant_idx++) {
        if (OB_SUCCESS != (tmp_ret = (task_worker_->try_tenant_disaster_recovery(tenant_id_array.at(tenant_idx), true/*only_for_display*/, task_cnt)))) {
          LOG_WARN("fail to try tenant disaster recovery for display", KR(tmp_ret), "tenant_id", tenant_id_array.at(tenant_idx));
        }
      }
    } else if (OB_FAIL(task_worker_->try_tenant_disaster_recovery(effective_tenant_id_, true/*only_for_display*/, task_cnt))) {
      LOG_WARN("fail to try tenant disaster recovery for display", KR(ret), K_(effective_tenant_id));
    }

    if (FAILEDx(task_worker_->get_task_plan_display(task_stats))) {
      LOG_WARN("fail to get tasks", KR(ret));
    } else {
      LOG_INFO("success to get task plans from worker", KR(ret), K(task_stats));
      ObArray<Column> columns;
      for (int64_t j = 0; OB_SUCC(ret) && j < task_stats.count(); ++j) {
        const ObLSReplicaTaskDisplayInfo *task_stat = &(task_stats.at(j));
        columns.reuse();
        if (OB_ISNULL(task_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task stat", KR(ret));
        } else if (OB_FAIL(get_full_row_(table_schema, *task_stat, columns))) {
          LOG_WARN("fail to get full row", "table_schema", *table_schema, "task_stat", *task_stat, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("fail to project row", K(columns), KR(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(cur_row_), KR(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualLSReplicaTaskPlan::get_full_row_(
    const share::schema::ObTableSchema *table,
    const ObLSReplicaTaskDisplayInfo &task_stat,
    common::ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  char *source_ip_str = nullptr;
  char *target_ip_str = nullptr;
  char *execute_ip_str = nullptr;
  int64_t source_port = 0;
  int64_t target_port = 0;
  int64_t execute_port = 0;
  arena_allocator_.reuse(); 
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is nullptr", KR(ret));
  } else if (OB_ISNULL(target_ip_str = static_cast<char *>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc target ip buf failed", KR(ret), "size", OB_MAX_SERVER_ADDR_SIZE);
  } else if (OB_ISNULL(source_ip_str = static_cast<char *>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc source ip buf failed", KR(ret), "size", OB_MAX_SERVER_ADDR_SIZE);
  } else if (OB_ISNULL(execute_ip_str = static_cast<char *>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc execute ip buf failed", KR(ret), "size", OB_MAX_SERVER_ADDR_SIZE);
  } else  if (task_stat.get_source_server().is_valid()
              && false == task_stat.get_source_server().ip_to_string(source_ip_str, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to change source_server_ to string", KR(ret), K(task_stat));
  } else  if (task_stat.get_target_server().is_valid()
              && false == task_stat.get_target_server().ip_to_string(target_ip_str, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to change target_server_ to string", KR(ret), K(task_stat));
  } else  if (task_stat.get_execute_server().is_valid()
              && false == task_stat.get_execute_server().ip_to_string(execute_ip_str, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to change execute_server_ to string", KR(ret), K(task_stat));
  } else {
    // get source ip and port
    if (task_stat.get_source_server().is_valid()) {
      source_port = task_stat.get_source_server().get_port();
    }
    // get target ip and port
    if (task_stat.get_target_server().is_valid()) {
      target_port = task_stat.get_target_server().get_port();
    }
    // get execute ip and port
    if (task_stat.get_execute_server().is_valid()) {
      execute_port = task_stat.get_execute_server().get_port();
    }
    ADD_COLUMN(set_int, table, "tenant_id", task_stat.get_tenant_id(), columns);
    ADD_COLUMN(set_int, table, "ls_id", task_stat.get_ls_id().id(), columns);
    ADD_COLUMN(set_varchar, table, "task_type", ob_disaster_recovery_task_type_strs(task_stat.get_task_type()), columns);
    ADD_COLUMN(set_int, table, "priority", static_cast<int64_t>(task_stat.get_task_priority()), columns);
    ADD_COLUMN(set_varchar, table, "target_replica_svr_ip", target_ip_str, columns);
    ADD_COLUMN(set_int, table, "target_replica_svr_port", target_port, columns);
    ADD_COLUMN(set_int, table, "target_paxos_replica_number", task_stat.get_target_replica_paxos_replica_number(), columns);
    ADD_COLUMN(set_varchar, table, "target_replica_type", ob_replica_type_strs(task_stat.get_target_replica_type()), columns); 
    ADD_COLUMN(set_varchar, table, "source_replica_svr_ip", task_stat.get_source_server().is_valid() ? source_ip_str : "", columns);
    ADD_COLUMN(set_int, table, "source_replica_svr_port", source_port, columns); 
    ADD_COLUMN(set_int, table, "source_paxos_replica_number", task_stat.get_source_replica_paxos_replica_number(), columns);
    ADD_COLUMN(set_varchar, table, "source_replica_type", task_stat.get_source_server().is_valid() ? ob_replica_type_strs(task_stat.get_source_replica_type()) : "", columns); 
    ADD_COLUMN(set_varchar, table, "task_exec_svr_ip", execute_ip_str, columns);
    ADD_COLUMN(set_int, table, "task_exec_svr_port", execute_port, columns);
    ADD_COLUMN(set_varchar, table, "comment", task_stat.get_comment().string(), columns);
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
