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

#include "ob_all_replica_task.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {
using namespace balancer;

ObAllReplicaTask::ObAllReplicaTask()
    : inited_(false),
      tenant_stat_(),
      rereplication_(),
      locality_checker_(),
      schema_service_(NULL),
      results_(),
      table_schema_(NULL),
      arena_allocator_(),
      index_(OB_INVALID_INDEX)
{}

ObAllReplicaTask::~ObAllReplicaTask()
{
  tenant_stat_.reuse();
}

int ObAllReplicaTask::init(share::ObPartitionTableOperator& pt_operator,
    share::schema::ObMultiVersionSchemaService& schema_service,
    share::ObRemotePartitionTableOperator& remote_pt_operator, rootserver::ObServerManager& server_mgr,
    rootserver::ObUnitManager& unit_mgr, rootserver::ObZoneManager& zone_mgr, rootserver::ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tenant_stat_.init(unit_mgr,
                 server_mgr,
                 pt_operator,
                 remote_pt_operator,
                 zone_mgr,
                 task_mgr,
                 check_stop_provider,
                 unit_mgr.get_sql_proxy()))) {
    LOG_WARN("init tenant stat failed", K(ret));
  } else if (OB_FAIL(rereplication_.init(
                 schema_service, zone_mgr, pt_operator, task_mgr, tenant_stat_, check_stop_provider, unit_mgr))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(locality_checker_.init(
                 schema_service, tenant_stat_, task_mgr, zone_mgr, check_stop_provider, server_mgr, unit_mgr))) {
    LOG_WARN("fail to init locality checker", K(ret));
  } else {
    results_.reset();
    schema_service_ = &schema_service;
    index_ = 0;
    table_schema_ = NULL;
    inited_ = true;
  }
  return ret;
}
int ObAllReplicaTask::get_condition(uint64_t& specific_tenant_id)
{
  int ret = OB_SUCCESS;
  specific_tenant_id = OB_INVALID_TENANT_ID;
  LOG_DEBUG("get condition", K(ret), K(specific_tenant_id));
  return ret;
}

int ObAllReplicaTask::inner_open()
{
  int ret = OB_SUCCESS;
  uint64_t specific_tenant_id = OB_INVALID_TENANT_ID;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_condition(specific_tenant_id))) {
    LOG_WARN("fail to get specific_tenant_id", K(ret));
  } else {
    ObArray<uint64_t> tenant_ids;
    ObSchemaGetterGuard schema_guard;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_REPLICA_TASK_TID);
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
      LOG_WARN("get tenant ids failed", K(ret));
    } else if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema_))) {
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (NULL == table_schema_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret));
    } else {
      results_.reuse();
      index_ = 0;
      for (int i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
        const uint64_t tenant_id = tenant_ids[i];
        if (OB_INVALID_TENANT_ID == specific_tenant_id || tenant_id == specific_tenant_id) {
          common::ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_BALANCER);
          TenantSchemaGetter stat_finder(tenant_id);
          ObLeaderBalanceGroupContainer balance_group_container(schema_guard, stat_finder, allocator);
          if (OB_FAIL(balance_group_container.init(tenant_id))) {
            LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
          } else if (OB_FAIL(balance_group_container.build())) {
            LOG_WARN("fail to build balance index builder", K(ret));
          } else if (OB_FAIL(tenant_stat_.reuse_replica_count_mgr())) {
            LOG_WARN("reuse_replica_count_mgr failed", K(ret));
          } else if (OB_FAIL(tenant_stat_.gather_stat(
                         tenant_id, &schema_guard, balance_group_container.get_hash_index()))) {
            LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
          } else if (OB_FAIL(locality_checker_.get_filter_result(balance_group_container.get_hash_index(), results_))) {
            LOG_WARN("add locality tasks failed", K(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

// TODO: currently only display replicas whose locality are not matched.
// In future, we can display rebuild, migrate and other tasks.
int ObAllReplicaTask::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (index_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is invalid", K(ret), K_(index));
  } else if (results_.count() == index_) {
    ret = OB_ITER_END;
  } else {
    ObArray<Column> columns;
    columns.reuse();
    if (OB_FAIL(get_full_row(table_schema_, results_[index_], columns))) {
      LOG_WARN("get_full_row failed", "table_schema", table_schema_, "task", results_[index_], K(ret));
    } else if (OB_FAIL(project_row(columns, cur_row_))) {
      LOG_WARN("project_row failed", K(columns), K(ret));
    } else {
      row = &cur_row_;
      index_++;
    }
  }
  return ret;
}

int ObAllReplicaTask::get_full_row(const ObTableSchema* table, ObReplicaTask& task, ObIArray<Column>& columns)
{
  int n = 0;
  int ret = OB_SUCCESS;
  char* src_ip = NULL;
  char* dst_ip = NULL;
  char* zone = NULL;
  char* region = NULL;
  const char* cmd_type_str = NULL;
  arena_allocator_.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(task), K(ret));
  } else {
    if (NULL == (src_ip = static_cast<char*>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (false == task.src_.ip_to_string(src_ip, OB_MAX_SERVER_ADDR_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert server ip to string failed", K(ret), "server", task.src_);
    } else if (NULL == (dst_ip = static_cast<char*>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (false == task.dst_.ip_to_string(dst_ip, OB_MAX_SERVER_ADDR_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert server ip to string failed", K(ret), "server", task.dst_);
    } else if (NULL == (zone = static_cast<char*>(arena_allocator_.alloc(MAX_ZONE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc zone buf failed", "size", MAX_ZONE_LENGTH, K(ret));
    } else if (0 > (n = snprintf(zone, MAX_ZONE_LENGTH, "%s", task.zone_.ptr())) || n >= MAX_ZONE_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src len", strlen(task.zone_.ptr()), K(ret));
    } else if (NULL == (region = static_cast<char*>(arena_allocator_.alloc(MAX_REGION_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc region buf failed", "size", MAX_REGION_LENGTH, K(ret));
    } else if (0 > (n = snprintf(region, MAX_REGION_LENGTH, "%s", task.region_.ptr())) || n >= MAX_REGION_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_REGION_LENGTH, "src len", strlen(task.region_.ptr()), K(ret));
    } else if (NULL == task.comment_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("comment should not be null", K(ret));
    } else if (NULL == (cmd_type_str = ObRebalanceTask::get_task_type_str(task.cmd_type_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("task type str should not be null", K(ret), K_(task.cmd_type));
    } else {
      ADD_COLUMN(set_int, table, "tenant_id", static_cast<int64_t>(task.tenant_id_), columns);
      ADD_COLUMN(set_int, table, "table_id", static_cast<int64_t>(task.table_id_), columns);
      ADD_COLUMN(set_int, table, "partition_id", task.partition_id_, columns);
      ADD_COLUMN(set_varchar, table, "src_ip", src_ip, columns);
      ADD_COLUMN(set_int, table, "src_port", task.src_.get_port(), columns);
      ADD_COLUMN(set_int, table, "src_replica_type", task.replica_type_, columns);
      ADD_COLUMN(set_varchar, table, "zone", zone, columns);
      ADD_COLUMN(set_varchar, table, "region", region, columns);
      ADD_COLUMN(set_varchar, table, "dst_ip", dst_ip, columns);
      ADD_COLUMN(set_int, table, "dst_port", task.dst_.get_port(), columns);
      ADD_COLUMN(set_int, table, "dst_replica_type", task.dst_replica_type_, columns);
      ADD_COLUMN(set_varchar, table, "cmd_type", cmd_type_str, columns);
      ADD_COLUMN(set_varchar, table, "comment", task.comment_, columns);
    }
  }
  return ret;
}

ObAllReplicaTaskI1::ObAllReplicaTaskI1() : ObAllReplicaTask()
{}

ObAllReplicaTaskI1::~ObAllReplicaTaskI1()
{}

int ObAllReplicaTaskI1::get_condition(uint64_t& specific_tenant_id)
{
  int ret = OB_SUCCESS;
  specific_tenant_id = OB_INVALID_TENANT_ID;
  if (1 == get_key_ranges().count()) {
    // optimize speicific cases: specified tenant_id
    const int64_t ROW_KEY_COUNT = 1;  // index
    ObRowkey start_key = get_key_ranges().at(0).start_key_;
    ObRowkey end_key = get_key_ranges().at(0).end_key_;
    if ((ROW_KEY_COUNT != start_key.get_obj_cnt()) || (ROW_KEY_COUNT != end_key.get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "row key count not match");
    } else {
      const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
      for (int64_t j = 0; OB_SUCC(ret) && j < ROW_KEY_COUNT; ++j) {
        if (start_key_obj_ptr[j].is_min_value() || end_key_obj_ptr[j].is_max_value() ||
            !start_key_obj_ptr[j].is_integer_type() || !end_key_obj_ptr[j].is_integer_type() ||
            (start_key_obj_ptr[j] != end_key_obj_ptr[j])) {
          // skip
        } else {
          switch (j) {
            case 0: {  // tenant_id
              specific_tenant_id = end_key_obj_ptr[j].get_int();
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid index", K(ret), K(j));
            }
          }  // end of switch
        }    // end of else
      }      // end of for
    }
  }
  LOG_DEBUG("get condition with i1", K(ret), K(specific_tenant_id));
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
