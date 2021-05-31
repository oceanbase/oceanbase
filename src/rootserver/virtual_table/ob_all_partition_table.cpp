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

#include "ob_all_partition_table.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {

ObAllPartitionTable::ObAllPartitionTable()
    : inited_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      server_mgr_(NULL),
      schema_guard_(NULL),
      table_schema_(NULL),
      partition_info_(),
      all_pt_iter_(),
      tenant_pt_iter_(),
      table_pt_iter_(),
      pt_iter_(NULL),
      arena_allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP)
{}

ObAllPartitionTable::~ObAllPartitionTable()
{}

int ObAllPartitionTable::init(ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service,
    ObServerManager& server_mgr, ObSchemaGetterGuard* schema_guard)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == schema_guard) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_guard is null", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    server_mgr_ = &server_mgr;
    schema_guard_ = schema_guard;
    inited_ = true;
  }
  return ret;
}

int ObAllPartitionTable::inner_open()
{
  int ret = OB_SUCCESS;
  uint64_t specific_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t specific_table_id = OB_INVALID_ID;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_TABLE_TID);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema_))) {
    LOG_WARN("get_table_schema failed", K(table_id), K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret));
  } else if (OB_FAIL(get_condition(specific_tenant_id, specific_table_id))) {
    LOG_WARN("fail to get condition", K(ret));
  } else {
    const bool ignore_row_checksum = true;
    if (OB_INVALID_TENANT_ID == specific_tenant_id) {
      if (OB_FAIL(all_pt_iter_.init(*pt_operator_, *schema_service_, ignore_row_checksum))) {
        LOG_WARN("all partition iterator init failed", K(ret));
      } else {
        pt_iter_ = &all_pt_iter_;
      }
    } else if (OB_INVALID_ID == specific_table_id) {
      if (OB_FAIL(tenant_pt_iter_.init(*pt_operator_, *schema_service_, specific_tenant_id, ignore_row_checksum))) {
        LOG_WARN("tenant partition iterator init failed", K(ret), K(specific_tenant_id));
      } else {
        pt_iter_ = &tenant_pt_iter_;
      }
    } else {
      if (OB_FAIL(table_pt_iter_.init(specific_table_id, *schema_guard_, *pt_operator_))) {
        LOG_WARN("table partition iterator init failed", K(ret), K(specific_tenant_id), K(specific_table_id));
      } else {
        pt_iter_ = &table_pt_iter_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(pt_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pt_iter is null", K(ret));
    } else if (OB_FAIL(pt_iter_->get_filters().filter_delete_server(*server_mgr_))) {
      LOG_WARN("set server_trace_filter failed", K(ret));
    } else if (OB_FAIL(pt_iter_->get_filters().set_filter_permanent_offline(*server_mgr_))) {
      LOG_WARN("set permanent_offline_filter failed", K(ret));
    } else if (OB_FAIL(pt_iter_->get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
      LOG_WARN("set replica_status_filter failed", K(ret));
    }
  }
  return ret;
}

int ObAllPartitionTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(pt_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_iter is null", K(ret));
  } else {
    while (OB_SUCC(ret) && 0 == partition_info_.replica_count()) {
      if (OB_FAIL(pt_iter_->next(partition_info_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next partition_info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t index = 0;
      const ObPartitionReplica& replica = partition_info_.get_replicas_v2().at(index);
      ObArray<Column> columns;
      columns.reuse();
      if (OB_FAIL(get_full_row(table_schema_, replica, columns))) {
        LOG_WARN("get_full_row failed", "table_schema", table_schema_, "replica", replica, K(ret));
      } else if (OB_FAIL(project_row(columns, cur_row_))) {
        LOG_WARN("project_row failed", K(columns), K(ret));
      } else {
        row = &cur_row_;
        if (OB_FAIL(partition_info_.get_replicas_v2().remove(index))) {
          LOG_WARN("partition_info remove fail", K(ret), K(replica), K(partition_info_));
        }
      }
    }
  }
  return ret;
}

int ObAllPartitionTable::get_full_row(
    const ObTableSchema* table, const ObPartitionReplica& replica, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  char* ip = NULL;
  char* zone = NULL;
  char* member_list = NULL;
  char* column_checksum = NULL;
  const char* column_checksum_tmp = "";
  char* fail_list = NULL;
  arena_allocator_.reset();
  const char* replica_status = ob_replica_status_str(replica.replica_status_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(replica), K(ret));
  } else {
    if (NULL == (ip = static_cast<char*>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (NULL == (zone = static_cast<char*>(arena_allocator_.alloc(MAX_ZONE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc zone buf failed", "size", MAX_ZONE_LENGTH, K(ret));
    } else if (NULL == (member_list = static_cast<char*>(arena_allocator_.alloc(MAX_MEMBER_LIST_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc member_list failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (NULL == (fail_list = static_cast<char*>(arena_allocator_.alloc(OB_MAX_FAILLIST_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc fail_list failed", "size", OB_MAX_FAILLIST_LENGTH, K(ret));
    } else if (NULL == (column_checksum = static_cast<char*>(arena_allocator_.alloc(COLUMN_CHECKSUM_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc column_checksum failed", "size", COLUMN_CHECKSUM_LENGTH, K(ret));
    } else if (false == replica.server_.ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert server ip to string failed", K(ret), "server", replica.server_);
    } else if (OB_FAIL(databuff_printf(zone, MAX_ZONE_LENGTH, "%s", replica.zone_.ptr()))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src len", strlen(replica.zone_.ptr()), K(ret));
    } else if (OB_FAIL(
                   ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
      LOG_WARN("member_list2text failed", K(replica), K(ret));
    } else if (OB_FAIL(databuff_printf(fail_list, OB_MAX_FAILLIST_LENGTH, "%s", replica.fail_list_.ptr()))) {
      LOG_WARN("fail list to text failed", K(replica), K(ret));
    } else if (OB_FAIL(databuff_printf(column_checksum, COLUMN_CHECKSUM_LENGTH, "%s", column_checksum_tmp))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", COLUMN_CHECKSUM_LENGTH, "src len", strlen(column_checksum_tmp), K(ret));
    } else {
      const int64_t data_size = REPLICA_TYPE_LOGONLY == replica.replica_type_ ? 0 : replica.data_size_;
      const int64_t required_size = REPLICA_TYPE_LOGONLY == replica.replica_type_ ? 0 : replica.required_size_;
      ADD_COLUMN(set_int, table, "tenant_id", static_cast<int64_t>(extract_tenant_id(replica.table_id_)), columns);
      ADD_COLUMN(set_int, table, "table_id", static_cast<int64_t>(replica.table_id_), columns);
      ADD_COLUMN(set_int, table, "partition_id", replica.partition_id_, columns);
      ADD_COLUMN(set_varchar, table, "svr_ip", ip, columns);
      ADD_COLUMN(set_int, table, "svr_port", replica.server_.get_port(), columns);
      ADD_COLUMN(set_int, table, "sql_port", replica.sql_port_, columns);
      ADD_COLUMN(set_int, table, "unit_id", static_cast<int64_t>(replica.unit_id_), columns);
      ADD_COLUMN(set_int, table, "partition_cnt", replica.partition_cnt_, columns);
      ADD_COLUMN(set_varchar, table, "zone", zone, columns);
      ADD_COLUMN(set_int, table, "role", replica.role_, columns);
      ADD_COLUMN(set_varchar, table, "member_list", member_list, columns);
      ADD_COLUMN(set_int, table, "row_count", replica.row_count_, columns);
      ADD_COLUMN(set_int, table, "data_size", data_size, columns);
      ADD_COLUMN(set_int, table, "data_version", replica.data_version_, columns);
      ADD_COLUMN(set_int, table, "data_checksum", replica.data_checksum_, columns);
      ADD_COLUMN(set_int, table, "row_checksum", static_cast<int64_t>(replica.row_checksum_.checksum_), columns);
      ADD_COLUMN(set_varchar, table, "column_checksum", column_checksum, columns);
      ADD_COLUMN(set_int, table, "is_original_leader", replica.is_original_leader_, columns);
      ADD_COLUMN(set_int, table, "to_leader_time", replica.to_leader_time_, columns);
      ADD_COLUMN(set_int, table, "create_time", 0, columns);  // DEPRECATED
      ADD_COLUMN(set_int, table, "rebuild", replica.rebuild_, columns);
      ADD_COLUMN(set_int, table, "replica_type", replica.replica_type_, columns);
      ADD_COLUMN(set_varchar, table, "status", replica_status, columns);
      ADD_COLUMN(set_int, table, "partition_checksum", replica.partition_checksum_, columns);
      ADD_COLUMN(set_int, table, "required_size", required_size, columns);
      ADD_COLUMN(set_int, table, "is_restore", replica.is_restore_, columns);
      ADD_COLUMN(set_int, table, "quorum", replica.quorum_, columns);
      ADD_COLUMN(set_varchar, table, "fail_list", fail_list, columns);
      ADD_COLUMN(set_int, table, "recovery_timestamp", replica.recovery_timestamp_, columns);
      ADD_COLUMN(set_int, table, "memstore_percent", replica.get_memstore_percent(), columns);
      ADD_COLUMN(set_int, table, "data_file_id", replica.data_file_id_, columns);
    }
  }
  return ret;
}

int ObAllPartitionTable::get_condition(uint64_t& specific_tenant_id, uint64_t& specific_table_id)
{
  int ret = OB_SUCCESS;
  specific_tenant_id = OB_INVALID_TENANT_ID;
  specific_table_id = OB_INVALID_ID;
  if (1 == get_key_ranges().count()) {
    // optimize specific cases:
    // 1) specified tenant_id
    // 2) specified (tenant_id, table_id)
    const int64_t ROW_KEY_COUNT = 5;  // index
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
            case 1: {  // table_id
              specific_table_id = end_key_obj_ptr[j].get_int();
              break;
            }
            case 2:
            case 3:
            case 4: {
              // do nothing
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
  LOG_DEBUG("get condition", K(ret), K(specific_tenant_id), K(specific_table_id));
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
