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

#include "ob_core_meta_table.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {
ObCoreMetaTable::ObCoreMetaTable() : inited_(false), pt_operator_(NULL), schema_guard_(NULL)
{}

ObCoreMetaTable::~ObCoreMetaTable()
{}

int ObCoreMetaTable::init(ObPartitionTableOperator& pt_operator, ObSchemaGetterGuard* schema_guard)
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
    schema_guard_ = schema_guard;
    inited_ = true;
  }
  return ret;
}

int ObCoreMetaTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!start_to_read_) {
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID);
    const uint64_t core_table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    ObPartitionInfo partition_info;
    partition_info.set_allocator(allocator_);
    if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema)) || NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (OB_FAIL(
                   pt_operator_->get(core_table_id, ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID, partition_info))) {
      LOG_WARN("pt_operator get failed",
          K(core_table_id),
          "partition_id",
          static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
          K(ret));
    } else {
      ObArray<Column> columns;
      FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCCESS == ret)
      {
        columns.reuse();
        if (OB_FAIL(get_full_row(table_schema, *replica, columns))) {
          LOG_WARN("get_full_row failed", "table_schema", *table_schema, "replica", *replica, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("project_row failed", K(columns), K(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("add_row failed", K(cur_row_), K(ret));
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
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObCoreMetaTable::get_full_row(
    const ObTableSchema* table, const ObPartitionReplica& replica, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  char* ip = NULL;
  char* zone = NULL;
  char* member_list = NULL;
  char* column_checksum = NULL;
  const char* column_checksum_tmp = "";
  char* fail_list = NULL;
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
    if (NULL == (ip = static_cast<char*>(allocator_->alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (NULL == (zone = static_cast<char*>(allocator_->alloc(MAX_ZONE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc zone buf failed", "size", MAX_ZONE_LENGTH, K(ret));
    } else if (NULL == (member_list = static_cast<char*>(allocator_->alloc(MAX_MEMBER_LIST_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc member_list failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
    } else if (NULL == (column_checksum = static_cast<char*>(allocator_->alloc(COLUMN_CHECKSUM_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc column_checksum failed", "size", COLUMN_CHECKSUM_LENGTH, K(ret));
    } else if (NULL == (fail_list = static_cast<char*>(allocator_->alloc(OB_MAX_FAILLIST_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc zone buf failed", "size", OB_MAX_FAILLIST_LENGTH, K(ret));
    } else if (false == replica.server_.ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert server ip to string failed", K(ret), "server", replica.server_);
    } else if (OB_FAIL(databuff_printf(zone, MAX_ZONE_LENGTH, "%s", replica.zone_.ptr()))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src_len", strlen(replica.zone_.ptr()), K(ret));
    } else if (OB_FAIL(
                   ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
      LOG_WARN("member_list2text failed", K(replica), K(ret));
    } else if (OB_FAIL(databuff_printf(column_checksum, COLUMN_CHECKSUM_LENGTH, "%s", column_checksum_tmp))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", COLUMN_CHECKSUM_LENGTH, "src_len", strlen(column_checksum_tmp), K(ret));
    } else if (OB_FAIL(databuff_printf(fail_list, OB_MAX_FAILLIST_LENGTH, "%s", replica.fail_list_.ptr()))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", K(ret), "buf_len", OB_MAX_FAILLIST_LENGTH, "src_len", replica.fail_list_.length());
    } else {
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
      ADD_COLUMN(set_int, table, "data_size", replica.data_size_, columns);
      ADD_COLUMN(set_int, table, "data_version", replica.data_version_, columns);
      ADD_COLUMN(set_int, table, "data_checksum", replica.data_checksum_, columns);
      ADD_COLUMN(set_int, table, "row_checksum", static_cast<int64_t>(replica.row_checksum_.checksum_), columns);
      ADD_COLUMN(set_varchar, table, "column_checksum", column_checksum, columns);
      ADD_COLUMN(set_int, table, "is_original_leader", replica.is_original_leader_, columns);
      ADD_COLUMN(set_int, table, "is_previous_leader", replica.to_leader_time_, columns);
      ADD_COLUMN(set_int, table, "create_time", 0, columns);  // DEPRECATED
      ADD_COLUMN(set_int, table, "rebuild", replica.rebuild_, columns);
      ADD_COLUMN(set_int, table, "replica_type", replica.replica_type_, columns);
      ADD_COLUMN(set_int, table, "required_size", replica.required_size_, columns);
      ADD_COLUMN(set_varchar, table, "status", replica_status, columns);
      ADD_COLUMN(set_int, table, "is_restore", replica.is_restore_, columns);
      ADD_COLUMN(set_int, table, "partition_checksum", replica.partition_checksum_, columns);
      ADD_COLUMN(set_int, table, "quorum", replica.quorum_, columns);
      ADD_COLUMN(set_varchar, table, "fail_list", fail_list, columns);
      ADD_COLUMN(set_int, table, "recovery_timestamp", replica.recovery_timestamp_, columns);
      ADD_COLUMN(set_int, table, "memstore_percent", replica.get_memstore_percent(), columns);
      ADD_COLUMN(set_int, table, "data_file_id", replica.data_file_id_, columns);
    }

    if (OB_FAIL(ret)) {
      if (NULL != ip) {
        allocator_->free(ip);
        ip = NULL;
      }
      if (NULL != zone) {
        allocator_->free(zone);
        zone = NULL;
      }
      if (NULL != member_list) {
        allocator_->free(member_list);
        member_list = NULL;
      }
      if (NULL != column_checksum) {
        allocator_->free(column_checksum);
        column_checksum = NULL;
      }
      if (NULL != fail_list) {
        allocator_->free(fail_list);
        fail_list = NULL;
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
