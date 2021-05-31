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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_partition_location.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace observer {
ObAllVirtualPartitionLocation::ObAllVirtualPartitionLocation()
    : ObVirtualTableIterator(),
      inited_(false),
      arena_allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP),
      pt_operator_(NULL),
      partition_info_(),
      next_replica_idx_(0)
{}

ObAllVirtualPartitionLocation::~ObAllVirtualPartitionLocation()
{}

int ObAllVirtualPartitionLocation::init(ObPartitionTableOperator& pt_operator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    partition_info_.set_allocator(&arena_allocator_);
    partition_info_.reuse();
    next_replica_idx_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObAllVirtualPartitionLocation::inner_open()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  int64_t partition_id = OB_INVALID_INDEX;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_query_key(tenant_id, table_id, partition_id))) {
    SERVER_LOG(WARN, "get query key fail", K(ret));
  } else if (tenant_id != extract_tenant_id(table_id)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tenant_id, table_id not matched", K(tenant_id), K(table_id), K(ret));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "pt is null", K(ret));
  } else if (OB_FAIL(pt_operator_->get(table_id, partition_id, partition_info_))) {
    SERVER_LOG(WARN, "fail to fetch partition", K(ret), K(table_id), K(partition_id));
  }
  return ret;
}

int ObAllVirtualPartitionLocation::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (!partition_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "partition_info is invalid", K(ret), K_(partition_info));
  } else if (next_replica_idx_ == partition_info_.replica_count()) {
    ret = OB_ITER_END;
  } else if (next_replica_idx_ < 0 || next_replica_idx_ > partition_info_.replica_count()) {
    ret = OB_SIZE_OVERFLOW;
    SERVER_LOG(WARN,
        "replica idx out of range",
        K(ret),
        K_(next_replica_idx),
        "replica_count",
        partition_info_.replica_count());
  } else {
    const ObPartitionReplica& replica = partition_info_.get_replicas_v2().at(next_replica_idx_);
    if (OB_FAIL(fill_row(replica, row))) {
      SERVER_LOG(WARN, "fail to fill row", K(ret), K(replica));
    } else {
      next_replica_idx_++;
    }
  }
  return ret;
}

int ObAllVirtualPartitionLocation::fill_row(const ObPartitionReplica& replica, common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  char* ip = NULL;
  char* member_list = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (!replica.is_valid()) {
    SERVER_LOG(WARN, "replica is invalid", K(ret), K(replica));
  } else if (NULL == (ip = static_cast<char*>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, K(ret));
  } else if (NULL == (member_list = static_cast<char*>(arena_allocator_.alloc(MAX_MEMBER_LIST_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "alloc member_list failed", "size", MAX_MEMBER_LIST_LENGTH, K(ret));
  } else if (false == replica.server_.ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "convert server ip to string failed", K(ret), "server", replica.server_);
  } else if (OB_FAIL(ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
    SERVER_LOG(WARN, "member_list2text failed", K(replica), K(ret));
  } else {
    ObObj* cells = cur_row_.cells_;
    const int64_t col_count = output_column_ids_.count();
    ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
    uint64_t col_id = OB_INVALID_ID;
    uint64_t cell_idx = 0;
    for (int64_t m = 0; OB_SUCC(ret) && m < col_count; ++m) {
      col_id = output_column_ids_.at(m);
      switch (col_id) {
        case TABLE_ID: {
          cells[cell_idx].set_int(replica.table_id_);
          break;
        }
        case PARTITION_ID: {
          cells[cell_idx].set_int(replica.partition_id_);
          break;
        }
        case TENANT_ID: {
          cells[cell_idx].set_int(extract_tenant_id(replica.table_id_));
          break;
        }
        case SVR_IP: {
          cells[cell_idx].set_varchar(ip);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case SVR_PORT: {
          cells[cell_idx].set_int(replica.server_.get_port());
          break;
        }
        case UNIT_ID: {
          cells[cell_idx].set_int(replica.unit_id_);
          break;
        }
        case PARTITION_CNT: {
          cells[cell_idx].set_int(replica.partition_cnt_);
          break;
        }
        case ZONE: {
          cells[cell_idx].set_varchar(replica.zone_.ptr());
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case ROLE: {
          cells[cell_idx].set_int(replica.role_);
          break;
        }
        case MEMBER_LIST: {
          cells[cell_idx].set_varchar(member_list);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case REPLICA_TYPE: {
          cells[cell_idx].set_int(replica.replica_type_);
          break;
        }
        case STATUS: {
          cells[cell_idx].set_varchar(ob_replica_status_str(replica.replica_status_));
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case DATA_VERSION: {
          cells[cell_idx].set_int(replica.data_version_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid cell_idx", K(ret), K(cell_idx), K(col_id), K(col_count), K(output_column_ids_));
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualPartitionLocation::get_query_key(uint64_t& tenant_id, uint64_t& table_id, int64_t& partition_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (1 != key_ranges_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_id, table_id and partition_id must all be specified");
  } else {
    const int64_t ROW_KEY_COUNT = 5;
    tenant_id = OB_INVALID_ID;
    table_id = OB_INVALID_ID;
    partition_id = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
      const ObRowkey& start_key = key_ranges_.at(i).start_key_;
      const ObRowkey& end_key = key_ranges_.at(i).end_key_;
      if ((ROW_KEY_COUNT != start_key.get_obj_cnt()) || (ROW_KEY_COUNT != end_key.get_obj_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_id, table_id and partition_id must all be specified");
      } else {
        const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
        const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
        for (int64_t j = 0; OB_SUCC(ret) && j < ROW_KEY_COUNT; ++j) {
          int64_t col_id = OB_APP_MIN_COLUMN_ID + j;
          if (TENANT_ID != col_id && TABLE_ID != col_id && PARTITION_ID != col_id) {
            // skip
          } else if (start_key_obj_ptr[j].is_min_value() || end_key_obj_ptr[j].is_max_value() ||
                     !start_key_obj_ptr[j].is_int() || !end_key_obj_ptr[j].is_int() ||
                     start_key_obj_ptr[j] != end_key_obj_ptr[j]) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "tenant_id, table_id and partition_id must all be specified", K(ret), K(j));
            LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_id, table_id and partition_id must all be specified");
          } else if (TENANT_ID == col_id) {
            tenant_id = start_key_obj_ptr[j].get_int();
          } else if (TABLE_ID == col_id) {
            table_id = start_key_obj_ptr[j].get_int();
          } else if (PARTITION_ID == col_id) {
            partition_id = start_key_obj_ptr[j].get_int();
          }
        }
      }
    }
    if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id || OB_INVALID_INDEX == partition_id) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,
          "tenant_id, table_id and partition_id must all be specified",
          K(ret),
          K(table_id),
          K(partition_id),
          K(tenant_id));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_id, table_id and partition_id must all be specified");
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
