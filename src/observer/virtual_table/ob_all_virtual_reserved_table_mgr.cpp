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

#include "observer/virtual_table/ob_all_virtual_reserved_table_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_pg_storage.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualReservedTableMgr::ObAllVirtualReservedTableMgr()
    : ObVirtualTableScannerIterator(),
      is_inited_(false),
      svr_port_(0),
      tables_handle_(),
      table_idx_(0),
      snapshot_version_(0),
      point_info_(NULL),
      pg_recovery_mgr_(NULL),
      point_iter_(),
      partition_itertor_(NULL)
{
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
}

ObAllVirtualReservedTableMgr::~ObAllVirtualReservedTableMgr()
{
  reset();
}

void ObAllVirtualReservedTableMgr::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  snapshot_version_ = 0;
  svr_port_ = 0;
  tables_handle_.reset();
  table_idx_ = 0;
  point_info_ = NULL;
  pg_recovery_mgr_ = NULL;
  point_iter_.reset();
  if (OB_NOT_NULL(partition_itertor_)) {
    ObPartitionService::get_instance().revert_pg_iter(partition_itertor_);
  }
  partition_itertor_ = NULL;
}

int ObAllVirtualReservedTableMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get server ip", K(ret));
  } else if (OB_ISNULL(partition_itertor_ = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to alloc partition allocator", K(ret));
  } else {
    svr_port_ = OBSERVER.get_self().get_port();
    table_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

const char* ObAllVirtualReservedTableMgr::get_reserve_type_name_(ObRecoveryPointType type)
{
  const char* type_name = NULL;
  switch (type) {
    case ObRecoveryPointType::RESTORE_POINT: {
      type_name = "restore point";
      break;
    }
    case ObRecoveryPointType::BACKUP: {
      type_name = "backup point";
      break;
    }
    default: {
      type_name = "";
      break;
    }
  }
  return type_name;
}

int ObAllVirtualReservedTableMgr::get_point_next_table_(storage::ObITable*& table)
{
  int ret = OB_SUCCESS;
  if (table_idx_ >= tables_handle_.get_count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(table = tables_handle_.get_table(table_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table shouldn't NULL here", K(ret));
  } else {
    ++table_idx_;
  }

  return ret;
}

int ObAllVirtualReservedTableMgr::get_mgr_next_point_()
{
  int ret = OB_SUCCESS;
  if (!point_iter_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "point iter is not valid.", K(ret));
  } else if (OB_FAIL(point_iter_.get_next(point_info_)) && ret == OB_ITER_END) {
    // do nothing
  } else if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "get next point info failed.", K(ret));
  } else if (OB_ISNULL(point_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "point info is null.", K(ret));
  } else if (OB_FAIL(point_info_->get_tables(tables_handle_))) {
    SERVER_LOG(WARN, "get point info table failed.", K(ret));
  } else {
    snapshot_version_ = point_info_->get_snapshot_version();
    table_idx_ = 0;
  }
  return ret;
}

int ObAllVirtualReservedTableMgr::get_mgr_next_table_(storage::ObITable*& table)
{
  int ret = OB_SUCCESS;
  if (!point_iter_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "point iter is not valid.", K(ret));
  } else {
    while (OB_FAIL(get_point_next_table_(table)) && ret == OB_ITER_END) {
      if (OB_FAIL(get_mgr_next_point_()) && ret == OB_ITER_END) {
        // all the tables of the mgr has been iterated
        break;
      } else if (OB_FAIL(ret)) {
        SERVER_LOG(WARN, "get mgr next point failed.", K(ret));
        break;
      } else {
        // do nothing
      }
    }  // while
  }    // else
  return ret;
}

int ObAllVirtualReservedTableMgr::get_next_table_(storage::ObITable*& table)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(partition_itertor_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "partition iterator should not be null", K(ret));
  } else {
    while (pg_recovery_mgr_ == NULL || (OB_FAIL(get_mgr_next_table_(table)) && ret == OB_ITER_END)) {
      point_iter_.reset();
      // SERVER_LOG(INFO, "get_next_table_ ", K(ret), K(pg_recovery_mgr_ == NULL), K(partition_itertor_), KPC(table));
      if (OB_FAIL(partition_itertor_->get_next(partition)) && ret == OB_ITER_END) {
        break;
      } else if (OB_FAIL(ret)) {
        SERVER_LOG(WARN, "fail to get next partition", K(ret));
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "partition is null", K(ret));
      } else if (OB_ISNULL(pg_recovery_mgr_ = &(partition->get_pg_storage().get_recovery_data_mgr()))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "pg recovery mgr is null", K(ret));
      } else if (OB_FAIL(point_iter_.init(*pg_recovery_mgr_))) {
        SERVER_LOG(WARN, "point iter init failed.", K(ret));
      } else {
        // do nothing
      }
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
    SERVER_LOG(WARN, "get next table failed.", K(ret));
  }
  return ret;
}

int ObAllVirtualReservedTableMgr::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  storage::ObITable* table = NULL;

  ObObj* cells = cur_row_.cells_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_table_(table))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next table");
    }
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "table is NULL", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    const common::ObPartitionKey& pkey = table->get_partition_key();
    const ObITable::TableKey& table_key = table->get_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          cells[i].set_varchar(svr_ip_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(svr_port_);
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        }
        case TABLE_ID: {
          cells[i].set_int(pkey.table_id_);
          break;
        }
        case TABLE_TYPE: {
          cells[i].set_int(table_key.table_type_);
          break;
        }
        case PARTITION_ID: {
          cells[i].set_int(pkey.get_partition_id());
          break;
        }
        case INDEX_ID: {
          cells[i].set_int(table_key.table_id_);
          break;
        }
        case BASE_VERSION: {
          cells[i].set_int(table_key.trans_version_range_.base_version_);
          break;
        }
        case MULTI_VERSION_START: {
          cells[i].set_int(table_key.trans_version_range_.multi_version_start_);
          break;
        }
        case SNAPSHOT_VERSION: {
          cells[i].set_int(table_key.trans_version_range_.snapshot_version_);
          break;
        }
        case OB_VERSION: {
          cells[i].set_int(table_key.version_);
          break;
        }
        case SIZE: {
          int64_t size = 0;
          size = static_cast<ObSSTable*>(table)->get_meta().occupy_size_;
          cells[i].set_int(size);
          break;
        }
        case REF: {
          cells[i].set_int(table->get_ref());
          break;
        }
        case RESERVE_TYPE: {
          ObRecoveryPointType type = ObRecoveryPointType::UNKNOWN_TYPE;
          point_info_->get_type(type);
          cells[i].set_varchar(get_reserve_type_name_(type));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case RESERVE_POINT_VERSION: {
          cells[i].set_int(snapshot_version_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
        }  // default
      }    // switch
    }      // for
  }        // else
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
