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

#include "observer/virtual_table/ob_all_virtual_table_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "storage/ob_table_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualTableMgr::ObAllVirtualTableMgr()
    : ObVirtualTableScannerIterator(), addr_(), pg_iter_(nullptr), pg_tables_handle_(), table_idx_(0), memtable_idx_(0)
{}

ObAllVirtualTableMgr::~ObAllVirtualTableMgr()
{
  reset();
}

void ObAllVirtualTableMgr::reset()
{
  addr_.reset();
  if (nullptr != pg_iter_) {
    ObPartitionService::get_instance().revert_pg_iter(pg_iter_);
    pg_iter_ = nullptr;
  }
  pg_tables_handle_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTableMgr::init()
{
  int ret = OB_SUCCESS;

  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(pg_iter_ = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc pg iterator", K(ret));
  } else if (OB_FAIL(ObTableMgr::get_instance().get_all_tables(memtable_handle_))) {
    STORAGE_LOG(WARN, "failed to get all memtables", K(ret));
  } else {
    table_idx_ = 0;
    memtable_idx_ = 0;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTableMgr::get_next_table(ObITable*& table)
{
  int ret = OB_SUCCESS;
  table = nullptr;
  if (memtable_idx_ < memtable_handle_.get_count()) {
    table = memtable_handle_.get_table(memtable_idx_);
    ++memtable_idx_;
  } else if (table_idx_ < sstables_.count()) {
    table = sstables_.at(table_idx_);
    ++table_idx_;
  } else {
    sstables_.reuse();
    while (OB_SUCC(ret) && nullptr == table) {
      ObIPartitionGroup* pg = nullptr;
      if (OB_FAIL(pg_iter_->get_next(pg))) {
        if (OB_ITER_END == ret) {
          break;
        } else {
          STORAGE_LOG(WARN, "fail to get next pg", K(ret));
        }
      } else if (OB_FAIL(pg->get_all_tables(pg_tables_handle_))) {
        STORAGE_LOG(WARN, "fail to get all tables", K(ret));
      } else if (OB_FAIL(pg_tables_handle_.get_all_sstables(sstables_))) {
        STORAGE_LOG(WARN, "failed to get all sstables", K(ret));
      } else if (sstables_.count() > 0) {
        table_idx_ = 0;
        table = sstables_.at(table_idx_);
        ++table_idx_;
      }
    }
  }
  return ret;
}

int ObAllVirtualTableMgr::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  storage::ObITable* table = NULL;

  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret), K(start_to_read_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid pointer", K(allocator_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_table(table))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t col_count = output_column_ids_.count();
    const common::ObPartitionKey& pkey = table->get_partition_key();
    const ObITable::TableKey& table_key = table->get_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        case TABLE_TYPE:
          cur_row_.cells_[i].set_int(table_key.table_type_);
          break;
        case TABLE_ID:
          cur_row_.cells_[i].set_int(pkey.table_id_);
          break;
        case PARTITION_ID:
          cur_row_.cells_[i].set_int(pkey.get_partition_id());
          break;
        case INDEX_ID:
          cur_row_.cells_[i].set_int(table_key.table_id_);
          break;
        case BASE_VERSION:
          cur_row_.cells_[i].set_int(table_key.trans_version_range_.base_version_);
          break;
        case MULTI_VERSION_START:
          cur_row_.cells_[i].set_int(table_key.trans_version_range_.multi_version_start_);
          break;
        case SNAPSHOT_VERSION:
          cur_row_.cells_[i].set_int(table_key.trans_version_range_.snapshot_version_);
          break;
        case MAX_MERGED_VERSION: {
          int64_t max_merged_version = 0;
          if (table->is_sstable()) {
            max_merged_version = static_cast<ObSSTable*>(table)->get_max_merged_trans_version();
          }
          cur_row_.cells_[i].set_int(max_merged_version);
          break;
        }
        case UPPER_TRANS_VERSION: {
          cur_row_.cells_[i].set_int(table->get_upper_trans_version());
          break;
        }
        case START_LOG_TS:
          cur_row_.cells_[i].set_uint64(table_key.log_ts_range_.start_log_ts_);
          break;
        case END_LOG_TS:
          cur_row_.cells_[i].set_uint64(table_key.log_ts_range_.end_log_ts_);
          break;
        case MAX_LOG_TS:
          cur_row_.cells_[i].set_uint64(table_key.log_ts_range_.max_log_ts_);
          break;
        case OB_VERSION:
          cur_row_.cells_[i].set_int(table_key.version_);
          break;
        case LOGICAL_DATA_VERSION: {
          int64_t data_version = 0;
          if (table->is_memtable()) {
            data_version = table_key.version_;
          } else {
            data_version = static_cast<ObSSTable*>(table)->get_logical_data_version();
          }
          cur_row_.cells_[i].set_int(data_version);
          break;
        }
        case SIZE: {
          int64_t size = 0;
          if (table->is_memtable()) {
            size = static_cast<memtable::ObMemtable*>(table)->get_occupied_size();
          } else {
            size = static_cast<ObSSTable*>(table)->get_meta().occupy_size_;
          }
          cur_row_.cells_[i].set_int(size);
          break;
        }
        case COMPACT_ROW: {
          int64_t compact_row = 0;
          if (table->is_sstable()) {
            compact_row = static_cast<ObSSTable*>(table)->has_compact_row();
          }
          cur_row_.cells_[i].set_int(compact_row);
          break;
        }
        case IS_ACTIVE: {
          int64_t is_active = 0;
          if (table->is_memtable()) {
            is_active = static_cast<memtable::ObMemtable*>(table)->is_active_memtable();
          }
          cur_row_.cells_[i].set_int(is_active);
          break;
        }
        case TIMESTAMP: {
          cur_row_.cells_[i].set_int(table->get_timestamp());
          break;
        }
        case REF:
          cur_row_.cells_[i].set_int(table->get_ref());
          break;
        case WRITE_REF: {
          int64_t write_ref = 0;
          if (table->is_memtable()) {
            write_ref = static_cast<memtable::ObMemtable*>(table)->get_write_ref();
          }
          cur_row_.cells_[i].set_int(write_ref);
          break;
        }
        case TRX_COUNT: {
          int64_t trx_count = 0;
          if (table->is_memtable()) {
            trx_count = static_cast<memtable::ObMemtable*>(table)->get_active_trx_count();
          } else {
            trx_count = 0;
          }
          cur_row_.cells_[i].set_int(trx_count);
          break;
        }
        case PENDING_CB_COUNT: {
          int64_t pending_cb_cnt = 0;
          if (table->is_memtable()) {
            pending_cb_cnt = static_cast<memtable::ObMemtable*>(table)->get_pending_cb_count();
          } else {
            pending_cb_cnt = 0;
          }
          cur_row_.cells_[i].set_int(pending_cb_cnt);
          break;
        }
        case CONTAIN_UNCOMMITTED_ROW: {
          bool contain_uncommitted_row = false;
          if (table->is_memtable()) {
            contain_uncommitted_row = true;
          } else {
            contain_uncommitted_row = static_cast<storage::ObSSTable*>(table)->contain_uncommitted_row();
          }
          cur_row_.cells_[i].set_bool(contain_uncommitted_row);
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
