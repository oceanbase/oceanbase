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

#include "observer/virtual_table/ob_all_virtual_memstore_info.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase {
namespace observer {

ObAllVirtualMemstoreInfo::ObAllVirtualMemstoreInfo()
    : ObVirtualTableScannerIterator(),
      partition_service_(NULL),
      addr_(),
      pkey_(),
      ptt_iter_(NULL),
      tables_handle_(),
      memtables_(),
      memtable_array_pos_(0)
{}

ObAllVirtualMemstoreInfo::~ObAllVirtualMemstoreInfo()
{
  reset();
}

void ObAllVirtualMemstoreInfo::reset()
{
  addr_.reset();
  pkey_.reset();
  memtables_.reset();
  tables_handle_.reset();
  if (NULL != ptt_iter_) {
    if (NULL == partition_service_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      partition_service_->revert_pg_partition_iter(ptt_iter_);
      ptt_iter_ = NULL;
    }
  }
  partition_service_ = NULL;
  memtable_array_pos_ = 0;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualMemstoreInfo::make_this_ready_to_read()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid pointer", K(allocator_), K(ret));
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualMemstoreInfo::get_next_memtable(memtable::ObMemtable*& mt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_) || OB_ISNULL(ptt_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid pointer", K(partition_service_), K(ptt_iter_), K(ret));
  }

  // memtable_array_pos_ == memtable_array_.count() means all the memtables of this
  // partition have been disposed or the first time get_next_memtable() is invoked,
  // when get_next_memtable() is invoked the first time, memtable_array_pos_ and
  // memtable_array_.count() are both 0
  while (OB_SUCCESS == ret && memtable_array_pos_ == memtables_.count()) {
    memtables_.reset();
    tables_handle_.reset();
    memtable_array_pos_ = 0;
    ObPGPartition* pg_partition = NULL;
    ObIPartitionStorage* storage = NULL;

    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS != (ret = ptt_iter_->get_next(pg_partition))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "scan next pg partition failed.", K(ret));
      }
    } else if (NULL == pg_partition) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "get pg partition failed", K(ret));
    } else if (NULL == (storage = pg_partition->get_storage())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "get storage failed", K(ret));
    } else if (OB_FAIL(storage->get_all_tables(tables_handle_))) {
      SERVER_LOG(WARN, "fail to get all stores", K(ret));
    } else if (tables_handle_.get_count() == 0) {
      continue;
    } else if (OB_FAIL(tables_handle_.get_all_memtables(memtables_))) {
      SERVER_LOG(WARN, "failed to get all memtables", K(ret));
    } else {
      pkey_ = pg_partition->get_partition_key();
    }
  }
  if (OB_SUCC(ret)) {
    mt = memtables_.at(memtable_array_pos_++);
    if (OB_ISNULL(mt)) {
      ret = OB_ERR_SYS;
      SERVER_LOG(ERROR, "memtable must not null", K(ret), K(mt));
    }
  }
  return ret;
}

int ObAllVirtualMemstoreInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObMemtable* mt = NULL;
  if (NULL == allocator_ || NULL == partition_service_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "allocator_ or partition_service_ shouldn't be NULL", K(allocator_), K(partition_service_), K(ret));
  } else if (!start_to_read_ && OB_SUCCESS != (ret = make_this_ready_to_read())) {
    SERVER_LOG(WARN, "fail to make_this_ready_to_read", K(ret), K(start_to_read_));
  } else if (NULL == ptt_iter_ && NULL == (ptt_iter_ = partition_service_->alloc_pg_partition_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc pg partitioin iter", K(ret));
  } else if (OB_SUCCESS != (ret = get_next_memtable(mt))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_memtable failed", K(ret));
    }
  } else if (NULL == mt) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "mt shouldn't NULL here", K(ret), K(mt));
  } else {
    memtable::ObMtStat& mt_stat = mt->get_mt_stat();
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // tenant_id
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey_.table_id_));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cur_row_.cells_[i].set_int(pkey_.table_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // parition_idx
          cur_row_.cells_[i].set_int(pkey_.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // partition_cnt
          cur_row_.cells_[i].set_int(pkey_.get_partition_cnt());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // mem_versioin
          if (OB_SUCCESS != (ret = mt->get_version().version_to_string(mem_version_buf_, sizeof(mem_version_buf_)))) {
            SERVER_LOG(WARN, "fail to convert version to string", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(mem_version_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // base version
          cur_row_.cells_[i].set_int(mt->get_key().trans_version_range_.base_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // multi_version_start
          cur_row_.cells_[i].set_int(mt->get_key().trans_version_range_.multi_version_start_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // snapshot_version
          cur_row_.cells_[i].set_int(mt->get_key().trans_version_range_.snapshot_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // is_active
          cur_row_.cells_[i].set_int(mt->is_active_memtable());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // mem_used
          cur_row_.cells_[i].set_int(mt->get_occupied_size());
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // hash_item_count
          cur_row_.cells_[i].set_int(mt->get_hash_item_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // hash_mem_used
          cur_row_.cells_[i].set_int(mt->get_hash_alloc_memory());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // btree_item_count
          cur_row_.cells_[i].set_int(mt->get_btree_item_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // btree_mem_used
          cur_row_.cells_[i].set_int(mt->get_btree_alloc_memory());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // insert_row_count
          cur_row_.cells_[i].set_int(mt_stat.insert_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // update_row_count
          cur_row_.cells_[i].set_int(mt_stat.update_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // delete_row_count
          cur_row_.cells_[i].set_int(mt_stat.delete_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // purge_row_count
          cur_row_.cells_[i].set_int(mt_stat.purge_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          // purge_queue_count
          cur_row_.cells_[i].set_int(mt_stat.purge_queue_count_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  } else {
    memtables_.reset();
    tables_handle_.reset();
    memtable_array_pos_ = 0;
    // revert ptt_iter_ no matter ret is OB_ITER_END or other errors
    if (NULL != ptt_iter_) {
      if (NULL == partition_service_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "partition_service_ is null, ", K(ret));
      } else {
        partition_service_->revert_pg_partition_iter(ptt_iter_);
        ptt_iter_ = NULL;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
