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

#include "storage/tablelock/ob_table_lock_iterator.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/blocksstable/ob_datum_row.h"  // ObStoreRowFlag

namespace oceanbase
{
namespace storage
{

int ObTableLockScanIterator::init(ObLockMemtable *lock_memtable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_memtable->get_table_lock_store_info(table_lock_store_info_))) {
    STORAGE_LOG(WARN, "get_table_lock_store_info failed");
  } else if (OB_FAIL(row_.init(allocator_, COLUMN_CNT))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(buf_.reserve(TABLE_LOCK_BUF_LENGTH))) {
    STORAGE_LOG(WARN, "Failed to reserve tx local buffer", K(ret));
  } else {
    row_.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
    idx_ = 0;
    is_inited_ = true;
  }

  return ret;
}

int ObTableLockScanIterator::inner_get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "table lock memtable scan iterator is not inited");
  } else if (idx_ == table_lock_store_info_.count()) {
    ret = OB_ITER_END;
  } else {
    int64_t pos = 0;

    if (OB_SUCC(ret)) {
      transaction::tablelock::ObTableLockOp &store_info = table_lock_store_info_[idx_];
      int64_t serialize_size = store_info.get_serialize_size();

      if (OB_FAIL(buf_.reserve(serialize_size))) {
        STORAGE_LOG(WARN, "Failed to reserve local buffer", K(ret));
      } else {
        if (OB_FAIL(store_info.serialize(buf_.get_ptr(), serialize_size, pos))) {
          STORAGE_LOG(WARN, "failed to serialize store info", K(ret), K(store_info), K(pos));
        } else {
          row_.storage_datums_[TABLE_LOCK_KEY_COLUMN].set_int((int)(idx_));
          row_.storage_datums_[TABLE_LOCK_KEY_COLUMN + 1].set_int(-4096);
          row_.storage_datums_[TABLE_LOCK_KEY_COLUMN + 2].set_int(0);
          row_.storage_datums_[TABLE_LOCK_VAL_COLUMN].set_string(
            ObString(serialize_size, buf_.get_ptr()));
          row_.set_first_multi_version_row();
          row_.set_last_multi_version_row();
          row_.set_compacted_multi_version_row();

          row = &row_;
          STORAGE_LOG(INFO, "write ctx info", K(store_info), K(idx_));
          idx_++;
        }
      }
    }
  }

  return ret;
}

void ObTableLockScanIterator::reset()
{
  idx_ = -1;
  table_lock_store_info_.reset();
  buf_.reset();
  row_.reset();
  allocator_.reset();
  is_inited_ = false;
}

void ObTableLockScanIterator::reuse()
{
  reset();
}

}  // namespace storage
}  // namespace oceanbase
