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

#ifndef OCEANBASE_STORAGE_OB_TABLE_LOCK_ITERATOR
#define OCEANBASE_STORAGE_OB_TABLE_LOCK_ITERATOR

#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace transaction
{
namespace tablelock
{
class ObLockMemtable;
struct ObTableLockOp;
}  // namespace tablelock
}  // namespace transaction

namespace storage
{

struct ObTableLockBuffer
{
  ObTableLockBuffer() = delete;
  ObTableLockBuffer(common::ObIAllocator &allocator) : allocator_(allocator)
  {
    buf_ = nullptr;
    buf_len_ = 0;
  }
  ~ObTableLockBuffer() { reset(); }
  OB_INLINE char *get_ptr() { return buf_; }
  OB_INLINE void reset()
  {
    if (nullptr != buf_) {
      allocator_.free(buf_);
    }
    buf_ = nullptr;
    buf_len_ = 0;
  }
  OB_INLINE int reserve(const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to reserve local buffer", K(buf_len));
    } else if (buf_len > buf_len_) {
      reset();
      if (OB_ISNULL(buf_ = reinterpret_cast<char *>(allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(buf_len));
      } else {
        buf_len_ = buf_len;
      }
    }
    return ret;
  }

  common::ObIAllocator &allocator_;
  char *buf_;
  int64_t buf_len_;
};

static const int64_t TABLE_LOCK_KEY_COLUMN = 0;
static const int64_t TABLE_LOCK_VAL_COLUMN = 3;

/**
 * @brief Using for dump table lock memtable
 */
class ObTableLockScanIterator : public memtable::ObIMemtableIterator
{
public:
  ObTableLockScanIterator()
    : allocator_(), row_(), buf_(allocator_), idx_(0), table_lock_store_info_(), is_inited_(false)
  {}

  ~ObTableLockScanIterator() {}

  virtual int init(ObLockMemtable *lock_memtable_);

  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);
  virtual void reset();
  virtual void reuse();

private:
  const static int64_t TABLE_LOCK_BUF_LENGTH = 1000;
  const static int64_t TABLE_LOCK_ARRAY_LENGTH = 20;
  const static int64_t COLUMN_CNT = 4;

  ObArenaAllocator allocator_;
  blocksstable::ObDatumRow row_;
  ObTableLockBuffer buf_;
  int64_t idx_;
  ObSEArray<oceanbase::transaction::tablelock::ObTableLockOp, TABLE_LOCK_ARRAY_LENGTH> table_lock_store_info_;
  bool is_inited_;
};

}  // namespace storage

}  // namespace oceanbase

#endif