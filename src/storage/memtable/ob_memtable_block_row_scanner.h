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

#ifndef OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_ROW_SCANNER_H_
#define OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_ROW_SCANNER_H_

#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace storage
{
  class PushdownFilterInfo;
}
namespace memtable
{
// This class provides batch scan and filter pushdown function for memtable.
// It is similar to class ObMicroBlockRowScanner for micor block (both are
// intermediate classes between Iterator and ObBlockRowStore).
class ObMemtableBlockRowScanner {
private:
  static const int64_t MAX_ROW_BATCH_SIZE = 225;
  // If this batch reach the border key while constructing current batch.
  bool last_batch_;
  // If iterator reach the end while constructing current batch.
  bool end_of_iter_;
  // If this class has been inited.
  bool is_inited_;
  // Next index to get a new row in current batch.
  int64_t cur_idx_;
  // The number of rows after constructing current batch.
  int64_t batch_size_;
  common::ObIAllocator *allocator_;
  // The bitmap result after pushdown filter calculated.
  const common::ObBitmap *result_bitmap_;
  const storage::ObITableReadInfo *read_info_;
  // Current batch of rows.
  blocksstable::ObDatumRow rows_[MAX_ROW_BATCH_SIZE];
public:
  ObMemtableBlockRowScanner();
  virtual ~ObMemtableBlockRowScanner();
  OB_INLINE bool is_full() const { return batch_size_ == MAX_ROW_BATCH_SIZE; }
  OB_INLINE bool has_reached_iter_end() const { return end_of_iter_; }
  OB_INLINE bool has_reached_border_key() const { return last_batch_; }
  OB_INLINE void set_reach_iter_end() { end_of_iter_ = true; }
  OB_INLINE void set_last_batch() { last_batch_ = true; }
  OB_INLINE void set_bitmap(const common::ObBitmap *bitmap) { result_bitmap_ = bitmap; }
  OB_INLINE void increase_size() { ++batch_size_; }
  OB_INLINE int64_t size() const { return batch_size_; }
  OB_INLINE const common::ObBitmap *get_bitmap() { return result_bitmap_; }
  OB_INLINE blocksstable::ObDatumRow *next_new_row() { return rows_ + (batch_size_); }
  int init(
      common::ObIAllocator *allocator,
      const int64_t capacity,
      char *trans_info_ptr,
      const storage::ObITableReadInfo *read_info);
  void reset();
  void reuse();
  blocksstable::ObDatumRow *get_next_row(bool fast_filter_skipped);
  bool has_cached_row();
  int filter_pushdown_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      storage::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &bitmap);
  // TODO(jianxian): this function is the same as ObIMicroBlockReader::filter_white_filter,
  // and it needs to be considered whether it can be reused.
  int filter_white_filter(
      const sql::ObWhiteFilterExecutor &filter,
      const common::ObObj &obj,
      bool &filtered);
};
} // end of namespace memtable
} // end of namespace oceanbase
#endif
