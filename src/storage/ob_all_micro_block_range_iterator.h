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

#ifndef OCEANBASE_STORAGE_OB_ALL_MICRO_BLOCK_RANGE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_ALL_MICRO_BLOCK_RANGE_ITERATOR_H

#include "storage/ob_sstable.h"
#include "storage/blocksstable/ob_micro_block_index_reader.h"

namespace oceanbase {
namespace storage {

class ObAllMicroBlockRangeIterator {
  typedef blocksstable::ObBlockIndexIterator IndexCursor;

public:
  ObAllMicroBlockRangeIterator();
  ~ObAllMicroBlockRangeIterator();
  void reset();
  int init(const uint64_t index_id, const ObTableHandle& base_store, const common::ObExtStoreRange& range,
      const bool is_reverse_scan);
  int get_next_range(const common::ObStoreRange*& range);

private:
  int open_next_macro_block();
  int prefetch_block_index();
  int get_end_keys(common::ObIAllocator& allocator);
  int get_cur_micro_range();
  bool is_end_of_macro_block() const;
  int advance_micro_block_cursor();
  void reset_micro_block_cursor();
  bool is_first_macro_block() const;
  bool is_last_macro_block() const;

private:
  static const int64_t PREFETCH_CNT = 1;
  static const int64_t HANDLE_CNT = PREFETCH_CNT + 1;
  ObMacroBlockIterator macro_block_iterator_;
  blocksstable::ObMicroBlockIndexReader index_reader_;
  blocksstable::ObMacroBlockHandle macro_handles_[HANDLE_CNT];
  blocksstable::ObMacroBlockMetaHandle meta_handles_[HANDLE_CNT];
  common::ObArenaAllocator allocators_[HANDLE_CNT];
  common::ObStoreRowkey macro_block_start_keys_[HANDLE_CNT];
  common::ObArray<common::ObStoreRowkey> end_keys_;
  common::ObStoreRange micro_range_;
  const common::ObExtStoreRange* range_;
  ObSSTable* sstable_;
  int64_t macro_block_cnt_;
  int64_t cur_macro_block_cursor_;
  int64_t prefetch_macro_block_cursor_;
  int64_t cur_micro_block_cursor_;
  bool is_reverse_scan_;
  bool is_inited_;
  blocksstable::ObStorageFileHandle file_handle_;
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OCEANBASE_STORAGE_OB_ALL_MICRO_BLOCK_RANGE_ITERATOR_H */
