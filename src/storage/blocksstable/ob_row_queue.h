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

#ifndef OB_ROW_QUEUE_H_
#define OB_ROW_QUEUE_H_

#include "lib/container/ob_raw_se_array.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace blocksstable {

class ObRowQueue {
public:
  ObRowQueue() : cell_cnt_(0), cur_pos_(0), row_type_(MAX_ROW_STORE), is_inited_(false)
  {}
  int init(const ObRowStoreType row_type, const int64_t cell_cnt);
  int add_empty_row(common::ObIAllocator& allocator);
  int add_row(const storage::ObStoreRow& row, common::ObIAllocator& allocator);
  int get_next_row(const storage::ObStoreRow*& row);
  OB_INLINE bool is_empty()
  {
    return 0 == rows_.count();
  }
  OB_INLINE bool has_next() const
  {
    return cur_pos_ < rows_.count();
  }
  OB_INLINE storage::ObStoreRow* get_first()
  {
    storage::ObStoreRow* row = NULL;
    if (!is_empty()) {
      row = rows_.at(0);
    }
    return row;
  }
  OB_INLINE storage::ObStoreRow* get_last()
  {
    storage::ObStoreRow* row = NULL;
    if (!is_empty()) {
      row = rows_.at(rows_.count() - 1);
    }
    return row;
  }
  OB_INLINE int64_t count() const
  {
    return rows_.count();
  }
  void reset()
  {
    cell_cnt_ = 0;
    cur_pos_ = 0;
    row_type_ = MAX_ROW_STORE;
    rows_.reset();
    is_inited_ = false;
  }

  void reuse()
  {
    cur_pos_ = 0;
    rows_.reuse();
  }
  TO_STRING_KV(K_(cell_cnt), K_(cur_pos), "count", rows_.count(), K_(row_type));

private:
  int alloc_row(storage::ObStoreRow*& row, common::ObIAllocator& allocator);

private:
  static const int64_t DEFAULT_MULTIVERSION_ROW_COUNT = 64;
  int64_t cell_cnt_;
  int64_t cur_pos_;
  common::ObRawSEArray<storage::ObStoreRow*, DEFAULT_MULTIVERSION_ROW_COUNT> rows_;
  ObRowStoreType row_type_;
  bool is_inited_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
