/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_COMMON_OB_DATUM_ROW_STORE_H
#define OCEANBASE_COMMON_OB_DATUM_ROW_STORE_H
#include <stdint.h>
#include <utility>
#include "common/row/ob_row.h"
#include "common/row/ob_row_iterator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/string/ob_string.h"
#include "ob_datum_row.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRowStore
{
public:
  struct BlockInfo;
  class Iterator
  {
  public:
    friend class ObDatumRowStore;
    int get_next_row(ObDatumRow &row);
  private:
    explicit Iterator(const ObDatumRowStore &row_store);
  protected:
    const ObDatumRowStore &row_store_;
    const BlockInfo *cur_iter_block_;
    int64_t cur_iter_pos_;
  };
public:
  ObDatumRowStore();
  ~ObDatumRowStore();
  void clear_rows();
  int add_row(const ObDatumRow &row);
  inline int64_t get_row_count() const { return row_count_; }
  inline int64_t get_col_count() const { return col_count_; }
  Iterator begin() const;
  TO_STRING_KV(N_BLOCK_NUM, blocks_.get_block_count(),
               N_ROW_COUNT, row_count_,
               N_COLUMN_COUNT, col_count_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDatumRowStore);

  static const int64_t BIG_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t NORMAL_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

  // non-circular doubly linked list
  class BlockList
  {
  public:
    BlockList();
    void reset();
    int add_last(BlockInfo *block);
    BlockInfo *get_first() { return first_; }
    BlockInfo *get_last() { return last_; }
    const BlockInfo *get_first() const { return first_; }
    const BlockInfo *get_last() const { return last_; }
    int64_t get_block_count() const { return count_; }
    int64_t get_used_mem_size() const { return used_mem_size_; }
  private:
    BlockInfo *first_;
    BlockInfo *last_;
    int64_t count_;
    int64_t used_mem_size_;  // bytes of all blocks
  };
private:
  int new_block(int64_t block_size, BlockInfo *&block);
private:
  DefaultPageAllocator inner_alloc_;
  BlockList blocks_;
  int64_t row_count_;
  int64_t col_count_;
};

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_DATUM_ROW_STORE_H */
