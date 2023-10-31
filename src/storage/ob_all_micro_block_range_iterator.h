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

#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"

namespace oceanbase
{
namespace storage
{
class ObAllMicroBlockRangeIterator
{
public:
  ObAllMicroBlockRangeIterator();
  ~ObAllMicroBlockRangeIterator();
  void reset();

  int open(
      const blocksstable::ObSSTable &sstable,
      const blocksstable::ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse_scan);
  int get_next_range(const blocksstable::ObDatumRange *&range);
private:
  int locate_bound_micro_block(
      const blocksstable::ObDatumRowkey &rowkey,
      const bool lower_bound,
      blocksstable::ObMicroBlockId &bound_block,
      bool &is_beyond_range);
  int generate_cur_range(const bool is_first_range, const bool is_last_range);
  int deep_copy_rowkey(const blocksstable::ObDatumRowkey &src_key, blocksstable::ObDatumRowkey &dest_key, char *&key_buf);
private:
  int64_t schema_rowkey_cnt_;
  blocksstable::ObIndexBlockTreeCursor tree_cursor_;
  blocksstable::ObMicroBlockId start_bound_micro_block_;
  blocksstable::ObMicroBlockId end_bound_micro_block_;
  blocksstable::ObDatumRowkey curr_key_;
  blocksstable::ObDatumRowkey prev_key_;
  char *curr_key_buf_;
  char *prev_key_buf_;
  ObIAllocator *allocator_;
  blocksstable::ObDatumRange micro_range_;
  const blocksstable::ObDatumRange *range_;
  bool is_reverse_scan_;
  bool is_iter_end_;
  bool is_inited_;
};

}
}
#endif /* OCEANBASE_STORAGE_OB_ALL_MICRO_BLOCK_RANGE_ITERATOR_H */
