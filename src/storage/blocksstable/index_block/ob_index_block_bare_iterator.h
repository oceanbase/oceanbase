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

#ifndef OB_INDEX_BLOCK_BARE_ITERATOR_H_
#define OB_INDEX_BLOCK_BARE_ITERATOR_H_

#include "storage/blocksstable/ob_macro_block_bare_iterator.h"

namespace oceanbase
{
namespace blocksstable
{

// Specifically designed for iterating the index micro block within macro blocks
// to retrieve logic micro block id of all micro blocks in the macro block
class ObIndexBlockBareIterator : protected ObMicroBlockBareIterator
{
public:
  ObIndexBlockBareIterator(const uint64_t tenant_id = MTL_ID());
  virtual ~ObIndexBlockBareIterator();
  void reset();

  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const bool is_macro_meta_block,
      const bool need_check_data_integrity);
  
  int64_t get_row_count() const { return row_count_; }
  int get_next_logic_micro_id(ObLogicMicroBlockId &logic_micro_id, int64_t &micro_checksum);

  // no need print row_
  INHERIT_TO_STRING_KV(
      "ObMicroBlockBareIterator", ObMicroBlockBareIterator,
      K(rowkey_column_count_), K(cur_row_idx_), K(row_count_));

private:
  int64_t rowkey_column_count_;
  int64_t cur_row_idx_;
  int64_t row_count_;
  blocksstable::ObDatumRow row_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif /*OB_INDEX_BLOCK_BARE_ITERATOR_H_*/