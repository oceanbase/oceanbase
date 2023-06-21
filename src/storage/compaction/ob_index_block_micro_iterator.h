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

#ifndef OCEANBASE_COMPACTION_OB_INDEX_BLOCK_MICRO_ITERATOR_H_
#define OCEANBASE_COMPACTION_OB_INDEX_BLOCK_MICRO_ITERATOR_H_

#include "common/ob_range.h"
#include "share/io/ob_io_manager.h"
#include "share/schema/ob_table_schema.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_index_block_tree_cursor.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
namespace compaction
{
// Load MacroBlock data and iterate by micro block
class ObMacroBlockDataIterator
{
public:
  ObMacroBlockDataIterator();
  virtual ~ObMacroBlockDataIterator();

  void reset();

  int init(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const common::ObIArray<blocksstable::ObMicroIndexInfo> &micro_block_infos,
      const common::ObIArray<blocksstable::ObDatumRowkey> &endkeys,
      const blocksstable::ObDatumRange *range = nullptr);

  int next_micro_block(blocksstable::ObMicroBlock &micro_block);

  OB_INLINE const common::ObIArray<blocksstable::ObMicroIndexInfo> &get_micro_block_infos()
  {
    return *micro_block_infos_;
  }
  OB_INLINE const common::ObIArray<blocksstable::ObDatumRowkey> &get_end_keys() { return *endkeys_; }
  OB_INLINE bool is_left_border() { return 0 == cur_micro_cursor_; }
  OB_INLINE bool is_right_border() { return micro_block_infos_->count() - 1 == cur_micro_cursor_; }
  OB_INLINE int64_t get_micro_index() const { return cur_micro_cursor_; }

  OB_INLINE int64_t get_range_block_count();
private:
  const char *macro_buf_;
  int64_t macro_buf_size_;
  blocksstable::ObDatumRange range_;
  const common::ObIArray<blocksstable::ObMicroIndexInfo> *micro_block_infos_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *endkeys_;
  int64_t cur_micro_cursor_;
  bool is_inited_;
};

class ObIndexBlockMicroIterator
{
public:
  ObIndexBlockMicroIterator();
  virtual ~ObIndexBlockMicroIterator() {}

  int init(
      const blocksstable::ObDatumRange &range,
      const ObITableReadInfo &read_info,
      const blocksstable::MacroBlockId &macro_id,
      const common::ObIArray<blocksstable::ObMicroIndexInfo> &micro_block_infos,
      const common::ObIArray<blocksstable::ObDatumRowkey> &endkeys,
      const ObRowStoreType row_store_type,
      const blocksstable::ObSSTable *sstable);
  void reset();
  int next(const blocksstable::ObMicroBlock *&micro_block);

  OB_INLINE bool is_left_border() { return data_iter_.is_left_border(); }
  OB_INLINE bool is_right_border() { return data_iter_.is_right_border(); }
private:
  int check_range_include_rowkey_array(
      const blocksstable::ObDatumRange range,
      const common::ObIArray<blocksstable::ObDatumRowkey> &endkeys,
      const blocksstable::ObStorageDatumUtils &datum_utils);

private:
  ObMacroBlockDataIterator data_iter_;
  blocksstable::ObDatumRange range_;
  blocksstable::ObMicroBlock micro_block_;
  blocksstable::ObMacroBlockHandle macro_handle_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockMicroIterator);
};


} // namespace compaction
} // namespace oceanbase

#endif // OCEANBASE_COMPACTION_OB_INDEX_BLOCK_MICRO_ITERATOR_H_
