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

#ifndef OB_SSTABLE_SEC_META_ITERATOR_H_
#define OB_SSTABLE_SEC_META_ITERATOR_H_

#include "storage/ob_micro_block_handle_mgr.h"
#include "ob_index_block_tree_cursor.h"

namespace oceanbase
{
namespace storage
{
class ObBlockMetaTree;
}
namespace blocksstable
{

class ObSSTableSecMetaIterator
{
public:
  ObSSTableSecMetaIterator();
  virtual ~ObSSTableSecMetaIterator() { reset(); }
  void reset();
  int open(
      const ObDatumRange &query_range,
      const ObMacroBlockMetaType meta_type,
      const ObSSTable &sstable,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse_scan = false,
      const int64_t sample_step = 0);
  virtual int get_next(ObDataMacroBlockMeta &macro_meta);
  TO_STRING_KV(K_(is_reverse_scan), K_(is_inited), K_(start_bound_micro_block),
      K_(end_bound_micro_block), K_(idx_cursor), K_(curr_handle_idx), K_(prefetch_handle_idx),
      K_(prev_block_row_cnt), K_(curr_block_start_idx), K_(curr_block_end_idx), K_(curr_block_idx),
      K_(step_cnt), K_(is_prefetch_end), KPC_(query_range), KPC_(rowkey_read_info));

private:
  void set_iter_end();
  int adjust_index(const int64_t begin_idx, const int64_t end_idx, const int64_t row_cnt);
  int init_micro_reader(const ObRowStoreType row_store_type, ObIAllocator &allocator);
  int init_by_type(const ObMacroBlockMetaType meta_type);
  OB_INLINE bool is_handle_buffer_empty() const { return curr_handle_idx_ == prefetch_handle_idx_; }
  OB_INLINE int64_t handle_buffer_count() const
  {
    return prefetch_handle_idx_ - curr_handle_idx_;
  }

  OB_INLINE bool is_target_row_in_curr_block() const
  {
    return curr_block_idx_ >= curr_block_start_idx_ && curr_block_idx_ <= curr_block_end_idx_;
  }

  int locate_bound_micro_block(
      const ObDatumRowkey &rowkey,
      const bool lower_bound,
      ObMicroBlockId &bound_block,
      bool &is_beyond_range);
  int prefetch_micro_block(int64_t prefetch_depth);
  int open_next_micro_block(MacroBlockId &macro_id);
  int open_meta_root_block();

  // TODO: opt with prefetch
  int get_micro_block(
      const MacroBlockId &macro_id,
      const ObIndexBlockRowHeader &idx_row_header,
      ObMicroBlockDataHandle &data_handle);
private:
  static const int32_t HANDLE_BUFFER_SIZE = 16;
  static const int32_t MAX_SECONDAY_META_COLUMN_COUNT = OB_MAX_ROWKEY_COLUMN_NUMBER + 3;
  int64_t tenant_id_;
  const ObITableReadInfo *rowkey_read_info_;
  ObSSTableMetaHandle sstable_meta_hdl_;
  common::ObQueryFlag prefetch_flag_;
  ObIndexBlockTreeCursor idx_cursor_;
  ObMacroBlockReader macro_reader_;
  ObDataMicroBlockCache *block_cache_;
  ObIMicroBlockReader *micro_reader_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  storage::ObBlockMetaTree *block_meta_tree_;
  const ObDatumRange *query_range_;
  ObMicroBlockId start_bound_micro_block_;
  ObMicroBlockId end_bound_micro_block_;
  storage::ObMicroBlockDataHandle micro_handles_[HANDLE_BUFFER_SIZE];
  ObDatumRow row_;
  int64_t curr_handle_idx_;
  int64_t prefetch_handle_idx_;
  int64_t prev_block_row_cnt_;
  int64_t curr_block_start_idx_;
  int64_t curr_block_end_idx_;
  int64_t curr_block_idx_;
  int64_t step_cnt_;
  ObRowStoreType row_store_type_;
  bool is_reverse_scan_;
  bool is_prefetch_end_;
  bool is_range_end_key_multi_version_;
  bool is_inited_;
};

} // blocksstable
} // oceanbase

#endif // OB_SSTABLE_SEC_META_ITERATOR_H_
