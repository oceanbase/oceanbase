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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_TREE_CURSOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_TREE_CURSOR_H_

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObCGRowKeyTransHelper final
{
public:
  ObCGRowKeyTransHelper();
  ~ObCGRowKeyTransHelper() = default;
  int trans_to_cg_range(
      const int64_t start_row_offset,
      const ObDatumRange &range);
  int trans_to_cg_rowkey(
      const int64_t start_row_offset,
      const ObDatumRowkey &rowkey,
      const bool is_start_key = true);
  const ObDatumRowkey &get_result_start_key() const { return result_range_.start_key_; }
  const ObDatumRowkey &get_result_end_key() const { return result_range_.end_key_; }
  const ObDatumRange &get_result_range() const { return result_range_; }

private:
  ObDatumRange result_range_;
  ObStorageDatum datums_[2];
  DISALLOW_COPY_AND_ASSIGN(ObCGRowKeyTransHelper);
};

struct ObIndexBlockTreePathItem
{
  ObIndexBlockTreePathItem()
    : macro_block_id_(), curr_row_idx_(0), row_count_(0), start_row_offset_(0),
      block_data_(), cache_handle_(), row_store_type_(),
      is_root_micro_block_(false),  is_block_data_held_(false),
      is_block_allocated_(false), is_block_transformed_(false), block_from_cache_(false) {}

  ~ObIndexBlockTreePathItem() { reset(); }

  void reset();

  MacroBlockId macro_block_id_;
  int64_t curr_row_idx_;
  int64_t row_count_;
  int64_t start_row_offset_;
  ObMicroBlockData block_data_;
  ObMicroBlockBufferHandle cache_handle_;
  uint8_t row_store_type_;
  bool is_root_micro_block_;
  bool is_block_data_held_;
  bool is_block_allocated_;
  bool is_block_transformed_;
  bool block_from_cache_;
  TO_STRING_KV(K_(macro_block_id), K_(row_store_type), K_(start_row_offset),
      K_(is_root_micro_block), K_(is_block_data_held), K_(is_block_allocated),
      K_(is_block_transformed), K_(block_from_cache), K_(curr_row_idx), K_(row_count));
};

class ObIndexBlockTreePath
{
public:
  ObIndexBlockTreePath();
  ~ObIndexBlockTreePath();
  void reset();
  int init();

  int push(ObIndexBlockTreePathItem *item_ptr);
  int pop(ObIndexBlockTreePathItem *&item_ptr);
  int at(int64_t depth, const ObIndexBlockTreePathItem *&item_ptr);
  int get_next_item_ptr(ObIndexBlockTreePathItem *&next_item_ptr);
  OB_INLINE bool empty() { return path_.empty(); }
  OB_INLINE int64_t depth() { return path_.count(); }

  OB_INLINE ObIAllocator *get_allocator() { return &allocator_; }

  TO_STRING_KV(K_(item_stack), KPC(next_item_), K_(path));
public:
  class PathItemStack
  {
  public:
    static const uint8_t MAX_TREE_FIX_BUF_LENGTH = 8;
    PathItemStack(ObIAllocator &allocator);
    ~PathItemStack() { reset(); }
    void reset();

    int acquire_next_item_ptr(ObIndexBlockTreePathItem *&next_item_ptr);
    int release_item(ObIndexBlockTreePathItem *release_item_ptr);
    int top(ObIndexBlockTreePathItem *&curr_top_item);
    TO_STRING_KV(K_(buf_capacity), K_(idx));
  private:
    int expand();
    int get_curr_item(ObIndexBlockTreePathItem *&curr_item);
    void release_item_memory(ObIndexBlockTreePathItem &release_item);
  private:
    ObIAllocator *allocator_;
    ObIndexBlockTreePathItem fix_buf_[MAX_TREE_FIX_BUF_LENGTH];
    ObIndexBlockTreePathItem *var_buf_;
    int64_t buf_capacity_;
    int64_t idx_;
  };

private:
  ObFIFOAllocator allocator_;
  PathItemStack item_stack_;
  ObIndexBlockTreePathItem *next_item_;
  ObSEArray<ObIndexBlockTreePathItem *, PathItemStack::MAX_TREE_FIX_BUF_LENGTH> path_;
};


// Methods, status and context to iterate through an index block tree
// (without update block cache)
class ObIndexBlockTreeCursor
{
public:
  enum MoveDepth
  {
    ONE_LEVEL = 0,
    ROOT      = 1,
    MACRO     = 2,
    LEAF      = 3,
    DEPTH_MAX,
  };
  enum TreeType
  {
    INDEX_BLOCK     = 0,
    DATA_MACRO_META = 1,
    TREE_TYPE_MAX,
  };

public:
  ObIndexBlockTreeCursor();
  virtual ~ObIndexBlockTreeCursor();

  void reset();

  int init(
      const blocksstable::ObSSTable &sstable,
      ObIAllocator &allocator,
      const ObITableReadInfo *read_info,
      const TreeType tree_type = TreeType::INDEX_BLOCK);

  // Interfaces to move cursor on index block tree
  int drill_down(
      const ObDatumRowkey &rowkey,
      const MoveDepth depth,
      bool &is_beyond_the_range);
  int drill_down(
      const ObDatumRowkey &rowkey,
      const MoveDepth depth,
      const bool is_lower_bound,
      bool &equal,
      bool &is_beyond_the_range);
  int pull_up_to_root();
  int move_forward(const bool is_reverse_scan);

  TO_STRING_KV(K_(cursor_path), K_(is_normal_cg_sstable), K_(curr_path_item));
public:
  // Interfaces to get data on index tree via cursor
  int get_idx_parser(const ObIndexBlockRowParser *&parser);
  int get_idx_row_header(const ObIndexBlockRowHeader *&idx_header);
  int get_macro_block_id(MacroBlockId &macro_id);

  // Need to release held item at the end of lifetime
  int get_child_micro_infos(
      const ObDatumRange &range,
      ObArenaAllocator &endkey_allocator,
      ObIArray<ObDatumRowkey> &endkeys,
      ObIArray<ObMicroIndexInfo> &micro_index_infos,
      ObIndexBlockTreePathItem &hold_item);
  int release_held_path_item(ObIndexBlockTreePathItem &held_item);
  int get_current_endkey(ObDatumRowkey &endkey, const bool get_schema_rowkey = false);
  int estimate_range_macro_count(const blocksstable::ObDatumRange &range, int64_t &macro_count, int64_t &ratio);

private:
  int set_reader(const ObRowStoreType store_type);
  template <typename T>
  int init_reader(T *&cache_reader_ptr);
  // TODO: prefetch
  int get_next_level_block(
      const MacroBlockId &macro_block_id,
      const ObIndexBlockRowHeader &idx_row_header,
      const int64_t curr_row_offset);
  int load_micro_block_data(const MacroBlockId &macro_block_id,
                            const int64_t block_offset,
                            const ObIndexBlockRowHeader &idx_row_header);
  int get_transformed_data_header(
      const ObIndexBlockTreePathItem &path_item,
      const ObIndexBlockDataHeader *&idx_data_header);
  int read_next_level_row(const int64_t row_idx);
  int get_next_level_row_cnt(int64_t &row_cnt);

  // Index blocks store endkeys, so locate with lower_bound() can make sure we find
  // the exact micro block by which the search key is included in range
  int drill_down();
  int pull_up(const bool cascade = false, const bool is_reverse_scan = false);
  int locate_rowkey_in_curr_block(const ObDatumRowkey &rowkey, bool &is_beyond_the_range);
  int search_rowkey_in_transformed_block(
      const ObDatumRowkey &rowkey,
      const ObIndexBlockDataHeader &idx_data_header,
      int64_t &row_idx,
      bool &equal,
      const bool lower_bound = true);
  int locate_range_in_curr_block(const ObDatumRange &range, int64_t &begin_idx, int64_t &end_idx);
  int move_to_upper_bound(const ObDatumRowkey &rowkey);
  // get micro block infos in current intermediate micro block
  int get_micro_block_infos(
      const ObDatumRange &range,
      ObIArray<ObMicroIndexInfo> &micro_index_infos);
  // get micro block endkeys in current intermediate micro block
  int get_micro_block_endkeys(
      const ObDatumRange &range,
      ObArenaAllocator &endkey_allocator,
      ObIArray<ObMicroIndexInfo> &micro_index_infos,
      ObIArray<ObDatumRowkey> &end_keys);
  int check_reach_target_depth(const MoveDepth target_depth, bool &reach_target_depth);
  int init_curr_endkey(ObDatumRow &row_buf, const int64_t datum_cnt);

private:
  static const int64_t OB_INDEX_BLOCK_MAX_COL_CNT =
      common::OB_MAX_ROWKEY_COLUMN_NUMBER + OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER + 1;
  ObIndexBlockTreePath cursor_path_;
  ObIndexMicroBlockCache *index_block_cache_;
  ObIMicroBlockReader *reader_;
  ObMicroBlockReaderHelper micro_reader_helper_;

  // Micro Block Cache Read/Prefetch
  TreeType tree_type_;
  ObCGRowKeyTransHelper rowkey_helper_;
  int64_t tenant_id_;
  int64_t rowkey_column_cnt_;

  ObIndexBlockTreePathItem *curr_path_item_;

  ObDatumRow row_;
  ObDatumRowkey vector_endkey_;
  ObIndexBlockRowParser idx_row_parser_;
  const ObITableReadInfo *read_info_;
  ObSSTableMetaHandle sstable_meta_handle_;
  bool is_normal_cg_sstable_;
  bool is_inited_;
};

OB_INLINE ObIndexBlockTreeCursor::TreeType *get_index_tree_type_map()
{
  static ObIndexBlockTreeCursor::TreeType tree_type_map[] = {
      ObIndexBlockTreeCursor::DATA_MACRO_META,
      ObIndexBlockTreeCursor::TREE_TYPE_MAX
  };
  STATIC_ASSERT(ARRAYSIZEOF(tree_type_map) == ObMacroBlockMetaType::MAX + 1,
      "tree type map count mismatch with macro block meta type count");
  return tree_type_map;
};

} // namespace oceanbase
} // namespace blocksstable

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_TREE_CURSOR_H_
