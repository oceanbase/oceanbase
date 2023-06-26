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

#ifndef OB_INDEX_BLOCK_MACRO_ITERATOR_H_
#define OB_INDEX_BLOCK_MACRO_ITERATOR_H_

#include "ob_index_block_tree_cursor.h"
#include "ob_sstable_sec_meta_iterator.h"
#include "ob_datum_rowkey.h"

namespace oceanbase {
namespace blocksstable {

struct ObMacroBlockDesc
{
  blocksstable::MacroBlockId macro_block_id_;
  blocksstable::ObDataMacroBlockMeta *macro_meta_;
  ObDatumRange range_;
  int64_t row_store_type_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t max_merged_trans_version_;
  int32_t row_count_;
  int32_t row_count_delta_;
  bool contain_uncommitted_row_;
  bool is_deleted_;
  ObMacroBlockDesc()
    : macro_block_id_(),
      macro_meta_(nullptr),
      range_(),
      row_store_type_(FLAT_ROW_STORE),
      schema_version_(0),
      snapshot_version_(0),
      max_merged_trans_version_(0),
      row_count_(0),
      row_count_delta_(0),
      contain_uncommitted_row_(true),
      is_deleted_(false) {}
  OB_INLINE bool is_valid() const
  {
    return macro_block_id_.is_valid() && range_.is_valid();
  }
  OB_INLINE bool is_valid_with_macro_meta() const
  {
    return OB_NOT_NULL(macro_meta_) && macro_meta_->is_valid();
  }
  OB_INLINE void reset() { new (this) ObMacroBlockDesc(); }
  TO_STRING_KV(K_(macro_block_id), KP_(macro_meta), K_(range),
              K_(row_store_type), K_(schema_version),
              K_(snapshot_version), K_(max_merged_trans_version), K_(row_count),
              K_(row_count_delta), K_(contain_uncommitted_row), K_(is_deleted));
};

class ObIMacroBlockIterator
{
public:
  ObIMacroBlockIterator() = default;
  virtual ~ObIMacroBlockIterator() = default;

  virtual void reset() = 0;
  virtual int open(
      ObSSTable &sstable,
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse = false,
      const bool need_record_micro_info = false) = 0;
  virtual int get_next_macro_block(blocksstable::ObMacroBlockDesc &block_desc) = 0;
  virtual const ObIArray<blocksstable::ObMicroIndexInfo> &get_micro_index_infos() const = 0;
  virtual const ObIArray<ObDatumRowkey> &get_micro_endkeys() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

// This Iterator will not iterate Lob block
class ObIndexBlockMacroIterator final : public ObIMacroBlockIterator
{
public:
  ObIndexBlockMacroIterator();
  virtual ~ObIndexBlockMacroIterator();

  virtual void reset() override;
  virtual int open(
      ObSSTable &sstable,
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse = false,
      const bool need_record_micro_info = false) override;
  int get_next_macro_block(MacroBlockId &macro_block_id);
  int get_next_macro_block(blocksstable::ObMacroBlockDesc &block_desc);
  virtual const ObIArray<blocksstable::ObMicroIndexInfo> &get_micro_index_infos() const override
  {
    return micro_index_infos_;
  }
  virtual const ObIArray<ObDatumRowkey> &get_micro_endkeys() const override
  {
    return micro_endkeys_;
  }
  TO_STRING_KV(KP_(sstable), K_(tree_cursor), K_(cur_idx), K_(begin), K_(end),
               K_(curr_key), K_(prev_key), K_(is_iter_end), K_(is_reverse_scan));
private:
  int locate_macro_block(
      const bool need_move_to_bound,
      const bool cursor_at_begin_bound,
      const bool lower_bound,
      const ObDatumRowkey &rowkey,
      MacroBlockId &logic_id,
      bool &is_beyonod_the_range);
  void reuse_micro_info_array();
  int deep_copy_rowkey(const blocksstable::ObDatumRowkey &src_key, blocksstable::ObDatumRowkey &dest_key, char *&key_buf);

private:
  const blocksstable::ObSSTable *sstable_;
  const ObDatumRange *iter_range_;
  ObIndexBlockTreeCursor tree_cursor_;
  common::ObIAllocator *allocator_; // allocator for member struct and macro endkeys
  int64_t cur_idx_;
  MacroBlockId begin_;
  MacroBlockId end_;
  ObDatumRowkey curr_key_;
  ObDatumRowkey prev_key_;
  char *curr_key_buf_;
  char *prev_key_buf_;

  // For micro block iterator in macro block
  // TODO @saitong replace the native array
  common::ObArray<blocksstable::ObMicroIndexInfo> micro_index_infos_;
  common::ObArray<ObDatumRowkey> micro_endkeys_;
  common::ObArenaAllocator micro_endkey_allocator_;
  ObIndexBlockTreePathItem hold_item_;
  bool need_record_micro_info_;
  bool is_iter_end_;
  bool is_reverse_scan_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockMacroIterator);
};

// Wrap-up of iterate both index block tree and secondary meta in sstable
class ObDualMacroMetaIterator final : public ObIMacroBlockIterator
{
public:
  ObDualMacroMetaIterator();
  virtual ~ObDualMacroMetaIterator() {}

  void reset() override;
  virtual int open(
      ObSSTable &sstable,
      const ObDatumRange &query_range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse_scan = false,
      const bool need_record_micro_info = false) override;
  virtual int get_next_macro_block(blocksstable::ObMacroBlockDesc &block_desc) override;

  virtual const ObIArray<blocksstable::ObMicroIndexInfo> &get_micro_index_infos() const
  {
    return macro_iter_.get_micro_index_infos();
  }
  virtual const ObIArray<ObDatumRowkey> &get_micro_endkeys() const
  {
    return macro_iter_.get_micro_endkeys();
  }
  TO_STRING_KV(K_(iter_end), K_(is_inited), K_(macro_iter), K_(sec_meta_iter));
private:
  ObIAllocator *allocator_; // allocator for member struct and macro endkeys
  ObIndexBlockMacroIterator macro_iter_;
  ObSSTableSecMetaIterator sec_meta_iter_;
  bool iter_end_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDualMacroMetaIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OB_INDEX_BLOCK_MACRO_ITERATOR_H_
