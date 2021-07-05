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

#ifndef OB_MACRO_BLOCK_ITERATOR_H_
#define OB_MACRO_BLOCK_ITERATOR_H_
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_macro_block_meta_mgr.h"
#include "blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace storage {
class ObSSTable;

struct ObMacroBlockDesc {
  uint64_t data_version_;
  int64_t block_idx_;
  int64_t block_cnt_;
  blocksstable::ObMacroBlockCtx macro_block_ctx_;
  blocksstable::ObFullMacroBlockMeta full_meta_;
  common::ObStoreRange range_;
  int64_t row_store_type_;
  int64_t schema_version_;
  int64_t data_seq_;
  int32_t row_count_;
  int32_t occupy_size_;
  int32_t micro_block_count_;
  int64_t data_checksum_;
  int64_t snapshot_version_;
  int32_t row_count_delta_;
  int64_t progressive_merge_round_;
  bool contain_uncommitted_row_;
  ObMacroBlockDesc();
  TO_STRING_KV(K_(data_version), K_(block_idx), K_(block_cnt), K_(macro_block_ctx), K_(full_meta), K_(range),
      K_(row_store_type), K_(schema_version), K_(data_seq), K_(row_count), K_(occupy_size), K_(micro_block_count),
      K_(data_checksum), K_(snapshot_version), K_(row_count_delta), K_(progressive_merge_round),
      K_(contain_uncommitted_row));
};

class ObMacroBlockRowComparor {
public:
  ObMacroBlockRowComparor();
  virtual ~ObMacroBlockRowComparor();
  bool operator()(const blocksstable::MacroBlockId& block_id, const common::ObStoreRowkey& rowkey);
  bool operator()(const common::ObStoreRowkey& rowkey, const blocksstable::MacroBlockId& block_id);
  void reset();
  OB_INLINE void set_sstable(ObSSTable& sstable)
  {
    sstable_ = &sstable;
  }
  OB_INLINE void set_use_collation_free(const bool use_collation_free)
  {
    use_collation_free_ = use_collation_free;
  }
  OB_INLINE void set_prefix_check(const bool is_prefix_check)
  {
    is_prefix_check_ = is_prefix_check;
  }
  OB_INLINE bool is_prefix_check() const
  {
    return is_prefix_check_;
  }
  OB_INLINE int get_ret()
  {
    return ret_;
  }

private:
  int compare_(const blocksstable::MacroBlockId& block_id, const common::ObStoreRowkey& rowkey, int32_t& cmp_ret);

private:
  int ret_;
  bool use_collation_free_;
  bool is_prefix_check_;
  blocksstable::ObFullMacroBlockMeta full_meta_;
  common::ObStoreRowkey macro_block_rowkey_;
  ObSSTable* sstable_;
};

class ObMacroBlockIterator {
public:
  ObMacroBlockIterator();
  virtual ~ObMacroBlockIterator();
  void reset();
  int open(ObSSTable& sstable, const common::ObExtStoreRowkey& ext_rowkey);
  int open(ObSSTable& sstable, const bool is_reverse = false, const bool check_lob = false);
  int open(ObSSTable& sstable, const common::ObExtStoreRange& ext_range, const bool is_reverse = false);
  int get_next_macro_block(blocksstable::ObMacroBlockCtx& macro_block_ctx);
  int get_next_macro_block(
      blocksstable::ObMacroBlockCtx& macro_block_ctx, blocksstable::ObFullMacroBlockMeta& full_meta);
  int get_next_macro_block(ObMacroBlockDesc& block_desc);
  int get_macro_block_count(int64_t& macro_block_count);
  OB_INLINE int64_t get_start_idx() const
  {
    return begin_;
  }
  TO_STRING_KV(K_(sstable), K_(is_reverse_scan), K_(step), K_(cur_idx), K_(begin), K_(end), K_(check_lob));
  bool is_end_macro_block()
  {
    return (cur_idx_ < begin_ || cur_idx_ > end_);
  }

private:
  int locate_macro_block(const common::ObExtStoreRowkey& ext_rowkey, int64_t& block_idx);
  int locate_macro_block_without_helper(const common::ObExtStoreRowkey& ext_rowkey, int64_t& block_idx);
  ObSSTable* sstable_;
  bool is_reverse_scan_;
  int32_t step_;
  ObMacroBlockRowComparor comparor_;
  int64_t cur_idx_;
  int64_t begin_;
  int64_t end_;
  bool check_lob_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockIterator);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_MACRO_BLOCK_ITERATOR_H_ */
