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

#define USING_LOG_PREFIX STORAGE
#include "ob_index_block_macro_iterator.h"

namespace oceanbase {
using namespace storage;
namespace blocksstable {

ObIndexBlockMacroIterator::ObIndexBlockMacroIterator()
  : sstable_(nullptr), iter_range_(nullptr),
    tree_cursor_(), allocator_(nullptr),
    cur_idx_(-1), begin_(),end_(), curr_key_(), prev_key_(),
    curr_key_buf_(nullptr), prev_key_buf_(nullptr), micro_index_infos_(),
    micro_endkeys_(), micro_endkey_allocator_(), hold_item_(), need_record_micro_info_(false),
    is_iter_end_(false), is_reverse_scan_(false) {}

ObIndexBlockMacroIterator::~ObIndexBlockMacroIterator()
{
  reset();
}

void ObIndexBlockMacroIterator::reset() {
  sstable_ = nullptr;
  iter_range_ = nullptr;
  if (need_record_micro_info_ && hold_item_.is_block_allocated_) {
    tree_cursor_.release_held_path_item(hold_item_);
  }
  tree_cursor_.reset();
  if (allocator_ != nullptr) {
    if (nullptr != curr_key_buf_) {
      allocator_->free(curr_key_buf_);
    }
    curr_key_buf_ = nullptr;
    if (nullptr != prev_key_buf_) {
      allocator_->free(prev_key_buf_);
    }
    prev_key_buf_ = nullptr;
  }
  allocator_ = nullptr;
  cur_idx_ = -1;
  begin_.reset();
  end_.reset();
  curr_key_.reset();
  prev_key_.reset();
  micro_index_infos_.reset();
  micro_endkeys_.reset();
  micro_endkey_allocator_.reset();
  need_record_micro_info_ = false;
  is_iter_end_ = false;
  is_reverse_scan_ = false;
}

int ObIndexBlockMacroIterator::open(
    ObSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse,
    const bool need_record_micro_info) {
  int ret = OB_SUCCESS;
  bool start_beyond_range = false;
  bool end_beyond_range = false;
  ObDatumRowkey sstable_endkey;
  int cmp_ret = 0;

  if (OB_NOT_NULL(sstable_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Need reset before iterate on a new sstable", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("SSTable is not valid", K(ret), K(sstable), K(range));
  } else if (sstable.is_empty()) {
    is_iter_end_ = true;
  } else if (OB_FAIL(sstable.get_last_rowkey(allocator, sstable_endkey))) {
    LOG_WARN("Fail to get last rowkey of sstable", K(ret));
  } else if (OB_FAIL(sstable_endkey.compare(
      range.get_start_key(), rowkey_read_info.get_datum_utils(), cmp_ret))) {
    LOG_WARN("Fail to compare sstable endkey and range start key", K(ret));
  } else if (cmp_ret < 0 || (0 == cmp_ret && !range.get_border_flag().inclusive_start())) {
    is_iter_end_ = true;
  } else if (OB_FAIL(tree_cursor_.init(sstable, allocator, &rowkey_read_info))) {
    LOG_WARN("Fail to init tree cursor", K(ret), K(sstable));
  } else {
    const int64_t schema_rowkey_cnt = rowkey_read_info.get_schema_rowkey_count();
    const bool end_is_multi_version_rowkey = schema_rowkey_cnt < range.get_end_key().get_datum_cnt();
    if (is_reverse) {
      if (OB_FAIL(locate_macro_block(
          false,
          true,
          (range.get_border_flag().inclusive_start()),
          range.get_start_key(),
          begin_,
          start_beyond_range))) {
        LOG_WARN("Fail to locate begin macro block", K(ret), K(range));
      } else if (OB_FAIL(locate_macro_block(
          true,
          false,
          (!range.get_border_flag().inclusive_end() || end_is_multi_version_rowkey),
          range.get_end_key(),
          end_,
          end_beyond_range))) {
        LOG_WARN("Fail to locate end macro block", K(ret), K(range));
      } else {
        curr_key_ = range.get_end_key();
        if (start_beyond_range) {
          is_iter_end_ = true;
        } else if (end_beyond_range) {
          // endkey beyond the open range
        }
      }
    } else {
      if (OB_FAIL(locate_macro_block(
          true,
          false,
          (!range.get_border_flag().inclusive_end() || end_is_multi_version_rowkey),
          range.get_end_key(),
          end_,
          end_beyond_range))) {
        LOG_WARN("Fail to locate end macro block", K(ret), K(range));
      } else if (OB_FAIL(locate_macro_block(
          false,
          true,
          (range.get_border_flag().inclusive_start()),
          range.get_start_key(),
          begin_,
          start_beyond_range))) {
        LOG_WARN("Fail to locate begin macro block", K(ret), K(range));
      } else {
        // Always return MIN_ROWKEY as start_key of first iterated macro block
        // for check reuse boundary during compaction
        curr_key_.set_min_rowkey();
        if (start_beyond_range) {
          is_iter_end_ = true;
        } else if (end_beyond_range) {
        }
      }
    }
  }


  if (OB_SUCC(ret) && !is_iter_end_) {
    sstable_ = &sstable;
    iter_range_ = &range;
    allocator_ = &allocator;
    is_reverse_scan_ = is_reverse;
    need_record_micro_info_ = need_record_micro_info;
    is_iter_end_ = false;
  }

  return ret;
}

int ObIndexBlockMacroIterator::deep_copy_rowkey(const ObDatumRowkey &src_key, ObDatumRowkey &dest_key, char *&key_buf)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexBlockMacroIterator is not inited", K(ret), KP(allocator_));
  } else if (OB_UNLIKELY(!src_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum rowkey", K(ret), K(src_key));
  } else {
    if (key_buf != nullptr) {
      allocator_->free(key_buf);
    }
    dest_key.reset();
    const int64_t copy_size = src_key.get_deep_copy_size();
    if (OB_ISNULL(key_buf = reinterpret_cast<char *>(allocator_->alloc(copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(copy_size));
    } else if (OB_FAIL(src_key.deep_copy(dest_key, key_buf, copy_size))) {
      STORAGE_LOG(WARN, "Failed to deep copy rowkey", K(ret), K(src_key), K(copy_size));
      allocator_->free(key_buf);
      key_buf = nullptr;
    }
  }

  return ret;
}


int ObIndexBlockMacroIterator::get_next_macro_block(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(macro_id))) {
    LOG_WARN("Fail to get macro row block id", K(ret), K(macro_id));
  } else {
    micro_endkey_allocator_.reuse();
    if ((macro_id == begin_ && is_reverse_scan_) || (macro_id == end_ && !is_reverse_scan_)) {
      is_iter_end_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    // traverse all node of this macro block and collect index info
    ObDatumRowkey rowkey;
    if (need_record_micro_info_) {
      reuse_micro_info_array();
      if (OB_FAIL(tree_cursor_.get_child_micro_infos(
                  *iter_range_,
                  micro_endkey_allocator_,
                  micro_endkeys_,
                  micro_index_infos_,
                  hold_item_))) {
        LOG_WARN("Fail to record child micro block info", K(ret), KPC(iter_range_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_rowkey(curr_key_, prev_key_, prev_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save prev key", K(ret), K_(curr_key));
    } else if (OB_FAIL(tree_cursor_.get_current_endkey(rowkey))) {
      LOG_WARN("Fail to get current endkey", K(ret), K_(tree_cursor));
    } else if (OB_FAIL(deep_copy_rowkey(rowkey, curr_key_, curr_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save curr key", K(ret), K(rowkey));
    } else if (OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to move cursor to next macro node", K(ret), K_(tree_cursor));
      }
    }
  }
  return ret;
}

// Sequential iter range: [prev_key, curr_key]
// Reverse iter range: [curr_key, prev_key]
int ObIndexBlockMacroIterator::get_next_macro_block(ObMacroBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get idx row parser", K(ret), K_(tree_cursor));
  } else if (OB_ISNULL(idx_row_parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx_row_parser", K(ret));
  } else if (OB_FAIL(idx_row_parser->get_header(idx_row_header))) {
    LOG_WARN("Fail to get idx row header", K(ret), KPC(idx_row_parser));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index row header", K(ret));
  } else {
    block_desc.range_.border_flag_.unset_inclusive_start();
    block_desc.range_.border_flag_.set_inclusive_end();
    block_desc.row_store_type_ = idx_row_header->row_store_type_;
    block_desc.schema_version_ = idx_row_header->schema_version_;
    block_desc.snapshot_version_ = idx_row_parser->get_snapshot_version();
    block_desc.max_merged_trans_version_ = idx_row_parser->get_max_merged_trans_version();
    block_desc.row_count_ = idx_row_header->get_row_count();
    block_desc.row_count_delta_ = idx_row_parser->get_row_count_delta();
    block_desc.contain_uncommitted_row_ = idx_row_header->contain_uncommitted_row();
    block_desc.is_deleted_ = idx_row_header->is_deleted();

    if (OB_FAIL(get_next_macro_block(block_desc.macro_block_id_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        if (is_reverse_scan_) {
          block_desc.range_.start_key_.set_min_rowkey();
        } else {
          block_desc.range_.end_key_.set_max_rowkey();
        }
      } else {
        LOG_WARN("Fail to get next macro block id", K(ret));
      }
    } else if (is_reverse_scan_) {
      block_desc.range_.start_key_ = curr_key_;
      block_desc.range_.end_key_ = prev_key_;;
    } else {
      block_desc.range_.start_key_ = prev_key_;
      block_desc.range_.end_key_ = curr_key_;;
    }
  }
  return ret;
}

int ObIndexBlockMacroIterator::locate_macro_block(
    const bool need_move_to_bound,
    const bool cursor_at_begin_bound,
    const bool lower_bound,
    const ObDatumRowkey &rowkey,
    MacroBlockId &macro_id,
    bool &is_beyond_the_range)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  if (OB_FAIL(tree_cursor_.pull_up_to_root())) {
    LOG_WARN("Fail to pull up sstable index tree cursor to root", K(ret));
  } else if (OB_FAIL(tree_cursor_.drill_down(
      rowkey, ObIndexBlockTreeCursor::MACRO, lower_bound, equal, is_beyond_the_range))) {
    LOG_WARN("Fail to drill down to macro level", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(macro_id))) {
    LOG_WARN("Fail to get macro block logic id", K(ret));
  }
  return ret;
}

void ObIndexBlockMacroIterator::reuse_micro_info_array()
{
  micro_index_infos_.reuse();
  micro_endkeys_.reuse();
  tree_cursor_.release_held_path_item(hold_item_);
}

ObDualMacroMetaIterator::ObDualMacroMetaIterator()
  : allocator_(nullptr), macro_iter_(), sec_meta_iter_(), iter_end_(false), is_inited_(false) {}

void ObDualMacroMetaIterator::reset()
{
  macro_iter_.reset();
  sec_meta_iter_.reset();
  allocator_ = nullptr;
  iter_end_ = false;
  is_inited_ = false;
}

int ObDualMacroMetaIterator::open(
    ObSSTable &sstable,
    const ObDatumRange &query_range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan,
    const bool need_record_micro_info)
{
  UNUSED(need_record_micro_info);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_FAIL(macro_iter_.open(
      sstable,
      query_range,
      rowkey_read_info,
      allocator,
      is_reverse_scan,
      true /* need record micro index info */))) {
    LOG_WARN("Fail to open index block tree iterator", K(ret));
  } else if (OB_FAIL(sec_meta_iter_.open(
      query_range,
      blocksstable::DATA_BLOCK_META,
      sstable,
      rowkey_read_info,
      allocator,
      is_reverse_scan))) {
    LOG_WARN("Fail to open secondary meta iterator", K(ret));
  } else {
    allocator_ = &allocator;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDualMacroMetaIterator::get_next_macro_block(ObMacroBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta *macro_meta = block_desc.macro_meta_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dual macro meta iterator not inited", K(ret));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null pointer to read macro meta", K(ret), K(block_desc), KP(macro_meta));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_SUCC(macro_iter_.get_next_macro_block(block_desc))) {
    if (OB_FAIL(sec_meta_iter_.get_next(*macro_meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("Fail to get next secondary meta iterator", K(ret), K_(macro_iter), K_(sec_meta_iter));
    } else if (OB_UNLIKELY(block_desc.macro_block_id_ != macro_meta->get_macro_id())) {
      ret = OB_ERR_SYS;
      LOG_WARN("Logic macro block id from iterated macro block and merge info not match",
          K(ret), K(block_desc), KPC(macro_meta));
    }
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("Fail to get next macro block from index tree", K(ret));
  } else {
    int tmp_ret = sec_meta_iter_.get_next(*macro_meta);
    if (OB_UNLIKELY(OB_ITER_END != tmp_ret)) {
      ret = OB_ERR_SYS;
      LOG_WARN("Dual macro meta iterated different range", K(ret));
    } else {
      iter_end_ = true;
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
