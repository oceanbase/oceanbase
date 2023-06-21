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
#include "storage/ob_all_micro_block_range_iterator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace storage
{

ObAllMicroBlockRangeIterator::ObAllMicroBlockRangeIterator()
  : schema_rowkey_cnt_(0),
    tree_cursor_(),
    start_bound_micro_block_(),
    end_bound_micro_block_(),
    curr_key_(),
    prev_key_(),
    curr_key_buf_(nullptr),
    prev_key_buf_(nullptr),
    allocator_(nullptr),
    micro_range_(),
    range_(nullptr),
    is_reverse_scan_(false),
    is_iter_end_(false),
    is_inited_(false)
{
}

ObAllMicroBlockRangeIterator::~ObAllMicroBlockRangeIterator()
{
  reset();
}


void ObAllMicroBlockRangeIterator::reset()
{
  if (is_inited_) {
    schema_rowkey_cnt_ = 0;
    tree_cursor_.reset();
    start_bound_micro_block_.reset();
    end_bound_micro_block_.reset();
    curr_key_.reset();
    prev_key_.reset();
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
    micro_range_.reset();
    range_ = nullptr;
    is_reverse_scan_ = false;
    is_iter_end_ = false;
    is_inited_ = false;
  }
}

int ObAllMicroBlockRangeIterator::open(
    const ObSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Inited twice", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid sstable", K(ret));
  } else if (sstable.is_empty()) {
    is_iter_end_ = true;
    is_inited_ = true;
  } else if (OB_FAIL(tree_cursor_.init(sstable, allocator, &rowkey_read_info))) {
    LOG_WARN("Fail to init index tree cursor", K(ret), K(sstable));
  } else {
    schema_rowkey_cnt_ = rowkey_read_info.get_schema_rowkey_count();
    range_ = &range;
    bool start_key_beyond_range = false;
    bool end_key_beyond_range = false;
    if (is_reverse_scan) {
      if (OB_FAIL(locate_bound_micro_block(
          range.get_start_key(),
          range.get_border_flag().inclusive_start(),
          start_bound_micro_block_,
          start_key_beyond_range))) {
        LOG_WARN("Fail to locate start bound micro block", K(ret));
      } else if (OB_FAIL(locate_bound_micro_block(
          range.get_end_key(),
          !range.get_border_flag().inclusive_end(),
          end_bound_micro_block_,
          end_key_beyond_range))) {
        LOG_WARN("Fail to locate end bound micro block", K(ret));
      } else {
        curr_key_ = range.get_end_key();
      }
    } else {
      if (OB_FAIL(locate_bound_micro_block(
          range.get_end_key(),
          !range.get_border_flag().inclusive_end(),
          end_bound_micro_block_,
          end_key_beyond_range))) {
        LOG_WARN("Fail to locate end bound micro block", K(ret));
      } else if (OB_FAIL(locate_bound_micro_block(
          range.get_start_key(),
          range.get_border_flag().inclusive_start(),
          start_bound_micro_block_,
          start_key_beyond_range))) {
        LOG_WARN("Fail to locate start bound micro block", K(ret));
      } else {
        curr_key_ = range.get_start_key();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(start_key_beyond_range)) {
      is_iter_end_ = true;
      ret = OB_BEYOND_THE_RANGE;
    } else {
      allocator_ = &allocator;
      is_reverse_scan_ = is_reverse_scan;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::deep_copy_rowkey(const ObDatumRowkey &src_key, ObDatumRowkey &dest_key, char *&key_buf)
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

int ObAllMicroBlockRangeIterator::get_next_range(const ObDatumRange *&range)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  ObLogicMacroBlockId logic_id;
  ObMicroBlockId micro_block_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObAllMicroBlockRangeIterator is not inited", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_idx_row_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else {
    micro_block_id.macro_id_ = idx_row_header->get_macro_id();
    micro_block_id.offset_ = idx_row_header->get_block_offset();
    micro_block_id.size_ = idx_row_header->get_block_size();
    const bool is_first_block = micro_block_id == start_bound_micro_block_;
    const bool is_last_block = micro_block_id == end_bound_micro_block_;
    is_iter_end_ = is_reverse_scan_ ? is_first_block : is_last_block;

    ObDatumRowkey rowkey;
    if (OB_FAIL(deep_copy_rowkey(curr_key_, prev_key_, prev_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save prev key", K(ret), K_(curr_key));
    } else if (OB_FAIL(tree_cursor_.get_current_endkey(rowkey))) {
      LOG_WARN("Fail to get current endkey", K(ret), K_(tree_cursor));
    } else if (OB_FAIL(deep_copy_rowkey(rowkey, curr_key_, curr_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save curr key", K(ret), K(rowkey));
    } else if (OB_FAIL(generate_cur_range(is_first_block, is_last_block))) {
      LOG_WARN("Fail to generate current range", K(ret));
    } else if (!is_iter_end_ && OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to move cursor forward", K(ret), K_(tree_cursor));
      }
    }

    if (OB_SUCC(ret)) {
      range = &micro_range_;
    }
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::locate_bound_micro_block(
    const ObDatumRowkey &rowkey,
    const bool lower_bound,
    blocksstable::ObMicroBlockId &bound_block,
    bool &is_beyond_range)
{
  int ret = OB_SUCCESS;
  is_beyond_range = false;
  const ObIndexBlockRowHeader *idx_header = nullptr;
  ObLogicMacroBlockId logic_id;
  bool equal = false;
  if (OB_FAIL(tree_cursor_.pull_up_to_root())) {
    LOG_WARN("Fail to pull up tree cursor back to root", K(ret));
  } else if (OB_FAIL(tree_cursor_.drill_down(
      rowkey,
      ObIndexBlockTreeCursor::LEAF,
      lower_bound,
      equal,
      is_beyond_range))) {
    LOG_WARN("Fail to locate micro block address in index tree", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_idx_row_header(idx_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else {
    bound_block.macro_id_ = idx_header->get_macro_id();
    bound_block.offset_ = idx_header->get_block_offset();
    bound_block.size_ = idx_header->get_block_size();
  }
  return ret;
}

int ObAllMicroBlockRangeIterator::generate_cur_range(
    const bool is_first_block,
    const bool is_last_block)
{
  int ret = OB_SUCCESS;
  micro_range_.set_left_open();
  micro_range_.set_right_closed();
  ObDatumRowkey &start_key = is_reverse_scan_ ? curr_key_ : prev_key_;
  ObDatumRowkey &endkey = is_reverse_scan_ ? prev_key_ : curr_key_;
  if (is_first_block) {
    micro_range_.start_key_ = range_->get_start_key();
    if (range_->get_border_flag().inclusive_start()) {
      micro_range_.set_left_closed();
    }
    ObDatumRowkey &endkey = is_reverse_scan_ ? prev_key_ : curr_key_;
    micro_range_.end_key_ = endkey;
  } else if (is_last_block) {
    micro_range_.end_key_ = range_->get_end_key();
    if (!range_->get_border_flag().inclusive_end()) {
      micro_range_.set_right_open();
    }
    micro_range_.start_key_ = start_key;
  } else {
    micro_range_.start_key_ = start_key;
    micro_range_.end_key_ = endkey;
  }
  micro_range_.start_key_.datum_cnt_ = MIN(micro_range_.start_key_.datum_cnt_, schema_rowkey_cnt_);
  micro_range_.end_key_.datum_cnt_ = MIN(micro_range_.end_key_.datum_cnt_, schema_rowkey_cnt_);
  return ret;
}

}
}
