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
#include "storage/ob_micro_block_index_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace storage
{

  ObMicroBlockIndexIterator::ObMicroBlockIndexIterator()
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
    has_fetched_micro_info_(false),
    is_inited_(false)
{
}

ObMicroBlockIndexIterator::~ObMicroBlockIndexIterator()
{
  reset();
}


void ObMicroBlockIndexIterator::reset()
{
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
  has_fetched_micro_info_ = false;
  is_inited_ = false;
}

int ObMicroBlockIndexIterator::open(
    ObSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey sstable_endkey;
  ObArenaAllocator tmp_arena("MicroIterTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  int cmp_ret = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Inited twice", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid sstable or range", K(ret), K(sstable), K(range));
  } else if (sstable.is_empty()) {
    is_iter_end_ = true;
    is_inited_ = true;
  } else if (OB_FAIL(sstable.get_last_rowkey(tmp_arena, sstable_endkey))) {
    LOG_WARN("Fail to get last rowkey of sstable", K(ret));
  } else if (OB_FAIL(sstable_endkey.compare(
      range.get_start_key(), rowkey_read_info.get_datum_utils(), cmp_ret))) {
    LOG_WARN("Fail to compare sstable endkey and range start key", K(ret));
  } else if (cmp_ret < 0 || (0 == cmp_ret && !range.get_border_flag().inclusive_start())) {
    is_iter_end_ = true;
    ret = OB_BEYOND_THE_RANGE;
  } else if (OB_FAIL(tree_cursor_.init(sstable, allocator, &rowkey_read_info))) {
    LOG_WARN("Fail to init index tree cursor", K(ret), K(sstable));
  } else {
    const int64_t store_rowkey_cnt = sstable.is_normal_cg_sstable() ? 1
        : rowkey_read_info.get_schema_rowkey_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(); // include multi-version
    const bool is_percise_rowkey = store_rowkey_cnt == range.get_end_key().get_datum_cnt();
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
          !range.get_border_flag().inclusive_end() || is_percise_rowkey,
          end_bound_micro_block_,
          end_key_beyond_range))) {
        LOG_WARN("Fail to locate end bound micro block", K(ret));
      } else {
        curr_key_ = range.get_end_key();
      }
    } else {
      if (OB_FAIL(locate_bound_micro_block(
          range.get_end_key(),
          !range.get_border_flag().inclusive_end() || is_percise_rowkey,
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
      has_fetched_micro_info_ = false;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMicroBlockIndexIterator::deep_copy_rowkey(const ObDatumRowkey &src_key, ObDatumRowkey &dest_key, char *&key_buf)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexIterator is not inited", K(ret), KP(allocator_));
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

int ObMicroBlockIndexIterator::get_next(ObMicroIndexInfo &micro_index_info)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  ObMicroBlockId micro_block_id;
  micro_index_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexIterator is not inited", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (has_fetched_micro_info_ && OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do not reach the end_bound but cursor fail to move forward.",
                              K(ret), K_(tree_cursor), K(micro_block_id), K_(start_bound_micro_block),
                              K_(end_bound_micro_block), K_(is_reverse_scan));
    } else {
      LOG_WARN("Fail to move cursor forward", K(ret), K_(tree_cursor));
    }
  } else if (OB_FAIL(tree_cursor_.get_idx_row_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get macro block idx parser", K(ret));
  } else if (OB_ISNULL(idx_row_parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx_row_parser", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_parent_node_macro_id(micro_index_info.parent_macro_id_))) {
    LOG_WARN("Fail to get parent_macro_id_", K(ret));
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
    } else {
      micro_index_info.endkey_.set_compact_rowkey(&curr_key_);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!micro_index_info.endkey_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid micro index info", K(ret), K(micro_index_info.endkey_));
    } else if (FALSE_IT(micro_index_info.row_header_ = idx_row_header)) {
    } else if (OB_FAIL(idx_row_parser->parse_minor_meta_and_agg_row(
        micro_index_info.minor_meta_info_, micro_index_info.agg_row_buf_, micro_index_info.agg_buf_size_))) {
      LOG_WARN("Fail to parse minor meta and agg row", K(ret));
    } else {
      has_fetched_micro_info_ = true;
    }
  }
  return ret;
}

int ObMicroBlockIndexIterator::locate_bound_micro_block(
    const ObDatumRowkey &rowkey,
    const bool lower_bound,
    blocksstable::ObMicroBlockId &bound_block,
    bool &is_beyond_range)
{
  int ret = OB_SUCCESS;
  is_beyond_range = false;
  const ObIndexBlockRowHeader *idx_header = nullptr;
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

int ObMicroBlockIndexIterator::generate_cur_range(
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
