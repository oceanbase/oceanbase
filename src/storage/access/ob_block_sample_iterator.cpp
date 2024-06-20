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

#include "ob_block_sample_iterator.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
/*
 * -------------------------------------------- ObBlockSampleSSTableEndkeyIterator --------------------------------------------
 */

ObBlockSampleSSTableEndkeyIterator::ObBlockSampleSSTableEndkeyIterator()
  : macro_count_(0),
    micro_count_(0),
    curr_key_(),
    tree_cursor_(),
    start_bound_micro_block_(),
    end_bound_micro_block_(),
    curr_block_id_(),
    sample_level_(ObIndexBlockTreeCursor::LEAF),
    is_reverse_scan_(false),
    is_iter_end_(false),
    is_inited_(false)
{
}

ObBlockSampleSSTableEndkeyIterator::~ObBlockSampleSSTableEndkeyIterator()
{
  reset();
}


void ObBlockSampleSSTableEndkeyIterator::reset()
{
  if (is_inited_) {
    macro_count_ = 0;
    micro_count_ = 0;
    tree_cursor_.reset();
    curr_key_.reset();
    start_bound_micro_block_.reset();
    end_bound_micro_block_.reset();
    curr_block_id_.reset();
    sample_level_ = ObIndexBlockTreeCursor::LEAF;
    is_reverse_scan_ = false;
    is_iter_end_ = false;
    is_inited_ = false;
  }
}

int ObBlockSampleSSTableEndkeyIterator::open(
    const ObSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "Inited twice", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid sstable", K(ret), K(sstable), K(range));
  } else if (sstable.is_empty()) {
    is_iter_end_ = true;
    is_inited_ = true;
  } else if (OB_FAIL(tree_cursor_.init(sstable, allocator, &rowkey_read_info))) {
    STORAGE_LOG(WARN, "Fail to init index tree cursor", K(ret), K(sstable));
  } else {
    sample_level_ = ObIndexBlockTreeCursor::LEAF;
    is_reverse_scan_ = is_reverse_scan;

    if (OB_FAIL(tree_cursor_.estimate_range_macro_count(range, macro_count_, micro_count_))) {
      if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Fail to estimate macro block count in range", K(ret), K(range), K(tree_cursor_));
      }
    } else if (OB_FAIL(locate_bound(range))) {
      STORAGE_LOG(WARN, "Fail to locate bound", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
    STORAGE_LOG(TRACE, "open iter", K(ret), K(macro_count_), K(micro_count_), K(sstable));
  }

  return ret;
}

int ObBlockSampleSSTableEndkeyIterator::upgrade_to_macro(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObBlockSampleSSTableEndkeyIterator is not inited", K(ret));
  } else if (is_iter_end_) {
  } else {
    sample_level_ = ObIndexBlockTreeCursor::MACRO;
    if (OB_FAIL(locate_bound(range))) {
      STORAGE_LOG(WARN, "Fail to locate bound", K(ret));
    }
  }

  return ret;
}

int ObBlockSampleSSTableEndkeyIterator::move_forward()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObBlockSampleSSTableEndkeyIterator is not inited", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (curr_block_id_ == (is_reverse_scan_ ? start_bound_micro_block_ : end_bound_micro_block_)) {
    is_iter_end_ = true;
  } else if (OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
    STORAGE_LOG(WARN, "Fail to move cursor forward", K(ret), K(tree_cursor_));
  } else if (OB_FAIL(tree_cursor_.get_current_endkey(curr_key_))) {
    STORAGE_LOG(WARN, "Fail to get current endkey", K(ret), K(tree_cursor_));
  } else if (OB_FAIL(get_current_block_id(curr_block_id_))) {
    STORAGE_LOG(WARN, "Fail to get current block id", K(ret));
  }

  return ret;
}

int ObBlockSampleSSTableEndkeyIterator::locate_bound(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  bool is_start_beyond_range = false;
  bool is_end_beyond_range = false;
  ObMicroBlockId wider_end_bound_id;
  if (is_reverse_scan_) {
    if (OB_FAIL(locate_bound_micro_block(range.get_start_key(), start_bound_micro_block_, is_start_beyond_range))) {
      STORAGE_LOG(WARN, "Fail to locate start bound endkey", K(ret));
    } else if (is_start_beyond_range) {
    } else if (OB_FAIL(locate_bound_micro_block(range.get_end_key(), wider_end_bound_id, is_end_beyond_range))) {
      STORAGE_LOG(WARN, "Fail to locate end bound endkey", K(ret));
    } else if (OB_FAIL(tree_cursor_.move_forward(true))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Fail to move cursor backward", K(ret), K(tree_cursor_));
      }
    } else if (OB_FAIL(get_current_block_id(end_bound_micro_block_))) {
      STORAGE_LOG(WARN, "Fail to get current block id", K(ret));
    }
  } else {
    if (OB_FAIL(locate_bound_micro_block(range.get_end_key(), wider_end_bound_id, is_end_beyond_range))) {
      STORAGE_LOG(WARN, "Fail to locate end bound endkey", K(ret));
    } else if (OB_FAIL(tree_cursor_.move_forward(true))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Fail to move cursor backward", K(ret), K(tree_cursor_));
      }
    } else if (OB_FAIL(get_current_block_id(end_bound_micro_block_))) {
      STORAGE_LOG(WARN, "Fail to get current block id", K(ret));
    } else if (OB_FAIL(locate_bound_micro_block(range.get_start_key(), start_bound_micro_block_, is_start_beyond_range))) {
      STORAGE_LOG(WARN, "Fail to locate start bound endkey", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_iter_end_) {
    if (is_start_beyond_range || start_bound_micro_block_ == wider_end_bound_id) {
      is_iter_end_ = true;
    } else if (OB_FAIL(tree_cursor_.get_current_endkey(curr_key_))) {
      STORAGE_LOG(WARN, "Fail to get current endkey", K(ret), K(tree_cursor_));
    } else {
      curr_block_id_ = is_reverse_scan_ ? end_bound_micro_block_ : start_bound_micro_block_;
    }
  }

  return ret;
}

int ObBlockSampleSSTableEndkeyIterator::locate_bound_micro_block(
    const ObDatumRowkey &rowkey,
    blocksstable::ObMicroBlockId &bound_block,
    bool &is_beyond_range)
{
  int ret = OB_SUCCESS;

  bool equal = false;
  is_beyond_range = false;
  if (OB_FAIL(tree_cursor_.pull_up_to_root())) {
    STORAGE_LOG(WARN, "Fail to pull up tree cursor back to root", K(ret));
  } else if (OB_FAIL(tree_cursor_.drill_down(rowkey, sample_level_, !is_reverse_scan_, equal, is_beyond_range))) {
    STORAGE_LOG(WARN, "Fail to locate micro block address in index tree", K(ret));
  } else if (OB_FAIL(get_current_block_id(bound_block))) {
    STORAGE_LOG(WARN, "Fail to get current block id", K(ret));
  }

  return ret;
}

int ObBlockSampleSSTableEndkeyIterator::get_current_block_id(ObMicroBlockId &micro_block_id)
{
  int ret = OB_SUCCESS;

  ObLogicMacroBlockId logic_id;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  if (OB_FAIL(tree_cursor_.get_idx_row_header(idx_row_header))) {
    STORAGE_LOG(WARN, "Fail to get index block row header", K(ret), K(tree_cursor_));
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(micro_block_id.macro_id_))) {
    STORAGE_LOG(WARN, "Fail to get macro block id", K(ret));
  } else {
    micro_block_id.offset_ = idx_row_header->get_block_offset();
    micro_block_id.size_ = idx_row_header->get_block_size();
  }

  return ret;
}

/*
 * -------------------------------------------- ObBlockSampleRangeIterator --------------------------------------------
 */

ObBlockSampleRangeIterator::ObBlockSampleRangeIterator()
  : sample_range_(nullptr),
    allocator_(nullptr),
    datum_utils_(nullptr),
    schema_rowkey_column_count_(0),
    batch_size_(1),
    curr_key_(),
    prev_key_(),
    curr_range_(),
    endkey_iters_(),
    endkey_comparor_(),
    endkey_heap_(endkey_comparor_),
    is_range_iter_end_(false),
    is_reverse_scan_(false),
    is_inited_(false)
{
}

ObBlockSampleRangeIterator::~ObBlockSampleRangeIterator()
{
  reset();
}

void ObBlockSampleRangeIterator::reset()
{
  sample_range_ = nullptr;
  datum_utils_ = nullptr;
  schema_rowkey_column_count_ = 0;
  batch_size_ = 1;
  if (OB_NOT_NULL(allocator_)) {
    // free memory
    if (OB_NOT_NULL(curr_key_.key_buf_)) {
      allocator_->free(curr_key_.key_buf_);
      curr_key_.key_buf_ = nullptr;
    }
    if (OB_NOT_NULL(prev_key_.key_buf_)) {
      allocator_->free(prev_key_.key_buf_);
      prev_key_.key_buf_ = nullptr;
    }
    for (int64_t i = 0 ; i < endkey_iters_.count() ; ++i) {
      ObBlockSampleSSTableEndkeyIterator *iter = endkey_iters_[i];
      iter->~ObBlockSampleSSTableEndkeyIterator();
      allocator_->free(iter);
    }
  }
  curr_key_.reset();
  prev_key_.reset();
  curr_range_.reset();
  endkey_iters_.reset();
  endkey_comparor_.reset();
  endkey_heap_.reset();
  allocator_ = nullptr;
  is_range_iter_end_ = false;
  is_reverse_scan_ = false;
  is_inited_ = false;
}

int ObBlockSampleRangeIterator::open(
    ObGetTableParam &get_table_param,
    const blocksstable::ObDatumRange &range,
    ObIAllocator &allocator,
    const double percent,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObBlockSampleRangeIterator has been inited", K(ret));
  } else if (OB_UNLIKELY(!get_table_param.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(get_table_param), K(range));
  } else {
    allocator_ = &allocator;
    sample_range_ = &range;
    is_reverse_scan_ = is_reverse_scan;
    datum_utils_  = &get_table_param.tablet_iter_.get_tablet()->get_rowkey_read_info().get_datum_utils();
    schema_rowkey_column_count_ = get_table_param.tablet_iter_.get_tablet()->get_rowkey_read_info().get_schema_rowkey_count();
    endkey_comparor_.init(*datum_utils_, is_reverse_scan_);

    if (OB_FAIL(init_and_push_endkey_iterator(get_table_param))) {
      STORAGE_LOG(WARN, "Fail to init and push endkey iterator", K(ret));
    } else if (endkey_iters_.empty()) {
      is_range_iter_end_ = true;
    } else if (OB_FAIL(calculate_level_and_batch_size(percent))) {
      STORAGE_LOG(WARN, "Fail to calculate macro level and batch size", K(ret));
    } else if (is_reverse_scan_) {
      curr_key_.key_ = range.get_end_key();
      if (sample_range_->get_border_flag().inclusive_end()) {
        curr_range_.set_right_closed();
      } else {
        curr_range_.set_left_closed();
      }
    } else {
      curr_key_.key_ = range.get_start_key();
      if (sample_range_->get_border_flag().inclusive_start()) {
        curr_range_.set_left_closed();
      } else {
        curr_range_.set_right_closed();
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObBlockSampleRangeIterator::get_next_range(const ObDatumRange *&range)
{
  int ret = OB_SUCCESS;

  range = nullptr;
  ObBlockSampleSSTableEndkeyIterator *min_iter = nullptr;
  if (IS_NOT_INIT) {
    STORAGE_LOG(WARN, "The ObBlockSampleRangeIterator is not inited", K(ret));
  } else if (is_range_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(deep_copy_rowkey(curr_key_.key_, prev_key_))) {
    STORAGE_LOG(WARN, "Fail to deep copy from current key to prev key", K(ret), K(curr_key_.key_));
  } else if (OB_FAIL(get_next_batch(min_iter))) {
    if (OB_ITER_END == ret) {
      generate_cur_range(is_reverse_scan_ ? sample_range_->get_start_key() : sample_range_->get_end_key());
      curr_range_.set_inclusive(sample_range_->get_border_flag());
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "Fail to get next batch", K(ret));
    }
  } else if (OB_FAIL(deep_copy_rowkey(min_iter->get_endkey(), curr_key_))) {
    STORAGE_LOG(WARN, "Fail to deep copy to current key", K(ret), KPC(min_iter));
  } else if (FALSE_IT(generate_cur_range(curr_key_.key_))) {
  } else if (OB_FAIL(move_endkey_iter(*min_iter))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "Fail to move forward endkey iterator", K(ret), K(*min_iter));
    }
  }
  if (OB_SUCC(ret)) {
    range = &curr_range_;
  }

  return ret;
}

int ObBlockSampleRangeIterator::init_and_push_endkey_iterator(ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!get_table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(get_table_param));
  } else {
    get_table_param.tablet_iter_.table_iter()->resume();
    while (OB_SUCC(ret)) {
      void *buf = nullptr;
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      ObBlockSampleSSTableEndkeyIterator *iter = nullptr;
      if (OB_FAIL(get_table_param.tablet_iter_.table_iter()->get_next(table))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "Fail to get next table iter", K(ret), K(get_table_param.tablet_iter_.table_iter()));
        }
      } else if (!table->is_sstable() || table->is_ddl_mem_sstable()) {
      } else if (OB_ISNULL(table) || OB_ISNULL(sample_range_) || OB_ISNULL(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null sstable", K(ret), KP(table), KP(sample_range_), KP(allocator_));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable*>(table))) {
      } else if (sstable->is_empty()) {
        STORAGE_LOG(DEBUG, "Skip empty sstable", KPC(sstable));
      } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObBlockSampleSSTableEndkeyIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory for ObBlockSampleSSTableEndkeyIterator", K(ret));
      } else {
        iter = new (buf) ObBlockSampleSSTableEndkeyIterator();

        if (OB_FAIL(iter->open(*sstable, *sample_range_,
            get_table_param.tablet_iter_.get_tablet()->get_rowkey_read_info(),
            *allocator_, is_reverse_scan_))) {
          STORAGE_LOG(WARN, "Fail to open ObBlockSampleSSTableEndkeyIterator", K(ret), KPC(sstable), KPC(sample_range_));
        } else if (OB_FAIL(endkey_iters_.push_back(iter))) {
          STORAGE_LOG(WARN, "Fail to push back ObBlockSampleSSTableEndkeyIterator iter", K(ret), KPC(iter));
        }
        if (OB_FAIL(ret)) {
          iter->~ObBlockSampleSSTableEndkeyIterator();
          allocator_->free(iter);
          iter = nullptr;
        } else if (iter->is_reach_end()) {
        } else if (OB_FAIL(endkey_heap_.push(iter))) {
          STORAGE_LOG(WARN, "Fail to push iter key to heap", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    get_table_param.tablet_iter_.table_iter()->resume();
  }

  return ret;
}

int ObBlockSampleRangeIterator::calculate_level_and_batch_size(const double percent)
{
  int ret = OB_SUCCESS;

  const int64_t macro_threshold = EXPECTED_MIN_MACRO_SAMPLE_BLOCK_COUNT * 100 / percent;
  int64_t macro_count = 0;
  int64_t micro_count = 0;
  for (int i = 0 ; i < endkey_iters_.count() ; ++i) {
    macro_count += endkey_iters_[i]->get_macro_count();
    micro_count += endkey_iters_[i]->get_micro_count();
  }
  if (macro_count > macro_threshold) {
    // switch to macro level, reset previous info
    curr_key_.reset();
    endkey_heap_.reset();
    batch_size_ = macro_count * percent / 100 / EXPECTED_OPEN_RANGE_NUM;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < endkey_iters_.count() ; ++i) {
      ObBlockSampleSSTableEndkeyIterator *iter = endkey_iters_[i];
      if (OB_FAIL(iter->upgrade_to_macro(*sample_range_))) {
        STORAGE_LOG(WARN, "Fail to upgrade to macro level", K(ret));
      } else if (!iter->is_reach_end() && OB_FAIL(endkey_heap_.push(iter))) {
        STORAGE_LOG(WARN, "Fail to push endkey heap", K(ret), K(iter), K(endkey_heap_));
      }
    }
  } else {
    // micro level
   batch_size_ = micro_count * percent / 100 / EXPECTED_OPEN_RANGE_NUM;
  }
  STORAGE_LOG(TRACE, "calculate", K(ret), K(macro_count), K(micro_count), K(macro_threshold), K(batch_size_));
  return ret;
}

int ObBlockSampleRangeIterator::get_next_batch(ObBlockSampleSSTableEndkeyIterator *&iter)
{
  int ret = OB_SUCCESS;

  int64_t batch_count = 0;
  while (OB_SUCC(ret)) {
    iter = nullptr;
    if (endkey_heap_.empty()) {
      is_range_iter_end_ = true;
      ret = OB_ITER_END;
    } else {
      iter = endkey_heap_.top();
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null endkey iter ptr", K(ret), KP(iter));
      } else if (OB_FAIL(endkey_heap_.pop())) {
        STORAGE_LOG(WARN, "Fail to pop from endkey heap", K(ret), K(endkey_heap_));
      } else if (++batch_count >= batch_size_) {
        int cmp_ret = 0;
        if (OB_FAIL(curr_key_.key_.compare(iter->get_endkey(), *datum_utils_, cmp_ret))) {
          STORAGE_LOG(WARN, "Fail to compare curr endkey", K(ret), KPC(iter));
        } else if (0 != cmp_ret) {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(move_endkey_iter(*iter))) {
        STORAGE_LOG(WARN, "Fail to move forward endkey iterator", K(ret), KPC(iter));
      }
    }
  }

  return ret;
}

void ObBlockSampleRangeIterator::generate_cur_range(const ObDatumRowkey &curr_bound_key)
{
  curr_range_.start_key_ = is_reverse_scan_ ? curr_bound_key : prev_key_.key_;
  curr_range_.end_key_ = is_reverse_scan_ ? prev_key_.key_ : curr_bound_key;
  curr_range_.start_key_.datum_cnt_ = MIN(curr_range_.start_key_.datum_cnt_, schema_rowkey_column_count_);
  curr_range_.end_key_.datum_cnt_ = MIN(curr_range_.end_key_.datum_cnt_, schema_rowkey_column_count_);
}

int ObBlockSampleRangeIterator::deep_copy_rowkey(const blocksstable::ObDatumRowkey &src_key, ObBlockSampleEndkey &dest)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockSampleRangeIterator is not inited", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The ObBlockSampleRangeIterator allocator is null", K(ret), KP(allocator_));
  } else if (OB_UNLIKELY(!src_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum rowkey", K(ret), K(src_key));
  } else if (dest.buf_size_ < src_key.get_deep_copy_size()) {
    if (OB_NOT_NULL(dest.key_buf_)) {
      allocator_->free(dest.key_buf_);
    }
    if (OB_ISNULL(dest.key_buf_ = reinterpret_cast<char *>(allocator_->alloc(src_key.get_deep_copy_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(src_key.get_deep_copy_size()));
    } else {
      dest.buf_size_ = src_key.get_deep_copy_size();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(dest.key_.reset())) {
  } else if (OB_FAIL(src_key.deep_copy(dest.key_, dest.key_buf_, dest.buf_size_))) {
    STORAGE_LOG(WARN, "Fail to deep copy rowkey", K(ret), K(src_key));
  }

  return ret;
}

int ObBlockSampleRangeIterator::move_endkey_iter(ObBlockSampleSSTableEndkeyIterator &iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockSampleRangeIterator is not inited", K(ret), KP(allocator_));
  } else if (OB_FAIL(iter.move_forward())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "Fail to move forward endkey iter", K(ret), K(iter));
    }
  } else if (!iter.is_reach_end() && OB_FAIL(endkey_heap_.push(&iter))) {
    STORAGE_LOG(WARN, "Fail to push endkey heap", K(ret), K(iter), K(endkey_heap_));
  }

  return ret;
}

void ObBlockSampleRangeIterator::ObBlockSampleSSTableEndkeyComparor::init(
      const blocksstable::ObStorageDatumUtils &utils,
      const bool is_reverse_scan)
{
  datum_utils_ = &utils;
  is_reverse_scan_ = is_reverse_scan;
  ret_ = OB_SUCCESS;
}

bool ObBlockSampleRangeIterator::ObBlockSampleSSTableEndkeyComparor::operator()(
      const ObBlockSampleSSTableEndkeyIterator *left, const ObBlockSampleSSTableEndkeyIterator *right)
{
  int cmp_ret = 0;

  if (OB_UNLIKELY(nullptr == datum_utils_)) {
    ret_ = OB_NOT_INIT;
    STORAGE_LOG_RET(WARN, ret_, "The ObBlockSampleSSTableEndkeyComparor is invalid", K(ret_));
  } else if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    STORAGE_LOG_RET(WARN, ret_, "The ObBlockSampleSSTableEndkeyComparor is under error", K(ret_));
  } else if (OB_UNLIKELY(left == nullptr || right == nullptr)) {
    ret_ = OB_INVALID_ARGUMENT;
    STORAGE_LOG_RET(WARN, ret_, "Invalid argument", K(ret_), KP(left), KP(right));
  } else {
    ret_ = left->get_endkey().compare(right->get_endkey(), *datum_utils_, cmp_ret);
  }

  return is_reverse_scan_ ? cmp_ret < 0 : cmp_ret > 0;
}

void ObBlockSampleRangeIterator::ObBlockSampleEndkey::reset()
{
  key_buf_ = nullptr;
  buf_size_ = 0;
  key_.reset();
}

/*
 * -------------------------------------------- ObBlockSampleIterator --------------------------------------------
 */

ObBlockSampleIterator::ObBlockSampleIterator(const SampleInfo &sample_info)
  : ObISampleIterator(sample_info),
    access_ctx_(nullptr),
    read_info_(nullptr),
    scan_merge_(nullptr),
    block_num_(0),
    sample_block_cnt_(0),
    range_allocator_(),
    range_iterator_(),
    micro_range_(),
    has_opened_range_(false)
{
}

ObBlockSampleIterator::~ObBlockSampleIterator()
{
}

void ObBlockSampleIterator::reuse()
{
  block_num_ = 0;
  sample_block_cnt_ = 0;
  range_allocator_.reuse();
  range_iterator_.reset();
  micro_range_.reset();
  has_opened_range_ = false;
}

void ObBlockSampleIterator::reset()
{
  access_ctx_ = nullptr;
  read_info_ = nullptr;
  scan_merge_ = nullptr;
  block_num_ = 0;
  sample_block_cnt_ = 0;
  range_allocator_.reset();
  range_iterator_.reset();
  micro_range_.reset();
  has_opened_range_ = false;
}

int ObBlockSampleIterator::open(ObMultipleScanMerge &scan_merge,
                                ObTableAccessContext &access_ctx,
                                const blocksstable::ObDatumRange &range,
                                ObGetTableParam &get_table_param,
                                const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!access_ctx.is_valid() || !get_table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(access_ctx), K(get_table_param));
  }  else if (OB_FAIL(range_iterator_.open(get_table_param, range, *access_ctx.stmt_allocator_, sample_info_->percent_, is_reverse_scan))) {
    STORAGE_LOG(WARN, "Fail to init micro block iterator", K(ret));
  } else {
    scan_merge_ = &scan_merge;
    has_opened_range_ = false;
    access_ctx_ = &access_ctx;
    read_info_ = &get_table_param.tablet_iter_.get_tablet()->get_rowkey_read_info();
  }

  return ret;
}

int ObBlockSampleIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;

  const blocksstable::ObDatumRange *range = nullptr;
  row = nullptr;
  if (OB_ISNULL(scan_merge_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block sample iterator is not opened", K(ret));
  } else {
    while (OB_SUCC(ret) && (!has_opened_range_ || OB_FAIL(scan_merge_->get_next_row(row)))) {
      if (OB_ITER_END == ret || OB_SUCCESS == ret) {
        ret = OB_SUCCESS;
        has_opened_range_ = false;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(range_iterator_.get_next_range(range))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next range", K(ret), K(block_num_));
          }
        } else if (OB_ISNULL(range)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "range is null", K(ret), K(block_num_));
        } else if (return_this_sample(block_num_++)) {
          STORAGE_LOG(DEBUG, "open a range", K(*range), K_(block_num));
          ++sample_block_cnt_;
          micro_range_.reset();
          micro_range_ = *range;
          if (OB_FAIL(open_range(micro_range_))) {
            STORAGE_LOG(WARN, "Failed to open range", K(ret), K(micro_range_), K(block_num_));
          }
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret)  {
      STORAGE_LOG(WARN, "failed to get next row from ObBlockSampleIterator", K(ret), K(block_num_));
    }
#ifdef ENABLE_DEBUG_LOG
    if (OB_ITER_END == ret) {
      STORAGE_LOG(INFO, "block sample scan finish", K(ret), K_(sample_block_cnt), K_(block_num), KPC_(sample_info));
    }
#endif
  }

  return ret;
}

int ObBlockSampleIterator::open_range(blocksstable::ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  scan_merge_->reuse();
  range_allocator_.reuse();
  access_ctx_->scan_mem_->reuse_arena();
  if (OB_FAIL(range.start_key_.to_store_rowkey(read_info_->get_columns_desc(), range_allocator_, range.start_key_.store_rowkey_))) {
    STORAGE_LOG(WARN, "Failed to convert start key", K(ret), K(range));
  } else if (OB_FAIL(range.end_key_.to_store_rowkey(read_info_->get_columns_desc(), range_allocator_, range.end_key_.store_rowkey_))) {
    STORAGE_LOG(WARN, "Failed to convert end key", K(ret), K(range));
  } else if (OB_FAIL(scan_merge_->open(range))) {
    STORAGE_LOG(WARN, "failed to open ObMultipleScanMerge", K(ret), K(range));
  } else {
    has_opened_range_ = true;
  }

  return ret;
}

int ObBlockSampleIterator::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_merge_->get_next_rows(count, capacity))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get next rows", K(ret));
    } else if (count > 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBlockSampleIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRange *range = nullptr;
  if (OB_ISNULL(scan_merge_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block sample iterator is not opened", K(ret));
  } else {
    while (OB_SUCC(ret) && (!has_opened_range_ || OB_FAIL(inner_get_next_rows(count, capacity)))) {
      if (OB_ITER_END == ret || OB_SUCCESS == ret) {
        ret = OB_SUCCESS;
        has_opened_range_ = false;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(range_iterator_.get_next_range(range))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next range", K(ret), K(block_num_));
          }
        } else if (OB_ISNULL(range)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "range is null", K(ret), K(block_num_));
        } else if (return_this_sample(block_num_++)) {
          STORAGE_LOG(DEBUG, "open a range", K(*range), K_(block_num));
          micro_range_.reset();
          micro_range_ = *range;
          if (OB_FAIL(open_range(micro_range_))) {
            STORAGE_LOG(WARN, "Failed to open range", K(ret), K(micro_range_), K(block_num_));
          }
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret)  {
      STORAGE_LOG(WARN, "failed to get next row from ObBlockSampleIterator", K(ret), K(block_num_));
    }
  }

  return ret;
}

}
}
