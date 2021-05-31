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

#include "ob_partition_range_spliter.h"
#include "ob_macro_block_iterator.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
namespace storage {

ObMacroEndkeyIterator::ObMacroEndkeyIterator()
    : range_para_(), cur_idx_(0), skip_cnt_(0), iter_idx_(0), is_inited_(false)
{}

void ObMacroEndkeyIterator::reset()
{
  range_para_.reset();
  cur_idx_ = 0;
  skip_cnt_ = 0;
  iter_idx_ = 0;
  is_inited_ = false;
}

int ObMacroEndkeyIterator::open(const ObSSTableRangePara& range_para, const int64_t skip_cnt, const int64_t iter_idx)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMacroEndkeyIterator init twice", K(*this), K(ret));
  } else if (OB_UNLIKELY(!range_para.is_valid() || range_para.get_macro_count() == 0 || iter_idx < 0 || skip_cnt <= 0 ||
                         skip_cnt >= range_para.get_macro_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid argument to open ObMacroEndkeyIterator", K(ret), K(range_para), K(skip_cnt), K(iter_idx));
  } else {
    range_para_ = range_para;
    skip_cnt_ = skip_cnt;
    iter_idx_ = iter_idx;
    // start from a specified point
    cur_idx_ = range_para.start_ + skip_cnt_ / 2;
    is_inited_ = true;
  }

  return ret;
}

int ObMacroEndkeyIterator::get_endkey_cnt(int64_t& endkey_cnt) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMacroEndkeyIterator is not init", K(ret));
  } else {
    endkey_cnt = (range_para_.get_macro_count() - skip_cnt_ / 2 - 1) / skip_cnt_ + 1;
  }

  return ret;
}

int ObMacroEndkeyIterator::get_next_macro_block_endkey(ObMacroEndkey& endkey)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  blocksstable::ObMacroBlockCtx macro_block_ctx;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMacroEndkeyIterator is not init", K(ret));
  } else if (OB_ISNULL(range_para_.sstable_)) {
    ret = OB_SUCCESS;
    STORAGE_LOG(WARN, "Unexpected null sstable", K(ret), K_(range_para));
  } else if (cur_idx_ >= range_para_.end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(range_para_.sstable_->get_macro_block_ctx(cur_idx_, macro_block_ctx))) {
    STORAGE_LOG(WARN, "Fail to get_macro_ctx", K(ret), K_(cur_idx), KPC(range_para_.sstable_));
  } else if (OB_FAIL(range_para_.sstable_->get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret), K(macro_block_ctx));
  } else if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid macro block meta", K(ret), K(full_meta), K_(range_para));
  } else {
    endkey.reset();
    endkey.obj_ptr_ = full_meta.meta_->endkey_;
    endkey.obj_cnt_ = full_meta.schema_->schema_rowkey_col_cnt_;
    endkey.iter_idx_ = iter_idx_;
    cur_idx_ += skip_cnt_;
  }
  return ret;
}

ObPartitionParallelRanger::ObPartitionParallelRanger(ObArenaAllocator& allocator)
    : store_range_(nullptr),
      endkey_iters_(),
      allocator_(allocator),
      comparor_(),
      range_heap_(comparor_),
      last_macro_endkey_(nullptr),
      total_endkey_cnt_(0),
      sample_cnt_(0),
      parallel_target_count_(0),
      is_inited_(false)
{}

ObPartitionParallelRanger::~ObPartitionParallelRanger()
{
  reset();
}

void ObPartitionParallelRanger::reset()
{
  ObMacroEndkeyIterator* endkey_iter = nullptr;
  for (int64_t i = 0; i < endkey_iters_.count(); i++) {
    if (OB_NOT_NULL(endkey_iter = endkey_iters_.at(i))) {
      endkey_iter->~ObMacroEndkeyIterator();
      allocator_.free(endkey_iter);
    }
  }
  store_range_ = nullptr;
  endkey_iters_.reset();
  range_heap_.reset();
  comparor_.reset();
  last_macro_endkey_ = nullptr;
  total_endkey_cnt_ = 0;
  sample_cnt_ = 0;
  parallel_target_count_ = 0;
  is_inited_ = false;
}

int ObPartitionParallelRanger::init(
    const ObStoreRange& range, ObIArray<ObSSTableRangePara>& range_paras, const int64_t parallel_target_count)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is inited twice", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid() || range_paras.empty() || parallel_target_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid arugment to init ObPartitionParallelRanger",
        K(ret),
        K(range),
        K(range_paras),
        K(parallel_target_count));
  } else if (parallel_target_count_ == 1) {
    //  construct single range
  } else if (OB_FAIL(calc_sample_count(range_paras, parallel_target_count))) {
    STORAGE_LOG(WARN, "Failed to calculate sample count", K(ret), K(range_paras), K(parallel_target_count));
  } else if (sample_cnt_ == 0) {
    // no enough macroblock count, construct single range
  } else if (OB_FAIL(init_macro_iters(range_paras))) {
    STORAGE_LOG(WARN, "Failed to init macro iters", K(ret), K(range_paras));
  } else if (OB_FAIL(build_parallel_range_heap())) {
    STORAGE_LOG(WARN, "Failed to build parallel range heap", K(ret));
  }
  if (OB_SUCC(ret)) {
    store_range_ = &range;
    parallel_target_count_ = parallel_target_count;
    is_inited_ = true;
    STORAGE_LOG(DEBUG, "succ to init partition parallel ranger", K(*this));
  }

  return ret;
}

int ObPartitionParallelRanger::calc_sample_count(
    ObIArray<ObSSTableRangePara>& range_paras, const int64_t parallel_target_count)
{
  int ret = OB_SUCCESS;
  int64_t max_macro_block_cnt = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < range_paras.count(); i++) {
    ObSSTableRangePara& range_para = range_paras.at(i);
    if (OB_UNLIKELY(!range_para.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected invalid range para", K(ret), K(range_para), K(i));
    } else {
      max_macro_block_cnt = MAX(range_para.get_macro_count(), max_macro_block_cnt);
    }
  }
  // decide sample count due to the largest minor sstable
  if (OB_SUCC(ret)) {
    sample_cnt_ = max_macro_block_cnt / parallel_target_count;
    if (sample_cnt_ > 2) {
      sample_cnt_ /= 2;
    }
    STORAGE_LOG(DEBUG,
        "finish calc sample cnt",
        K(range_paras),
        K(parallel_target_count),
        K(max_macro_block_cnt),
        K_(sample_cnt));
  }

  return ret;
}

int ObPartitionParallelRanger::init_macro_iters(ObIArray<ObSSTableRangePara>& range_paras)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sample_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpecte sample count to init macro endkey iters", K(ret), K_(sample_cnt));
  } else {
    ObMacroEndkeyIterator* endkey_iter = nullptr;
    total_endkey_cnt_ = 0;
    int64_t iter_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_paras.count(); i++) {
      ObSSTableRangePara& range_para = range_paras.at(i);
      int64_t sample_count = sample_cnt_;
      int64_t endkey_cnt = 0;
      void* buf = nullptr;
      if (range_para.get_macro_count() <= sample_cnt_) {
        // give second chance for small sstable of which macroblock count less than sample count
        sample_count = range_para.get_macro_count() / 2;
      }
      if (OB_FAIL(ret) || sample_count <= 0) {
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroEndkeyIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for endkey iter", K(ret));
      } else if (FALSE_IT(endkey_iter = new (buf) ObMacroEndkeyIterator())) {
      } else if (OB_FAIL(endkey_iter->open(range_para, sample_count, iter_idx))) {
        STORAGE_LOG(WARN, "Failed to scan macro block in sstable", K(ret), K(range_para), K(sample_count), K(iter_idx));
      } else if (OB_FAIL(endkey_iters_.push_back(endkey_iter))) {
        STORAGE_LOG(WARN, "Failed to push back macro block iter", K(ret));
      } else if (OB_FAIL(endkey_iter->get_endkey_cnt(endkey_cnt))) {
        STORAGE_LOG(WARN, "Failed to get endkey count from ite", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "succ to init sample macro iter", K(range_para), K_(sample_cnt), K(endkey_cnt), K(iter_idx));
        // last macro block should be ignored
        total_endkey_cnt_ += endkey_cnt;
        iter_idx++;
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < endkey_iters_.count(); i++) {
        if (OB_NOT_NULL(endkey_iter = endkey_iters_.at(i))) {
          endkey_iter->~ObMacroEndkeyIterator();
          allocator_.free(endkey_iter);
        }
      }
      endkey_iters_.reset();
    }
  }

  return ret;
}

int ObPartitionParallelRanger::build_parallel_range_heap()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(endkey_iters_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build parallel range heap", K(ret), K_(endkey_iters));
  } else {
    ObMacroEndkeyIterator* endkey_iter = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < endkey_iters_.count(); i++) {
      ObMacroEndkey endkey;
      if (OB_ISNULL(endkey_iter = endkey_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro block iter", K(ret), K(i));
      } else if (OB_FAIL(endkey_iter->get_next_macro_block_endkey(endkey))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "Failed to get next macro block endkey", K(ret), K(i));
        }
      } else if (OB_UNLIKELY(!endkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block endkey", K(ret), K(endkey));
      } else if (OB_FAIL(range_heap_.push(endkey))) {
        STORAGE_LOG(WARN, "Failed to push macro endkey to merge heap", K(ret), K(i), K(endkey));
      } else if (OB_FAIL(comparor_.get_error_code())) {
        STORAGE_LOG(WARN, "Failed to compare macro endkeys", K(ret));
      }
    }
  }

  return ret;
}

int ObPartitionParallelRanger::get_next_macro_endkey(ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is not inited", K(ret), K(*this));
  } else {
    ObMacroEndkey endkey;
    ObMacroEndkeyIterator* endkey_iter = nullptr;
    // already get last row from merge heap
    if (OB_NOT_NULL(last_macro_endkey_) && last_macro_endkey_->is_valid()) {
      if (OB_ISNULL(endkey_iter = endkey_iters_.at(last_macro_endkey_->iter_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro block iter", K(ret), K_(last_macro_endkey));
      } else if (OB_UNLIKELY(last_macro_endkey_->iter_idx_ != endkey_iter->iter_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected iter idx", K(ret), K(*last_macro_endkey_), K(*endkey_iter));
      } else if (OB_FAIL(endkey_iter->get_next_macro_block_endkey(endkey))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Failed to get next macro block endkey", K(ret));
        } else if (OB_FAIL(range_heap_.pop())) {
          STORAGE_LOG(WARN, "Failed to pop the last macro endkey", K(ret));
        } else if (OB_FAIL(comparor_.get_error_code())) {
          STORAGE_LOG(WARN, "Failed copmare macro endkeys", K(ret));
        }
      } else if (OB_UNLIKELY(!endkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block endkey", K(ret), K(endkey));
      } else if (OB_FAIL(range_heap_.replace_top(endkey))) {
        STORAGE_LOG(WARN, "Failed to replace top of the merge heap", K(ret), K(endkey));
      }
    }
    if (OB_SUCC(ret)) {
      if (range_heap_.empty()) {
        ret = OB_ITER_END;
        last_macro_endkey_ = nullptr;
      } else if (OB_FAIL(range_heap_.top(last_macro_endkey_))) {
        STORAGE_LOG(WARN, "Failed to get top macro endkey from heap", K(ret), KPC(last_macro_endkey_));
      } else if (OB_ISNULL(last_macro_endkey_) || OB_UNLIKELY(!last_macro_endkey_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro endkey", K(ret), KPC(last_macro_endkey_));
      } else {
        rowkey.assign(last_macro_endkey_->obj_ptr_, last_macro_endkey_->obj_cnt_);
      }
    }
  }

  return ret;
}

int ObPartitionParallelRanger::check_rowkey_equal(
    const ObStoreRowkey& rowkey1, const ObStoreRowkey& rowkey2, bool& equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (OB_UNLIKELY(!rowkey1.is_valid() || !rowkey2.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check rowkey equal", K(ret), K(rowkey1), K(rowkey2));
  } else if (rowkey1.is_min()) {
    equal = rowkey2.is_min();
  } else if (rowkey1.is_max()) {
    equal = rowkey2.is_max();
  } else if (0 == rowkey1.compare(rowkey2)) {
    equal = true;
  }
  return ret;
}

int ObPartitionParallelRanger::check_continuous(ObIArray<ObStoreRange>& range_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check continuous for range spliter", K(ret), K(range_array));
  } else {
    bool equal = false;
    ObStoreRowkey macro_endkey = store_range_->get_start_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); i++) {
      if (OB_UNLIKELY(!range_array.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid range", K(ret), K(i), K(range_array.at(i)));
      } else if (OB_FAIL(check_rowkey_equal(range_array.at(i).get_start_key(), macro_endkey, equal))) {
        STORAGE_LOG(WARN, "Failed to check rowkey equal", K(ret), K(i));
      } else if (!equal) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "Unexpected range array which is not continuous",
            K(ret),
            K(i),
            K(range_array.at(i)),
            K(macro_endkey),
            KPC(store_range_));
      } else {
        macro_endkey = range_array.at(i).get_end_key();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_rowkey_equal(store_range_->get_end_key(), macro_endkey, equal))) {
      STORAGE_LOG(WARN, "Failed to check rowkey equal", K(ret));
    } else if (!equal) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected range array which is not continuous", K(ret), K(macro_endkey), KPC(store_range_));
    }
  }
  return ret;
}

int ObPartitionParallelRanger::split_ranges(
    ObIAllocator& allocator, const bool for_compaction, ObIArray<ObStoreRange>& range_array)
{
  int ret = OB_SUCCESS;
  ObStoreRange split_range;

  range_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is not init", K(ret));
  } else if (sample_cnt_ == 0 || parallel_target_count_ == 1 || parallel_target_count_ > total_endkey_cnt_ + 1) {
    // cannot affort specified parallel target count, back into single whole range
    if (OB_FAIL(construct_single_range(allocator,
            store_range_->get_start_key(),
            store_range_->get_end_key(),
            store_range_->get_border_flag(),
            for_compaction,
            split_range))) {
      STORAGE_LOG(WARN, "failed to construct single range", K(ret), KPC(store_range_), K(for_compaction));
    } else if (OB_FAIL(range_array.push_back(split_range))) {
      STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(split_range));
    } else {
      STORAGE_LOG(
          DEBUG, "build single range", K(split_range), K_(sample_cnt), K_(parallel_target_count), K_(total_endkey_cnt));
    }
  } else {
    ObStoreRowkey macro_endkey;
    ObStoreRowkey last_macro_endkey = store_range_->get_start_key();
    const int64_t range_skip_cnt = total_endkey_cnt_ / parallel_target_count_;
    const int64_t endkey_left_cnt = total_endkey_cnt_ % parallel_target_count_;
    int64_t iter_cnt = 0;
    int64_t range_skip_revise_cnt = endkey_left_cnt > 0 ? 1 : 0;
    ObBorderFlag border_flag;
    border_flag.set_data(store_range_->get_border_flag().get_data());
    border_flag.set_inclusive_end();

    STORAGE_LOG(DEBUG,
        "start to iter endkeys to split range",
        KPC(store_range_),
        K_(total_endkey_cnt),
        K_(parallel_target_count),
        K(range_skip_cnt),
        K(endkey_left_cnt),
        K(range_skip_revise_cnt));

    while (OB_SUCC(ret) && OB_SUCC(get_next_macro_endkey(macro_endkey))) {
      if (macro_endkey.compare(store_range_->get_end_key()) >= 0) {
        // meet endkey which larger than endkey of the store_range, break
        break;
      } else if (++iter_cnt < range_skip_cnt + range_skip_revise_cnt) {
        // too many split ranges, need skip
      } else if (macro_endkey.compare(last_macro_endkey) <= 0) {
        // duplicate rowkey due to we change last start key with max trans version
        // continue
      } else if (OB_FAIL(construct_single_range(
                     allocator, last_macro_endkey, macro_endkey, border_flag, for_compaction, split_range))) {
        STORAGE_LOG(
            WARN, "failed to construct single range", K(ret), K(last_macro_endkey), K(macro_endkey), K(for_compaction));
      } else if (OB_FAIL(range_array.push_back(split_range))) {
        STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(split_range));
      } else {
        border_flag.unset_inclusive_start();
        split_range.reset();
        range_skip_revise_cnt = range_array.count() < endkey_left_cnt ? 1 : 0;
        iter_cnt = 0;
        last_macro_endkey = macro_endkey;
        if (range_array.count() == parallel_target_count_ - 1) {
          // enough ranges break and add the last range with max value
          break;
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Failed to get next macro endkey", K(ret), K(macro_endkey));
    } else {
      ret = OB_SUCCESS;
      macro_endkey = store_range_->get_end_key();
      if (!store_range_->get_border_flag().inclusive_end()) {
        border_flag.unset_inclusive_end();
      }
      if (OB_FAIL(construct_single_range(
              allocator, last_macro_endkey, macro_endkey, border_flag, for_compaction, split_range))) {
        STORAGE_LOG(
            WARN, "Failed to construct single range", K(ret), K(last_macro_endkey), K(macro_endkey), K(for_compaction));
      } else if (OB_FAIL(range_array.push_back(split_range))) {
        STORAGE_LOG(WARN, "Failed to push back merge range", K(ret), K(split_range));
      }
    }

    // check all ranges continuous
    if (OB_SUCC(ret) && OB_FAIL(check_continuous(range_array))) {
      STORAGE_LOG(WARN, "Failed to check range array continuous", K(ret), K(range_array));
    }
  }

  return ret;
}

int ObPartitionParallelRanger::build_new_rowkey(
    const ObStoreRowkey& rowkey, ObIAllocator& allocator, const bool for_compaction, ObStoreRowkey& new_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build new rowkey", K(ret), K(rowkey));
  } else {
    const int64_t extra_rowkey_cnt = for_compaction ? ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() : 0;
    const int64_t rowkey_col_cnt = rowkey.get_obj_cnt() + extra_rowkey_cnt;
    const int64_t total_size = rowkey.get_deep_copy_size() + sizeof(ObObj) * extra_rowkey_cnt;
    char* ptr = nullptr;
    if (OB_ISNULL(ptr = reinterpret_cast<char*>(allocator.alloc(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory", K(ret), K(total_size));
    } else {
      ObObj* obj_ptr = reinterpret_cast<ObObj*>(ptr);
      int64_t pos = sizeof(ObObj) * rowkey_col_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); i++) {
        if (OB_FAIL(obj_ptr[i].deep_copy(rowkey.get_obj_ptr()[i], ptr, total_size, pos))) {
          STORAGE_LOG(WARN, "Failed to deep copy object", K(ret), K(i), K(rowkey), K(total_size), K(pos));
        }
      }
      for (int64_t i = rowkey.get_obj_cnt(); OB_SUCC(ret) && i < rowkey_col_cnt; i++) {
        obj_ptr[i].set_max_value();
      }
      if (OB_SUCC(ret)) {
        new_rowkey.assign(obj_ptr, rowkey_col_cnt);
      }
    }
  }
  return ret;
}

int ObPartitionParallelRanger::construct_single_range(ObIAllocator& allocator, const ObStoreRowkey& start_key,
    const ObStoreRowkey& end_key, const ObBorderFlag& border_flag, const bool for_compaction, ObStoreRange& range)
{
  int ret = OB_SUCCESS;

  if (end_key.compare(start_key) < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid keys to construct range", K(ret), K(start_key), K(end_key));
  } else if (start_key.is_min()) {
    range.get_start_key().set_min();
  } else if (OB_FAIL(build_new_rowkey(start_key, allocator, for_compaction, range.get_start_key()))) {
    STORAGE_LOG(WARN, "Failed to deep copy macro start endkey", K(ret), K(start_key), K(for_compaction));
  }
  if (OB_FAIL(ret)) {
  } else if (end_key.is_max()) {
    range.get_end_key().set_max();
  } else if (OB_FAIL(build_new_rowkey(end_key, allocator, for_compaction, range.get_end_key()))) {
    STORAGE_LOG(WARN, "Failed to deep copy macro end endkey", K(ret), K(start_key), K(for_compaction));
  }
  if (OB_SUCC(ret)) {
    range.set_border_flag(border_flag);
    if (start_key.is_min()) {
      range.set_left_open();
    }
    if (end_key.is_max()) {
      range.set_right_open();
    }
    if (OB_NOT_NULL(store_range_)) {
      range.set_table_id(store_range_->get_table_id());
    }
  }

  return ret;
}

ObRangeSplitInfo::ObRangeSplitInfo()
    : store_range_(nullptr), range_paras_(), total_row_count_(0), total_size_(0), parallel_target_count_(1)
{}

ObRangeSplitInfo::~ObRangeSplitInfo()
{
  reset();
}

void ObRangeSplitInfo::reset()
{
  store_range_ = nullptr;
  range_paras_.reset();
  total_row_count_ = 0;
  total_size_ = 0;
  parallel_target_count_ = 1;
}

ObPartitionRangeSpliter::ObPartitionRangeSpliter() : allocator_(), parallel_ranger_(allocator_)
{}

ObPartitionRangeSpliter::~ObPartitionRangeSpliter()
{
  reset();
}

void ObPartitionRangeSpliter::reset()
{
  parallel_ranger_.reset();
  allocator_.reset();
}

int ObPartitionRangeSpliter::get_range_split_info(
    ObIArray<ObSSTable*>& sstables, const ObStoreRange& store_range, ObRangeSplitInfo& range_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sstables.empty() || !store_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObPartitionRangeSpliter", K(ret), K(sstables), K(store_range));
  } else if (FALSE_IT(range_info.reset())) {
  } else if (OB_FAIL(build_range_paras(sstables, store_range, range_info.range_paras_))) {
    STORAGE_LOG(WARN, "Failed to build range paras", K(ret));
  } else if (range_info.empty()) {
    range_info.total_row_count_ = 0;
    range_info.total_size_ = 0;
  } else if (OB_FAIL(get_size_info(range_info.range_paras_, range_info.total_row_count_, range_info.total_size_))) {
    STORAGE_LOG(WARN, "Failed to get range size info", K(ret));
  }
  if (OB_SUCC(ret)) {
    range_info.store_range_ = &store_range;
  }
  return ret;
}

int ObPartitionRangeSpliter::build_range_para(
    ObSSTable* sstable, const ObExtStoreRange& ext_range, ObSSTableRangePara& range_para)
{
  int ret = OB_SUCCESS;

  range_para.reset();
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build range para", K(ret), KP(sstable));
  } else if (FALSE_IT(range_para.sstable_ = sstable)) {
  } else if (sstable->get_macro_block_count() == 0) {
  } else if (ext_range.get_range().is_whole_range()) {
    range_para.start_ = 0;
    range_para.end_ = sstable->get_macro_block_count();
  } else {
    ObMacroBlockIterator macro_iter;
    int64_t macro_block_count = 0;
    if (OB_FAIL(macro_iter.open(*sstable, ext_range))) {
      STORAGE_LOG(WARN, "Failed to open macro block iter", K(ret), KPC(sstable), K(ext_range));
    } else if (OB_FAIL(macro_iter.get_macro_block_count(macro_block_count))) {
      STORAGE_LOG(WARN, "Failed to get macro block count", K(ret));
    } else if (macro_block_count > 0) {
      range_para.start_ = macro_iter.get_start_idx();
      range_para.end_ = range_para.start_ + macro_block_count;
    }
  }

  return ret;
}

int ObPartitionRangeSpliter::build_range_paras(
    ObIArray<ObSSTable*>& sstables, const ObStoreRange& store_range, ObIArray<ObSSTableRangePara>& range_paras)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange ext_range(store_range);
  if (OB_FAIL(ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "Failed to transform collation free and cutoff range", K(ret), K(ext_range));
  } else {
    ObSSTable* sstable = nullptr;
    ObSSTableRangePara range_para;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      if (OB_ISNULL(sstable = sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null sstable", K(ret), K(i), K(sstables));
      } else if (OB_FAIL(build_range_para(sstable, ext_range, range_para))) {
        STORAGE_LOG(WARN, "Failed to build range para", K(ret), KPC(sstable), K(ext_range));
      } else if (OB_UNLIKELY(!range_para.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid range para", K(ret), K(range_para));
      } else if (0 == range_para.get_macro_count()) {
        // skip empty range
      } else if (OB_FAIL(range_paras.push_back(range_para))) {
        STORAGE_LOG(WARN, "Failed to push back range para", K(ret), K(range_para));
      }
    }
  }

  return ret;
}

int ObPartitionRangeSpliter::get_size_info(
    ObIArray<ObSSTableRangePara>& range_paras, int64_t& total_row_count, int64_t& total_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_paras.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get size info", K(ret), K(range_paras));
  } else {
    int64_t row_count = 0;
    int64_t size = 0;
    total_row_count = 0;
    total_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_paras.count(); i++) {
      const ObSSTableRangePara& range_para = range_paras.at(i);
      if (OB_UNLIKELY(!range_para.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid range para", K(ret));
      } else if (OB_FAIL(get_single_range_info(range_para, row_count, size))) {
        STORAGE_LOG(WARN, "Failed to get single range info", K(ret), K(i), K(range_para));
      } else {
        total_row_count += row_count;
        total_size += size;
      }
    }
  }

  return ret;
}

int ObPartitionRangeSpliter::get_single_range_info(
    const ObSSTableRangePara& range_para, int64_t& total_row_count, int64_t& total_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!range_para.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get single range info", K(ret), K(range_para));
  } else if (range_para.start_ == 0 && range_para.end_ == range_para.sstable_->get_macro_block_count()) {
    total_row_count = range_para.sstable_->get_total_row_count();
    total_size = range_para.sstable_->get_occupy_size();
  } else {
    ObFullMacroBlockMeta full_meta;
    blocksstable::ObMacroBlockCtx macro_block_ctx;
    total_row_count = 0;
    total_size = 0;
    for (int64_t i = range_para.start_; OB_SUCC(ret) && i < range_para.end_; i++) {
      if (OB_FAIL(range_para.sstable_->get_macro_block_ctx(i, macro_block_ctx))) {
        STORAGE_LOG(WARN, "Fail to get_macro_ctx", K(ret), K(i), KPC(range_para.sstable_));
      } else if (OB_FAIL(range_para.sstable_->get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, full_meta))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret), K(macro_block_ctx));
      } else if (OB_UNLIKELY(!full_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block meta", K(ret), K(full_meta), K(range_para));
      } else {
        total_row_count += full_meta.meta_->row_count_;
        total_size += full_meta.meta_->occupy_size_;
      }
    }
  }

  return ret;
}

int ObPartitionRangeSpliter::split_ranges(ObRangeSplitInfo& range_info, ObIAllocator& allocator,
    const bool for_compaction, ObIArray<ObStoreRange>& range_array)
{
  int ret = OB_SUCCESS;
  parallel_ranger_.reset();

  if (OB_UNLIKELY(!range_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to split ranges", K(ret), K(range_info));
  } else if (range_info.parallel_target_count_ == 1 || range_info.store_range_->is_single_rowkey()) {
    ObStoreRange dst_range;
    if (OB_FAIL(parallel_ranger_.construct_single_range(allocator,
            range_info.store_range_->get_start_key(),
            range_info.store_range_->get_end_key(),
            range_info.store_range_->get_border_flag(),
            for_compaction,
            dst_range))) {
      STORAGE_LOG(WARN, "failed to construct single range", K(ret), K(range_info), K(for_compaction));
    } else if (FALSE_IT(dst_range.set_table_id(range_info.store_range_->get_table_id()))) {
    } else if (OB_FAIL(range_array.push_back(dst_range))) {
      STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(dst_range));
    }
  } else {
    if (OB_FAIL(parallel_ranger_.init(
            *range_info.store_range_, range_info.range_paras_, range_info.parallel_target_count_))) {
      STORAGE_LOG(WARN, "Failed to init parallel ranger", K(ret), K(range_info));
    } else if (OB_FAIL(parallel_ranger_.split_ranges(allocator, for_compaction, range_array))) {
      STORAGE_LOG(WARN, "Failed to split ranges", K(ret), K(for_compaction));
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_split_sstables(ObTablesHandle& read_tables, ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  ObSSTable* major_sstable = nullptr;
  sstables.reset();

  if (OB_UNLIKELY(read_tables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get split sstables", K(ret), K(read_tables));
  } else {
    int64_t major_size = 0;
    int64_t minor_size = 0;
    int64_t memtable_size = 0;
    ObITable* table = nullptr;
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < read_tables.get_count(); i++) {
      if (OB_ISNULL(table = read_tables.get_table(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null table", K(ret), K(i), K(read_tables));
      } else if (table->is_major_sstable()) {
        major_size = (reinterpret_cast<ObSSTable*>(table))->get_occupy_size();
        table_id = (reinterpret_cast<ObSSTable*>(table))->get_meta().index_id_;
      } else if (table->is_minor_sstable()) {
        minor_size += (reinterpret_cast<ObSSTable*>(table))->get_occupy_size();
      } else if (table->is_memtable() && OB_INVALID_ID != table_id) {
        int64_t mem_rows = 0;
        int64_t mem_size = 0;
        memtable::ObMemtable* memtable = reinterpret_cast<memtable::ObMemtable*>(table);
        if (OB_FAIL(memtable->estimate_phy_size(table_id, nullptr, nullptr, mem_size, mem_rows))) {
          STORAGE_LOG(WARN, "Failed to get estimate size from memtable", K(ret));
        } else {
          memtable_size += mem_size;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (memtable_size > major_size + minor_size) {
      // too big memtable size, use memtable to range split
    } else if (minor_size > MIN_SPLIT_TAGET_SSTABLE_SIZE && minor_size > major_size / 2 &&
               OB_FAIL(read_tables.get_all_minor_sstables(sstables))) {
      STORAGE_LOG(WARN, "Failed to get all minor sstables", K(ret));
    } else if (major_size > MIN_SPLIT_TAGET_SSTABLE_SIZE) {
      if (OB_FAIL(read_tables.get_last_major_sstable(major_sstable))) {
        STORAGE_LOG(WARN, "Failed to get last major sstable", K(ret));
      } else if (OB_FAIL(sstables.push_back(major_sstable))) {
        STORAGE_LOG(WARN, "Failed to push back major sstable", K(ret));
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_multi_range_size(
    ObTablesHandle& read_tables, const ObIArray<ObStoreRange>& range_array, int64_t& total_size)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTable*> sstables;
  total_size = 0;

  if (OB_UNLIKELY(read_tables.empty() || range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get multi range size", K(ret), K(read_tables), K(range_array));
  } else if (OB_FAIL(get_split_sstables(read_tables, sstables))) {
    STORAGE_LOG(WARN, "Failed to get all sstables", K(ret), K(read_tables));
  } else if (sstables.empty()) {
    // only memtable, can not support arbitary range split
    total_size = 0;
  } else {
    RangeSplitInfoArray range_info_array;
    if (OB_FAIL(get_range_split_infos(sstables, range_array, range_info_array, total_size))) {
      STORAGE_LOG(WARN, "Failed to get range split info array", K(ret));
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::split_multi_ranges(RangeSplitInfoArray& range_info_array,
    const int64_t expected_task_count, const int64_t total_size, common::ObIAllocator& allocator,
    ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_info_array.empty() || total_size <= 0 || expected_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument to calc parallel target array",
        K(ret),
        K(range_info_array),
        K(total_size),
        K(expected_task_count));
  } else {
    ObArenaAllocator local_allocator;
    const int64_t avg_task_size = MAX(total_size / expected_task_count, MIN_SPLIT_TASK_SIZE);
    const int64_t split_task_size = MAX(avg_task_size / 2, MIN_SPLIT_TASK_SIZE);
    const int64_t task_size_high_watermark =
        MAX(avg_task_size * SPLIT_TASK_SIZE_HIGH_WATER_MARK_FACTOR / 100, MIN_SPLIT_TASK_SIZE);
    const int64_t task_size_low_watermark =
        max(avg_task_size * SPLIT_TASK_SIZE_LOW_WATER_MARK_FACTOR / 100, MIN_SPLIT_TASK_SIZE);
    int64_t sum_size = 0;
    RangeSplitArray range_split_array;
    RangeSplitArray refra_range_split_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_info_array.count(); i++) {
      ObRangeSplitInfo& range_info = range_info_array.at(i);
      int64_t cur_avg_task_size = 0;
      if (range_info.empty() || range_info.total_size_ < avg_task_size ||
          sum_size + range_info.total_size_ < task_size_high_watermark) {
        range_info.set_parallel_target(1);
        cur_avg_task_size = MAX(range_info.total_size_, MIN_SPLIT_TASK_SIZE);
      } else {
        range_info.set_parallel_target(range_info.total_size_ / split_task_size);
        cur_avg_task_size = range_info.total_size_ / range_info.parallel_target_count_;
      }
      STORAGE_LOG(DEBUG, "start to split range array", K(range_info), K(cur_avg_task_size), K(avg_task_size));
      range_spliter_.reset();
      range_split_array.reset();
      if (OB_FAIL(range_spliter_.split_ranges(range_info, local_allocator, false, range_split_array))) {
        STORAGE_LOG(WARN, "Failed to split ranges", K(ret), K(range_info));
      } else {
        STORAGE_LOG(DEBUG, "get split ranges", K(range_split_array));
        for (int64_t i = 0; OB_SUCC(ret) && i < range_split_array.count(); i++) {
          if (sum_size >= avg_task_size ||
              (sum_size >= task_size_low_watermark && sum_size + cur_avg_task_size >= task_size_high_watermark)) {
            if (OB_FAIL(merge_and_push_range_array(refra_range_split_array, allocator, multi_range_split_array))) {
              STORAGE_LOG(WARN, "Failed to merge and push split range array", K(ret), K(refra_range_split_array));
            } else {
              STORAGE_LOG(
                  DEBUG, "succ to build refra split ranges", K(refra_range_split_array), K(sum_size), K(avg_task_size));
              refra_range_split_array.reset();
              sum_size = 0;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(refra_range_split_array.push_back(range_split_array.at(i)))) {
            STORAGE_LOG(WARN, "Failed to push back store range", K(ret), K(range_split_array.at(i)));
          } else {
            sum_size += cur_avg_task_size;
          }
        }
      }
    }
    if (OB_SUCC(ret) && refra_range_split_array.count() > 0) {
      if (OB_FAIL(merge_and_push_range_array(refra_range_split_array, allocator, multi_range_split_array))) {
        STORAGE_LOG(WARN, "Failed to merge and push split range array", K(ret), K(refra_range_split_array));
      } else {
        STORAGE_LOG(
            DEBUG, "succ to build refra split ranges", K(refra_range_split_array), K(sum_size), K(avg_task_size));
        refra_range_split_array.reset();
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::merge_and_push_range_array(const RangeSplitArray& src_range_split_array,
    ObIAllocator& allocator, ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(src_range_split_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arugment to merge range array", K(ret), K(src_range_split_array));
  } else {
    const ObStoreRange* last_range = nullptr;
    RangeSplitArray dst_range_array;
    ObStoreRange dst_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_range_split_array.count(); i++) {
      const ObStoreRange& cur_range = src_range_split_array.at(i);
      if (OB_NOT_NULL(last_range) && cur_range.get_start_key().compare(last_range->get_end_key()) == 0) {
        // continous range
        if (!cur_range.get_border_flag().inclusive_end()) {
          // stop find next range
          if (OB_FAIL(cur_range.get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(cur_range));
          } else if (FALSE_IT(dst_range.get_border_flag().unset_inclusive_end())) {
          } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
            STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
          } else {
            dst_range.reset();
            last_range = nullptr;
          }
        } else {
          last_range = &cur_range;
        }
      } else {
        if (OB_NOT_NULL(last_range)) {
          // break range
          if (OB_FAIL(last_range->get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), KPC(last_range));
          } else if (FALSE_IT(dst_range.get_border_flag().set_inclusive_end())) {
          } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
            STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
          } else {
            dst_range.reset();
            last_range = nullptr;
          }
        }
        if (OB_SUCC(ret)) {
          dst_range.set_table_id(cur_range.get_table_id());
          dst_range.set_border_flag(cur_range.get_border_flag());
          if (!cur_range.get_border_flag().inclusive_end()) {
            // only deal with right close | left open situation
            if (OB_FAIL(cur_range.deep_copy(allocator, dst_range))) {
              STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(cur_range));
            } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
              STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
            } else {
              dst_range.reset();
            }
          } else if (OB_FAIL(cur_range.get_start_key().deep_copy(dst_range.get_start_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy start key", K(ret), K(cur_range));
          } else {
            last_range = &cur_range;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(last_range)) {
      if (OB_FAIL(last_range->get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), KPC(last_range));
      } else if (FALSE_IT(dst_range.get_border_flag().set_inclusive_end())) {
      } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
        STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
      } else {
        dst_range.reset();
        last_range = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(multi_range_split_array.push_back(dst_range_array))) {
        STORAGE_LOG(WARN, "Failed to push back range split array", K(ret), K(dst_range_array));
      } else {
        STORAGE_LOG(DEBUG, "succ to merge range split array", K(ret), K(src_range_split_array), K(dst_range_array));
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::build_single_range_array(const ObIArray<ObStoreRange>& range_array,
    ObIAllocator& allocator, ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arugment to build single range array", K(ret), K(range_array));
  } else {
    RangeSplitArray range_split_array;
    ObStoreRange store_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); i++) {
      if (OB_FAIL(range_array.at(i).deep_copy(allocator, store_range))) {
        STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(i), K(range_array.at(i)));
      } else if (OB_FAIL(range_split_array.push_back(store_range))) {
        STORAGE_LOG(WARN, "Failed to push back store range", K(ret), K(store_range));
      } else {
        store_range.reset();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(multi_range_split_array.push_back(range_split_array))) {
        STORAGE_LOG(WARN, "Failed to push range split array", K(ret), K(range_split_array));
      } else {
        STORAGE_LOG(DEBUG, "Fast split for single task", K(range_array));
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_split_multi_ranges(ObTablesHandle& read_tables,
    const ObIArray<ObStoreRange>& range_array, const int64_t expected_task_count, ObIAllocator& allocator,
    ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTable*> sstables;
  bool single_array = false;
  multi_range_split_array.reset();

  if (OB_UNLIKELY(read_tables.empty() || range_array.empty() || expected_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument to get split multi ranges",
        K(ret),
        K(read_tables),
        K(range_array),
        K(expected_task_count));
  } else if (OB_UNLIKELY(expected_task_count == 1)) {
    STORAGE_LOG(DEBUG, "Unexpected only one split task", K(expected_task_count), K(range_array));
    single_array = true;
  } else if (OB_FAIL(get_split_sstables(read_tables, sstables))) {
    STORAGE_LOG(WARN, "Failed to get split sstables", K(ret), K(read_tables));
  } else if (sstables.empty()) {
    // only small sstables, no need split
    STORAGE_LOG(DEBUG, "empty split sstables", K(read_tables));
    single_array = true;
  } else {
    RangeSplitInfoArray range_info_array;
    int64_t total_size = 0;
    if (OB_FAIL(get_range_split_infos(sstables, range_array, range_info_array, total_size))) {
      STORAGE_LOG(WARN, "Failed to get range split info array", K(ret));
    } else if (total_size == 0) {
      STORAGE_LOG(DEBUG, "too small sstables to split range", K(total_size), K(range_info_array));
      single_array = true;
    } else if (OB_FAIL(split_multi_ranges(
                   range_info_array, expected_task_count, total_size, allocator, multi_range_split_array))) {
      STORAGE_LOG(WARN, "Failed to split multi ranges", K(ret));
    }
  }

  if (OB_SUCC(ret) && single_array) {
    if (OB_FAIL(build_single_range_array(range_array, allocator, multi_range_split_array))) {
      STORAGE_LOG(WARN, "Failed to build single range array", K(ret));
    }
  }
  return ret;
}

int ObPartitionMultiRangeSpliter::get_range_split_infos(ObIArray<ObSSTable*>& sstables,
    const ObIArray<ObStoreRange>& range_array, RangeSplitInfoArray& range_info_array, int64_t& total_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sstables.empty() || range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get range split info", K(ret), K(sstables), K(range_array));
  } else {
    ObRangeSplitInfo range_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); i++) {
      if (FALSE_IT(range_spliter_.reset())) {
      } else if (OB_FAIL(range_spliter_.get_range_split_info(sstables, range_array.at(i), range_info))) {
        STORAGE_LOG(WARN, "Failed to get range split info", K(ret), K(i), K(range_array.at(i)));
      } else if (OB_FAIL(range_info_array.push_back(range_info))) {
        STORAGE_LOG(WARN, "Failed to push back range info", K(ret), K(range_info));
      } else {
        STORAGE_LOG(DEBUG, "get single range split info", K(range_info));
        total_size += range_info.total_size_;
        range_info.reset();
      }
    }
    STORAGE_LOG(DEBUG, "get total range split info", K(total_size), K(sstables), K(range_info_array));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
